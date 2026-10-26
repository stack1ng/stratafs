use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use futures_util::lock::Mutex;
use redb::{
    CommitError, Database, Durability, Error, ReadOnlyTable, ReadableDatabase, ReadableTable,
    StorageError, Table, TableDefinition, TableError, TransactionError,
};
use web_time::Instant;

use stratafs_keyed_notify::KeyedNotify;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
    TransactionHandle, assert_valid_range,
};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

pub trait SpawnBlocking: Send + Sync {
    fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> + Send;
}

#[cfg(threads)]
pub struct ThreadPool;

#[cfg(threads)]
impl SpawnBlocking for ThreadPool {
    fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> + Send {
        blocking::unblock(work)
    }
}

pub struct Inline;

impl SpawnBlocking for Inline {
    async fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        work()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RedbFailure {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Transaction(#[from] TransactionError),
    #[error(transparent)]
    Table(#[from] TableError),
    #[error(transparent)]
    Commit(#[from] CommitError),
    #[error("what the commit depends on could not be flushed first")]
    FlushFirst(#[source] io::Error),
}

// CommitDurability is how often commits reach the disk, each of which costs a full flush. A commit that does
// not reach it is lost if the machine stops before a later commit does, or before the store closes cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitDurability {
    EveryCommit,
    // AtMostEvery flushes a commit once the last flushed one is at least this old, so writes that stop
    // stay unflushed until the next commit or the close.
    AtMostEvery(Duration),
    // Never flushes only when the store closes.
    Never,
}

pub struct RedbStore<S> {
    database: Database,
    writer: Mutex<Writer>,
    notify: Arc<KeyedNotify>,
    spawner: S,
    durability: CommitDurability,
    flush_first: Arc<dyn Fn() -> io::Result<()> + Send + Sync>,
}

// Writer is what the one write transaction at a time keeps between commits.
struct Writer {
    written: Vec<Bytes>,
    last_flushed_at: Option<Instant>,
}

impl<S> RedbStore<S> {
    // flush_first makes durable whatever the store's commits depend on that is kept elsewhere, such as the
    // content their values name. It runs before every commit that reaches the disk, including the close.
    pub fn new(
        database: Database,
        spawner: S,
        durability: CommitDurability,
        flush_first: impl Fn() -> io::Result<()> + Send + Sync + 'static,
    ) -> Result<Self, Error> {
        let transaction = database.begin_write()?;
        transaction.open_table(TABLE)?;
        transaction.commit()?;
        Ok(Self {
            database,
            writer: Mutex::new(Writer {
                written: Vec::new(),
                last_flushed_at: None,
            }),
            notify: Arc::new(KeyedNotify::new()),
            spawner,
            durability,
            flush_first: Arc::new(flush_first),
        })
    }
}

impl<S> Drop for RedbStore<S> {
    fn drop(&mut self) {
        // Closing the database flushes every commit, so what they depend on is flushed first. Nothing is left to
        // report a failure to; the commits are then flushed regardless, as redb's close does.
        let _ = (self.flush_first)();
    }
}

impl<S: SpawnBlocking + 'static> KeyValueStore for RedbStore<S> {
    type Rejection = std::convert::Infallible;
    // set reports values over redb's limit as SetRejection::TooLarge, so Failure excludes ValueTooLarge.
    type Failure = RedbFailure;
    type ReadTransaction<'txn> = RedbTransaction<'txn, ReadOnlyTable<&'static [u8], &'static [u8]>>;
    type Transaction<'txn> = RedbTransaction<'txn, Table<'txn, &'static [u8], &'static [u8]>>;

    const LARGEST_ACCEPTED_ENTRY: usize = 3 * 1024 * 1024 * 1024;

    async fn transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<
            'txn,
            Result<(T, TransactionHandle<'txn, 'store, Self::Transaction<'txn>>), E>,
        > + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        let mut writer = self.writer.lock().await;
        writer.written.clear();
        let result: Result<Result<T, E>, RedbFailure> = async {
            let mut transaction = self.database.begin_write()?;
            let flushes = match self.durability {
                CommitDurability::EveryCommit => true,
                CommitDurability::AtMostEvery(interval) => writer
                    .last_flushed_at
                    .is_none_or(|flushed_at| flushed_at.elapsed() >= interval),
                CommitDurability::Never => false,
            };
            transaction
                .set_durability(if flushes {
                    Durability::Immediate
                } else {
                    Durability::None
                })
                .expect("the store makes no persistent savepoints, so it may set any durability");
            let output = work(TransactionHandle::new(RedbTransaction {
                table: transaction.open_table(TABLE)?,
                written: &mut writer.written,
            }))
            .await
            .map(|(output, _)| output);
            let output = match output {
                Ok(output) => output,
                Err(error) => {
                    transaction.abort()?;
                    return Ok(Err(error));
                }
            };
            let notify = self.notify.clone();
            let flush_first = self.flush_first.clone();
            let keys = std::mem::take(&mut writer.written);
            let commit = move || {
                let flushed_first = if flushes {
                    flush_first().map_err(RedbFailure::FlushFirst)
                } else {
                    Ok(())
                };
                let commit =
                    flushed_first.and_then(|()| transaction.commit().map_err(RedbFailure::Commit));
                if commit.is_ok() {
                    notify.notify_waiters(&keys);
                }
                (commit, keys)
            };
            // Only a commit that flushes always waits on the disk, so only it goes to the spawner. Any other
            // commit almost always stays in memory and finishes faster here than the handoff would take; it
            // touches the disk only in the rare commit that grows the file, which redb flushes.
            let (commit, keys) = if flushes {
                self.spawner.spawn_blocking(commit).await
            } else {
                commit()
            };
            writer.written = keys;
            commit?;
            if flushes {
                writer.last_flushed_at = Some(Instant::now());
            }
            Ok(Ok(output))
        }
        .await;
        match result {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) => Err(TransactError::Aborted(error)),
            Err(failure) => Err(TransactError::Failed(failure)),
        }
    }

    async fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        let result: Result<Result<T, E>, RedbFailure> = async {
            let transaction = self.database.begin_read()?;
            let mut written = Vec::new();
            Ok(work(TransactionHandle::new(RedbTransaction {
                table: transaction.open_table(TABLE)?,
                written: &mut written,
            }))
            .await)
        }
        .await;
        match result {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) => Err(TransactError::Aborted(error)),
            Err(failure) => Err(TransactError::Failed(failure)),
        }
    }

    async fn changed(&self, key: Bytes, seen: Option<Bytes>) -> Result<(), Self::Failure> {
        let (key, seen) = (&key, &seen);
        self.notify
            .wait_until(key, || async move {
                let transaction = self.database.begin_read()?;
                let table = transaction.open_table(TABLE)?;
                let current = table.get(&key[..])?;
                Ok(current.as_ref().map(|value| value.value()) != seen.as_deref())
            })
            .await
    }
}

pub struct RedbTransaction<'txn, T> {
    table: T,
    written: &'txn mut Vec<Bytes>,
}

impl<'txn, T> ReadKeyValueTransaction for RedbTransaction<'txn, T>
where
    T: ReadableTable<&'static [u8], &'static [u8]> + Send + Sync,
{
    type Failure = RedbFailure;

    async fn get_range<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
        limit: usize,
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + use<'txn, T, R>, Self::Failure> {
        assert_valid_range(&range);
        let bounds = (
            range.start_bound().map(|key| &key[..]),
            range.end_bound().map(|key| &key[..]),
        );
        let entries = self
            .table
            .range::<&[u8]>(bounds)?
            .take(limit)
            .map(|entry| {
                let (key, value) = entry?;
                Ok((
                    Bytes::copy_from_slice(key.value()),
                    Bytes::copy_from_slice(value.value()),
                ))
            })
            .collect::<Result<Vec<_>, Self::Failure>>()?;
        Ok(entries.into_iter())
    }
}

impl KeyValueTransaction for RedbTransaction<'_, Table<'_, &'static [u8], &'static [u8]>> {
    type Rejection = std::convert::Infallible;

    fn set(
        &mut self,
        key: Bytes,
        mut value: impl Buf,
    ) -> Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>> {
        let joined;
        let contiguous = if value.chunk().len() == value.remaining() {
            value.chunk()
        } else {
            joined = value.copy_to_bytes(value.remaining());
            &joined[..]
        };
        let previous = match self.table.insert(&key[..], contiguous) {
            Ok(previous) => previous,
            Err(StorageError::ValueTooLarge(_)) => {
                return Err(StoreError::Rejected(SetRejection::TooLarge));
            }
            Err(failure) => return Err(StoreError::Failed(failure.into())),
        };
        let changed = previous.is_none_or(|previous| previous.value() != contiguous);
        value.advance(value.remaining());
        if changed {
            self.written.push(key);
        }
        Ok(())
    }

    fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure> {
        if self.table.remove(&key[..])?.is_some() {
            self.written.push(key);
        }
        Ok(())
    }

    fn clear_range(&mut self, range: impl RangeBounds<Bytes>) -> Result<(), Self::Failure> {
        assert_valid_range(&range);
        let bounds = (
            range.start_bound().map(|key| &key[..]),
            range.end_bound().map(|key| &key[..]),
        );
        let written = &mut *self.written;
        self.table.retain_in::<&[u8], _>(bounds, |key, _| {
            written.push(Bytes::copy_from_slice(key));
            false
        })?;
        Ok(())
    }
}
