use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use futures_util::lock::Mutex;
use redb::{
    CommitError, Database, Durability, Error, ReadOnlyTable, ReadableDatabase, ReadableTable,
    StorageError, Table, TableDefinition, TableError, TransactionError,
};

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
}

pub struct RedbStore<S> {
    database: Database,
    writer: Mutex<Writer>,
    notify: Arc<KeyedNotify>,
    spawner: S,
}

// Writer is what the one write transaction at a time keeps between commits.
struct Writer {
    written: Vec<Bytes>,
    // unflushed_commits is whether a commit since the last ensure_durable has not reached the disk.
    unflushed_commits: bool,
}

impl<S> RedbStore<S> {
    pub fn new(database: Database, spawner: S) -> Result<Self, Error> {
        let transaction = database.begin_write()?;
        transaction.open_table(TABLE)?;
        transaction.commit()?;
        Ok(Self {
            database,
            writer: Mutex::new(Writer {
                written: Vec::new(),
                unflushed_commits: false,
            }),
            notify: Arc::new(KeyedNotify::new()),
            spawner,
        })
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
            // A commit reaches the disk through ensure_durable, so it finishes here. Only the rare commit that grows
            // the file waits on the disk, since redb flushes the file when it grows it.
            transaction
                .set_durability(Durability::None)
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
            transaction.commit()?;
            writer.unflushed_commits = true;
            self.notify.notify_waiters(&writer.written);
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

    async fn ensure_durable(&self) -> Result<(), Self::Failure> {
        let mut writer = self.writer.lock().await;
        if !writer.unflushed_commits {
            return Ok(());
        }
        // An empty commit that reaches the disk takes every commit before it there too. It waits on the disk,
        // so it goes to the spawner.
        let mut transaction = self.database.begin_write()?;
        transaction
            .set_durability(Durability::Immediate)
            .expect("the store makes no persistent savepoints, so it may set any durability");
        self.spawner
            .spawn_blocking(move || transaction.commit())
            .await?;
        writer.unflushed_commits = false;
        Ok(())
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
