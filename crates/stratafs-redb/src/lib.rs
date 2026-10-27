#[cfg(disk)]
use std::io;
#[cfg(disk)]
use std::ops::Bound;
use std::ops::RangeBounds;
use std::sync::{Arc, PoisonError};

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use futures_util::lock::Mutex;
#[cfg(disk)]
use redb::BackendError;
use redb::{
    CommitError, Database, Durability, Error, ReadOnlyTable, ReadableDatabase, ReadableTable,
    StorageError, Table, TableDefinition, TableError, TransactionError,
};

use stratafs_keyed_notify::KeyedNotify;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
    TransactionHandle, assert_valid_range,
};
use stratafs_store::spawn::SpawnBlocking;

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

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
    // syncs is the failure the store's file syncs a durable commit against, when the store opened its own file.
    syncs: Option<Arc<std::sync::Mutex<DurableAgainst>>>,
}

// Writer is what the one write transaction at a time keeps between commits.
struct Writer {
    written: Vec<Bytes>,
    // current_durable_against is what every commit so far is durable against, or None while one is only in memory.
    current_durable_against: Option<DurableAgainst>,
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
                current_durable_against: Some(DurableAgainst::PowerFailure),
            }),
            notify: Arc::new(KeyedNotify::new()),
            spawner,
            syncs: None,
        })
    }

    // local keeps the store in a database file at path on this machine's disk, flushing the disk's own cache
    // only when ensure_durable is against a power failure.
    #[cfg(disk)]
    pub fn local(path: impl AsRef<std::path::Path>, spawner: S) -> Result<Self, Error> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let syncs = Arc::new(std::sync::Mutex::new(DurableAgainst::PowerFailure));
        let database = Database::builder().create_with_backend(SyncedFile {
            file: file.try_clone()?,
            backend: redb::backends::FileBackend::new(file)?,
            syncs: Arc::clone(&syncs),
        })?;
        let mut store = Self::new(database, spawner)?;
        store.syncs = Some(syncs);
        Ok(store)
    }
}

// SyncedFile is redb's file backend, syncing each commit that reaches the disk against the failure its store
// asks for.
#[cfg(disk)]
#[derive(Debug)]
struct SyncedFile {
    backend: redb::backends::FileBackend,
    file: std::fs::File,
    syncs: Arc<std::sync::Mutex<DurableAgainst>>,
}

#[cfg(disk)]
impl redb::StorageBackend for SyncedFile {
    fn len(&self) -> io::Result<u64> {
        self.backend.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.backend.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.backend.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        match *self.syncs.lock().unwrap_or_else(PoisonError::into_inner) {
            DurableAgainst::SoftwareFailure => sync_in_order(&self.file),
            DurableAgainst::PowerFailure => self.backend.sync_data(),
        }
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.backend.write(offset, data)
    }

    fn close(&self) -> io::Result<()> {
        self.backend.close()
    }

    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.backend.try_lock_range(start, end)
    }

    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.backend.try_lock_shared_range(start, end)
    }

    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.lock_range(start, end)
    }

    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.lock_shared_range(start, end)
    }

    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.backend.unlock_range(start, end)
    }

    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.backend.query_lock_range(start, end)
    }
}

// sync_in_order hands file to the disk with a barrier, so nothing written after it reaches the disk before it,
// without waiting for the disk to flush its own cache. Elsewhere than Apple's systems a sync always flushes it.
#[cfg(all(disk, target_vendor = "apple"))]
fn sync_in_order(file: &std::fs::File) -> io::Result<()> {
    nix::fcntl::fcntl(file, nix::fcntl::FcntlArg::F_BARRIERFSYNC)
        .map(|_| ())
        .map_err(io::Error::from)
}

#[cfg(all(disk, not(target_vendor = "apple")))]
fn sync_in_order(file: &std::fs::File) -> io::Result<()> {
    file.sync_data()
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
            writer.current_durable_against = None;
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

    async fn ensure_durable(&self, against: DurableAgainst) -> Result<(), Self::Failure> {
        let mut writer = self.writer.lock().await;
        if matches!(writer.current_durable_against, Some(durable) if durable >= against) {
            return Ok(());
        }
        if let Some(syncs) = &self.syncs {
            *syncs.lock().unwrap_or_else(PoisonError::into_inner) = against;
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
        writer.current_durable_against = Some(against);
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
