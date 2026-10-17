use std::ops::RangeBounds;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use futures_channel::oneshot;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::future::Shared;
use futures_util::lock::Mutex;
use redb::{
    CommitError, Database, Error, ReadOnlyTable, ReadableDatabase, ReadableTable, StorageError,
    Table, TableDefinition, TableError, TransactionError,
};

use stratafs_keyed_notify::KeyedNotify;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction, TransactionHandle,
    assert_valid_range,
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
pub enum RedbRejection {
    // this is a 1:1 mapping to the StorageError::ValueTooLarge variant
    #[error("The value being inserted exceeds the maximum of 3GiB: {0} bytes")]
    ValueTooLarge(usize),
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
    writer: Mutex<Vec<Bytes>>,
    notify: Arc<KeyedNotify>,
    spawner: S,
}

impl<S> RedbStore<S> {
    pub fn new(database: Database, spawner: S) -> Result<Self, Error> {
        let transaction = database.begin_write()?;
        transaction.open_table(TABLE)?;
        transaction.commit()?;
        Ok(Self {
            database,
            writer: Mutex::new(Vec::new()),
            notify: Arc::new(KeyedNotify::new()),
            spawner,
        })
    }
}

impl<S: SpawnBlocking + 'static> KeyValueStore for RedbStore<S> {
    type Rejection = RedbRejection;
    // never ValueTooLarge, transact reports that as a rejection
    type Failure = RedbFailure;
    type Observation = RedbObservation;
    type ReadTransaction<'txn> = RedbTransaction<'txn, ReadOnlyTable<&'static [u8], &'static [u8]>>;
    type Transaction<'txn> = RedbTransaction<'txn, Table<'txn, &'static [u8], &'static [u8]>>;

    async fn transact<T: Send>(
        &self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, Self::Transaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, Self::Failure>>
        + Send,
    ) -> Result<T, StoreError<Self::Rejection, Self::Failure>> {
        let mut written = self.writer.lock().await;
        written.clear();
        let result: Result<T, RedbFailure> = async {
            let transaction = self.database.begin_write()?;
            let committed = OnceLock::new();
            let output = work(TransactionHandle::new(RedbTransaction {
                table: transaction.open_table(TABLE)?,
                written: &mut written,
                committed: &committed,
            }))
            .await?;
            let committed = committed.into_inner();
            let notify = self.notify.clone();
            let keys = std::mem::take(&mut *written);
            let (commit, keys) = self
                .spawner
                .spawn_blocking(move || {
                    let commit = transaction.commit();
                    if commit.is_ok() {
                        if let Some((committed, _)) = committed {
                            let _ = committed.send(());
                        }
                        notify.notify_waiters(&keys);
                    }
                    (commit, keys)
                })
                .await;
            *written = keys;
            commit?;
            Ok(output)
        }
        .await;
        result.map_err(|failure| match failure {
            RedbFailure::Storage(StorageError::ValueTooLarge(size)) => {
                StoreError::Rejected(RedbRejection::ValueTooLarge(size))
            }
            failure => StoreError::Failed(failure),
        })
    }

    async fn read_transact<T: Send>(
        &self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, Self::Failure>>
        + Send,
    ) -> Result<T, StoreError<Self::Rejection, Self::Failure>> {
        let result: Result<T, RedbFailure> = async {
            let transaction = self.database.begin_read()?;
            let committed = OnceLock::new();
            let mut written = Vec::new();
            let output = work(TransactionHandle::new(RedbTransaction {
                table: transaction.open_table(TABLE)?,
                written: &mut written,
                committed: &committed,
            }))
            .await?;
            if let Some((committed, _)) = committed.into_inner() {
                let _ = committed.send(());
            }
            Ok(output)
        }
        .await;
        result.map_err(StoreError::Failed)
    }

    async fn changed(
        &self,
        observation: RedbObservation,
    ) -> Result<(), StoreError<NotCommitted, Self::Failure>> {
        if observation.committed.clone().await.is_err() {
            return Err(StoreError::Rejected(NotCommitted));
        }
        let observation = &observation;
        self.notify
            .wait_until(&observation.key, || async move {
                let transaction = self.database.begin_read()?;
                let table = transaction.open_table(TABLE)?;
                let current = table.get(&observation.key[..])?;
                Ok(current.as_ref().map(|value| value.value()) != observation.seen.as_deref())
            })
            .await
            .map_err(StoreError::Failed)
    }
}

pub struct RedbObservation {
    key: Bytes,
    seen: Option<Bytes>,
    committed: Shared<oneshot::Receiver<()>>,
}

pub struct RedbTransaction<'txn, T> {
    table: T,
    written: &'txn mut Vec<Bytes>,
    committed: &'txn OnceLock<(oneshot::Sender<()>, Shared<oneshot::Receiver<()>>)>,
}

impl<T> ReadKeyValueTransaction for RedbTransaction<'_, T>
where
    T: ReadableTable<&'static [u8], &'static [u8]> + Send + Sync,
{
    type Failure = RedbFailure;
    type Observation = RedbObservation;

    async fn get_range(
        &self,
        range: impl RangeBounds<Bytes> + Send,
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>, Self::Failure> {
        assert_valid_range(&range);
        let bounds = (
            range.start_bound().map(|key| &key[..]),
            range.end_bound().map(|key| &key[..]),
        );
        self.table
            .range::<&[u8]>(bounds)?
            .take(limit)
            .map(|entry| {
                let (key, value) = entry?;
                Ok((
                    Bytes::copy_from_slice(key.value()),
                    Bytes::copy_from_slice(value.value()),
                ))
            })
            .collect()
    }

    fn observe(&self, key: Bytes) -> Result<RedbObservation, Self::Failure> {
        let seen = self
            .table
            .get(&key[..])?
            .map(|value| Bytes::copy_from_slice(value.value()));
        Ok(RedbObservation {
            key,
            seen,
            committed: self
                .committed
                .get_or_init(|| {
                    let (committed, outcome) = oneshot::channel();
                    (committed, outcome.shared())
                })
                .1
                .clone(),
        })
    }
}

impl KeyValueTransaction for RedbTransaction<'_, Table<'_, &'static [u8], &'static [u8]>> {
    async fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Failure> {
        let previous = self.table.insert(&key[..], &value[..])?;
        if previous.is_none_or(|previous| previous.value() != &value[..]) {
            self.written.push(key);
        }
        Ok(())
    }

    async fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure> {
        if self.table.remove(&key[..])?.is_some() {
            self.written.push(key);
        }
        Ok(())
    }
}
