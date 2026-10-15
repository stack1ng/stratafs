use std::ops::RangeBounds;

use bytes::Bytes;
use futures_core::future::BoxFuture;
use futures_lite::future::yield_now;
use futures_util::lock::Mutex;
use redb::{
    Database, Error, ReadOnlyTable, ReadableDatabase, ReadableTable, Table, TableDefinition,
};

use stratafs_keyed_notify::KeyedNotify;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, assert_valid_range,
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
        yield_now().await;
        work()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RedbRejection {
    // this is a 1:1 mapping to the StorageError::ValueTooLarge variant
    #[error("The value being inserted exceeds the maximum of 3GiB: {0} bytes")]
    ValueTooLarge(usize),
}

pub struct RedbStore<S> {
    database: Database,
    writer: Mutex<Vec<Bytes>>,
    notify: KeyedNotify,
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
            notify: KeyedNotify::new(),
            spawner,
        })
    }
}

impl<S: SpawnBlocking> KeyValueStore for RedbStore<S> {
    type Rejection = RedbRejection;
    // never ValueTooLarge, transact reports that as a rejection
    type Failure = Error;
    type Observation = RedbObservation;
    type ReadTransaction<'txn> = RedbTransaction<ReadOnlyTable<&'static [u8], &'static [u8]>>;
    type Transaction<'txn> = RedbTransaction<Table<'txn, &'static [u8], &'static [u8]>>;

    async fn transact<T: Send>(
        &self,
        mut work: impl for<'t, 'txn> FnMut(
            &'t mut Self::Transaction<'txn>,
        ) -> BoxFuture<'t, Result<T, Self::Failure>>
        + Send,
    ) -> Result<T, StoreError<Self::Rejection, Self::Failure>> {
        let mut written = self.writer.lock().await;
        written.clear();
        let result: Result<T, Error> = async {
            let transaction = self.database.begin_write()?;
            let output = {
                let mut handle = RedbTransaction {
                    table: transaction.open_table(TABLE)?,
                    written: std::mem::take(&mut *written),
                };
                let output = work(&mut handle).await;
                *written = handle.written;
                output?
            };
            self.spawner
                .spawn_blocking(move || transaction.commit())
                .await?;
            self.notify.notify_waiters(&written);
            Ok(output)
        }
        .await;
        result.map_err(|error| match error {
            Error::ValueTooLarge(size) => StoreError::Rejected(RedbRejection::ValueTooLarge(size)),
            error => StoreError::Failed(error),
        })
    }

    async fn read_transact<T: Send>(
        &self,
        mut work: impl for<'t, 'txn> FnMut(
            &'t Self::ReadTransaction<'txn>,
        ) -> BoxFuture<'t, Result<T, Self::Failure>>
        + Send,
    ) -> Result<T, StoreError<Self::Rejection, Self::Failure>> {
        let result: Result<T, Error> = async {
            let transaction = self.database.begin_read()?;
            let handle = RedbTransaction {
                table: transaction.open_table(TABLE)?,
                written: Vec::new(),
            };
            work(&handle).await
        }
        .await;
        result.map_err(StoreError::Failed)
    }

    async fn changed(&self, observation: RedbObservation) -> Result<(), Self::Failure> {
        let observation = &observation;
        self.notify
            .wait_until(&observation.key, || async move {
                let transaction = self.database.begin_read()?;
                let table = transaction.open_table(TABLE)?;
                let current = table.get(&observation.key[..])?;
                Ok(current.as_ref().map(|value| value.value()) != observation.seen.as_deref())
            })
            .await
    }
}

pub struct RedbObservation {
    key: Bytes,
    seen: Option<Bytes>,
}

pub struct RedbTransaction<T> {
    table: T,
    written: Vec<Bytes>,
}

impl<T> ReadKeyValueTransaction for RedbTransaction<T>
where
    T: ReadableTable<&'static [u8], &'static [u8]> + Send + Sync,
{
    type Failure = Error;
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
        Ok(RedbObservation { key, seen })
    }
}

impl KeyValueTransaction for RedbTransaction<Table<'_, &'static [u8], &'static [u8]>> {
    fn set(&mut self, key: Bytes, value: Bytes) -> Result<(), Self::Failure> {
        let previous = self.table.insert(&key[..], &value[..])?;
        if previous.is_none_or(|previous| previous.value() != &value[..]) {
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
}
