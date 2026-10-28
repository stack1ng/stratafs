mod environment;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::convert::Infallible;
use std::ffi::{CString, NulError, c_int};
use std::ops::{Bound, RangeBounds};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::{Arc, PoisonError};

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use futures_util::lock::Mutex;
use mdbx_sys as ffi;

use stratafs_keyed_notify::KeyedNotify;
use stratafs_range_bounds_map::RangeBoundsMap;
use stratafs_store::content::{ContentHash, ContentStore, GetRejection, UnknownContent};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
    TransactionHandle, assert_valid_range,
};
use stratafs_store::spawn::SpawnBlocking;
use stratafs_store::store::Store;

use crate::environment::{Environment, Snapshot};

// MdbxFailure is a failure libmdbx or the system reported, by its code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{}", environment::describe(*.0))]
pub struct MdbxFailure(c_int);

#[derive(Debug, thiserror::Error)]
pub enum MdbxOpenError {
    #[error("the store's file is already open")]
    AlreadyOpen,
    #[error("the store's path holds a nul byte")]
    Path(#[from] NulError),
    #[error("the store's file has pages too small for the largest entry the store accepts")]
    PagesTooSmall,
    #[error(transparent)]
    Failed(#[from] MdbxFailure),
}

// MdbxConfiguration is how libmdbx opens a store's file, as libmdbx takes it: options and geometry set before it
// opens the file, flags and mode it opens the file with, and shared options, which libmdbx keeps in the lock file
// every process shares, set once the file is open. The store allows its two tables before the options, and adds
// MDBX_NOSTICKYTHREADS to the flags.
#[derive(Debug, Clone, Copy)]
pub struct MdbxConfiguration<'a> {
    pub options: &'a [(ffi::MDBX_option_t, u64)],
    pub geometry: MdbxGeometry,
    pub flags: ffi::MDBX_env_flags_t,
    pub mode: ffi::mdbx_mode_t,
    pub shared_options: &'a [(ffi::MDBX_option_t, u64)],
}

// MdbxGeometry is how libmdbx sizes a store's file, as mdbx_env_set_geometry takes it, in bytes. -1 leaves
// libmdbx's default, or what the file already has.
#[derive(Debug, Clone, Copy)]
pub struct MdbxGeometry {
    pub size_lower: isize,
    pub size_now: isize,
    pub size_upper: isize,
    pub growth_step: isize,
    pub shrink_threshold: isize,
    pub page_size: isize,
}

// MdbxStore keeps a file system's metadata and its content in one libmdbx file, so its commits order every change
// to either.
pub struct MdbxStore<M, S> {
    metadata: M,
    content: MdbxContent<S>,
}

// MdbxMetadata is the key value store of a libmdbx file. Its handles share the file.
pub struct MdbxMetadata<S>(Arc<Database<S>>);

// MdbxContent is the content of a libmdbx file.
pub struct MdbxContent<S>(Arc<Database<S>>);

// Database is the libmdbx file a store's handles share.
struct Database<S> {
    environment: Arc<Environment>,
    writer: Mutex<Writer>,
    // pending is the content put since a commit last wrote it. Every commit writes it ahead of its own writes, so no
    // commit names content that it or an earlier commit did not write.
    pending: std::sync::Mutex<HashMap<ContentHash, Bytes>>,
    notify: KeyedNotify,
    spawner: S,
}

// Writer is what the one write transaction at a time keeps between commits.
struct Writer {
    written: Vec<Bytes>,
    // durable is the transaction id of the newest commit of any process when the store last made its commits
    // durable, with what it made them durable against, or None before it first did.
    durable: Option<(u64, DurableAgainst)>,
}

// Writes are what a write transaction wrote, kept until it commits. None means the range was cleared.
type Writes = RangeBoundsMap<Bytes, Option<Bytes>>;

// Commit is how an attempt to commit a write transaction ended.
enum Commit {
    Committed,
    // Another process committed since the transaction's snapshot, so its work runs again on a new one.
    Behind,
    // Another process's write transaction, or a sync, holds libmdbx's writer lock, so the commit waits for it
    // and tries again.
    Waiting,
}

impl<S> MdbxMetadata<S> {
    // open opens the store's file at path as configuration tells libmdbx to. Several processes may have the store
    // open at once, each once, unless configuration opens it exclusively.
    pub fn open(
        path: impl AsRef<Path>,
        configuration: &MdbxConfiguration<'_>,
        spawner: S,
    ) -> Result<Self, MdbxOpenError> {
        let path = CString::new(path.as_ref().as_os_str().as_bytes())?;
        Ok(Self(Arc::new(Database {
            environment: Arc::new(Environment::open(&path, configuration)?),
            writer: Mutex::new(Writer {
                written: Vec::new(),
                durable: None,
            }),
            pending: std::sync::Mutex::new(HashMap::new()),
            notify: KeyedNotify::new(),
            spawner,
        })))
    }
}

impl<S> MdbxContent<S> {
    // of is the content of the file metadata is the key value store of.
    pub fn of(metadata: &MdbxMetadata<S>) -> Self {
        Self(Arc::clone(&metadata.0))
    }
}

impl<M, S> MdbxStore<M, S> {
    // new is a store of metadata and content. Making it durable syncs content's file, so metadata must be kept in
    // that file too.
    pub fn new(metadata: M, content: MdbxContent<S>) -> Self {
        Self { metadata, content }
    }
}

impl<S> Database<S> {
    // commit writes the content put so far, then what a transaction wrote on its snapshot, unless another process
    // committed since the snapshot, or something else holds libmdbx's writer lock. Without a transaction it writes the
    // content alone. It takes libmdbx's writer lock only while it is free, so it runs in place, and libmdbx syncs
    // inside it as the store's configuration tells it to.
    fn commit(
        &self,
        writer: &mut Writer,
        transaction: Option<(&Snapshot<'_>, &Writes)>,
    ) -> Result<Commit, MdbxFailure> {
        let Some(mut writing) = self.environment.try_write()? else {
            return Ok(Commit::Waiting);
        };
        if let Some((snapshot, _)) = transaction
            && !snapshot.is_latest()?
        {
            return Ok(Commit::Behind);
        }
        let pending: Vec<_> = self
            .lock_pending()
            .iter()
            .map(|(hash, content)| (*hash, content.clone()))
            .collect();
        for (hash, content) in &pending {
            writing.put_content(hash, content)?;
        }
        writer.written.clear();
        for ((start, end), write) in transaction
            .into_iter()
            .flat_map(|(_, writes)| writes.iter())
        {
            match write {
                Some(value) => {
                    let Bound::Included(key) = start else {
                        unreachable!("a buffered set covers exactly its key")
                    };
                    writing.put(key, value)?;
                    writer.written.push(key.clone());
                }
                None => writing.clear(
                    start.map(|key| &key[..]),
                    end.map(|key| &key[..]),
                    &mut writer.written,
                )?,
            }
        }
        writing.commit()?;
        let mut still_pending = self.lock_pending();
        for (hash, _) in &pending {
            still_pending.remove(hash);
        }
        drop(still_pending);
        self.notify.notify_waiters(&writer.written);
        Ok(Commit::Committed)
    }

    // write_pending commits the content put so far on its own, unless none is waiting for a commit.
    async fn write_pending(&self, writer: &mut Writer) -> Result<(), MdbxFailure>
    where
        S: SpawnBlocking,
    {
        while !self.lock_pending().is_empty() {
            match self.commit(writer, None)? {
                Commit::Committed => break,
                Commit::Behind => unreachable!("a commit without a snapshot is never behind"),
                Commit::Waiting => self.wait_for_writer().await?,
            }
        }
        Ok(())
    }

    // wait_for_writer returns once no write transaction holds libmdbx's writer lock. Waiting blocks, so it goes to
    // the spawner.
    async fn wait_for_writer(&self) -> Result<(), MdbxFailure>
    where
        S: SpawnBlocking,
    {
        let environment = Arc::clone(&self.environment);
        self.spawner
            .spawn_blocking(move || environment.wait_for_writer())
            .await
    }

    // ensure_durable makes every commit of any process so far, and the content put so far, durable against a
    // failure, unless they already are.
    async fn ensure_durable(&self, against: DurableAgainst) -> Result<(), MdbxFailure>
    where
        S: SpawnBlocking,
    {
        let mut writer = self.writer.lock().await;
        self.write_pending(&mut writer).await?;
        let newest = self.environment.newest_commit()?;
        if writer
            .durable
            .is_some_and(|(through, done)| through == newest && done >= against)
        {
            return Ok(());
        }
        // The sync waits on the disk, and on any other process's write transaction, so it goes to the spawner.
        let environment = Arc::clone(&self.environment);
        self.spawner
            .spawn_blocking(move || environment.sync(against))
            .await?;
        writer.durable = Some((newest, against));
        Ok(())
    }

    fn lock_pending(&self) -> std::sync::MutexGuard<'_, HashMap<ContentHash, Bytes>> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<S: SpawnBlocking + 'static> KeyValueStore for MdbxMetadata<S> {
    // Work runs again when another process commits first, so the store refuses nothing.
    type Rejection = Infallible;
    type Failure = MdbxFailure;
    type ReadTransaction<'txn> = MdbxReadTransaction<'txn>;
    type Transaction<'txn> = MdbxTransaction<'txn>;

    // The largest key libmdbx stores, so a whole entry fits as its key. Values may be almost 2 GiB.
    const LARGEST_ACCEPTED_ENTRY: usize = environment::LARGEST_KEY;

    async fn transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<
            'txn,
            Result<(T, TransactionHandle<'txn, 'store, Self::Transaction<'txn>>), E>,
        > + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        let database = &self.0;
        let mut writer = database.writer.lock().await;
        loop {
            // Work reads a snapshot and keeps its writes to itself, so it holds no lock of libmdbx's while it
            // runs, and its writes reach libmdbx in one step.
            let snapshot = database
                .environment
                .snapshot()
                .map_err(TransactError::Failed)?;
            let (output, transaction) = work(TransactionHandle::new(MdbxTransaction {
                snapshot,
                writes: Writes::new(),
            }))
            .await
            .map_err(TransactError::Aborted)?;
            let MdbxTransaction { snapshot, writes } = transaction.into_inner();
            if writes.is_empty() {
                return Ok(output);
            }
            loop {
                match database
                    .commit(&mut writer, Some((&snapshot, &writes)))
                    .map_err(TransactError::Failed)?
                {
                    Commit::Committed => return Ok(output),
                    Commit::Behind => break,
                    Commit::Waiting => database
                        .wait_for_writer()
                        .await
                        .map_err(TransactError::Failed)?,
                }
            }
        }
    }

    async fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        let snapshot = self
            .0
            .environment
            .snapshot()
            .map_err(TransactError::Failed)?;
        work(TransactionHandle::new(MdbxReadTransaction { snapshot }))
            .await
            .map_err(TransactError::Aborted)
    }

    // changed sees only this process's commits.
    async fn changed(&self, key: Bytes, seen: Option<Bytes>) -> Result<(), Self::Failure> {
        let (key, seen) = (&key, &seen);
        self.0
            .notify
            .wait_until(key, || async move {
                let current = self.0.environment.snapshot()?.get(key)?;
                Ok(current != *seen)
            })
            .await
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.0.ensure_durable(against)
    }
}

impl<S> Clone for MdbxMetadata<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S: SpawnBlocking + 'static> ContentStore for MdbxContent<S> {
    type Failure = MdbxFailure;
    type Content = Bytes;

    // A put keeps content in memory until the next commit, which writes it ahead of anything that names it.
    async fn put(&self, mut content: impl Buf + Send) -> Result<ContentHash, Self::Failure> {
        let content = content.copy_to_bytes(content.remaining());
        let hash = ContentHash::of(&content);
        self.0.lock_pending().insert(hash, content);
        Ok(hash)
    }

    async fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> Result<Bytes, StoreError<GetRejection, Self::Failure>> {
        let pending = self.0.lock_pending().get(hash).cloned();
        if let Some(content) = pending {
            within(offset, size, content.len() as u64)?;
            return Ok(content.slice(offset as usize..(offset + size) as usize));
        }
        let snapshot = self.0.environment.snapshot()?;
        let len = snapshot.content_length(hash)?.ok_or(StoreError::Rejected(
            GetRejection::UnknownContent(UnknownContent),
        ))?;
        within(offset, size, len)?;
        let mut read = vec![0; usize::try_from(size).expect("a read fits in memory")];
        snapshot.read_content(hash, offset, &mut read)?;
        Ok(Bytes::from(read))
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.0.ensure_durable(against)
    }
}

impl<S> Clone for MdbxContent<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<M: KeyValueStore, S: SpawnBlocking + 'static> Store for MdbxStore<M, S> {
    type Metadata = M;
    type Content = MdbxContent<S>;
    type Failure = MdbxFailure;

    fn metadata(&self) -> &M {
        &self.metadata
    }

    fn content(&self) -> &MdbxContent<S> {
        &self.content
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.content.0.ensure_durable(against)
    }
}

// within accepts a read of size bytes from offset of content len bytes long, and refuses one that reaches past it.
fn within(offset: u64, size: u64, len: u64) -> Result<(), StoreError<GetRejection, MdbxFailure>> {
    match offset.checked_add(size) {
        Some(end) if end <= len => Ok(()),
        Some(_) | None => Err(StoreError::Rejected(GetRejection::OutOfBounds { len })),
    }
}

pub struct MdbxReadTransaction<'txn> {
    snapshot: Snapshot<'txn>,
}

impl<'txn> ReadKeyValueTransaction for MdbxReadTransaction<'txn> {
    type Failure = MdbxFailure;

    async fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Failure> {
        self.snapshot.get(&key)
    }

    async fn get_range<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
        limit: usize,
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + use<'txn, R>, Self::Failure> {
        assert_valid_range(&range);
        let entries = self
            .snapshot
            .entries(
                range.start_bound().map(|key| &key[..]),
                range.end_bound().map(|key| &key[..]),
            )?
            .take(limit)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(entries.into_iter())
    }
}

pub struct MdbxTransaction<'txn> {
    snapshot: Snapshot<'txn>,
    writes: Writes,
}

impl<'txn> ReadKeyValueTransaction for MdbxTransaction<'txn> {
    type Failure = MdbxFailure;

    async fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Failure> {
        match self.writes.get(&key) {
            Some(written) => Ok(written.clone()),
            None => self.snapshot.get(&key),
        }
    }

    async fn get_range<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
        limit: usize,
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + use<'txn, R>, Self::Failure> {
        assert_valid_range(&range);
        let mut entries = Vec::new();
        // The snapshot is read only where the transaction cleared nothing, and what it set there replaces it.
        let readable = self
            .writes
            .matching_ranges((range.start_bound(), range.end_bound()), |write| {
                !matches!(write, Some(None))
            });
        for (start, end) in readable {
            if entries.len() >= limit {
                break;
            }
            let mut written = self
                .writes
                .overlapping((start.as_ref(), end.as_ref()))
                .filter_map(|((start, _), write)| match (start, write) {
                    (Bound::Included(key), Some(value)) => Some((key.clone(), value.clone())),
                    _ => None,
                });
            let mut stored = self.snapshot.entries(
                start.as_ref().map(|key| &key[..]),
                end.as_ref().map(|key| &key[..]),
            )?;
            let (mut next_written, mut next_stored) = (written.next(), stored.next().transpose()?);
            while entries.len() < limit {
                let entry = match (next_written.take(), next_stored.take()) {
                    (None, None) => break,
                    (Some(set), None) => {
                        next_written = written.next();
                        set
                    }
                    (None, Some(kept)) => {
                        next_stored = stored.next().transpose()?;
                        kept
                    }
                    (Some(set), Some(kept)) => match set.0.cmp(&kept.0) {
                        Ordering::Less => {
                            next_written = written.next();
                            next_stored = Some(kept);
                            set
                        }
                        Ordering::Equal => {
                            next_written = written.next();
                            next_stored = stored.next().transpose()?;
                            set
                        }
                        Ordering::Greater => {
                            next_written = Some(set);
                            next_stored = stored.next().transpose()?;
                            kept
                        }
                    },
                };
                entries.push(entry);
            }
        }
        Ok(entries.into_iter())
    }
}

impl KeyValueTransaction for MdbxTransaction<'_> {
    type Rejection = Infallible;

    fn set(
        &mut self,
        key: Bytes,
        mut value: impl Buf,
    ) -> Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>> {
        if !self
            .snapshot
            .environment()
            .fits(key.len(), value.remaining())
        {
            return Err(StoreError::Rejected(SetRejection::TooLarge));
        }
        self.writes
            .insert(&key..=&key, Some(value.copy_to_bytes(value.remaining())));
        Ok(())
    }

    fn clear_range(&mut self, range: impl RangeBounds<Bytes>) -> Result<(), Self::Failure> {
        assert_valid_range(&range);
        self.writes.insert(range, None);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
