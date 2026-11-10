#![deny(clippy::disallowed_methods)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::ready;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use futures_core::future::BoxFuture;
use futures_util::future::join_all;
use smallvec::SmallVec;
use storekey::{BorrowDecode, Encode};
use stratafs_runtime::Runtime;
use stratafs_serialized::little_endian_newtypes;
use stratafs_store::content::{ContentId, ContentStore};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, TransactError, TransactionHandle,
};
use stratafs_store::store::Store;

use crate::attributes::Attributes;
use crate::error::{EngineFailure, Posix};
pub use crate::eventual::Eventual;
use crate::kernel_cache::KernelCache;
use crate::spillable::ContentHash;
pub use crate::strong::StrongUntilDisconnectCached;

pub mod annotations;
pub mod attributes;
pub mod content;
pub mod directories;
pub mod error;
pub mod eventual;
pub mod kernel_cache;
mod key;
pub mod spillable;
pub mod strong;
#[cfg(test)]
mod test_support;

pub trait FsConfig: Send + Sync + 'static {
    const LARGEST_INLINE_ENTRY: usize;

    // content_hash is the hash kept with the reference to a blob of content. A transaction's attempts take content
    // with the same hash and length for the same content.
    fn content_hash(content: &[u8]) -> ContentHash;
}

// WrittenBlob is a blob one of a transaction's attempts wrote. A later attempt that spills the same content takes it
// instead of writing it again.
pub(crate) struct WrittenBlob {
    pub(crate) hash: ContentHash,
    pub(crate) length: u64,
    pub(crate) id: ContentId,
    // taken_by is the attempt that last referenced the blob
    pub(crate) taken_by: u32,
}

// WrittenBlobs are the blobs one transaction's attempts wrote, of which most transactions have at most one.
pub(crate) type WrittenBlobs = SmallVec<[WrittenBlob; 1]>;

const SMALLEST_INLINE_ENTRY: usize = 1024;

pub const LARGEST_NAME_LENGTH: usize = 255;

// MetadataRejection and MetadataFailure are why a transaction on a store's metadata was refused or failed.
type MetadataRejection<St> = <<St as Store>::Metadata as KeyValueStore>::Rejection;
type MetadataFailure<St> = <<St as Store>::Metadata as KeyValueStore>::Failure;

// WriteTransaction and ReadTransaction are the engine's transactions on store St that a write's work and a read run in.
type WriteTransaction<'txn, 'engine, St, A, N, S> = FsTransaction<
    'txn,
    'engine,
    <<St as Store>::Metadata as KeyValueStore>::Transaction<'txn>,
    <St as Store>::Content,
    A,
    N,
    S,
>;
type ReadTransaction<'txn, 'engine, St, A, N, S> = FsTransaction<
    'txn,
    'engine,
    <<St as Store>::Metadata as KeyValueStore>::ReadTransaction<'txn>,
    <St as Store>::Content,
    A,
    N,
    S,
>;

// Failed is why an engine operation on store St failed: the store refused or failed it, the consistency failed with X,
// or its work failed with E.
type Failed<St, X, E> =
    TransactError<MetadataRejection<St>, EngineFailure<MetadataFailure<St>, X>, E>;

pub struct FsEngine<St: Store, A, N, S, C> {
    allocate_inode: A,
    allocate_content_id: fn() -> ContentId,
    now: N,
    // mount is the mount the engine is, whose id stamps every change it makes.
    mount: Mount<St>,
    // durability_limit is the most ensure_durable makes commits durable against, or None when it makes nothing
    // durable.
    durability_limit: Option<DurableAgainst>,
    // consistency keeps the kernel's cache in line with what its reads promise. Dropping it stops its work.
    consistency: C,
    config: PhantomData<S>,
}

// Mount is one mount of a volume, as its consistency works with it.
pub struct Mount<St> {
    pub(crate) store: Arc<St>,
    pub(crate) kernel: Arc<dyn KernelCache>,
    pub(crate) runtime: Arc<dyn Runtime>,
    pub(crate) id: MountId,
}

// Consistency is what a client's reads promise about other clients' writes. Each promise is a type of its own.
pub trait Consistency: Send + Sync + 'static {}

// Cached is a consistency that lets the kernel cache what the engine hands it.
pub trait Cached<St: Store>: Consistency {
    // Failure is why the consistency could not let a write or a reply go ahead.
    type Failure: std::error::Error + Posix + Send + Sync + 'static;
    // Prepared is what prepare did for one write, carried to its next attempt.
    type Prepared: Clone + Send + Sync;
    // Ticket marks when a read began.
    type Ticket: Copy + Send + Sync;
    // InodeState is what the consistency keeps in the store of its own about some inodes.
    type InodeState: Send + Sync;

    // inode_state reads the consistency's own state of inodes in metadata, the transaction their attributes are read
    // or written in, so both are of one moment.
    fn inode_state<T: ReadKeyValueTransaction<Failure = MetadataFailure<St>>>(
        &self,
        metadata: &T,
        inodes: &[Inode],
    ) -> impl Future<Output = Result<Self::InodeState, Self::Failure>> + Send;

    // may_commit says whether a write that made changes may commit, reading metadata, the write's transaction, after
    // its work. It may change its own state of the inodes in metadata, and must wait on nothing else.
    fn may_commit<T: KeyValueTransaction<Failure = MetadataFailure<St>>>(
        &self,
        mount: &Mount<St>,
        prepared: Option<&Self::Prepared>,
        changes: &HashMap<Inode, Change>,
        metadata: &mut T,
    ) -> impl Future<Output = Result<bool, Self::Failure>> + Send;

    // prepare readies the changes of a write may_commit refused, before the engine runs the write again. When it
    // fails, it first undoes what it did.
    fn prepare(
        &self,
        mount: &Mount<St>,
        prepared: Option<Self::Prepared>,
        changes: HashMap<Inode, Change>,
        purge_waits: PurgeWaits,
    ) -> impl Future<Output = Result<Self::Prepared, Self::Failure>> + Send;

    // finish ends a write prepare readied: once it will not commit, with no changes, or once it committed the changes
    // it made and the kernel was handed what it committed.
    fn finish(
        &self,
        mount: &Mount<St>,
        prepared: Self::Prepared,
        committed: Option<&HashMap<Inode, Change>>,
    ) -> impl Future<Output = ()> + Send;

    // reading marks the start of a read the engine may hand to the kernel.
    fn reading(&self) -> Self::Ticket;

    // hold makes sure every later change to inodes reaches this mount, and says whether every one was held already
    // when ticket was taken. When one was not, the read is taken again.
    fn hold(
        &self,
        mount: &Mount<St>,
        inodes: &[Inode],
        ticket: Self::Ticket,
    ) -> impl Future<Output = Result<bool, Self::Failure>> + Send;

    // result_handling says how the result of reads, taken since ticket by an operation begun at began, is handled.
    // no_cache_honoured says the kernel asks again about a NoCache result, as Linux does for names and attributes. It
    // must not wait: the engine asks while nothing else can be handed to the kernel for those inodes.
    #[allow(clippy::too_many_arguments)]
    fn result_handling(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        state: &Self::InodeState,
        ticket: Self::Ticket,
        began: Self::Ticket,
        purge_waits: PurgeWaits,
        no_cache_honoured: bool,
    ) -> ResultHandling;

    // result_handling_listed says how a listing's result, taken since ticket, is handled, as result_handling does.
    // Only inodes held already count, since the kernel keeps nothing of a listing but the attributes of items it holds.
    fn result_handling_listed(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        state: &Self::InodeState,
        ticket: Self::Ticket,
    ) -> ResultHandling;

    // settled resolves once reads handled with Retry, taken since ticket by an operation of purge_waits, are worth
    // taking again.
    fn settled(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        state: &Self::InodeState,
        ticket: Self::Ticket,
        purge_waits: PurgeWaits,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;

    // reread reads again the inodes a write committed for its reply, with their state, failing when another write
    // changed one since. A write's reply may bind names, so it cannot be read again; one not handed over at once is
    // handled as reread, which shows any write in flight since.
    #[allow(clippy::type_complexity)]
    fn reread(
        &self,
        mount: &Mount<St>,
        committed: &[(Inode, Attributes)],
    ) -> impl Future<Output = Result<(Vec<(Inode, Attributes)>, Self::InodeState), Self::Failure>> + Send;

    // cached tells the consistency the kernel now caches inode as seen.
    fn cached(&self, mount: &Mount<St>, inode: Inode, seen: &Attributes);

    // forget tells the consistency the kernel no longer caches inode. It is called while nothing can be handed to
    // the kernel for inode, so it must not wait.
    fn forget(&self, mount: &Mount<St>, inode: Inode);

    // write_back says whether the kernel may keep writes before handing them to the engine.
    fn write_back(&self) -> bool;
}

// PurgeWaits is which of the kernel's purges wait for an operation in flight. FSKit's purge of an inode waits for a
// file's getattr, read, write, or change of size or times, and not for a lookup, a listing, a change to a directory,
// nor any other change to a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PurgeWaits {
    // OfOwnInode is an operation a purge of its own inode waits for.
    OfOwnInode,
    // OfAnyInode is an operation a purge of any inode on the mount may wait for, as the kernel keeps only so many such
    // requests in flight.
    OfAnyInode,
    // No is an operation no purge waits for.
    No,
    // Unknown is an operation it is not known which purges wait for.
    Unknown,
}

// ResultHandling is how a read's result is handled: handed to the kernel to cache, handed over with the kernel told not
// to cache it, or dropped so the read runs again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultHandling {
    Cache,
    NoCache,
    Retry,
}

// StrongUncached reads everything from the store, without any caching.
pub struct StrongUncached;

impl Consistency for StrongUncached {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, BorrowDecode)]
pub struct Inode(pub u64);

// MountId names one engine, a mount of a volume, for as long as it runs. It is made afresh each time a volume is
// opened, so it says nothing about any earlier mount.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MountId(pub u64);

impl Inode {
    pub const ROOT: Inode = Inode(0);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

little_endian_newtypes! {
    Inode(u64),
    MountId(u64),
    Timestamp(i64),
}

pub struct FsTransaction<'txn, 'engine, T, C, A, N, S> {
    metadata: TransactionHandle<'txn, 'engine, T>,
    // changes is what the transaction did to each inode it changed.
    changes: HashMap<Inode, Change>,
    // allocated is the inode numbers the transaction allocated; reuse is those an earlier attempt allocated, used
    // first.
    allocated: Vec<Inode>,
    reuse: Vec<Inode>,
    // written is the blobs the transaction's attempts wrote, and attempt is this attempt's number among them. A read
    // writes no blob, so it has none.
    written: Option<&'txn Mutex<WrittenBlobs>>,
    attempt: u32,
    mount: MountId,
    content: &'engine C,
    allocate_inode: &'engine A,
    allocate_content_id: fn() -> ContentId,
    now: &'engine N,
    config: PhantomData<S>,
}

// Change is what a transaction did to an inode: the attributes it last wrote, or the attributes of an inode it
// deleted, as they stood before.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    Written(Attributes),
    Deleted(Attributes),
}

impl<T, C, A, N: Fn() -> Timestamp, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub fn now(&self) -> Timestamp {
        (self.now)()
    }
}

impl<St, A, N, S, C> FsEngine<St, A, N, S, C>
where
    St: Store,
    A: Send + Sync,
    N: Fn() -> Timestamp + Send + Sync,
    S: FsConfig,
    C: Cached<St>,
{
    // new makes an engine whose consistency keeps kernel's cache of each inode in line with the store, running its
    // tasks on runtime. An engine whose inodes no kernel caches takes NoKernel.
    //
    // new returns `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
    // As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
    // such as redb's (https://github.com/rust-lang/rust/issues/110338). The long return type comes from that form.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    pub fn new(
        store: St,
        allocate_inode: A,
        allocate_content_id: fn() -> ContentId,
        now: N,
        mount: MountId,
        durability_limit: Option<DurableAgainst>,
        consistency: C,
        kernel: impl KernelCache,
        runtime: Arc<dyn Runtime>,
    ) -> impl Future<
        Output = Result<
            Self,
            Failed<St, C::Failure, StoreError<MetadataRejection<St>, MetadataFailure<St>>>,
        >,
    > + Send {
        const {
            assert!(
                S::LARGEST_INLINE_ENTRY <= <St::Metadata as KeyValueStore>::LARGEST_ACCEPTED_ENTRY,
                "the largest inline entry exceeds the largest entry the metadata store accepts"
            );
            assert!(
                S::LARGEST_INLINE_ENTRY >= SMALLEST_INLINE_ENTRY,
                "the largest inline entry is too small to hold the engine's fixed size entries"
            );
        };
        async move {
            let engine = Self {
                allocate_inode,
                allocate_content_id,
                now,
                mount: Mount {
                    store: Arc::new(store),
                    kernel: Arc::new(kernel),
                    runtime,
                    id: mount,
                },
                durability_limit,
                consistency,
                config: PhantomData,
            };
            engine
                .transact(PurgeWaits::No, |mut transaction| {
                    Box::pin(async move {
                        let created = transaction.create_root_if_missing().await;
                        created.map(|()| ((), transaction))
                    })
                })
                .await?;
            Ok(engine)
        }
    }
}

impl<St: Store, A: Sync, N: Sync, S: Sync, C: Send + Sync> FsEngine<St, A, N, S, C> {
    // ensure_durable makes every transaction committed before it is called durable against a failure, or against
    // the durability limit when that is less, and resolves once they are. Without a limit it resolves at once.
    // It returns `impl Future + Send` instead of being `async fn` for the reason spillable.rs gives.
    #[allow(clippy::manual_async_fn)]
    pub fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), St::Failure>> + Send {
        async move {
            match self.durability_limit {
                Some(limit) => self.mount.store.ensure_durable(against.min(limit)).await,
                None => Ok(()),
            }
        }
    }
}

impl<St: Store, A, N, S, C> FsEngine<St, A, N, S, C> {
    // content is the content store, which loads a read's ContentLoadPlan once the read's transaction ends.
    pub fn content(&self) -> &St::Content {
        self.mount.store.content()
    }

    // consistency is what the engine's reads promise about other clients' writes.
    pub fn consistency(&self) -> &C {
        &self.consistency
    }
}

impl<St: Store, A: Sync, N: Sync, S: FsConfig, C: Cached<St>> FsEngine<St, A, N, S, C> {
    // transact runs work in a transaction, which commits once the consistency says its changes may, and runs it
    // again after the consistency prepares them. When work succeeds it hands its transaction back with its result.
    #[allow(clippy::manual_async_fn)]
    pub fn transact<'engine, T: Send + 'engine, E: Send + 'engine>(
        &'engine self,
        purge_waits: PurgeWaits,
        mut work: impl for<'txn> FnMut(
            WriteTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, WriteTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
    ) -> impl Future<Output = Result<T, Failed<St, C::Failure, E>>> + Send {
        async move {
            let (result, _, _, changes, prepared, _) = self
                .committed(purge_waits, &mut work, |_| Vec::new())
                .await?;
            if let Some(prepared) = prepared {
                self.consistency
                    .finish(&self.mount, prepared, Some(&changes))
                    .await;
            }
            Ok(result)
        }
    }

    // transact_and_serve runs work as transact does, and hands the kernel what it committed with hand as the
    // consistency's result handling for every inode and attributes reads finds in its result says, as serve does. Once
    // the work committed, a failure to serve hands over what it committed with NoCache instead, so a change that
    // happened does not fail; only an integration that cannot reply NoCache gets the failure.
    #[allow(clippy::manual_async_fn)]
    pub fn transact_and_serve<'engine, T: Send + 'engine, E: Send + 'engine, R: Send>(
        &'engine self,
        purge_waits: PurgeWaits,
        mut work: impl for<'txn> FnMut(
            WriteTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, WriteTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Send + Sync,
        mut hand: impl FnMut(
            &T,
            &[(Inode, Attributes)],
            &(dyn Fn() -> ResultHandling + Sync),
        ) -> Option<R>
        + Send,
    ) -> impl Future<Output = Result<R, Failed<St, C::Failure, E>>> + Send {
        async move {
            let (result, committed, mut state, changes, prepared, mut ticket) =
                self.committed(purge_waits, &mut work, reads).await?;
            let mut read = committed.clone();
            // The reply goes as committed while at_once, and as the consistency rereads it after.
            let mut at_once = true;
            let served = loop {
                match self.held(ticket, &read).await {
                    Ok(true) => {}
                    Ok(false) => {
                        ticket = self.consistency.reading();
                        at_once = false;
                        continue;
                    }
                    Err(failure) => break Err(failure),
                }
                if !at_once {
                    match self.consistency.reread(&self.mount, &committed).await {
                        Ok(reread) => (read, state) = reread,
                        Err(failure) => break Err(failure),
                    }
                }
                let cached = AtomicBool::new(false);
                let result_handling = || {
                    let handling = self.consistency.result_handling(
                        &self.mount,
                        &read,
                        &state,
                        ticket,
                        ticket,
                        PurgeWaits::Unknown,
                        false,
                    );
                    cached.store(handling == ResultHandling::Cache, Ordering::Relaxed);
                    handling
                };
                match hand(&result, &read, &result_handling) {
                    Some(replied) => break Ok(self.handed(&read, cached.into_inner(), replied)),
                    None => {
                        at_once = false;
                        if let Err(failure) = self.settled(&read, &state, ticket, purge_waits).await
                        {
                            break Err(failure);
                        }
                        ticket = self.consistency.reading();
                    }
                }
            };
            let served = served.or_else(|failure| {
                hand(&result, &committed, &|| ResultHandling::NoCache).ok_or(failure)
            });
            if let Some(prepared) = prepared {
                self.consistency
                    .finish(&self.mount, prepared, Some(&changes))
                    .await;
            }
            served.map_err(consistency_failed)
        }
    }

    // committed runs work until the consistency says its changes may commit and it commits, and returns its result,
    // what reads finds in it with the state of those inodes, the changes it committed, what the consistency prepared
    // for it, and the ticket of its committed attempt.
    async fn committed<'engine, T: Send + 'engine, E: Send + 'engine>(
        &'engine self,
        purge_waits: PurgeWaits,
        work: &mut (
                 impl for<'txn> FnMut(
            WriteTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, WriteTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send
             ),
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Sync,
    ) -> Result<
        (
            T,
            Vec<(Inode, Attributes)>,
            C::InodeState,
            HashMap<Inode, Change>,
            Option<C::Prepared>,
            C::Ticket,
        ),
        Failed<St, C::Failure, E>,
    > {
        let mut prepared = None;
        let mut reuse = Vec::new();
        let written = Mutex::new(WrittenBlobs::new());
        let mut attempts = 0;
        // a failed commit may have applied, so the blobs its attempt referenced stay
        let mut may_have_committed = false;
        let outcome = loop {
            let attempts_before = attempts;
            let ticket = self.consistency.reading();
            let attempt = self
                .mount
                .store
                .metadata()
                .transact(|metadata| {
                    // the store runs work again only when the attempt before did not commit
                    attempts += 1;
                    let mut transaction = self.transaction(metadata);
                    transaction.reuse = reuse.clone();
                    transaction.written = Some(&written);
                    transaction.attempt = attempts;
                    let working = work(transaction);
                    let (prepared, reads) = (prepared.clone(), &reads);
                    Box::pin(async move {
                        let (result, mut transaction) = working.await.map_err(Attempt::Work)?;
                        let may_commit = self
                            .consistency
                            .may_commit(
                                &self.mount,
                                prepared.as_ref(),
                                &transaction.changes,
                                &mut *transaction.metadata,
                            )
                            .await
                            .map_err(Attempt::Consistency)?;
                        if !may_commit {
                            return Err(Attempt::Refused(
                                transaction.changes,
                                transaction.allocated,
                            ));
                        }
                        let read = reads(&result);
                        let inodes: Vec<Inode> = read.iter().map(|(inode, _)| *inode).collect();
                        let state = self
                            .consistency
                            .inode_state(&*transaction.metadata, &inodes)
                            .await
                            .map_err(Attempt::Consistency)?;
                        Ok((
                            (result, read, state, transaction.changes),
                            transaction.metadata,
                        ))
                    })
                })
                .await;
            let failed = match attempt {
                Ok((result, read, state, changes)) => {
                    may_have_committed = true;
                    break Ok((result, read, state, changes, prepared, ticket));
                }
                Err(TransactError::Aborted(Attempt::Refused(changes, allocated))) => {
                    reuse = allocated;
                    match self
                        .consistency
                        .prepare(&self.mount, prepared.take(), changes, purge_waits)
                        .await
                    {
                        Ok(readied) => {
                            prepared = Some(readied);
                            continue;
                        }
                        Err(failure) => break Err(consistency_failed(failure)),
                    }
                }
                Err(TransactError::Aborted(Attempt::Work(error))) => TransactError::Aborted(error),
                Err(TransactError::Aborted(Attempt::Consistency(failure))) => {
                    consistency_failed(failure)
                }
                Err(TransactError::Rejected(rejection)) => TransactError::Rejected(rejection),
                Err(TransactError::Failed(failure)) => {
                    // a store that failed before it ran the work committed nothing
                    may_have_committed = attempts > attempts_before;
                    TransactError::Failed(EngineFailure::Store(failure))
                }
            };
            if let Some(prepared) = prepared {
                self.consistency.finish(&self.mount, prepared, None).await;
            }
            break Err(failed);
        };
        let written = written.into_inner().unwrap_or_else(PoisonError::into_inner);
        let unreferenced = written
            .iter()
            .filter(|blob| !(may_have_committed && blob.taken_by == attempts));
        join_all(unreferenced.map(|blob| async move {
            if let Err(error) = self.content().delete(&blob.id).await {
                tracing::warn!(id = blob.id.0, %error, "deleting a blob no commit references failed, so it stays");
            }
        }))
        .await;
        outcome
    }

    // serve runs read and hands its result to the kernel with hand, as the consistency's result handling for every
    // inode and attributes reads finds in it says, running read again on Retry. hand must ask for the result handling
    // while nothing else can be handed to the kernel for those inodes, and reply as it says, returning None on Retry.
    // An integration that cannot reply NoCache returns None then too, and serve waits as on Retry. read hands its
    // transaction back with its result, as transact's work does, so the consistency reads its state in it too.
    pub fn serve<'engine, T: Send, E: Send, R: Send>(
        &'engine self,
        purge_waits: PurgeWaits,
        no_cache_honoured: bool,
        read: impl for<'txn> FnMut(
            ReadTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, ReadTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Send + Sync,
        hand: impl FnMut(&T, &[(Inode, Attributes)], &(dyn Fn() -> ResultHandling + Sync)) -> Option<R>
        + Send,
    ) -> impl Future<Output = Result<R, Failed<St, C::Failure, E>>> + Send {
        self.serve_loaded(
            purge_waits,
            no_cache_honoured,
            read,
            |result| ready(Ok(result)),
            reads,
            hand,
        )
    }

    // serve_loaded serves as serve does, but runs load on what read returns once read's transaction ends, such as to
    // load a read's content, and hands load's result to the kernel. reads finds the inodes in what read returns.
    #[allow(clippy::manual_async_fn)]
    pub fn serve_loaded<'engine, T: Send, U: Send, E: Send, R: Send, L>(
        &'engine self,
        purge_waits: PurgeWaits,
        no_cache_honoured: bool,
        mut read: impl for<'txn> FnMut(
            ReadTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, ReadTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
        mut load: impl FnMut(T) -> L + Send,
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Send + Sync,
        mut hand: impl FnMut(
            &U,
            &[(Inode, Attributes)],
            &(dyn Fn() -> ResultHandling + Sync),
        ) -> Option<R>
        + Send,
    ) -> impl Future<Output = Result<R, Failed<St, C::Failure, E>>> + Send
    where
        L: Future<Output = Result<U, E>> + Send,
    {
        async move {
            // began marks when the operation began, its first read's ticket.
            let mut began = None;
            loop {
                let ticket = self.consistency.reading();
                let began = *began.get_or_insert(ticket);
                let (result, read, state) = self.read_with_state(&mut read, &reads).await?;
                let result = load(result).await.map_err(TransactError::Aborted)?;
                if !self.held(ticket, &read).await.map_err(consistency_failed)? {
                    continue;
                }
                let cached = AtomicBool::new(false);
                let result_handling = || {
                    let handling = self.consistency.result_handling(
                        &self.mount,
                        &read,
                        &state,
                        ticket,
                        began,
                        purge_waits,
                        no_cache_honoured,
                    );
                    cached.store(handling == ResultHandling::Cache, Ordering::Relaxed);
                    handling
                };
                match hand(&result, &read, &result_handling) {
                    Some(replied) => return Ok(self.handed(&read, cached.into_inner(), replied)),
                    None => self
                        .settled(&read, &state, ticket, purge_waits)
                        .await
                        .map_err(consistency_failed)?,
                }
            }
        }
    }

    // serve_listed serves read, which reads part of a directory listing, as serve does but holding nothing new: the
    // kernel keeps nothing of a listing but the attributes of items it holds already.
    #[allow(clippy::manual_async_fn)]
    pub fn serve_listed<'engine, T: Send, E: Send, R: Send>(
        &'engine self,
        mut read: impl for<'txn> FnMut(
            ReadTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, ReadTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Send + Sync,
        mut hand: impl FnMut(
            &T,
            &[(Inode, Attributes)],
            &(dyn Fn() -> ResultHandling + Sync),
        ) -> Option<R>
        + Send,
    ) -> impl Future<Output = Result<R, Failed<St, C::Failure, E>>> + Send {
        async move {
            loop {
                let ticket = self.consistency.reading();
                let (result, read, state) = self.read_with_state(&mut read, &reads).await?;
                let result_handling = || {
                    self.consistency
                        .result_handling_listed(&self.mount, &read, &state, ticket)
                };
                match hand(&result, &read, &result_handling) {
                    Some(replied) => return Ok(replied),
                    None => self
                        .settled(&read, &state, ticket, PurgeWaits::Unknown)
                        .await
                        .map_err(consistency_failed)?,
                }
            }
        }
    }

    // read_with_state runs read in a read transaction, and reads in it too the consistency's state of every inode reads
    // finds in read's result. It returns the result, what reads finds in it, and their state.
    async fn read_with_state<'engine, T: Send, E: Send>(
        &'engine self,
        read: &mut (
                 impl for<'txn> FnMut(
            ReadTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, ReadTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send
             ),
        reads: &(impl Fn(&T) -> Vec<(Inode, Attributes)> + Sync),
    ) -> Result<(T, Vec<(Inode, Attributes)>, C::InodeState), Failed<St, C::Failure, E>> {
        // A read commits nothing, so a failure of the consistency is carried out of the transaction as its result.
        let (result, read, state) = self
            .mount
            .store
            .metadata()
            .read_transact(|metadata| {
                let reading = read(self.transaction(metadata));
                Box::pin(async move {
                    let (result, transaction) = reading.await?;
                    let read = reads(&result);
                    let inodes: Vec<Inode> = read.iter().map(|(inode, _)| *inode).collect();
                    let state = self
                        .consistency
                        .inode_state(&*transaction.metadata, &inodes)
                        .await;
                    Ok((result, read, state))
                })
            })
            .await
            .map_err(store_failed)?;
        Ok((result, read, state.map_err(consistency_failed)?))
    }

    // inode_state is the consistency's own state of inodes as the store has it now, read past the kernel.
    pub async fn inode_state(
        &self,
        inodes: &[Inode],
    ) -> Result<C::InodeState, Failed<St, C::Failure, Infallible>> {
        let state = self
            .mount
            .store
            .metadata()
            .read_transact(|metadata| {
                Box::pin(async move { Ok(self.consistency.inode_state(&*metadata, inodes).await) })
            })
            .await
            .map_err(store_failed)?;
        state.map_err(consistency_failed)
    }

    // held holds every inode of reads, and says whether each was held already when ticket was taken.
    fn held(
        &self,
        ticket: C::Ticket,
        reads: &[(Inode, Attributes)],
    ) -> impl Future<Output = Result<bool, C::Failure>> + Send {
        let inodes: Vec<Inode> = reads.iter().map(|(inode, _)| *inode).collect();
        async move { self.consistency.hold(&self.mount, &inodes, ticket).await }
    }

    // handed tells the consistency the kernel now caches each of reads, which a reply just handed over, when the reply
    // was cached.
    fn handed<R>(&self, reads: &[(Inode, Attributes)], cached: bool, replied: R) -> R {
        if cached {
            for (inode, read) in reads {
                self.consistency.cached(&self.mount, *inode, read);
            }
        }
        replied
    }

    // settled resolves once reads the consistency handled with Retry, of state, taken since ticket by an operation of
    // purge_waits, are worth taking again.
    fn settled<'a>(
        &'a self,
        reads: &'a [(Inode, Attributes)],
        state: &'a C::InodeState,
        ticket: C::Ticket,
        purge_waits: PurgeWaits,
    ) -> impl Future<Output = Result<(), C::Failure>> + Send + 'a {
        self.consistency
            .settled(&self.mount, reads, state, ticket, purge_waits)
    }

    // cached tells the consistency the kernel caches inode as seen, for a reply serve did not hand over.
    pub fn cached(&self, inode: Inode, seen: &Attributes) {
        self.consistency.cached(&self.mount, inode, seen);
    }

    // forget tells the consistency the kernel no longer caches inode. Nothing may be handed to the kernel for inode
    // meanwhile.
    pub fn forget(&self, inode: Inode) {
        self.consistency.forget(&self.mount, inode);
    }

    // write_back says whether the kernel may keep writes before handing them to the engine.
    pub fn write_back(&self) -> bool {
        self.consistency.write_back()
    }
}

// Attempt is why a write's transaction did not commit: its work failed, the consistency failed, or the consistency
// refused to let the work's changes commit yet, with the inode numbers it allocated.
enum Attempt<E, X> {
    Work(E),
    Consistency(X),
    Refused(HashMap<Inode, Change>, Vec<Inode>),
}

// consistency_failed is the transaction error for a failure of the consistency.
fn consistency_failed<R, F, X, E>(failure: X) -> TransactError<R, EngineFailure<F, X>, E> {
    TransactError::Failed(EngineFailure::Consistency(failure))
}

// store_failed is error with a failure of the store marked as the store's.
fn store_failed<R, F, X, E>(
    error: TransactError<R, F, E>,
) -> TransactError<R, EngineFailure<F, X>, E> {
    match error {
        TransactError::Rejected(rejection) => TransactError::Rejected(rejection),
        TransactError::Failed(failure) => TransactError::Failed(EngineFailure::Store(failure)),
        TransactError::Aborted(error) => TransactError::Aborted(error),
    }
}

impl<St: Store, A: Sync, N: Sync, S: FsConfig, C: Send + Sync> FsEngine<St, A, N, S, C> {
    pub fn read_transact<'engine, T: Send, E: Send>(
        &'engine self,
        mut work: impl for<'txn> FnMut(
            ReadTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<
        Output = Result<T, TransactError<MetadataRejection<St>, MetadataFailure<St>, E>>,
    > + Send {
        self.mount
            .store
            .metadata()
            .read_transact(move |metadata| work(self.transaction(metadata)))
    }

    // transaction is metadata as one of the engine's transactions. The store outlives the engine, so metadata may come
    // from a shorter borrow of it, which lets a transaction's future borrow the caller's closures.
    fn transaction<'txn, 'engine, T>(
        &'engine self,
        metadata: TransactionHandle<'txn, '_, T>,
    ) -> FsTransaction<'txn, 'engine, T, St::Content, A, N, S> {
        FsTransaction {
            metadata: TransactionHandle::new(metadata.into_inner()),
            changes: HashMap::new(),
            allocated: Vec::new(),
            reuse: Vec::new(),
            written: None,
            attempt: 0,
            mount: self.mount.id,
            content: self.mount.store.content(),
            allocate_inode: &self.allocate_inode,
            allocate_content_id: self.allocate_content_id,
            now: &self.now,
            config: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests;
