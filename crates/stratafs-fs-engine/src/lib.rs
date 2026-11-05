#![deny(clippy::disallowed_methods)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::ready;
use std::marker::PhantomData;
use std::sync::Arc;

use futures_core::future::BoxFuture;
use storekey::{BorrowDecode, Encode};
use stratafs_runtime::Runtime;
use stratafs_serialized::little_endian_newtypes;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, TransactError, TransactionHandle,
};
use stratafs_store::store::Store;

use crate::attributes::Attributes;
use crate::error::{EngineFailure, Posix};
pub use crate::eventual::Eventual;
use crate::observation::KernelCache;
pub use crate::strong::StrongUntilDisconnectCached;

pub mod annotations;
pub mod attributes;
pub mod content;
pub mod directories;
pub mod error;
pub mod eventual;
mod key;
pub mod observation;
pub mod spillable;
pub mod strong;
#[cfg(test)]
mod test_support;

pub trait FsConfig: Send + Sync + 'static {
    const LARGEST_INLINE_ENTRY: usize;
}

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
        purge: Purge,
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

    // vouch says whether reads, taken since ticket by an operation begun at began, may be handed to the kernel. The
    // engine asks while nothing else can be handed to the kernel for those inodes, so it must not wait.
    fn vouch(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        state: &Self::InodeState,
        ticket: Self::Ticket,
        began: Self::Ticket,
        purge: Purge,
    ) -> bool;

    // vouch_listed says whether a listing's reads, taken since ticket, may be handed to the kernel. Only inodes held
    // already count, since the kernel keeps nothing of a listing but the attributes of items it holds.
    fn vouch_listed(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        state: &Self::InodeState,
        ticket: Self::Ticket,
    ) -> bool;

    // settled resolves once reads vouch refused are worth taking again.
    fn settled(
        &self,
        mount: &Mount<St>,
        reads: &[(Inode, Attributes)],
        state: &Self::InodeState,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;

    // reread reads again the inodes a write committed for its reply, with their state, failing when another write
    // changed one since. A write's reply may bind names, so it cannot be read again; one not handed over at once is
    // vouched for as reread, which shows any write in flight since.
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

// Purge is whether the kernel's purge of an inode waits for an operation in flight on it. FSKit's waits for a file's
// getattr, read, write, or change of size or times, and not for a lookup, a listing, a change to a directory, nor
// any other change to a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Purge {
    Waits,
    GoesAhead,
    Unknown,
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
    mount: MountId,
    content: &'engine C,
    allocate_inode: &'engine A,
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
                .transact(Purge::GoesAhead, |mut transaction| {
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
        purge: Purge,
        mut work: impl for<'txn> FnMut(
            WriteTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, WriteTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
    ) -> impl Future<Output = Result<T, Failed<St, C::Failure, E>>> + Send {
        async move {
            let (result, _, _, changes, prepared, _) =
                self.committed(purge, &mut work, |_| Vec::new()).await?;
            if let Some(prepared) = prepared {
                self.consistency
                    .finish(&self.mount, prepared, Some(&changes))
                    .await;
            }
            Ok(result)
        }
    }

    // transact_and_serve runs work as transact does, and hands the kernel what it committed with hand once the
    // consistency vouches for every inode and attributes reads finds in its result, as serve does.
    #[allow(clippy::manual_async_fn)]
    pub fn transact_and_serve<'engine, T: Send + 'engine, E: Send + 'engine, R: Send>(
        &'engine self,
        purge: Purge,
        mut work: impl for<'txn> FnMut(
            WriteTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, WriteTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Send + Sync,
        mut hand: impl FnMut(&T, &[(Inode, Attributes)], &(dyn Fn() -> bool + Sync)) -> Option<R> + Send,
    ) -> impl Future<Output = Result<R, Failed<St, C::Failure, E>>> + Send {
        async move {
            let (result, committed, mut state, changes, prepared, mut ticket) =
                self.committed(purge, &mut work, reads).await?;
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
                    Err(failure) => break Err(consistency_failed(failure)),
                }
                if !at_once {
                    match self.consistency.reread(&self.mount, &committed).await {
                        Ok(reread) => (read, state) = reread,
                        Err(failure) => break Err(consistency_failed(failure)),
                    }
                }
                let vouch = || {
                    self.consistency.vouch(
                        &self.mount,
                        &read,
                        &state,
                        ticket,
                        ticket,
                        Purge::Unknown,
                    )
                };
                match hand(&result, &read, &vouch) {
                    Some(replied) => break Ok(self.handed(&read, replied)),
                    None => {
                        at_once = false;
                        if let Err(failure) = self.settled(&read, &state).await {
                            break Err(consistency_failed(failure));
                        }
                    }
                }
            };
            if let Some(prepared) = prepared {
                self.consistency
                    .finish(&self.mount, prepared, Some(&changes))
                    .await;
            }
            served
        }
    }

    // committed runs work until the consistency says its changes may commit and it commits, and returns its result,
    // what reads finds in it with the state of those inodes, the changes it committed, what the consistency prepared
    // for it, and the ticket of its committed attempt.
    async fn committed<'engine, T: Send + 'engine, E: Send + 'engine>(
        &'engine self,
        purge: Purge,
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
        loop {
            let ticket = self.consistency.reading();
            let attempt = self
                .mount
                .store
                .metadata()
                .transact(|metadata| {
                    let mut transaction = self.transaction(metadata);
                    transaction.reuse = reuse.clone();
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
                    return Ok((result, read, state, changes, prepared, ticket));
                }
                Err(TransactError::Aborted(Attempt::Refused(changes, allocated))) => {
                    reuse = allocated;
                    match self
                        .consistency
                        .prepare(&self.mount, prepared.take(), changes, purge)
                        .await
                    {
                        Ok(readied) => {
                            prepared = Some(readied);
                            continue;
                        }
                        Err(failure) => return Err(consistency_failed(failure)),
                    }
                }
                Err(TransactError::Aborted(Attempt::Work(error))) => TransactError::Aborted(error),
                Err(TransactError::Aborted(Attempt::Consistency(failure))) => {
                    consistency_failed(failure)
                }
                Err(TransactError::Rejected(rejection)) => TransactError::Rejected(rejection),
                Err(TransactError::Failed(failure)) => {
                    TransactError::Failed(EngineFailure::Store(failure))
                }
            };
            if let Some(prepared) = prepared {
                self.consistency.finish(&self.mount, prepared, None).await;
            }
            return Err(failed);
        }
    }

    // serve runs read and hands its result to the kernel with hand, once the consistency vouches for every inode
    // and attributes reads finds in it, running read again until it does. hand must ask vouch while nothing else can
    // be handed to the kernel for those inodes, and reply only when it says yes, returning None otherwise. read hands
    // its transaction back with its result, as transact's work does, so the consistency reads its state in it too.
    pub fn serve<'engine, T: Send, E: Send, R: Send>(
        &'engine self,
        purge: Purge,
        read: impl for<'txn> FnMut(
            ReadTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, ReadTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Send + Sync,
        hand: impl FnMut(&T, &[(Inode, Attributes)], &(dyn Fn() -> bool + Sync)) -> Option<R> + Send,
    ) -> impl Future<Output = Result<R, Failed<St, C::Failure, E>>> + Send {
        self.serve_loaded(purge, read, |result| ready(Ok(result)), reads, hand)
    }

    // serve_loaded serves as serve does, but runs load on what read returns once read's transaction ends, such as to
    // load a read's content, and hands load's result to the kernel. reads finds the inodes in what read returns.
    #[allow(clippy::manual_async_fn)]
    pub fn serve_loaded<'engine, T: Send, U: Send, E: Send, R: Send, L>(
        &'engine self,
        purge: Purge,
        mut read: impl for<'txn> FnMut(
            ReadTransaction<'txn, 'engine, St, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<(T, ReadTransaction<'txn, 'engine, St, A, N, S>), E>,
        > + Send,
        mut load: impl FnMut(T) -> L + Send,
        reads: impl Fn(&T) -> Vec<(Inode, Attributes)> + Send + Sync,
        mut hand: impl FnMut(&U, &[(Inode, Attributes)], &(dyn Fn() -> bool + Sync)) -> Option<R> + Send,
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
                let vouch = || {
                    self.consistency
                        .vouch(&self.mount, &read, &state, ticket, began, purge)
                };
                match hand(&result, &read, &vouch) {
                    Some(replied) => return Ok(self.handed(&read, replied)),
                    None => self
                        .settled(&read, &state)
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
        mut hand: impl FnMut(&T, &[(Inode, Attributes)], &(dyn Fn() -> bool + Sync)) -> Option<R> + Send,
    ) -> impl Future<Output = Result<R, Failed<St, C::Failure, E>>> + Send {
        async move {
            loop {
                let ticket = self.consistency.reading();
                let (result, read, state) = self.read_with_state(&mut read, &reads).await?;
                let vouch = || {
                    self.consistency
                        .vouch_listed(&self.mount, &read, &state, ticket)
                };
                match hand(&result, &read, &vouch) {
                    Some(replied) => return Ok(replied),
                    None => self
                        .settled(&read, &state)
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

    // handed tells the consistency the kernel now caches each of reads, which a reply just handed over.
    fn handed<R>(&self, reads: &[(Inode, Attributes)], replied: R) -> R {
        for (inode, read) in reads {
            self.consistency.cached(&self.mount, *inode, read);
        }
        replied
    }

    // settled resolves once reads the consistency did not vouch for, of state, are worth taking again.
    fn settled<'a>(
        &'a self,
        reads: &'a [(Inode, Attributes)],
        state: &'a C::InodeState,
    ) -> impl Future<Output = Result<(), C::Failure>> + Send + 'a {
        self.consistency.settled(&self.mount, reads, state)
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
            mount: self.mount.id,
            content: self.mount.store.content(),
            allocate_inode: &self.allocate_inode,
            now: &self.now,
            config: PhantomData,
        }
    }
}
