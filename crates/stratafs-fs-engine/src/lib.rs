use std::marker::PhantomData;
use std::sync::{Arc, PoisonError};

use futures_core::future::BoxFuture;
use futures_util::task::Spawn;
use storekey::{BorrowDecode, Encode};
use stratafs_serialized::little_endian_newtypes;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, TransactError, TransactionHandle};
use stratafs_store::store::Store;

use crate::attributes::Attributes;
use crate::observation::{AutoInvalidators, KernelCache};

pub mod annotations;
pub mod attributes;
pub mod content;
pub mod directories;
pub mod error;
mod key;
pub mod observation;
pub mod spillable;
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

pub struct FsEngine<St: Store, A, N, S, C> {
    allocate_inode: A,
    now: N,
    // mount is the mount the engine is, which stamps every change it makes.
    mount: MountId,
    // store keeps the metadata and the content, and makes them durable together. Auto invalidators hold it too.
    store: Arc<St>,
    // durability_limit is the most ensure_durable makes commits durable against, or None when it makes nothing
    // durable.
    durability_limit: Option<DurableAgainst>,
    consistency: Arc<C>,
    // auto_invalidators invalidate the kernel's cache of each inode it caches once the inode changes.
    auto_invalidators: Arc<AutoInvalidators>,
    config: PhantomData<S>,
}

// Dropping the engine stops its auto invalidators.
impl<St: Store, A, N, S, C> Drop for FsEngine<St, A, N, S, C> {
    fn drop(&mut self) {
        let running = std::mem::take(
            &mut *self
                .auto_invalidators
                .running
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for auto_invalidator in running.into_values() {
            auto_invalidator.stop.abort();
        }
    }
}

// Consistency is what a client's reads promise about other clients' writes. Each promise is a type of its own.
pub trait Consistency: Send + Sync + 'static {}

// Cached is a consistency that serves reads from a cache.
pub trait Cached<St: Store>: Consistency {
    type Failure: std::error::Error + Send + Sync + 'static;

    // invalidated resolves once an inode cached as seen needs to be invalidated, at once if seen is already out
    // of date.
    fn invalidated(
        &self,
        store: &St,
        inode: Inode,
        seen: &Attributes,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;
}

// StrongCached serves reads from a cache, which forces any write to verify that all other
// StrongCached clients have been invalidated before it can return.
pub struct StrongCached;

// StrongUncached reads everything from the store, without any caching.
pub struct StrongUncached;

// Eventual serves reads from a cache that may lag behind other clients' writes.
pub struct Eventual;

impl Consistency for StrongCached {}
impl Consistency for StrongUncached {}
impl Consistency for Eventual {}

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
    mount: MountId,
    content: &'engine C,
    allocate_inode: &'engine A,
    now: &'engine N,
    config: PhantomData<S>,
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
    C: Consistency,
{
    // new makes an engine that invalidates kernel's cache of each inode it caches once the inode changes, running
    // the auto invalidators that do so on spawner. An engine whose inodes no kernel caches takes NoKernel.
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
        spawner: impl Spawn + Send + Sync + 'static,
    ) -> impl Future<
        Output = Result<
            Self,
            TransactError<
                MetadataRejection<St>,
                MetadataFailure<St>,
                StoreError<MetadataRejection<St>, MetadataFailure<St>>,
            >,
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
                mount,
                store: Arc::new(store),
                durability_limit,
                consistency: Arc::new(consistency),
                auto_invalidators: Arc::new(AutoInvalidators::new(kernel, spawner)),
                config: PhantomData,
            };
            engine
                .transact(|mut transaction| {
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
                Some(limit) => self.store.ensure_durable(against.min(limit)).await,
                None => Ok(()),
            }
        }
    }
}

impl<St: Store, A, N, S, C> FsEngine<St, A, N, S, C> {
    // content is the content store, which loads a read's ContentLoadPlan once the read's transaction ends.
    pub fn content(&self) -> &St::Content {
        self.store.content()
    }

    // consistency is what the engine's reads promise about other clients' writes.
    pub fn consistency(&self) -> &C {
        &self.consistency
    }
}

impl<St: Store, A: Sync, N: Sync, S: FsConfig, C: Send + Sync> FsEngine<St, A, N, S, C> {
    // transact runs work in a transaction. When work succeeds it hands its transaction back with its result.
    #[allow(clippy::type_complexity)]
    pub fn transact<'engine, T: Send + 'engine, E: Send + 'engine>(
        &'engine self,
        mut work: impl for<'txn> FnMut(
            FsTransaction<
                'txn,
                'engine,
                <St::Metadata as KeyValueStore>::Transaction<'txn>,
                St::Content,
                A,
                N,
                S,
            >,
        ) -> BoxFuture<
            'txn,
            Result<
                (
                    T,
                    FsTransaction<
                        'txn,
                        'engine,
                        <St::Metadata as KeyValueStore>::Transaction<'txn>,
                        St::Content,
                        A,
                        N,
                        S,
                    >,
                ),
                E,
            >,
        > + Send,
    ) -> impl Future<
        Output = Result<T, TransactError<MetadataRejection<St>, MetadataFailure<St>, E>>,
    > + Send {
        self.store.metadata().transact(move |metadata| {
            let working = work(self.transaction(metadata));
            Box::pin(async move {
                let (result, transaction) = working.await?;
                Ok((result, transaction.metadata))
            })
        })
    }

    pub fn read_transact<'engine, T: Send, E: Send>(
        &'engine self,
        mut work: impl for<'txn> FnMut(
            FsTransaction<
                'txn,
                'engine,
                <St::Metadata as KeyValueStore>::ReadTransaction<'txn>,
                St::Content,
                A,
                N,
                S,
            >,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<
        Output = Result<T, TransactError<MetadataRejection<St>, MetadataFailure<St>, E>>,
    > + Send {
        self.store
            .metadata()
            .read_transact(move |metadata| work(self.transaction(metadata)))
    }

    fn transaction<'txn, 'engine, T>(
        &'engine self,
        metadata: TransactionHandle<'txn, 'engine, T>,
    ) -> FsTransaction<'txn, 'engine, T, St::Content, A, N, S> {
        FsTransaction {
            metadata,
            mount: self.mount,
            content: self.store.content(),
            allocate_inode: &self.allocate_inode,
            now: &self.now,
            config: PhantomData,
        }
    }
}
