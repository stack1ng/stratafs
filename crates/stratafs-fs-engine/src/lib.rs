use std::marker::PhantomData;

use async_lock::RwLock;
use futures_core::future::BoxFuture;
use storekey::{BorrowDecode, Encode};
use stratafs_store::content::ContentStore;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, TransactError, TransactionHandle};

use crate::error::IoFailure;

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

pub struct FsEngine<M, C, A, N, S> {
    metadata: M,
    content: C,
    allocate_inode: A,
    now: N,
    // writing is held by every write transaction together, and by ensure_durable alone, so no transaction
    // commits between ensure_durable making the content durable and making the metadata durable.
    writing: RwLock<()>,
    config: PhantomData<S>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode, BorrowDecode)]
pub struct Inode(pub u64);

impl Inode {
    pub const ROOT: Inode = Inode(0);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

pub struct FsTransaction<'txn, 'engine, T, C, A, N, S> {
    metadata: TransactionHandle<'txn, 'engine, T>,
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

impl<M, C, A, N, S> FsEngine<M, C, A, N, S>
where
    M: KeyValueStore,
    C: ContentStore,
    A: Send + Sync,
    N: Fn() -> Timestamp + Send + Sync,
    S: FsConfig,
{
    // new returns `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
    // As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
    // such as redb's (https://github.com/rust-lang/rust/issues/110338). The long return type comes from that form.
    #[allow(clippy::type_complexity)]
    pub fn new(
        metadata: M,
        content: C,
        allocate_inode: A,
        now: N,
    ) -> impl Future<
        Output = Result<
            Self,
            TransactError<M::Rejection, M::Failure, StoreError<M::Rejection, M::Failure>>,
        >,
    > + Send {
        const {
            assert!(
                S::LARGEST_INLINE_ENTRY <= M::LARGEST_ACCEPTED_ENTRY,
                "the largest inline entry exceeds the largest entry the metadata store accepts"
            );
            assert!(
                S::LARGEST_INLINE_ENTRY >= SMALLEST_INLINE_ENTRY,
                "the largest inline entry is too small to hold the engine's fixed size entries"
            );
        };
        async move {
            let engine = Self {
                metadata,
                content,
                allocate_inode,
                now,
                writing: RwLock::new(()),
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

impl<M: KeyValueStore, C: ContentStore, A: Sync, N: Sync, S: Sync> FsEngine<M, C, A, N, S> {
    // ensure_durable makes every transaction committed before it is called durable against a failure, and
    // resolves once they are. The content goes first, so durable metadata never names content that is not.
    // It returns `impl Future + Send` instead of being `async fn` for the reason spillable.rs gives.
    #[allow(clippy::manual_async_fn)]
    pub fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), IoFailure<M::Failure, C::Failure>>> + Send {
        async move {
            let _alone = self.writing.write().await;
            self.content
                .ensure_durable(against)
                .await
                .map_err(IoFailure::ContentWrite)?;
            self.metadata
                .ensure_durable(against)
                .await
                .map_err(IoFailure::Metadata)
        }
    }
}

impl<M, C, A, N, S> FsEngine<M, C, A, N, S> {
    // content is the content store, which loads a read's ContentLoadPlan once the read's transaction ends.
    pub fn content(&self) -> &C {
        &self.content
    }
}

impl<M: KeyValueStore, C: Sync, A: Sync, N: Sync, S: FsConfig> FsEngine<M, C, A, N, S> {
    // transact runs work in a transaction. When work succeeds it hands its transaction back with its result.
    // It returns `impl Future + Send` instead of being `async fn` for the reason spillable.rs gives.
    #[allow(clippy::type_complexity, clippy::manual_async_fn)]
    pub fn transact<'engine, T: Send + 'engine, E: Send + 'engine>(
        &'engine self,
        mut work: impl for<'txn> FnMut(
            FsTransaction<'txn, 'engine, M::Transaction<'txn>, C, A, N, S>,
        ) -> BoxFuture<
            'txn,
            Result<
                (
                    T,
                    FsTransaction<'txn, 'engine, M::Transaction<'txn>, C, A, N, S>,
                ),
                E,
            >,
        > + Send,
    ) -> impl Future<Output = Result<T, TransactError<M::Rejection, M::Failure, E>>> + Send {
        async move {
            let _together = self.writing.read().await;
            self.metadata
                .transact(move |metadata| {
                    let working = work(self.transaction(metadata));
                    Box::pin(async move {
                        let (result, transaction) = working.await?;
                        Ok((result, transaction.metadata))
                    })
                })
                .await
        }
    }

    pub fn read_transact<'engine, T: Send, E: Send>(
        &'engine self,
        mut work: impl for<'txn> FnMut(
            FsTransaction<'txn, 'engine, M::ReadTransaction<'txn>, C, A, N, S>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<Output = Result<T, TransactError<M::Rejection, M::Failure, E>>> + Send {
        self.metadata
            .read_transact(move |metadata| work(self.transaction(metadata)))
    }

    fn transaction<'txn, 'engine, T>(
        &'engine self,
        metadata: TransactionHandle<'txn, 'engine, T>,
    ) -> FsTransaction<'txn, 'engine, T, C, A, N, S> {
        FsTransaction {
            metadata,
            content: &self.content,
            allocate_inode: &self.allocate_inode,
            now: &self.now,
            config: PhantomData,
        }
    }
}
