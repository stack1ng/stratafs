use std::marker::PhantomData;

use futures_core::future::BoxFuture;
use storekey::Encode;
use stratafs_store::kv::{KeyValueStore, TransactError, TransactionHandle};

pub mod attributes;
pub mod content;
pub mod directories;
pub mod error;
mod key;
pub mod observation;

pub trait FsConfig: Send + Sync + 'static {
    const LARGEST_INLINE_ENTRY: usize;
}

const SMALLEST_INLINE_ENTRY: usize = 1024;

pub struct FsEngine<M, C, A, N, S> {
    metadata: M,
    content: C,
    allocate_inode: A,
    now: N,
    config: PhantomData<S>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Encode)]
pub struct Inode(pub u64);

impl Inode {
    pub const ROOT: Inode = Inode(0);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub i64);

pub struct FsTransaction<'txn, 'engine, T, C, A, N> {
    metadata: TransactionHandle<'txn, 'engine, T>,
    content: &'engine C,
    allocate_inode: &'engine A,
    now: &'engine N,
}

impl<T, C, A, N: Fn() -> Timestamp> FsTransaction<'_, '_, T, C, A, N> {
    pub fn now(&self) -> Timestamp {
        (self.now)()
    }
}

impl<M: KeyValueStore, C, A, N, S: FsConfig> FsEngine<M, C, A, N, S> {
    pub fn new(metadata: M, content: C, allocate_inode: A, now: N) -> Self {
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
        Self {
            metadata,
            content,
            allocate_inode,
            now,
            config: PhantomData,
        }
    }
}

impl<M: KeyValueStore, C: Sync, A: Sync, N: Sync, S: FsConfig> FsEngine<M, C, A, N, S> {
    pub fn transact<'engine, T: Send, E: Send>(
        &'engine self,
        mut work: impl for<'txn> FnMut(
            FsTransaction<'txn, 'engine, M::Transaction<'txn>, C, A, N>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<Output = Result<T, TransactError<M::Rejection, M::Failure, E>>> + Send {
        self.metadata
            .transact(move |metadata| work(self.transaction(metadata)))
    }

    pub fn read_transact<'engine, T: Send, E: Send>(
        &'engine self,
        mut work: impl for<'txn> FnMut(
            FsTransaction<'txn, 'engine, M::ReadTransaction<'txn>, C, A, N>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<Output = Result<T, TransactError<M::Rejection, M::Failure, E>>> + Send {
        self.metadata
            .read_transact(move |metadata| work(self.transaction(metadata)))
    }

    fn transaction<'txn, 'engine, T>(
        &'engine self,
        metadata: TransactionHandle<'txn, 'engine, T>,
    ) -> FsTransaction<'txn, 'engine, T, C, A, N> {
        FsTransaction {
            metadata,
            content: &self.content,
            allocate_inode: &self.allocate_inode,
            now: &self.now,
        }
    }
}
