use std::sync::Arc;

use async_lock::RwLock;
use bytes::Bytes;
use futures_core::future::BoxFuture;
use futures_lite::future::block_on;

use stratafs_store::content::ContentStore;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::kv::{KeyValueStore, TransactError, TransactionHandle};
use stratafs_store::store::Store;

// LockSynchronizedStore is a store over a separate metadata store and content store, made durable together with a
// lock. Every metadata write transaction holds it shared, and ensure_durable holds it alone while it makes the
// content durable, then the metadata. It makes every writer wait on every ensure_durable, so it is correct but slow
// when ensure_durable is frequent.
pub struct LockSynchronizedStore<M, C: ContentStore> {
    content: C,
    metadata: LockSynchronizedMetadata<M>,
}

// LockSynchronizedMetadata is the metadata store of a LockSynchronizedStore. Its own ensure_durable makes only the
// metadata durable.
pub struct LockSynchronizedMetadata<M> {
    // metadata is shared, so dropping the store can keep it from ever closing.
    metadata: Arc<M>,
    // writing is held by every write transaction together, and by ensure_durable alone, so no transaction
    // commits between ensure_durable making the content durable and making the metadata durable.
    writing: RwLock<()>,
}

#[derive(Debug, thiserror::Error)]
pub enum LockSynchronizedFailure<M, C> {
    #[error(transparent)]
    Metadata(M),
    #[error(transparent)]
    Content(C),
}

impl<M, C: ContentStore> LockSynchronizedStore<M, C> {
    pub fn new(metadata: M, content: C) -> Self {
        Self {
            metadata: LockSynchronizedMetadata {
                metadata: Arc::new(metadata),
                writing: RwLock::new(()),
            },
            content,
        }
    }
}

impl<M: KeyValueStore, C: ContentStore + 'static> Store for LockSynchronizedStore<M, C> {
    type Metadata = LockSynchronizedMetadata<M>;
    type Content = C;
    type Failure = LockSynchronizedFailure<M::Failure, C::Failure>;

    fn metadata(&self) -> &LockSynchronizedMetadata<M> {
        &self.metadata
    }

    fn content(&self) -> &C {
        &self.content
    }

    // The content goes first, so durable metadata never names content that is not.
    async fn ensure_durable(&self, against: DurableAgainst) -> Result<(), Self::Failure> {
        let _alone = self.metadata.writing.write().await;
        self.content
            .ensure_durable(against)
            .await
            .map_err(LockSynchronizedFailure::Content)?;
        self.metadata
            .metadata
            .ensure_durable(against)
            .await
            .map_err(LockSynchronizedFailure::Metadata)
    }
}

// Dropping the store makes the content durable before the metadata store closes, since closing a metadata store may
// make its commits durable. When the content fails, the metadata store never closes, so it keeps only the commits it
// made durable before.
impl<M, C: ContentStore> Drop for LockSynchronizedStore<M, C> {
    fn drop(&mut self) {
        if let Err(error) = block_on(self.content.ensure_durable(DurableAgainst::PowerFailure)) {
            tracing::warn!(%error, "making the content durable on close failed, so the metadata store stays open");
            std::mem::forget(Arc::clone(&self.metadata.metadata));
        }
    }
}

impl<M: KeyValueStore> KeyValueStore for LockSynchronizedMetadata<M> {
    type Rejection = M::Rejection;
    type Failure = M::Failure;
    type ReadTransaction<'txn> = M::ReadTransaction<'txn>;
    type Transaction<'txn> = M::Transaction<'txn>;

    const LARGEST_ACCEPTED_ENTRY: usize = M::LARGEST_ACCEPTED_ENTRY;

    async fn transact<'store, T: Send, E: Send>(
        &'store self,
        work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<
            'txn,
            Result<(T, TransactionHandle<'txn, 'store, Self::Transaction<'txn>>), E>,
        > + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        let _together = self.writing.read().await;
        self.metadata.transact(work).await
    }

    fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<Output = Result<T, TransactError<Self::Rejection, Self::Failure, E>>> + Send
    {
        self.metadata.read_transact(work)
    }

    fn changed(
        &self,
        key: Bytes,
        seen: Option<Bytes>,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.metadata.changed(key, seen)
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.metadata.ensure_durable(against)
    }
}
