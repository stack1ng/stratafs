// The store every mount of a volume shares, so every engine sees the same metadata and change notifications. Each
// mount reaches it through its own handle, which yields to other tasks around each transaction and fails those its
// faults say to.

use std::cell::Cell;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use futures_lite::future::yield_now;
use futures_util::future::BoxFuture;
use object_store::memory::InMemory;
use redb::backends::InMemoryBackend;
use stratafs_fs_engine::spillable::referenced_content;
use stratafs_lock_synchronized::LockSynchronizedStore;
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_redb::{RedbFailure, RedbStore};
use stratafs_runtime::Runtime;
use stratafs_store::content::{ContentId, ContentReference};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::kv::{KeyValueStore, TransactError, TransactionHandle};
use stratafs_store::spawn::Inline;
use stratafs_store::store::Store;
use stratafs_tree::error::TreeFailure;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;

use crate::faults::Faults;

// TestTreeConfig has the smallest nodes that still hold a OneKibInline entry.
pub(crate) struct TestTreeConfig;

impl TreeConfig for TestTreeConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = 4096;
    const MERGE_BELOW_PERCENT: u8 = 25;
    // a run lasts seconds of virtual time, so its garbage collector runs among its operations
    const GARBAGE_COLLECTION_INTERVAL: Duration = Duration::from_secs(1);

    fn referenced_content(key: &[u8], value: &[u8]) -> Option<ContentReference> {
        referenced_content(key, value)
    }
}

pub(crate) type InMemoryStore = TreeVolume<
    LockSynchronizedStore<RedbStore<Inline>, ObjectStoreContentStore>,
    fn() -> u128,
    TestTreeConfig,
>;

pub(crate) type SharedStore = Shared<InMemoryStore>;

type MetadataRejection<S> = <<S as Store>::Metadata as KeyValueStore>::Rejection;
type MetadataFailure<S> = <<S as Store>::Metadata as KeyValueStore>::Failure;

// Shared is one mount's handle on the volume's store. It is generic over the store, so the futures it hands on are
// proven Send for every store at once, for the reason FsEngine::new gives.
pub(crate) struct Shared<S: Store> {
    metadata: SharedMetadata<S>,
}

pub(crate) struct SharedMetadata<S: Store> {
    store: Arc<S>,
    faults: Arc<Faults>,
    fault: fn() -> MetadataFailure<S>,
}

impl SharedStore {
    pub(crate) fn new(store: &Arc<InMemoryStore>, faults: &Arc<Faults>) -> Self {
        Shared {
            metadata: SharedMetadata {
                store: Arc::clone(store),
                faults: Arc::clone(faults),
                fault: injected_failure,
            },
        }
    }
}

fn injected_failure() -> MetadataFailure<InMemoryStore> {
    TreeFailure::Store(RedbFailure::Storage(redb::StorageError::Io(
        io::Error::other("the transaction failed on purpose"),
    )))
}

// new_store is a fresh, empty store for one volume, whose garbage collector runs on runtime.
pub(crate) async fn new_store(runtime: Arc<dyn Runtime>) -> Arc<InMemoryStore> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .expect("redb opens a database in memory");
    NEXT_NODE.set(1);
    NEXT_CONTENT_ID.set(1);
    let allocate_node: fn() -> u128 = next_node;
    let tree = Tree::new(
        LockSynchronizedStore::new(
            RedbStore::new(database, Inline).expect("redb starts a store in memory"),
            ObjectStoreContentStore::new(InMemory::new()),
        ),
        allocate_node,
        runtime,
    )
    .await
    .expect("a tree starts in an empty store");
    let volume = tree
        .open(tree.create().await.expect("a tree creates a volume"))
        .await
        .expect("a tree opens the volume it created");
    Arc::new(volume)
}

thread_local! {
    // NEXT_NODE is the next node id of the store a run made, from 1 in every replay, since its garbage collector times
    // its passes by them. A run's tasks all run on its thread.
    static NEXT_NODE: Cell<u128> = const { Cell::new(1) };
    // NEXT_CONTENT_ID is the next content id of the store a run made, from 1 in every replay, as NEXT_NODE is.
    static NEXT_CONTENT_ID: Cell<u128> = const { Cell::new(1) };
}

// next_node allocates a node id no other node of the run's tree has.
fn next_node() -> u128 {
    NEXT_NODE.replace(NEXT_NODE.get() + 1)
}

// next_content_id allocates a content id no other content of the run's store has, whichever mount spills it.
pub(crate) fn next_content_id() -> ContentId {
    ContentId(NEXT_CONTENT_ID.replace(NEXT_CONTENT_ID.get() + 1))
}

impl<S: Store> Store for Shared<S> {
    type Metadata = SharedMetadata<S>;
    type Content = S::Content;
    type Failure = S::Failure;

    fn metadata(&self) -> &SharedMetadata<S> {
        &self.metadata
    }

    fn content(&self) -> &S::Content {
        self.metadata.store.content()
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.metadata.store.ensure_durable(against)
    }
}

impl<S: Store> SharedMetadata<S> {
    // faulty runs the transaction transacting makes between yields to other tasks, unless the mount's faults fail it.
    async fn faulty<T, E, F>(
        &self,
        transacting: impl FnOnce() -> F,
    ) -> Result<T, TransactError<MetadataRejection<S>, MetadataFailure<S>, E>>
    where
        F: Future<Output = Result<T, TransactError<MetadataRejection<S>, MetadataFailure<S>, E>>>,
    {
        yield_now().await;
        let transaction = self
            .faults
            .begin()
            .ok_or_else(|| TransactError::Failed((self.fault)()))?;
        let result = transacting().await;
        self.faults.end(transaction);
        yield_now().await;
        result
    }
}

impl<S: Store> KeyValueStore for SharedMetadata<S> {
    type Rejection = <S::Metadata as KeyValueStore>::Rejection;
    type Failure = <S::Metadata as KeyValueStore>::Failure;
    type ReadTransaction<'txn> = <S::Metadata as KeyValueStore>::ReadTransaction<'txn>;
    type Transaction<'txn> = <S::Metadata as KeyValueStore>::Transaction<'txn>;

    const LARGEST_ACCEPTED_ENTRY: usize = <S::Metadata as KeyValueStore>::LARGEST_ACCEPTED_ENTRY;

    fn transact<'store, T: Send, E: Send>(
        &'store self,
        work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<
            'txn,
            Result<(T, TransactionHandle<'txn, 'store, Self::Transaction<'txn>>), E>,
        > + Send,
    ) -> impl Future<Output = Result<T, TransactError<Self::Rejection, Self::Failure, E>>> + Send
    {
        self.faulty(move || self.store.metadata().transact(work))
    }

    fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> impl Future<Output = Result<T, TransactError<Self::Rejection, Self::Failure, E>>> + Send
    {
        self.faulty(move || self.store.metadata().read_transact(work))
    }

    fn changed(
        &self,
        key: bytes::Bytes,
        seen: Option<bytes::Bytes>,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.store.metadata().changed(key, seen)
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.store.metadata().ensure_durable(against)
    }
}
