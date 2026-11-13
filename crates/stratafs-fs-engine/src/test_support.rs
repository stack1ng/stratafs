use noncrypto_digests::Xxh3_128;
use std::cell::Cell;
use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use futures_executor::ThreadPool;
use futures_lite::future::block_on;
use futures_util::FutureExt;
use object_store::memory::InMemory;
use redb::backends::InMemoryBackend;
use stratafs_lock_synchronized::LockSynchronizedStore;
use stratafs_object_store::{ObjectStoreContentStore, ObjectStoreError};
use stratafs_pubsub::PubSub;
use stratafs_pubsub_conformance::{MemoryClient, MemoryVolume};
use stratafs_redb::{RedbFailure, RedbStore};
use stratafs_runtime::{TaskDroppingRuntime, ThreadPoolRuntime};
use stratafs_store::content::{ContentId, ContentReference, ContentStore, GetRejection};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::kv::TransactError;
use stratafs_store::spawn::Inline;
use stratafs_tree::error::TreeFailure;
use stratafs_tree::header::TreeVolumeId;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;

use crate::attributes::Restrictions;
use crate::holds::ConnectFailure;
use crate::kernel_cache::{KernelCache, NoKernel};
use crate::spillable::{ContentHash, referenced_content};
use crate::{Cached, Eventual, FsConfig, FsEngine, Inode, MountId, PurgeWaits, Timestamp};

// TEST_MOUNT is the mount test engines are.
pub(crate) const TEST_MOUNT: MountId = MountId(1);

// PUBLISH_TIMEOUT is how long a test engine's publish waits for every other holder to handle it.
pub(crate) const PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) struct OneKibInline;

impl FsConfig for OneKibInline {
    const LARGEST_INLINE_ENTRY: usize = 1024;
    const CHECKED_CONTENT_BYTES: u64 = 1024 * 1024;

    type HashFunction = Xxh3_128;
}

impl ContentHash {
    // xxh3 is content's hash by OneKibInline's hash function.
    pub(crate) fn xxh3(content: &[u8]) -> Self {
        Self::of::<Xxh3_128>(content)
    }
}

// TestTreeConfig has the smallest nodes that still hold a OneKibInline entry.
pub(crate) struct TestTreeConfig;

impl TreeConfig for TestTreeConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = 4096;
    const MERGE_BELOW_PERCENT: u8 = 25;

    fn referenced_content(key: &[u8], value: &[u8]) -> Option<ContentReference> {
        referenced_content(key, value)
    }
}

pub(crate) type InMemoryStore = TreeVolume<
    LockSynchronizedStore<RedbStore<Inline>, ListedContent>,
    fn() -> u128,
    TestTreeConfig,
>;

// ListedContent keeps content in memory, and lists the id of every blob it keeps and every get made of it.
#[derive(Clone)]
pub(crate) struct ListedContent {
    store: ObjectStoreContentStore,
    ids: Arc<Mutex<BTreeSet<ContentId>>>,
    gets: Arc<Mutex<Vec<(ContentId, u64, u64)>>>,
}

impl ListedContent {
    fn new() -> Self {
        Self {
            store: ObjectStoreContentStore::new(InMemory::new()),
            ids: Arc::default(),
            gets: Arc::default(),
        }
    }

    // gets is the id, offset and size of every get made so far, in order.
    pub(crate) fn gets(&self) -> Vec<(ContentId, u64, u64)> {
        self.gets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    // ids is the id of every blob the store keeps.
    pub(crate) fn ids(&self) -> BTreeSet<ContentId> {
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ContentStore for ListedContent {
    type Failure = ObjectStoreError;
    type Content = Bytes;

    async fn put(&self, id: &ContentId, content: impl Buf + Send) -> Result<(), Self::Failure> {
        self.store.put(id, content).await?;
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(*id);
        Ok(())
    }

    fn get(
        &self,
        id: &ContentId,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<Bytes, StoreError<GetRejection, Self::Failure>>> + Send {
        self.gets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((*id, offset, size));
        self.store.get(id, offset, size)
    }

    async fn delete(&self, id: &ContentId) -> Result<(), Self::Failure> {
        self.store.delete(id).await?;
        self.ids
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id);
        Ok(())
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.store.ensure_durable(against)
    }
}

pub(crate) type MetadataFailure = TreeFailure<RedbFailure, Infallible>;

pub(crate) type InMemoryEngine = TestEngine<Eventual>;

pub(crate) type TestEngine<C, P = MemoryClient<u64>> =
    FsEngine<InMemoryStore, P, fn() -> u64, fn() -> Timestamp, OneKibInline, C>;

thread_local! {
    static CLOCK: Cell<i64> = const { Cell::new(0) };
    static NEXT_INODE: Cell<u64> = const { Cell::new(1) };
    static NEXT_NODE: Cell<u128> = const { Cell::new(1) };
    static NEXT_CONTENT_ID: Cell<u128> = const { Cell::new(1) };
}

pub(crate) fn set_clock(at: i64) {
    CLOCK.set(at);
}

fn clock() -> Timestamp {
    Timestamp(CLOCK.get())
}

fn next_inode() -> u64 {
    NEXT_INODE.replace(NEXT_INODE.get() + 1)
}

fn next_node() -> u128 {
    NEXT_NODE.replace(NEXT_NODE.get() + 1)
}

fn next_content_id() -> ContentId {
    ContentId(NEXT_CONTENT_ID.replace(NEXT_CONTENT_ID.get() + 1))
}

pub(crate) fn new_test_engine() -> InMemoryEngine {
    new_test_engine_for(NoKernel)
}

// new_test_engine_for is a test engine whose inodes kernel caches.
pub(crate) fn new_test_engine_for(kernel: impl KernelCache) -> InMemoryEngine {
    new_test_engine_with(Eventual::default(), kernel)
}

// new_test_engine_with is a test engine with consistency, whose inodes kernel caches.
pub(crate) fn new_test_engine_with<C: Cached<InMemoryStore, MemoryClient<u64>>>(
    consistency: C,
    kernel: impl KernelCache,
) -> TestEngine<C> {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    volumes.engine_with(volume, consistency, kernel)
}

// new_test_engine_reaching is a test engine with consistency, whose inodes kernel caches, of a volume whose pub/sub
// connect reaches and whose publishes wait timeout for holders.
pub(crate) fn new_test_engine_reaching<P: PubSub<Message = u64>, C: Cached<InMemoryStore, P>>(
    connect: impl Fn() -> BoxFuture<'static, Result<P, ConnectFailure>> + Send + Sync + 'static,
    timeout: Duration,
    consistency: C,
    kernel: impl KernelCache,
) -> TestEngine<C, P> {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    volumes.engine_reaching(volume, TEST_MOUNT, connect, timeout, consistency, kernel)
}

pub(crate) type TestTree =
    Tree<LockSynchronizedStore<RedbStore<Inline>, ListedContent>, fn() -> u128, TestTreeConfig>;

// TestVolumes is a tree of volumes in memory, the content their engines share, and the pub/sub they reach. Only a test
// runs its garbage collection.
pub(crate) struct TestVolumes {
    tree: TestTree,
    pub(crate) content: ListedContent,
    pubsub: Arc<MemoryVolume<u64>>,
}

impl TestVolumes {
    pub(crate) fn new() -> Self {
        let database = redb::Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        NEXT_INODE.set(1);
        NEXT_NODE.set(1);
        NEXT_CONTENT_ID.set(1);
        let allocate_node: fn() -> u128 = next_node;
        let content = ListedContent::new();
        // node ids count per thread, so the tree runs no garbage collector on another thread
        let tree = block_on(Tree::new(
            LockSynchronizedStore::new(RedbStore::new(database, Inline).unwrap(), content.clone()),
            allocate_node,
            Arc::new(TaskDroppingRuntime),
        ))
        .unwrap();
        let pubsub = Arc::new(MemoryVolume::new(Arc::new(ThreadPoolRuntime(
            ThreadPool::new().unwrap(),
        ))));
        Self {
            tree,
            content,
            pubsub,
        }
    }

    pub(crate) fn create(&self) -> TreeVolumeId {
        block_on(self.tree.create()).unwrap()
    }

    pub(crate) fn collect_garbage(&self, volume: TreeVolumeId) -> Result<(), MetadataFailure> {
        block_on(async {
            self.tree
                .open(volume)
                .await
                .unwrap()
                .collect_garbage()
                .await
        })
    }

    // collect_garbage_and_delete runs two passes, the second deleting the blobs the first left unreferenced.
    pub(crate) fn collect_garbage_and_delete(
        &self,
        volume: TreeVolumeId,
    ) -> Result<(), MetadataFailure> {
        self.collect_garbage(volume)?;
        self.collect_garbage(volume)
    }

    pub(crate) fn engine(&self, volume: TreeVolumeId) -> InMemoryEngine {
        self.engine_with(volume, Eventual::default(), NoKernel)
    }

    // engine_with is an engine of volume with consistency, whose inodes kernel caches, reaching the volumes' pub/sub.
    pub(crate) fn engine_with<C: Cached<InMemoryStore, MemoryClient<u64>>>(
        &self,
        volume: TreeVolumeId,
        consistency: C,
        kernel: impl KernelCache,
    ) -> TestEngine<C> {
        self.engine_reaching(
            volume,
            TEST_MOUNT,
            self.connect(),
            PUBLISH_TIMEOUT,
            consistency,
            kernel,
        )
    }

    // connect connects a client of the volumes' pub/sub.
    pub(crate) fn connect(
        &self,
    ) -> impl Fn() -> BoxFuture<'static, Result<MemoryClient<u64>, ConnectFailure>> + Send + Sync + 'static
    {
        let pubsub = Arc::clone(&self.pubsub);
        move || {
            let client = pubsub.connect();
            async move { Ok(client) }.boxed()
        }
    }

    // engine_reaching is the engine of volume that is mount, with consistency, whose inodes kernel caches, of a volume
    // whose pub/sub connect reaches and whose publishes wait timeout for holders.
    pub(crate) fn engine_reaching<P: PubSub<Message = u64>, C: Cached<InMemoryStore, P>>(
        &self,
        volume: TreeVolumeId,
        mount: MountId,
        connect: impl Fn() -> BoxFuture<'static, Result<P, ConnectFailure>> + Send + Sync + 'static,
        timeout: Duration,
        consistency: C,
        kernel: impl KernelCache,
    ) -> TestEngine<C, P> {
        let allocate_inode: fn() -> u64 = next_inode;
        let now: fn() -> Timestamp = clock;
        block_on(async {
            let volume = self.tree.open(volume).await.unwrap();
            FsEngine::new(
                volume,
                allocate_inode,
                next_content_id,
                now,
                mount,
                Some(DurableAgainst::PowerFailure),
                connect,
                timeout,
                consistency,
                kernel,
                Arc::new(ThreadPoolRuntime(ThreadPool::new().unwrap())),
            )
            .await
            .unwrap()
        })
    }
}

pub(crate) fn aborted<R: Debug, F: Debug, E>(error: TransactError<R, F, E>) -> E {
    match error {
        TransactError::Aborted(error) => error,
        TransactError::Rejected(rejection) => {
            panic!("the store refused the transaction: {rejection:?}")
        }
        TransactError::Failed(failure) => panic!("the transaction failed: {failure:?}"),
    }
}

// restrict sets inode's restrictions, which a setattr may change whatever they were before.
pub(crate) fn restrict(engine: &InMemoryEngine, inode: Inode, restrictions: Restrictions) {
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn
                .setattr(inode, |settable| settable.restrictions = restrictions)
                .await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
    .unwrap();
}
