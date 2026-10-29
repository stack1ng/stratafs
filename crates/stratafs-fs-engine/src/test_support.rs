use std::cell::Cell;
use std::convert::Infallible;
use std::fmt::Debug;

use futures_lite::future::block_on;
use object_store::memory::InMemory;
use redb::backends::InMemoryBackend;
use stratafs_lock_synchronized::LockSynchronizedStore;
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_redb::{RedbFailure, RedbStore};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::kv::TransactError;
use stratafs_store::spawn::Inline;
use stratafs_tree::error::TreeFailure;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;

use crate::attributes::Restrictions;
use crate::{Consistency, FsConfig, FsEngine, Inode, Timestamp};

pub(crate) struct OneKibInline;

impl FsConfig for OneKibInline {
    const LARGEST_INLINE_ENTRY: usize = 1024;
}

// TestTreeConfig has the smallest nodes that still hold a OneKibInline entry.
pub(crate) struct TestTreeConfig;

impl TreeConfig for TestTreeConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = 4096;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

pub(crate) type InMemoryStore = LockSynchronizedStore<
    TreeVolume<RedbStore<Inline>, fn() -> u128, TestTreeConfig>,
    ObjectStoreContentStore,
>;

pub(crate) type MetadataFailure = TreeFailure<RedbFailure, Infallible>;

pub(crate) type InMemoryEngine =
    FsEngine<InMemoryStore, fn() -> u64, fn() -> Timestamp, OneKibInline>;

thread_local! {
    static CLOCK: Cell<i64> = const { Cell::new(0) };
    static NEXT_INODE: Cell<u64> = const { Cell::new(1) };
    static NEXT_NODE: Cell<u128> = const { Cell::new(1) };
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

pub(crate) fn new_test_engine() -> InMemoryEngine {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    NEXT_INODE.set(1);
    NEXT_NODE.set(1);
    let allocate_inode: fn() -> u64 = next_inode;
    let allocate_node: fn() -> u128 = next_node;
    let now: fn() -> Timestamp = clock;
    block_on(async {
        let tree = Tree::new(RedbStore::new(database, Inline).unwrap(), allocate_node)
            .await
            .unwrap();
        let volume = tree.open(tree.create().await.unwrap()).await.unwrap();
        FsEngine::new(
            LockSynchronizedStore::new(volume, ObjectStoreContentStore::new(InMemory::new())),
            allocate_inode,
            now,
            Some(DurableAgainst::PowerFailure),
            Consistency::Eventual,
        )
        .await
        .unwrap()
    })
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
    block_on(engine.transact(|mut txn| {
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
