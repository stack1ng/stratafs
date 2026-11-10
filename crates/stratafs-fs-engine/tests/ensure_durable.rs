use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use futures_executor::ThreadPool;
use futures_lite::future::block_on;
use object_store::memory::InMemory;
use redb::Database;
use redb::backends::InMemoryBackend;
use stratafs_fs_engine::kernel_cache::NoKernel;
use stratafs_fs_engine::spillable::ContentHash;
use stratafs_fs_engine::{Eventual, FsConfig, FsEngine, MountId, Timestamp};
use stratafs_lock_synchronized::{LockSynchronizedMetadata, LockSynchronizedStore};
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_redb::RedbStore;
use stratafs_runtime::ThreadPoolRuntime;
use stratafs_store::content::ContentId;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::spawn::Inline;
use stratafs_store::store::Store;

type Log = Arc<Mutex<Vec<DurableAgainst>>>;

// LoggedStore keeps metadata and content in memory, and logs what each ensure_durable is against.
struct LoggedStore {
    store: LockSynchronizedStore<RedbStore<Inline>, ObjectStoreContentStore>,
    log: Log,
}

impl Store for LoggedStore {
    type Metadata = LockSynchronizedMetadata<RedbStore<Inline>>;
    type Content = ObjectStoreContentStore;
    type Failure = Infallible;

    fn metadata(&self) -> &Self::Metadata {
        self.store.metadata()
    }

    fn content(&self) -> &Self::Content {
        self.store.content()
    }

    async fn ensure_durable(&self, against: DurableAgainst) -> Result<(), Self::Failure> {
        self.log.lock().unwrap().push(against);
        Ok(())
    }
}

struct TestConfig;

impl FsConfig for TestConfig {
    const LARGEST_INLINE_ENTRY: usize = 1024;

    fn content_hash(content: &[u8]) -> ContentHash {
        ContentHash::xxh3(content)
    }
}

// durable_against is what the store makes commits durable against when an engine with durability_limit makes them
// durable against a software failure, then a power failure.
fn durable_against(durability_limit: Option<DurableAgainst>) -> Vec<DurableAgainst> {
    let log = Log::default();
    let database = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let store = LoggedStore {
        store: LockSynchronizedStore::new(
            RedbStore::new(database, Inline).unwrap(),
            ObjectStoreContentStore::new(InMemory::new()),
        ),
        log: Arc::clone(&log),
    };
    let allocate_inode: fn() -> u64 = || 7;
    let allocate_content_id: fn() -> ContentId = || ContentId(7);
    let now: fn() -> Timestamp = || Timestamp(1_500_000_000);
    let engine: FsEngine<_, _, _, TestConfig, _> = block_on(FsEngine::new(
        store,
        allocate_inode,
        allocate_content_id,
        now,
        MountId(1),
        durability_limit,
        Eventual::default(),
        NoKernel,
        Arc::new(ThreadPoolRuntime(ThreadPool::new().unwrap())),
    ))
    .unwrap();
    for against in [
        DurableAgainst::SoftwareFailure,
        DurableAgainst::PowerFailure,
    ] {
        block_on(engine.ensure_durable(against)).unwrap();
    }
    log.lock().unwrap().clone()
}

#[test]
fn ensure_durable_without_a_durability_limit_makes_nothing_durable() {
    assert_eq!(durable_against(None), []);
}

#[test]
fn ensure_durable_makes_commits_durable_against_at_most_the_durability_limit() {
    assert_eq!(
        durable_against(Some(DurableAgainst::SoftwareFailure)),
        [
            DurableAgainst::SoftwareFailure,
            DurableAgainst::SoftwareFailure
        ]
    );
    assert_eq!(
        durable_against(Some(DurableAgainst::PowerFailure)),
        [
            DurableAgainst::SoftwareFailure,
            DurableAgainst::PowerFailure
        ]
    );
}
