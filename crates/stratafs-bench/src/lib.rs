// stratafs-bench is a stratafs build for benchmarking: one store directory on this machine, holding every
// volume, with the tuning constants this build was made with.

pub mod suite;

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_executor::ThreadPool as SpawnerPool;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use stratafs_fs_engine::error::{EngineFailure, PosixError};
use stratafs_fs_engine::holds::{ConnectFailure, Failure};
use stratafs_fs_engine::kernel_cache::{KernelCache, NoKernel};
use stratafs_fs_engine::spillable::{ContentHash, referenced_content};
use stratafs_fs_engine::{Cached, Eventual, FsConfig, FsEngine, MountId, Timestamp};
use stratafs_runtime::{Runtime, ThreadPoolRuntime};
use stratafs_store::content::{ContentId, ContentReference};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::kv::{KeyValueStore, TransactError};
use stratafs_store::spawn::ThreadPool;
use stratafs_tree::header::TreeVolumeId;
use stratafs_tree::tree::PinnedConfigError;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;
use stratafs_websocket_pubsub::client::Client;

// The tuning constants come from the build's environment, so the benchmark builds once per configuration.
// A store remembers the tree constants it was made with, and refuses to open with others.
pub struct BenchTreeConfig;

impl TreeConfig for BenchTreeConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = number(option_env!("STRATAFS_BENCH_NODE_CAPACITY"), 16 * 1024);
    const MERGE_BELOW_PERCENT: u8 =
        number(option_env!("STRATAFS_BENCH_MERGE_BELOW_PERCENT"), 25) as u8;

    fn referenced_content(key: &[u8], value: &[u8]) -> Option<ContentReference> {
        referenced_content(key, value)
    }
}

pub struct BenchFsConfig;

impl FsConfig for BenchFsConfig {
    const LARGEST_INLINE_ENTRY: usize =
        number(option_env!("STRATAFS_BENCH_LARGEST_INLINE_ENTRY"), 4 * 1024);

    fn content_hash(content: &[u8]) -> ContentHash {
        ContentHash::xxh3(content)
    }
}

// The durability limit comes from the build's environment too, as STRATAFS_BENCH_DURABILITY: none, software or power.
const DURABILITY_LIMIT: Option<DurableAgainst> =
    durability_limit(option_env!("STRATAFS_BENCH_DURABILITY"));

// The metadata store comes from the build's environment too, as STRATAFS_BENCH_STORE: redb, with the content in files
// beside it, or sqlite or mdbx, with the content in the same database.
#[cfg(store = "redb")]
type Metadata = stratafs_redb::RedbStore<ThreadPool>;
#[cfg(store = "sqlite")]
type Metadata = stratafs_sqlite::SqliteMetadata<ThreadPool>;
#[cfg(store = "mdbx")]
type Metadata = stratafs_mdbx::MdbxMetadata<ThreadPool>;
#[cfg(store = "redb")]
type Content = stratafs_object_store::ObjectStoreContentStore;
#[cfg(store = "sqlite")]
type Content = stratafs_sqlite::SqliteContent<ThreadPool>;
#[cfg(store = "mdbx")]
type Content = stratafs_mdbx::MdbxContent<ThreadPool>;
#[cfg(store = "redb")]
type DatabaseError = redb::Error;
#[cfg(store = "sqlite")]
type DatabaseError = stratafs_sqlite::SqliteOpenError;
#[cfg(store = "mdbx")]
type DatabaseError = stratafs_mdbx::MdbxOpenError;

type MetadataRejection = <Metadata as KeyValueStore>::Rejection;
type MetadataFailure = <Metadata as KeyValueStore>::Failure;

// MDBX_SAFE is whether libmdbx keeps a durable point to roll back to after a crash, from the build's environment too,
// as STRATAFS_BENCH_MDBX_MODE: safe, which makes one after every MDBX_SYNC_BYTES written, or utterly, which makes none of
// its own and may wipe the one ensure_durable made.
#[cfg(store = "mdbx")]
const MDBX_SAFE: bool = mdbx_safe(option_env!("STRATAFS_BENCH_MDBX_MODE"));
#[cfg(store = "mdbx")]
const MDBX_SYNC_BYTES: usize = number(option_env!("STRATAFS_BENCH_MDBX_SYNC_BYTES"), 256 << 20);

// MDBX_CONFIGURATION opens the libmdbx store in 4 KiB pages, committing without a sync of its own.
#[cfg(store = "mdbx")]
const MDBX_CONFIGURATION: stratafs_mdbx::MdbxConfiguration<'static> =
    stratafs_mdbx::MdbxConfiguration {
        options: &[
            // libmdbx sizes how many changed pages a write transaction keeps in memory from the machine's memory,
            // which it fails to read on macOS 27.
            (mdbx_sys::MDBX_opt_txn_dp_limit, (1 << 30) / 4096),
            // The most read transactions libmdbx allows across every process.
            (mdbx_sys::MDBX_opt_max_readers, 32767),
        ],
        geometry: stratafs_mdbx::MdbxGeometry {
            size_lower: -1,
            size_now: -1,
            size_upper: 1 << 40,
            growth_step: 64 << 20,
            shrink_threshold: 0,
            page_size: 4096,
        },
        flags: mdbx_sys::MDBX_NOSUBDIR
            | if MDBX_SAFE {
                mdbx_sys::MDBX_SAFE_NOSYNC
            } else {
                mdbx_sys::MDBX_UTTERLY_NOSYNC
            },
        mode: 0o644,
        // Without a byte count, safe mode syncs a durable point each time the file fills up, where it could grow.
        shared_options: if MDBX_SAFE {
            &[(mdbx_sys::MDBX_opt_sync_bytes, MDBX_SYNC_BYTES as u64)]
        } else {
            &[]
        },
    };

// Database is the store directory's metadata and the content of every file, made durable together.
#[cfg(store = "redb")]
pub type Database = stratafs_lock_synchronized::LockSynchronizedStore<Metadata, Content>;
#[cfg(store = "sqlite")]
pub type Database = stratafs_sqlite::SqliteStore<Metadata, ThreadPool>;
#[cfg(store = "mdbx")]
pub type Database = stratafs_mdbx::MdbxStore<Metadata, ThreadPool>;

pub type BenchTree = Tree<Database, fn() -> u128, BenchTreeConfig>;

pub type BenchVolume = TreeVolume<Database, fn() -> u128, BenchTreeConfig>;
type VolumeRejection = <BenchVolume as KeyValueStore>::Rejection;
type VolumeFailure = <BenchVolume as KeyValueStore>::Failure;

// BenchEngine is a mount's engine with consistency C. Every mount reaches the coordinator whose URL is in the store
// directory's coordinator file.
pub type BenchEngine<C> =
    FsEngine<BenchVolume, Client<u64>, fn() -> u64, fn() -> Timestamp, BenchFsConfig, C>;

// Store is the store directory opened: the tree holding every volume.
pub struct Store {
    pub tree: BenchTree,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenStoreError {
    #[error("the store directory could not be made")]
    Directory(#[source] io::Error),
    #[error("the store is open in another mount")]
    AlreadyOpen,
    #[error("the store's database could not be opened")]
    Database(#[source] DatabaseError),
    #[error("the store's tree could not be opened")]
    Tree(
        #[source]
        TransactError<
            MetadataRejection,
            MetadataFailure,
            PinnedConfigError<MetadataRejection, MetadataFailure>,
        >,
    ),
    #[cfg(store = "redb")]
    #[error("the store's content could not be opened")]
    Content(#[source] stratafs_object_store::ObjectStoreError),
}

#[allow(clippy::type_complexity)]
#[derive(Debug, thiserror::Error)]
pub enum OpenVolumeError {
    #[error(transparent)]
    Store(#[from] OpenStoreError),
    #[error("the store has no such volume")]
    UnknownVolume,
    #[error("the volume could not be opened")]
    Volume(#[source] <BenchTree as ForkableKeyValueStore>::Failure),
    #[error("the volume's root directory could not be made")]
    Root(
        #[source]
        TransactError<
            VolumeRejection,
            EngineFailure<VolumeFailure, Failure<Client<u64>, BenchVolume>>,
            StoreError<VolumeRejection, VolumeFailure>,
        >,
    ),
    #[error("the runtime the coordinator's client runs on could not start")]
    Runtime(#[source] io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum CreateVolumeError {
    #[error(transparent)]
    Store(#[from] OpenStoreError),
    #[error("the volume could not be created")]
    Create(#[source] <BenchTree as ForkableKeyValueStore>::CreateError),
    #[error(transparent)]
    Open(#[from] OpenVolumeError),
    #[error("the engine's thread pool could not start")]
    Pool(#[source] io::Error),
}

impl From<&OpenVolumeError> for PosixError {
    fn from(error: &OpenVolumeError) -> Self {
        match error {
            OpenVolumeError::Store(OpenStoreError::AlreadyOpen) => PosixError::EBUSY,
            OpenVolumeError::UnknownVolume => PosixError::ENOENT,
            OpenVolumeError::Store(_)
            | OpenVolumeError::Volume(_)
            | OpenVolumeError::Root(_)
            | OpenVolumeError::Runtime(_) => PosixError::EIO,
        }
    }
}

// open_store opens the store in directory, making it first if the directory holds none. Its volumes' garbage
// collectors run on runtime.
pub async fn open_store(
    directory: &Path,
    runtime: Arc<dyn Runtime>,
) -> Result<Store, OpenStoreError> {
    std::fs::create_dir_all(directory).map_err(OpenStoreError::Directory)?;
    let allocate_node: fn() -> u128 = rand::random;
    let tree = Tree::new(open_database(directory)?, allocate_node, runtime)
        .await
        .map_err(OpenStoreError::Tree)?;
    Ok(Store { tree })
}

#[cfg(store = "redb")]
fn open_database(directory: &Path) -> Result<Database, OpenStoreError> {
    let content = stratafs_object_store::ObjectStoreContentStore::local(directory.join("content"))
        .map_err(OpenStoreError::Content)?;
    let metadata = stratafs_redb::RedbStore::local(directory.join("metadata.redb"), ThreadPool)
        .map_err(|error| match error {
            redb::Error::DatabaseAlreadyOpen => OpenStoreError::AlreadyOpen,
            error => OpenStoreError::Database(error),
        })?;
    Ok(Database::new(metadata, content))
}

#[cfg(store = "sqlite")]
fn open_database(directory: &Path) -> Result<Database, OpenStoreError> {
    let metadata =
        stratafs_sqlite::SqliteMetadata::local(directory.join("store.sqlite"), ThreadPool)
            .map_err(OpenStoreError::Database)?;
    let content = stratafs_sqlite::SqliteContent::of(&metadata);
    Ok(Database::new(metadata, content))
}

#[cfg(store = "mdbx")]
fn open_database(directory: &Path) -> Result<Database, OpenStoreError> {
    let metadata = stratafs_mdbx::MdbxMetadata::open(
        directory.join("store.mdbx"),
        &MDBX_CONFIGURATION,
        ThreadPool,
    )
    .map_err(|error| match error {
        stratafs_mdbx::MdbxOpenError::AlreadyOpen => OpenStoreError::AlreadyOpen,
        error => OpenStoreError::Database(error),
    })?;
    let content = stratafs_mdbx::MdbxContent::of(&metadata);
    Ok(Database::new(metadata, content))
}

// open_volume opens volume from the store in directory, as an engine with consistency that invalidates kernel's cache,
// running its work on runtime.
pub async fn open_volume<C: Cached<BenchVolume, Client<u64>>>(
    directory: &Path,
    volume: TreeVolumeId,
    consistency: C,
    kernel: impl KernelCache,
    runtime: Arc<dyn Runtime>,
) -> Result<BenchEngine<C>, OpenVolumeError> {
    let store = open_store(directory, Arc::clone(&runtime)).await?;
    engine(directory, store, volume, consistency, kernel, runtime).await
}

// create_volume makes a new, empty volume in the store in directory, and returns its id.
pub async fn create_volume(directory: &Path) -> Result<TreeVolumeId, CreateVolumeError> {
    // No kernel caches the volume's inodes, so its pool runs only the volume's garbage collector.
    let pool = SpawnerPool::builder()
        .pool_size(1)
        .create()
        .map_err(CreateVolumeError::Pool)?;
    let runtime: Arc<dyn Runtime> = Arc::new(ThreadPoolRuntime(pool));
    let store = open_store(directory, Arc::clone(&runtime)).await?;
    let volume = store
        .tree
        .create()
        .await
        .map_err(CreateVolumeError::Create)?;
    // Opening the engine makes the volume's root directory, so a mount finds it ready.
    engine(
        directory,
        store,
        volume,
        Eventual::default(),
        NoKernel,
        runtime,
    )
    .await?;
    Ok(volume)
}

async fn engine<C: Cached<BenchVolume, Client<u64>>>(
    directory: &Path,
    store: Store,
    volume: TreeVolumeId,
    consistency: C,
    kernel: impl KernelCache,
    runtime: Arc<dyn Runtime>,
) -> Result<BenchEngine<C>, OpenVolumeError> {
    let opened = store.tree.open(volume).await.map_err(|error| match error {
        StoreError::Rejected(UnknownVolume) => OpenVolumeError::UnknownVolume,
        StoreError::Failed(failure) => OpenVolumeError::Volume(failure),
    })?;
    let allocate_inode: fn() -> u64 = rand::random;
    let allocate_content_id: fn() -> ContentId = || ContentId(rand::random());
    FsEngine::new(
        opened,
        allocate_inode,
        allocate_content_id,
        now as fn() -> Timestamp,
        MountId(rand::random()),
        DURABILITY_LIMIT,
        connect(directory, volume)?,
        PUBLISH_TIMEOUT,
        consistency,
        kernel,
        runtime,
    )
    .await
    .map_err(OpenVolumeError::Root)
}

// PUBLISH_TIMEOUT is how long a write waits for every other mount holding what it changes. It is longer than the
// coordinator takes to drop a mount that went silent, so only a coordinator that stopped answering fails a write.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

// connect connects a mount of volume to the coordinator. Each connect reads the coordinator's URL from the directory's
// coordinator file, so making a volume needs no coordinator, and joins /volume/<its id> there, as each volume's inodes
// are its own keys. The client runs on a Tokio runtime of its own, which connect keeps.
#[allow(clippy::type_complexity)]
fn connect(
    directory: &Path,
    volume: TreeVolumeId,
) -> Result<
    impl Fn() -> BoxFuture<'static, Result<Client<u64>, ConnectFailure>> + Send + Sync + 'static,
    OpenVolumeError,
> {
    let coordinator = directory.join("coordinator");
    let volume = hex::encode(bytes::Bytes::from(volume));
    let client_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(OpenVolumeError::Runtime)?;
    let connect = move || {
        let (coordinator, volume) = (coordinator.clone(), volume.clone());
        let connecting = Connecting(client_runtime.spawn(async move {
            let url = std::fs::read_to_string(coordinator)?;
            let url = format!("{}/volume/{volume}", url.trim().trim_end_matches('/'));
            let client = Client::connect(&url).await?;
            Ok::<_, ConnectFailure>(client)
        }));
        async move {
            let mut connecting = connecting;
            (&mut connecting.0).await.map_err(ConnectFailure::from)?
        }
        .boxed()
    };
    Ok(connect)
}

// Connecting is a connect running on the client's runtime, which stops when nothing waits for it any more, such as
// once the engine's connect time limit passes.
struct Connecting<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for Connecting<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// now is the time in nanoseconds since the Unix epoch.
fn now() -> Timestamp {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970");
    Timestamp(i64::try_from(since_epoch.as_nanos()).expect("the clock is before 2262"))
}

// durability_limit reads the durability limit build setting, or power when the build does not set it. A setting that
// is not none, software or power stops the build.
const fn durability_limit(setting: Option<&str>) -> Option<DurableAgainst> {
    let Some(setting) = setting else {
        return Some(DurableAgainst::PowerFailure);
    };
    match setting.as_bytes() {
        b"none" => None,
        b"software" => Some(DurableAgainst::SoftwareFailure),
        b"power" => Some(DurableAgainst::PowerFailure),
        _ => panic!("the durability build setting is not none, software or power"),
    }
}

// mdbx_safe reads the libmdbx mode build setting, or safe when the build does not set it. A setting that is not safe
// or utterly stops the build.
#[cfg(store = "mdbx")]
const fn mdbx_safe(setting: Option<&str>) -> bool {
    let Some(setting) = setting else {
        return true;
    };
    match setting.as_bytes() {
        b"safe" => true,
        b"utterly" => false,
        _ => panic!("the libmdbx mode build setting is not safe or utterly"),
    }
}

// number reads a decimal build setting, or default when the build does not set it. A setting that is not a
// number stops the build.
const fn number(setting: Option<&str>, default: usize) -> usize {
    let Some(setting) = setting else {
        return default;
    };
    let digits = setting.as_bytes();
    assert!(!digits.is_empty(), "a numeric build setting is empty");
    let mut value = 0;
    let mut index = 0;
    while index < digits.len() {
        let digit = digits[index];
        assert!(
            digit.is_ascii_digit(),
            "a numeric build setting is not a number"
        );
        value = value * 10 + (digit - b'0') as usize;
        index += 1;
    }
    value
}
