// stratafs-bench is a stratafs build for benchmarking: one store directory on this machine, holding every
// volume, with the tuning constants this build was made with.

pub mod suite;

use std::io;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::{FsConfig, FsEngine, Timestamp};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::kv::{KeyValueStore, TransactError};
use stratafs_store::spawn::ThreadPool;
use stratafs_tree::header::TreeVolumeId;
use stratafs_tree::tree::PinnedConfigError;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;

// The tuning constants come from the build's environment, so the benchmark builds once per configuration.
// A store remembers the tree constants it was made with, and refuses to open with others.
pub struct BenchTreeConfig;

impl TreeConfig for BenchTreeConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = number(option_env!("STRATAFS_BENCH_NODE_CAPACITY"), 16 * 1024);
    const MERGE_BELOW_PERCENT: u8 =
        number(option_env!("STRATAFS_BENCH_MERGE_BELOW_PERCENT"), 25) as u8;
}

pub struct BenchFsConfig;

impl FsConfig for BenchFsConfig {
    const LARGEST_INLINE_ENTRY: usize =
        number(option_env!("STRATAFS_BENCH_LARGEST_INLINE_ENTRY"), 4 * 1024);
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
type DatabaseError = MetadataFailure;
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

pub type BenchTree = Tree<Metadata, fn() -> u128, BenchTreeConfig>;

pub type BenchVolume = TreeVolume<Metadata, fn() -> u128, BenchTreeConfig>;
type VolumeRejection = <BenchVolume as KeyValueStore>::Rejection;
type VolumeFailure = <BenchVolume as KeyValueStore>::Failure;

// BenchStore keeps a volume and the content of every file, made durable together.
#[cfg(store = "redb")]
pub type BenchStore = stratafs_lock_synchronized::LockSynchronizedStore<BenchVolume, Content>;
#[cfg(store = "sqlite")]
pub type BenchStore = stratafs_sqlite::SqliteStore<BenchVolume, ThreadPool>;
#[cfg(store = "mdbx")]
pub type BenchStore = stratafs_mdbx::MdbxStore<BenchVolume, ThreadPool>;

pub type BenchEngine = FsEngine<BenchStore, fn() -> u64, fn() -> Timestamp, BenchFsConfig>;

// Store is the store directory opened: the tree holding every volume, and the content of every file.
pub struct Store {
    pub tree: BenchTree,
    pub content: Content,
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
        TransactError<VolumeRejection, VolumeFailure, StoreError<VolumeRejection, VolumeFailure>>,
    ),
}

#[derive(Debug, thiserror::Error)]
pub enum CreateVolumeError {
    #[error(transparent)]
    Store(#[from] OpenStoreError),
    #[error("the volume could not be created")]
    Create(#[source] <BenchTree as ForkableKeyValueStore>::CreateError),
    #[error(transparent)]
    Open(#[from] OpenVolumeError),
}

impl From<&OpenVolumeError> for PosixError {
    fn from(error: &OpenVolumeError) -> Self {
        match error {
            OpenVolumeError::Store(OpenStoreError::AlreadyOpen) => PosixError::EBUSY,
            OpenVolumeError::UnknownVolume => PosixError::ENOENT,
            OpenVolumeError::Store(_) | OpenVolumeError::Volume(_) | OpenVolumeError::Root(_) => {
                PosixError::EIO
            }
        }
    }
}

// open_store opens the store in directory, making it first if the directory holds none.
pub async fn open_store(directory: &Path) -> Result<Store, OpenStoreError> {
    std::fs::create_dir_all(directory).map_err(OpenStoreError::Directory)?;
    let (metadata, content) = open_database(directory)?;
    let allocate_node: fn() -> u128 = rand::random;
    let tree = Tree::new(metadata, allocate_node)
        .await
        .map_err(OpenStoreError::Tree)?;
    Ok(Store { tree, content })
}

#[cfg(store = "redb")]
fn open_database(directory: &Path) -> Result<(Metadata, Content), OpenStoreError> {
    let content = stratafs_object_store::ObjectStoreContentStore::local(directory.join("content"))
        .map_err(OpenStoreError::Content)?;
    let metadata = stratafs_redb::RedbStore::local(directory.join("metadata.redb"), ThreadPool)
        .map_err(|error| match error {
            redb::Error::DatabaseAlreadyOpen => OpenStoreError::AlreadyOpen,
            error => OpenStoreError::Database(error),
        })?;
    Ok((metadata, content))
}

#[cfg(store = "sqlite")]
fn open_database(directory: &Path) -> Result<(Metadata, Content), OpenStoreError> {
    let metadata =
        stratafs_sqlite::SqliteMetadata::local(directory.join("store.sqlite"), ThreadPool)
            .map_err(OpenStoreError::Database)?;
    let content = stratafs_sqlite::SqliteContent::of(&metadata);
    Ok((metadata, content))
}

#[cfg(store = "mdbx")]
fn open_database(directory: &Path) -> Result<(Metadata, Content), OpenStoreError> {
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
    Ok((metadata, content))
}

// open_volume opens volume from the store in directory.
pub async fn open_volume(
    directory: &Path,
    volume: TreeVolumeId,
) -> Result<BenchEngine, OpenVolumeError> {
    let store = open_store(directory).await?;
    engine(store, volume).await
}

// create_volume makes a new, empty volume in the store in directory, and returns its id.
pub async fn create_volume(directory: &Path) -> Result<TreeVolumeId, CreateVolumeError> {
    let store = open_store(directory).await?;
    let volume = store
        .tree
        .create()
        .await
        .map_err(CreateVolumeError::Create)?;
    // Opening the engine makes the volume's root directory, so a mount finds it ready.
    engine(store, volume).await?;
    Ok(volume)
}

async fn engine(store: Store, volume: TreeVolumeId) -> Result<BenchEngine, OpenVolumeError> {
    let metadata = store.tree.open(volume).await.map_err(|error| match error {
        StoreError::Rejected(UnknownVolume) => OpenVolumeError::UnknownVolume,
        StoreError::Failed(failure) => OpenVolumeError::Volume(failure),
    })?;
    let allocate_inode: fn() -> u64 = rand::random;
    FsEngine::new(
        BenchStore::new(metadata, store.content),
        allocate_inode,
        now as fn() -> Timestamp,
        DURABILITY_LIMIT,
    )
    .await
    .map_err(OpenVolumeError::Root)
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
