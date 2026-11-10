// stores opens a volume from the store and with the consistency a mount names, and makes new volumes. Each choice is
// its own instance of the engine's types, and the matches below are the one place a choice picks its instance.

use std::error::Error;
use std::io;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use clap::{Args, ValueEnum};
use futures_executor::ThreadPool as SpawnerPool;
use futures_lite::future::block_on;
use futures_util::FutureExt;
use stratafs_fs_engine::kernel_cache::{KernelCache, NoKernel};
use stratafs_fs_engine::spillable::referenced_content;
use stratafs_fs_engine::strong::ConnectFailure;
use stratafs_fs_engine::{
    Cached, Eventual, FsConfig, FsEngine, MountId, StrongUntilDisconnectCached, Timestamp,
};
use stratafs_lock_synchronized::LockSynchronizedStore;
use stratafs_mdbx::{
    MdbxConfiguration, MdbxContent, MdbxGeometry, MdbxMetadata, MdbxOpenError, MdbxStore,
};
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_redb::RedbStore;
use stratafs_runtime::{Runtime, ThreadPoolRuntime};
use stratafs_sqlite::{SqliteContent, SqliteMetadata, SqliteStore};
use stratafs_store::content::{ContentId, ContentReference};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::spawn::ThreadPool;
use stratafs_store::store::Store;
use stratafs_tree::header::TreeVolumeId;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;
use stratafs_websocket_pubsub::client::Client;

use super::MountError;

// FuseTreeConfig and FuseFsConfig are the tuning constants of the daemon's stores, the benchmark's defaults. A store
// remembers the tree constants it was made with, and refuses to open with others.
pub struct FuseTreeConfig;

impl TreeConfig for FuseTreeConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = 16 * 1024;
    const MERGE_BELOW_PERCENT: u8 = 25;

    fn referenced_content(key: &[u8], value: &[u8]) -> Option<ContentReference> {
        referenced_content(key, value)
    }
}

pub struct FuseFsConfig;

impl FsConfig for FuseFsConfig {
    const LARGEST_INLINE_ENTRY: usize = 4 * 1024;
}

// MDBX_CONFIGURATION opens the libmdbx store in 4 KiB pages, committing without a sync of its own, but making a
// durable point to roll back to after a crash every 256 MiB written.
const MDBX_CONFIGURATION: MdbxConfiguration<'static> = MdbxConfiguration {
    // The most read transactions libmdbx allows across every process.
    options: &[(mdbx_sys::MDBX_opt_max_readers, 32767)],
    geometry: MdbxGeometry {
        size_lower: -1,
        size_now: -1,
        size_upper: 1 << 40,
        growth_step: 64 << 20,
        shrink_threshold: 0,
        page_size: 4096,
    },
    flags: mdbx_sys::MDBX_NOSUBDIR | mdbx_sys::MDBX_SAFE_NOSYNC,
    mode: 0o644,
    shared_options: &[(mdbx_sys::MDBX_opt_sync_bytes, 256 << 20)],
};

// PUBLISH_TIMEOUT is how long a strong mount's write waits for every other mount holding what it changes. It is longer
// than the coordinator takes to drop a mount that went silent, so only a coordinator that stopped answering fails a
// write.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum StoreKind {
    // mdbx keeps metadata and content in one libmdbx file.
    Mdbx,
    // redb keeps metadata in a redb file, and content in files beside it.
    Redb,
    // sqlite keeps metadata and content in one SQLite database.
    Sqlite,
}

// ConsistencyKind is what a mount's reads promise about other mounts' writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ConsistencyKind {
    // strong reads see every write another mount finished, while the mounts reach the coordinator.
    Strong,
    // eventual reads may lag behind other mounts' writes.
    Eventual,
}

// DurabilityLimit is the most the volume makes commits durable against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum DurabilityLimit {
    None,
    Software,
    Power,
}

#[derive(Debug, Args)]
pub struct CreateArguments {
    #[arg(help = "The store directory")]
    pub store: PathBuf,
    #[arg(long = "store", value_enum, help = "The store's database")]
    pub database: StoreKind,
}

#[derive(Debug, Args)]
pub struct MountArguments {
    #[arg(help = "The store directory")]
    pub store: PathBuf,
    #[arg(help = "The directory to mount the volume on")]
    pub mount_point: PathBuf,
    #[arg(long, value_parser = volume_id, help = "The volume's id, in hex, as create prints it")]
    pub volume: TreeVolumeId,
    #[arg(long = "store", value_enum, help = "The store's database")]
    pub database: StoreKind,
    #[arg(
        long,
        value_enum,
        help = "What the mount's reads promise about other mounts' writes"
    )]
    pub consistency: ConsistencyKind,
    #[arg(
        long,
        required_if_eq("consistency", "strong"),
        help = "The WebSocket URL of the coordinator strong mounts of the volume share, such as ws://127.0.0.1:7600"
    )]
    pub coordinator: Option<String>,
    #[arg(
        long,
        value_enum,
        default_value = "power",
        help = "The most fsync, the durability cadence and unmounting make commits durable against"
    )]
    pub durability_limit: DurabilityLimit,
    #[arg(
        long,
        value_parser = seconds,
        default_value = "30",
        help = "How often every committed operation becomes durable without an fsync, in whole seconds"
    )]
    pub durability_cadence: Duration,
    #[arg(
        long,
        help = "Let every user reach the mount, not only the one who mounted it. A mount by a user other than root needs user_allow_other in /etc/fuse.conf"
    )]
    pub allow_other: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("the store directory could not be made")]
    Directory(#[source] io::Error),
    #[error("the store directory holds no store of that database")]
    NoStore,
    #[error("the store's database could not be found")]
    Find(#[source] io::Error),
    #[error("the store is open in another mount")]
    AlreadyOpen,
    #[error("the store's database could not be opened")]
    Database(#[source] Box<dyn Error + Send + Sync>),
    #[error("the store's tree could not be opened")]
    Tree(#[source] Box<dyn Error + Send + Sync>),
    #[error("the store has no such volume")]
    UnknownVolume,
    #[error("the volume could not be opened")]
    Volume(#[source] Box<dyn Error + Send + Sync>),
    #[error("the volume could not be created")]
    Create(#[source] Box<dyn Error + Send + Sync>),
    #[error("the volume's root directory could not be made")]
    Root(#[source] Box<dyn Error + Send + Sync>),
    #[error("the volume could not be made durable")]
    Durable(#[source] Box<dyn Error + Send + Sync>),
    #[error("the runtime the engine or the coordinator's client runs on could not start")]
    Runtime(#[source] io::Error),
}

// mount serves the volume arguments name until it is unmounted, running the engine's work on runtime.
pub fn mount(
    arguments: &MountArguments,
    runtime: Arc<dyn Runtime>,
) -> Result<(), MountError<OpenError>> {
    match (arguments.database, arguments.consistency) {
        (StoreKind::Mdbx, ConsistencyKind::Strong) => {
            serve::<Mdbx, _>(arguments, runtime, || strong(arguments))
        }
        (StoreKind::Mdbx, ConsistencyKind::Eventual) => {
            serve::<Mdbx, _>(arguments, runtime, eventual)
        }
        (StoreKind::Redb, ConsistencyKind::Strong) => {
            serve::<Redb, _>(arguments, runtime, || strong(arguments))
        }
        (StoreKind::Redb, ConsistencyKind::Eventual) => {
            serve::<Redb, _>(arguments, runtime, eventual)
        }
        (StoreKind::Sqlite, ConsistencyKind::Strong) => {
            serve::<Sqlite, _>(arguments, runtime, || strong(arguments))
        }
        (StoreKind::Sqlite, ConsistencyKind::Eventual) => {
            serve::<Sqlite, _>(arguments, runtime, eventual)
        }
    }
}

// create makes a new, empty volume in the store arguments name, making the store first if it has none, and returns
// the volume's id. Nothing may have the store mounted.
pub fn create(arguments: &CreateArguments) -> Result<TreeVolumeId, OpenError> {
    match arguments.database {
        StoreKind::Mdbx => block_on(create_volume::<Mdbx>(&arguments.store)),
        StoreKind::Redb => block_on(create_volume::<Redb>(&arguments.store)),
        StoreKind::Sqlite => block_on(create_volume::<Sqlite>(&arguments.store)),
    }
}

// Database is a store's database on disk: its metadata and content as one store.
// FILE names its metadata's file in the store directory.
trait Database {
    type Store: Store;

    const FILE: &str;

    fn open(directory: &Path) -> Result<Self::Store, OpenError>;
}

type Volume<D> = TreeVolume<<D as Database>::Store, fn() -> u128, FuseTreeConfig>;

type Engine<D, C> = FsEngine<Volume<D>, fn() -> u64, fn() -> Timestamp, FuseFsConfig, C>;

struct Mdbx;

impl Database for Mdbx {
    type Store = MdbxStore<MdbxMetadata<ThreadPool>, ThreadPool>;

    const FILE: &str = "store.mdbx";

    fn open(directory: &Path) -> Result<Self::Store, OpenError> {
        let metadata =
            MdbxMetadata::open(directory.join(Self::FILE), &MDBX_CONFIGURATION, ThreadPool)
                .map_err(|error| match error {
                    MdbxOpenError::AlreadyOpen => OpenError::AlreadyOpen,
                    error => OpenError::Database(Box::new(error)),
                })?;
        let content = MdbxContent::of(&metadata);
        Ok(MdbxStore::new(metadata, content))
    }
}

struct Redb;

impl Database for Redb {
    type Store = LockSynchronizedStore<RedbStore<ThreadPool>, ObjectStoreContentStore>;

    const FILE: &str = "metadata.redb";

    fn open(directory: &Path) -> Result<Self::Store, OpenError> {
        let content = ObjectStoreContentStore::local(directory.join("content"))
            .map_err(|error| OpenError::Database(Box::new(error)))?;
        let metadata = RedbStore::local(directory.join(Self::FILE), ThreadPool).map_err(
            |error| match error {
                redb::Error::DatabaseAlreadyOpen => OpenError::AlreadyOpen,
                error => OpenError::Database(Box::new(error)),
            },
        )?;
        Ok(LockSynchronizedStore::new(metadata, content))
    }
}

struct Sqlite;

impl Database for Sqlite {
    type Store = SqliteStore<SqliteMetadata<ThreadPool>, ThreadPool>;

    const FILE: &str = "store.sqlite";

    fn open(directory: &Path) -> Result<Self::Store, OpenError> {
        let metadata = SqliteMetadata::local(directory.join(Self::FILE), ThreadPool)
            .map_err(|error| OpenError::Database(Box::new(error)))?;
        let content = SqliteContent::of(&metadata);
        Ok(SqliteStore::new(metadata, content))
    }
}

// serve mounts the volume arguments name from a store of database D, with the consistency made by consistency.
fn serve<D: Database, C: Cached<Volume<D>>>(
    arguments: &MountArguments,
    runtime: Arc<dyn Runtime>,
    consistency: impl FnOnce() -> Result<C, OpenError>,
) -> Result<(), MountError<OpenError>> {
    let limit = match arguments.durability_limit {
        DurabilityLimit::None => None,
        DurabilityLimit::Software => Some(DurableAgainst::SoftwareFailure),
        DurabilityLimit::Power => Some(DurableAgainst::PowerFailure),
    };
    super::mount(
        runtime,
        &arguments.mount_point,
        arguments.durability_cadence,
        arguments.allow_other,
        async |kernel, runtime| {
            // Only create makes a store, so a mount of a store that is not there makes nothing.
            if !directory_holds::<D>(&arguments.store)? {
                return Err(OpenError::NoStore);
            }
            let tree = open_tree::<D>(&arguments.store, Arc::clone(&runtime)).await?;
            engine::<D, C>(
                &tree,
                arguments.volume,
                limit,
                consistency()?,
                kernel,
                runtime,
            )
            .await
        },
    )
}

// create_volume makes a new, empty volume in the store of database D in directory, and returns its id.
async fn create_volume<D: Database>(directory: &Path) -> Result<TreeVolumeId, OpenError> {
    std::fs::create_dir_all(directory).map_err(OpenError::Directory)?;
    // No kernel caches the volume's inodes, so its pool runs only the volume's garbage collector.
    let pool = SpawnerPool::builder()
        .pool_size(1)
        .create()
        .map_err(OpenError::Runtime)?;
    let runtime: Arc<dyn Runtime> = Arc::new(ThreadPoolRuntime(pool));
    let tree = open_tree::<D>(directory, Arc::clone(&runtime)).await?;
    let volume = tree
        .create()
        .await
        .map_err(|error| OpenError::Create(Box::new(error)))?;
    // Opening the engine makes the volume's root directory, so a mount finds it ready.
    let engine = engine::<D, Eventual>(
        &tree,
        volume,
        Some(DurableAgainst::PowerFailure),
        Eventual::default(),
        NoKernel,
        runtime,
    )
    .await?;
    engine
        .ensure_durable(DurableAgainst::PowerFailure)
        .await
        .map_err(|error| OpenError::Durable(Box::new(error)))?;
    Ok(volume)
}

type StoreTree<D> = Tree<<D as Database>::Store, fn() -> u128, FuseTreeConfig>;

// directory_holds says whether directory holds a store of database D.
fn directory_holds<D: Database>(directory: &Path) -> Result<bool, OpenError> {
    directory
        .join(D::FILE)
        .try_exists()
        .map_err(OpenError::Find)
}

// open_tree opens the tree of the store of database D in directory, making the store first if the directory holds
// none. Its volumes' garbage collectors run on runtime.
async fn open_tree<D: Database>(
    directory: &Path,
    runtime: Arc<dyn Runtime>,
) -> Result<StoreTree<D>, OpenError> {
    let allocate_node: fn() -> u128 = rand::random;
    Tree::new(D::open(directory)?, allocate_node, runtime)
        .await
        .map_err(|error| OpenError::Tree(Box::new(error)))
}

// engine opens volume of tree as an engine that invalidates kernel's cache, running its work on runtime.
async fn engine<D: Database, C: Cached<Volume<D>>>(
    tree: &StoreTree<D>,
    volume: TreeVolumeId,
    durability_limit: Option<DurableAgainst>,
    consistency: C,
    kernel: impl KernelCache,
    runtime: Arc<dyn Runtime>,
) -> Result<Engine<D, C>, OpenError> {
    let volume = tree.open(volume).await.map_err(|error| match error {
        StoreError::Rejected(UnknownVolume) => OpenError::UnknownVolume,
        StoreError::Failed(failure) => OpenError::Volume(Box::new(failure)),
    })?;
    let allocate_inode: fn() -> u64 = rand::random;
    let allocate_content_id: fn() -> ContentId = || ContentId(rand::random());
    FsEngine::new(
        volume,
        allocate_inode,
        allocate_content_id,
        now as fn() -> Timestamp,
        MountId(rand::random()),
        durability_limit,
        consistency,
        kernel,
        runtime,
    )
    .await
    .map_err(|error| OpenError::Root(Box::new(error)))
}

fn eventual() -> Result<Eventual, OpenError> {
    Ok(Eventual::default())
}

type Strong = StrongUntilDisconnectCached<Client<u64>>;

// strong is the strong consistency of a mount of the volume arguments name, which joins /volume/<its id> at the
// coordinator, as each volume's inodes are its own keys. The client runs on a Tokio runtime of its own, which the
// consistency keeps.
fn strong(arguments: &MountArguments) -> Result<Strong, OpenError> {
    let coordinator = arguments
        .coordinator
        .as_deref()
        .expect("the command line requires a coordinator for a strong mount");
    let url = format!(
        "{}/volume/{}",
        coordinator.trim_end_matches('/'),
        hex::encode(Bytes::from(arguments.volume))
    );
    let client_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(OpenError::Runtime)?;
    let connect = move || {
        let url = url.clone();
        let connecting = Connecting(
            client_runtime
                .spawn(async move { Client::connect(&url).await.map_err(ConnectFailure::from) }),
        );
        async move {
            let mut connecting = connecting;
            (&mut connecting.0).await.map_err(ConnectFailure::from)?
        }
        .boxed()
    };
    Ok(StrongUntilDisconnectCached::new(connect, PUBLISH_TIMEOUT))
}

// Connecting is a connect running on the client's runtime, which stops when nothing waits for it any more, such as
// once the consistency's connect time limit passes.
struct Connecting<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for Connecting<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// volume_id reads a volume id written in hex.
fn volume_id(hex: &str) -> Result<TreeVolumeId, String> {
    let id = hex::decode(hex).map_err(|error| format!("the volume id is not hex: {error}"))?;
    TreeVolumeId::try_from(Bytes::from(id))
        .map_err(|error| format!("the volume id is not a volume's: {error}"))
}

// seconds reads a whole number of seconds above zero.
fn seconds(seconds: &str) -> Result<Duration, String> {
    let seconds: NonZeroU64 = seconds
        .parse()
        .map_err(|error| format!("not a whole number of seconds above zero: {error}"))?;
    Ok(Duration::from_secs(seconds.get()))
}

// now is the time in nanoseconds since the Unix epoch.
fn now() -> Timestamp {
    let since_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the clock is after 1970");
    Timestamp(i64::try_from(since_epoch.as_nanos()).expect("the clock is before 2262"))
}

#[cfg(test)]
mod tests;
