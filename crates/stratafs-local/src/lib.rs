// stratafs-local is a standard bundle of local implementations: the three stores on this machine's disk, a
// coordinator reached over WebSocket, and Ed25519 signing with a key given on the command line. Its binaries mount
// volumes for this repository's tests and benchmarks. It builds the engine specification an integration takes; each
// choice of store, consistency and signing is its own instance of the engine's types, and the matches below are the
// one place a choice picks its instance.

pub mod mount_options;

use std::error::Error;
use std::io;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use clap::ValueEnum;
use futures_executor::ThreadPool as SpawnerPool;
use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use noncrypto_digests::Xxh3_128;
use sha2::Sha256;
use stratafs_ed25519_signer::Ed25519Signer;
use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::holds::ConnectFailure;
use stratafs_fs_engine::spillable::referenced_content;
use stratafs_fs_engine::verification::Unsigned;
use stratafs_fs_engine::volume_creation::{CreateVolumeError, create_volume};
use stratafs_fs_engine::{
    Cached, EngineSpecification, Eventual, FsConfig, StrongUntilDisconnectCached, Timestamp,
};
use stratafs_lock_synchronized::LockSynchronizedStore;
use stratafs_mdbx::{
    MdbxConfiguration, MdbxContent, MdbxGeometry, MdbxMetadata, MdbxOpenError, MdbxStore,
};
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_redb::RedbStore;
use stratafs_runtime::{Runtime, ThreadPoolRuntime};
use stratafs_sqlite::{SqliteContent, SqliteMetadata, SqliteStore};
use stratafs_store::content::ContentReference;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::spawn::ThreadPool;
use stratafs_store::store::Store;
use stratafs_tree::header::TreeVolumeId;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_websocket_pubsub::client::Client;
use url::Url;

// The tuning constants come from the build's environment, so a benchmark builds once per configuration. A store
// remembers the tree constants it was made with, and refuses to open with others.
pub struct LocalTreeConfig;

impl TreeConfig for LocalTreeConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = number(option_env!("STRATAFS_LOCAL_NODE_CAPACITY"), 16 * 1024);
    const MERGE_BELOW_PERCENT: u8 =
        number(option_env!("STRATAFS_LOCAL_MERGE_BELOW_PERCENT"), 25) as u8;

    fn referenced_content(key: &[u8], value: &[u8]) -> Option<ContentReference> {
        referenced_content(key, value)
    }
}

// LocalFsConfig is the configuration of a volume that is not signed.
pub struct LocalFsConfig;

impl FsConfig for LocalFsConfig {
    const LARGEST_INLINE_ENTRY: usize =
        number(option_env!("STRATAFS_LOCAL_LARGEST_INLINE_ENTRY"), 4 * 1024);
    const CHECKED_CONTENT_BYTES: u64 = 64 * 1024 * 1024;

    type HashFunction = Xxh3_128;
    type Signer = Unsigned;
}

// SignedLocalFsConfig is the configuration of a signed volume, whose mounts sign with an Ed25519 key.
pub struct SignedLocalFsConfig;

impl FsConfig for SignedLocalFsConfig {
    const LARGEST_INLINE_ENTRY: usize = LocalFsConfig::LARGEST_INLINE_ENTRY;
    const CHECKED_CONTENT_BYTES: u64 = LocalFsConfig::CHECKED_CONTENT_BYTES;

    type HashFunction = Sha256;
    type Signer = Ed25519Signer;
}

// MDBX_SAFE is whether libmdbx keeps a durable point to roll back to after a crash, from the build's environment, as
// STRATAFS_LOCAL_MDBX_MODE: safe, which makes one after every MDBX_SYNC_BYTES written, or utterly, which makes none of
// its own and may wipe the one ensure_durable made.
const MDBX_SAFE: bool = mdbx_safe(option_env!("STRATAFS_LOCAL_MDBX_MODE"));
const MDBX_SYNC_BYTES: usize = number(option_env!("STRATAFS_LOCAL_MDBX_SYNC_BYTES"), 256 << 20);

// MDBX_CONFIGURATION opens the libmdbx store in 4 KiB pages, committing without a sync of its own.
const MDBX_CONFIGURATION: MdbxConfiguration<'static> = MdbxConfiguration {
    options: &[
        // libmdbx sizes how many changed pages a write transaction keeps in memory from the machine's memory,
        // which it fails to read on macOS 27.
        (mdbx_sys::MDBX_opt_txn_dp_limit, (1 << 30) / 4096),
        // The most read transactions libmdbx allows across every process.
        (mdbx_sys::MDBX_opt_max_readers, 32767),
    ],
    geometry: MdbxGeometry {
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

// DURABILITY_LIMIT is the most a mount that takes no durability limit of its own makes commits durable against, from
// the build's environment, as STRATAFS_LOCAL_DURABILITY: none, software or power.
pub const DURABILITY_LIMIT: Option<DurableAgainst> =
    durability_limit(option_env!("STRATAFS_LOCAL_DURABILITY"));

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
    #[error(transparent)]
    Create(CreateVolumeError),
    #[error("the runtime the store or the coordinator's client runs on could not start")]
    Runtime(#[source] io::Error),
}

impl From<&OpenError> for PosixError {
    fn from(error: &OpenError) -> Self {
        match error {
            OpenError::AlreadyOpen => PosixError::EBUSY,
            OpenError::NoStore | OpenError::UnknownVolume => PosixError::ENOENT,
            OpenError::Directory(_)
            | OpenError::Find(_)
            | OpenError::Database(_)
            | OpenError::Tree(_)
            | OpenError::Volume(_)
            | OpenError::Create(_)
            | OpenError::Runtime(_) => PosixError::EIO,
        }
    }
}

// LocalSpecification is the specification of a mount of a volume in store St.
pub type LocalSpecification<St, C, S> =
    EngineSpecification<St, Client<u64>, fn() -> u64, fn() -> Timestamp, S, C>;

// Integration is what mounts a volume once its specification is known, whichever store, consistency and configuration
// the mount chose: FUSE on Linux, the FSKit module on macOS.
pub trait Integration {
    type Mounted;

    fn mount<St, C, S>(self, specification: LocalSpecification<St, C, S>) -> Self::Mounted
    where
        St: Store,
        C: Cached<St, Client<u64>>,
        S: FsConfig;
}

// Mount is a mount of a volume as its command line or its mount options name it.
pub struct Mount<'a> {
    // directory is the store directory.
    pub directory: &'a Path,
    pub database: StoreKind,
    pub volume: TreeVolumeId,
    pub consistency: ConsistencyKind,
    // coordinator is the WebSocket URL of the coordinator the volume's mounts share.
    pub coordinator: &'a Url,
    // signer signs what the mount writes, for a signed volume.
    pub signer: Option<Ed25519Signer>,
    pub durability_limit: Option<DurableAgainst>,
    // runtime runs the engine's tasks and the store's garbage collector.
    pub runtime: Arc<dyn Runtime>,
}

// mount opens the store mount names and hands integration the specification of its volume's engine.
pub async fn mount<I: Integration>(
    mount: Mount<'_>,
    integration: I,
) -> Result<I::Mounted, OpenError> {
    match mount.database {
        StoreKind::Mdbx => mount_from::<Mdbx, I>(mount, integration).await,
        StoreKind::Redb => mount_from::<Redb, I>(mount, integration).await,
        StoreKind::Sqlite => mount_from::<Sqlite, I>(mount, integration).await,
    }
}

// create makes a new, empty volume in the store of database in directory, making the store first if it has none, and
// returns the volume's id. signer signs the volume, when there is one. Nothing may have the store mounted.
pub async fn create(
    directory: &Path,
    database: StoreKind,
    signer: Option<Ed25519Signer>,
) -> Result<TreeVolumeId, OpenError> {
    match database {
        StoreKind::Mdbx => create_in::<Mdbx>(directory, signer).await,
        StoreKind::Redb => create_in::<Redb>(directory, signer).await,
        StoreKind::Sqlite => create_in::<Sqlite>(directory, signer).await,
    }
}

// Database is a store's database on disk: its metadata and content as one store.
// FILE names its metadata's file in the store directory.
trait Database {
    type Store: Store;

    const FILE: &str;

    fn open(directory: &Path) -> Result<Self::Store, OpenError>;
}

type StoreTree<D> = Tree<<D as Database>::Store, fn() -> u128, LocalTreeConfig>;

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

// mount_from is mount for a store of database D.
async fn mount_from<D: Database, I: Integration>(
    mut mount: Mount<'_>,
    integration: I,
) -> Result<I::Mounted, OpenError> {
    // Only create makes a store, so a mount of a store that is not there makes nothing.
    let holds_store = mount.directory.join(D::FILE).try_exists();
    if !holds_store.map_err(OpenError::Find)? {
        return Err(OpenError::NoStore);
    }
    let tree = open_tree::<D>(mount.directory, Arc::clone(&mount.runtime)).await?;
    let volume = tree.open(mount.volume).await.map_err(|error| match error {
        StoreError::Rejected(UnknownVolume) => OpenError::UnknownVolume,
        StoreError::Failed(failure) => OpenError::Volume(Box::new(failure)),
    })?;
    let signer = mount.signer.take();
    Ok(match (mount.consistency, signer) {
        (ConsistencyKind::Strong, Some(signer)) => {
            integration.mount(specification::<_, _, SignedLocalFsConfig>(
                volume,
                &mount,
                StrongUntilDisconnectCached,
                signer,
            )?)
        }
        (ConsistencyKind::Eventual, Some(signer)) => {
            integration.mount(specification::<_, _, SignedLocalFsConfig>(
                volume,
                &mount,
                Eventual::default(),
                signer,
            )?)
        }
        (ConsistencyKind::Strong, None) => integration.mount(specification::<_, _, LocalFsConfig>(
            volume,
            &mount,
            StrongUntilDisconnectCached,
            Unsigned,
        )?),
        (ConsistencyKind::Eventual, None) => integration.mount(
            specification::<_, _, LocalFsConfig>(volume, &mount, Eventual::default(), Unsigned)?,
        ),
    })
}

// specification is the specification of mount's engine on volume, with consistency, signing with signer.
fn specification<St: Store, C, S: FsConfig>(
    volume: St,
    mount: &Mount<'_>,
    consistency: C,
    signer: S::Signer,
) -> Result<LocalSpecification<St, C, S>, OpenError> {
    Ok(EngineSpecification::with_defaults(
        volume,
        connect(mount.coordinator, mount.volume)?,
        consistency,
        signer,
        mount.durability_limit,
        Arc::clone(&mount.runtime),
    ))
}

// create_in is create for a store of database D.
async fn create_in<D: Database>(
    directory: &Path,
    signer: Option<Ed25519Signer>,
) -> Result<TreeVolumeId, OpenError> {
    std::fs::create_dir_all(directory).map_err(OpenError::Directory)?;
    // No kernel caches the volume's inodes, so its pool runs only the volume's garbage collector.
    let pool = SpawnerPool::builder()
        .pool_size(1)
        .create()
        .map_err(OpenError::Runtime)?;
    let runtime: Arc<dyn Runtime> = Arc::new(ThreadPoolRuntime(pool));
    let tree = open_tree::<D>(directory, Arc::clone(&runtime)).await?;
    let created = match signer {
        Some(signer) => create_volume::<_, SignedLocalFsConfig>(&tree, signer, runtime).await,
        None => create_volume::<_, LocalFsConfig>(&tree, Unsigned, runtime).await,
    };
    created.map_err(OpenError::Create)
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

// connect connects a mount of volume to the coordinator, joining /volume/<its id> there, as each volume's inodes are
// its own keys. The client runs on a Tokio runtime of its own, which connect keeps.
fn connect(
    coordinator: &Url,
    volume: TreeVolumeId,
) -> Result<
    impl Fn() -> BoxFuture<'static, Result<Client<u64>, ConnectFailure>> + Send + Sync + 'static,
    OpenError,
> {
    let url = format!(
        "{}/volume/{}",
        coordinator.as_str().trim_end_matches('/'),
        hex::encode(Bytes::from(volume))
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

// volume_id reads a volume id written in hex.
pub fn volume_id(hex: &str) -> Result<TreeVolumeId, String> {
    let id = hex::decode(hex).map_err(|error| format!("the volume id is not hex: {error}"))?;
    TreeVolumeId::try_from(Bytes::from(id))
        .map_err(|error| format!("the volume id is not a volume's: {error}"))
}

// error_chain is an error with each of its sources, the way a person needs to read it.
pub fn error_chain(error: &dyn Error) -> String {
    let mut chain = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    chain
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

#[cfg(test)]
mod tests;
