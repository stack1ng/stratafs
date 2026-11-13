use futures_lite::future::block_on;
use stratafs_fs_engine::kernel_cache::NoKernel;
use stratafs_fs_engine::{FsEngine, Inode};

use super::*;

// ReadsRoot says whether the specification opens as an engine no kernel caches, and reads its root directory.
struct ReadsRoot;

impl Integration for ReadsRoot {
    type Mounted = bool;

    fn mount<St, C, S>(self, specification: LocalSpecification<St, C, S>) -> bool
    where
        St: Store,
        C: Cached<St, Client<u64>>,
        S: FsConfig,
    {
        let Ok(engine) = block_on(FsEngine::new(specification, NoKernel)) else {
            return false;
        };
        let root = engine.read_transact(|transaction| {
            Box::pin(async move { transaction.getattr(Inode::ROOT).await })
        });
        matches!(block_on(root), Ok(Some(_)))
    }
}

fn runtime() -> Arc<dyn Runtime> {
    let pool = SpawnerPool::builder().pool_size(1).create().unwrap();
    Arc::new(ThreadPoolRuntime(pool))
}

// mounted hands ReadsRoot the volume of the store of database in directory, with signer.
fn mounted(
    directory: &Path,
    database: StoreKind,
    volume: TreeVolumeId,
    signer: Option<Ed25519Signer>,
) -> Result<bool, OpenError> {
    let mount = Mount {
        directory,
        database,
        volume,
        consistency: ConsistencyKind::Eventual,
        coordinator: &"ws://127.0.0.1:7600".parse().unwrap(),
        signer,
        durability_limit: None,
        runtime: runtime(),
    };
    block_on(super::mount(mount, ReadsRoot))
}

fn unknown_volume() -> TreeVolumeId {
    TreeVolumeId::try_from(Bytes::from(vec![7; 16])).unwrap()
}

#[test]
fn a_volume_id_reads_back_from_its_hex() {
    let id = unknown_volume();
    assert_eq!(volume_id(&hex::encode(Bytes::from(id))), Ok(id));
    assert!(volume_id("not hex").is_err());
    assert!(volume_id("0707").is_err());
}

#[test]
fn a_volume_made_in_a_store_opens_again() {
    for database in [StoreKind::Mdbx, StoreKind::Redb, StoreKind::Sqlite] {
        let directory = tempfile::tempdir().unwrap();
        let volume = block_on(create(directory.path(), database, None)).unwrap();
        let opened = mounted(directory.path(), database, volume, None);
        assert!(matches!(opened, Ok(true)), "{database:?}");
    }
}

#[test]
fn a_signed_volume_opens_with_its_key_only() {
    let directory = tempfile::tempdir().unwrap();
    let signer = Ed25519Signer::generate();
    let made = create(directory.path(), StoreKind::Sqlite, Some(signer.clone()));
    let volume = block_on(made).unwrap();
    let opened = |signer| mounted(directory.path(), StoreKind::Sqlite, volume, signer);
    assert!(matches!(opened(Some(signer)), Ok(true)));
    assert!(matches!(opened(Some(Ed25519Signer::generate())), Ok(false)));
}

#[test]
fn a_volume_a_store_never_made_is_unknown() {
    let directory = tempfile::tempdir().unwrap();
    block_on(create(directory.path(), StoreKind::Mdbx, None)).unwrap();
    let opened = mounted(directory.path(), StoreKind::Mdbx, unknown_volume(), None);
    assert!(matches!(opened, Err(OpenError::UnknownVolume)));
}

#[test]
fn a_mount_of_a_store_never_made_makes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let opened = mounted(directory.path(), StoreKind::Sqlite, unknown_volume(), None);
    assert!(matches!(opened, Err(OpenError::NoStore)));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
