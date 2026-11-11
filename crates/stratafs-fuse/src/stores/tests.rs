use super::*;

#[test]
fn a_volume_id_reads_back_from_its_hex() {
    let id = TreeVolumeId::try_from(Bytes::from(vec![7; 16])).unwrap();
    assert_eq!(volume_id(&hex::encode(Bytes::from(id))), Ok(id));
}

#[test]
fn a_volume_id_that_is_not_hex_or_not_a_volumes_is_refused() {
    assert!(volume_id("not hex").is_err());
    assert!(volume_id("0707").is_err());
}

#[test]
fn a_cadence_is_a_whole_number_of_seconds_above_zero() {
    assert_eq!(seconds("30"), Ok(Duration::from_secs(30)));
    assert!(seconds("0").is_err());
    assert!(seconds("1.5").is_err());
}

// reopened makes a volume in a fresh store of database D, and opens it again as a mount would.
fn reopened<D: Database>(database: StoreKind) {
    let directory = tempfile::tempdir().unwrap();
    let volume = create(&CreateArguments {
        store: directory.path().to_owned(),
        database,
    })
    .unwrap();
    let pool = SpawnerPool::builder().pool_size(1).create().unwrap();
    let runtime: Arc<dyn Runtime> = Arc::new(ThreadPoolRuntime(pool));
    block_on(async {
        let tree = open_tree::<D>(directory.path(), Arc::clone(&runtime))
            .await
            .unwrap();
        engine::<D, Eventual>(
            &tree,
            volume,
            None,
            no_coordinator(),
            Eventual::default(),
            NoKernel,
            runtime,
        )
        .await
        .unwrap();
    });
}

#[test]
fn a_volume_made_in_an_mdbx_store_opens_again() {
    reopened::<Mdbx>(StoreKind::Mdbx);
}

#[test]
fn a_volume_made_in_a_redb_store_opens_again() {
    reopened::<Redb>(StoreKind::Redb);
}

#[test]
fn a_volume_made_in_a_sqlite_store_opens_again() {
    reopened::<Sqlite>(StoreKind::Sqlite);
}

#[test]
fn a_volume_a_store_never_made_is_unknown() {
    let directory = tempfile::tempdir().unwrap();
    let pool = SpawnerPool::builder().pool_size(1).create().unwrap();
    let runtime: Arc<dyn Runtime> = Arc::new(ThreadPoolRuntime(pool));
    let opened = block_on(async {
        let tree = open_tree::<Mdbx>(directory.path(), Arc::clone(&runtime))
            .await
            .unwrap();
        engine::<Mdbx, Eventual>(
            &tree,
            TreeVolumeId::try_from(Bytes::from(vec![7; 16])).unwrap(),
            None,
            no_coordinator(),
            Eventual::default(),
            NoKernel,
            runtime,
        )
        .await
    });
    assert!(matches!(opened, Err(OpenError::UnknownVolume)));
}

#[test]
fn a_mount_of_a_store_never_made_makes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let arguments = MountArguments {
        store: directory.path().to_owned(),
        mount_point: directory.path().join("mounted"),
        volume: TreeVolumeId::try_from(Bytes::from(vec![7; 16])).unwrap(),
        database: StoreKind::Sqlite,
        consistency: ConsistencyKind::Eventual,
        coordinator: "ws://127.0.0.1:7600".to_owned(),
        durability_limit: DurabilityLimit::Power,
        durability_cadence: Duration::from_secs(30),
        allow_other: false,
    };
    let pool = SpawnerPool::builder().pool_size(1).create().unwrap();
    let mounted = mount(&arguments, Arc::new(ThreadPoolRuntime(pool)));
    assert!(matches!(mounted, Err(MountError::Open(OpenError::NoStore))));
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
