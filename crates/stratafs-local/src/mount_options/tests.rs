use super::*;

fn volume() -> TreeVolumeId {
    TreeVolumeId::try_from(Bytes::from(vec![0xcd; 16])).unwrap()
}

fn coordinator() -> Url {
    Url::parse("ws://127.0.0.1:7600").unwrap()
}

const VOLUME_HEX: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

#[test]
fn options_are_written_by_name_with_the_volume_in_hex() {
    let options = MountOptions {
        store: StoreKind::Sqlite,
        volume: volume(),
        consistency: ConsistencyKind::Eventual,
        coordinator: coordinator(),
        signing_key: None,
    };
    assert_eq!(
        options.to_string(),
        format!(
            "store=sqlite,volume={VOLUME_HEX},consistency=eventual,coordinator=ws://127.0.0.1:7600/"
        )
    );
}

#[test]
fn written_options_parse_back_with_their_signing_key() {
    let options = MountOptions {
        store: StoreKind::Mdbx,
        volume: volume(),
        consistency: ConsistencyKind::Strong,
        coordinator: coordinator(),
        signing_key: Some(Ed25519Signer::generate()),
    };
    assert_eq!(
        options.to_string().parse::<MountOptions>().unwrap(),
        options
    );
}

#[test]
fn a_later_option_overrides_an_earlier_one() {
    let options: MountOptions = format!(
        "store=redb,volume=ab01,volume={VOLUME_HEX},consistency=strong,consistency=eventual,coordinator=ws://a"
    )
    .parse()
    .unwrap();
    assert_eq!(options.volume, volume());
    assert_eq!(options.consistency, ConsistencyKind::Eventual);
}

#[test]
fn parsing_needs_a_store_a_volume_and_a_consistency() {
    let parsed = |options: &str| options.parse::<MountOptions>();
    assert!(matches!(parsed(""), Err(MountOptionsError::NoStore)));
    assert!(matches!(
        parsed("store=sqlite"),
        Err(MountOptionsError::NoVolume)
    ));
    let with_volume = format!("store=sqlite,volume={VOLUME_HEX}");
    assert!(matches!(
        parsed(&with_volume),
        Err(MountOptionsError::NoConsistency)
    ));
    assert!(matches!(
        parsed(&format!("{with_volume},consistency=strong")),
        Err(MountOptionsError::NoCoordinator)
    ));
}

#[test]
fn parsing_rejects_options_stratafs_does_not_take() {
    for option in ["rdonly", "volumes=ab01", "other=1", "durability_cadence=30"] {
        assert!(
            matches!(
                format!("store=sqlite,{option}").parse::<MountOptions>(),
                Err(MountOptionsError::Unknown(unknown)) if unknown == option
            ),
            "{option} parsed"
        );
    }
}

#[test]
fn parsing_rejects_values_that_are_not_what_their_option_names() {
    let with = |option: &str| {
        format!("store=sqlite,volume={VOLUME_HEX},consistency=strong,coordinator=ws://a,{option}")
            .parse::<MountOptions>()
    };
    assert!(matches!(
        with("store=ext4"),
        Err(MountOptionsError::NotAStore(_))
    ));
    for volume in ["volume=xyz", "volume=ab01", "volume=ab01=cd"] {
        assert!(
            matches!(with(volume), Err(MountOptionsError::NotAVolume(_))),
            "{volume}"
        );
    }
    for consistency in ["consistency=", "consistency=weak"] {
        let parsed = with(consistency);
        assert!(
            matches!(parsed, Err(MountOptionsError::NotAConsistency(_))),
            "{consistency}"
        );
    }
    assert!(matches!(
        with("coordinator=not a url"),
        Err(MountOptionsError::NotAUrl(_))
    ));
    assert!(matches!(
        with("signing-key=AAAA"),
        Err(MountOptionsError::NotAKey(_))
    ));
}
