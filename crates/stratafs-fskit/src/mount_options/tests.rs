use std::time::Duration;

use stratafs_tree::header::TreeVolumeId;

use super::*;

fn tree_volume(id: &[u8]) -> TreeVolumeId {
    TreeVolumeId::try_from(Bytes::copy_from_slice(id)).unwrap()
}

#[test]
fn a_volume_is_written_as_lowercase_hex_after_its_name() {
    let options = MountOptions {
        volume: Bytes::from_static(&[0xab, 0x01]),
        durability_cadence: None,
    };
    assert_eq!(options.to_string(), "volume=ab01");
}

#[test]
fn written_options_parse_back_to_their_volume() {
    let options = MountOptions {
        volume: tree_volume(&[0xcd; 16]),
        durability_cadence: Some(Duration::from_secs(5)),
    };
    assert_eq!(
        options
            .to_string()
            .parse::<MountOptions<TreeVolumeId>>()
            .unwrap(),
        options
    );
}

#[test]
fn parsing_accepts_uppercase_hex() {
    let options: MountOptions<Bytes> = "volume=AB01".parse().unwrap();
    assert_eq!(options.volume, Bytes::from_static(&[0xab, 0x01]));
}

#[test]
fn a_later_volume_overrides_an_earlier_one() {
    let options: MountOptions<Bytes> = "volume=ab01,volume=cd02".parse().unwrap();
    assert_eq!(options.volume, Bytes::from_static(&[0xcd, 0x02]));
}

#[test]
fn parsing_needs_a_volume() {
    assert!(matches!(
        "".parse::<MountOptions<Bytes>>(),
        Err(MountOptionsError::NoVolume)
    ));
}

#[test]
fn parsing_rejects_options_stratafs_does_not_take() {
    for option in ["rdonly", "volumes=ab01", "other=1"] {
        assert!(
            matches!(
                format!("volume=ab01,{option}").parse::<MountOptions<Bytes>>(),
                Err(MountOptionsError::Unknown(unknown)) if unknown == option
            ),
            "{option} parsed"
        );
    }
}

#[test]
fn parsing_rejects_anything_but_hex_for_the_volume() {
    for option in ["volume=xyz", "volume=ab01/", "volume=abc", "volume=ab01=cd"] {
        assert!(
            matches!(
                option.parse::<MountOptions<Bytes>>(),
                Err(MountOptionsError::NotHex(_))
            ),
            "{option} parsed"
        );
    }
}

#[test]
fn parsing_rejects_bytes_that_are_not_a_volume() {
    assert!(matches!(
        "volume=ab01".parse::<MountOptions<TreeVolumeId>>(),
        Err(MountOptionsError::NotAVolume(_))
    ));
}

#[test]
fn a_durability_cadence_is_written_in_seconds_after_the_volume() {
    let options = MountOptions {
        volume: Bytes::from_static(&[0xab, 0x01]),
        durability_cadence: Some(Duration::from_secs(30)),
    };
    assert_eq!(options.to_string(), "volume=ab01,durability_cadence=30");
}

#[test]
fn parsing_leaves_the_durability_cadence_out_unless_given() {
    let options: MountOptions<Bytes> = "volume=ab01".parse().unwrap();
    assert_eq!(options.durability_cadence, None);
}

#[test]
fn parsing_rejects_a_durability_cadence_that_is_not_whole_seconds_above_zero() {
    for cadence in ["0", "1.5", "-1", "", "30s"] {
        assert!(
            matches!(
                format!("volume=ab01,durability_cadence={cadence}").parse::<MountOptions<Bytes>>(),
                Err(MountOptionsError::NotSeconds(_))
            ),
            "{cadence} parsed"
        );
    }
}
