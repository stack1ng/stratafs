use stratafs_tree::header::TreeVolumeId;

use super::*;

fn tree_volume(id: &[u8]) -> TreeVolumeId {
    TreeVolumeId::try_from(Bytes::copy_from_slice(id)).unwrap()
}

#[test]
fn a_volume_is_written_as_lowercase_hex_after_the_scheme() {
    let url = VolumeUrl {
        volume: Bytes::from_static(&[0xab, 0x01]),
    };
    assert_eq!(url.to_string(), "stratavolume://ab01");
}

#[test]
fn a_written_url_parses_back_to_its_volume() {
    let url = VolumeUrl {
        volume: tree_volume(&[0xcd; 16]),
    };
    assert_eq!(
        url.to_string().parse::<VolumeUrl<TreeVolumeId>>().unwrap(),
        url
    );
}

#[test]
fn parsing_accepts_uppercase_hex() {
    let url: VolumeUrl<Bytes> = "stratavolume://AB01".parse().unwrap();
    assert_eq!(url.volume, Bytes::from_static(&[0xab, 0x01]));
}

#[test]
fn parsing_rejects_another_scheme() {
    assert!(matches!(
        "https://ab01".parse::<VolumeUrl<Bytes>>(),
        Err(VolumeUrlError::WrongScheme)
    ));
}

#[test]
fn parsing_rejects_anything_but_hex_after_the_scheme() {
    for url in [
        "stratavolume://xyz",
        "stratavolume://ab01/",
        "stratavolume://ab01?x",
        "stratavolume://abc",
    ] {
        assert!(
            matches!(
                url.parse::<VolumeUrl<Bytes>>(),
                Err(VolumeUrlError::NotHex(_))
            ),
            "{url} parsed"
        );
    }
}

#[test]
fn parsing_rejects_bytes_that_are_not_a_volume() {
    assert!(matches!(
        "stratavolume://ab01".parse::<VolumeUrl<TreeVolumeId>>(),
        Err(VolumeUrlError::NotAVolume(_))
    ));
}
