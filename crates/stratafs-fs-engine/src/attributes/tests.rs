use std::convert::Infallible;

use futures_lite::future::block_on;
use stratafs_opendal::OpendalError;
use stratafs_serialized::FINGERPRINT_LENGTH;
use zerocopy::IntoBytes;

use super::*;
use crate::test_support::{InMemoryEngine, MetadataFailure, aborted, new_test_engine, set_clock};

fn getattr(engine: &InMemoryEngine, inode: Inode) -> Option<Attributes> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })))
        .map_err(aborted)
        .unwrap()
}

fn setattr(
    engine: &InMemoryEngine,
    inode: Inode,
    update: impl Fn(&mut SettableAttributes) + Send + Sync,
) -> Result<Attributes, SetattrError<MetadataFailure, Infallible, OpendalError>> {
    let update = &update;
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.setattr(inode, update).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn root_created_at(at: i64) -> Attributes {
    Attributes {
        settable: SettableAttributes {
            mode: Mode::OWNER_READ
                | Mode::OWNER_WRITE
                | Mode::OWNER_EXECUTE
                | Mode::GROUP_READ
                | Mode::GROUP_EXECUTE
                | Mode::OTHERS_READ
                | Mode::OTHERS_EXECUTE,
            ownership: Ownership {
                user_id: 0,
                group_id: 0,
            },
            size: 0,
            last_accessed_at: Timestamp(at),
            content_changed_at: Timestamp(at),
            created_at: Timestamp(at),
        },
        kind: FileType::Directory,
        hard_link_count: 2,
        attribute_changed_at: Timestamp(at),
        parent: Inode::ROOT,
        change_version: 0,
    }
}

const MISSING: Inode = Inode(5);

#[test]
fn a_new_engine_has_a_root_directory_with_default_attributes() {
    set_clock(5);
    let engine = new_test_engine();
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), root_created_at(5));
}

#[test]
fn creating_the_root_again_leaves_an_existing_root_alone() {
    set_clock(5);
    let engine = new_test_engine();
    let changed = setattr(&engine, Inode::ROOT, |settable| {
        settable.mode = Mode::OWNER_READ;
    })
    .unwrap();
    set_clock(9);
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.create_root_if_missing().await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), changed);
}

#[test]
fn getattr_of_a_missing_inode_is_none() {
    let engine = new_test_engine();
    assert_eq!(getattr(&engine, MISSING), None);
}

#[test]
fn setattr_applies_the_update_and_records_the_change() {
    set_clock(5);
    let engine = new_test_engine();
    set_clock(9);
    let changed = setattr(&engine, Inode::ROOT, |settable| {
        settable.mode = Mode::OWNER_READ | Mode::OWNER_WRITE | Mode::OWNER_EXECUTE;
        settable.ownership = Ownership {
            user_id: 501,
            group_id: 20,
        };
        settable.last_accessed_at = Timestamp(3);
    })
    .unwrap();
    let mut expected = root_created_at(5);
    expected.settable.mode = Mode::OWNER_READ | Mode::OWNER_WRITE | Mode::OWNER_EXECUTE;
    expected.settable.ownership = Ownership {
        user_id: 501,
        group_id: 20,
    };
    expected.settable.last_accessed_at = Timestamp(3);
    expected.attribute_changed_at = Timestamp(9);
    expected.change_version = 1;
    assert_eq!(changed, expected);
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), expected);
}

#[test]
fn setattr_of_a_missing_inode_is_a_stale_file_handle() {
    let engine = new_test_engine();
    assert!(matches!(
        setattr(&engine, MISSING, |_| {}),
        Err(SetattrError::StaleFileHandle)
    ));
}

#[test]
fn stored_attributes_are_the_fingerprint_then_each_field_in_little_endian() {
    let mut attributes = root_created_at(-3);
    attributes.kind = FileType::SymbolicLink;
    attributes.settable.size = 6;
    attributes.settable.ownership = Ownership {
        user_id: 501,
        group_id: 20,
    };
    attributes.hard_link_count = 1;
    attributes.settable.content_changed_at = Timestamp(8);
    attributes.attribute_changed_at = Timestamp(9);
    attributes.settable.created_at = Timestamp(10);
    attributes.parent = Inode(11);
    attributes.change_version = 12;
    let mut expected = vec![2];
    expected.extend_from_slice(&0o755u16.to_le_bytes());
    expected.extend_from_slice(&1u32.to_le_bytes());
    expected.extend_from_slice(&501u32.to_le_bytes());
    expected.extend_from_slice(&20u32.to_le_bytes());
    expected.extend_from_slice(&6u64.to_le_bytes());
    expected.extend_from_slice(&(-3i64).to_le_bytes());
    expected.extend_from_slice(&8i64.to_le_bytes());
    expected.extend_from_slice(&9i64.to_le_bytes());
    expected.extend_from_slice(&10i64.to_le_bytes());
    expected.extend_from_slice(&11u64.to_le_bytes());
    expected.extend_from_slice(&12u64.to_le_bytes());
    let stored = Bytes::from(&StoredAttributes::from(&attributes));
    assert_eq!(stored[FINGERPRINT_LENGTH..], expected[..]);
    assert_eq!(
        Attributes::try_from(StoredAttributes::view(&stored).unwrap()).unwrap(),
        attributes
    );
}

#[test]
fn file_types_are_stored_as_their_variant_index() {
    for (kind, stored) in [
        (FileType::File, 0),
        (FileType::Directory, 1),
        (FileType::SymbolicLink, 2),
    ] {
        let mut attributes = root_created_at(0);
        attributes.kind = kind;
        assert_eq!(StoredAttributes::from(&attributes).kind, stored);
    }
}

#[test]
fn decoding_rejects_unknown_file_types_and_mode_bits() {
    let mut stored = StoredAttributes::from(&root_created_at(0));
    stored.kind = 3;
    assert!(matches!(
        Attributes::try_from(&stored),
        Err(CorruptValue::UnknownFileType)
    ));
    let mut stored = StoredAttributes::from(&root_created_at(0));
    stored.mode = U16::new(0o10000);
    assert!(matches!(
        Attributes::try_from(&stored),
        Err(CorruptValue::UnknownModeBits)
    ));
    let stored = Bytes::from(&StoredAttributes::from(&root_created_at(0)));
    assert!(matches!(
        StoredAttributes::view(&stored[..stored.len() - 1]),
        Err(stratafs_serialized::ViewError::WrongLength)
    ));
    assert_eq!(
        StoredAttributes::from(&root_created_at(0)).as_bytes().len(),
        71
    );
}
