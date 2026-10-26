use std::convert::Infallible;

use futures_lite::future::block_on;
use stratafs_object_store::ObjectStoreError;
use stratafs_serialized::FINGERPRINT_LENGTH;
use zerocopy::IntoBytes;

use super::*;
use crate::test_support::{
    InMemoryEngine, MetadataFailure, aborted, new_test_engine, restrict, set_clock,
};

fn getattr(engine: &InMemoryEngine, inode: Inode) -> Option<Attributes> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })))
        .map_err(aborted)
        .unwrap()
}

fn setattr(
    engine: &InMemoryEngine,
    inode: Inode,
    update: impl Fn(&mut SettableAttributes) + Send + Sync,
) -> Result<Attributes, SetattrError<MetadataFailure, Infallible, ObjectStoreError>> {
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
            restrictions: Restrictions::empty(),
            hints: Hints::empty(),
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
fn a_restricted_item_only_changes_its_restrictions_and_hints() {
    let engine = new_test_engine();
    for restriction in Restrictions::all() {
        restrict(&engine, Inode::ROOT, restriction);
        let refused: [&(dyn Fn(&mut SettableAttributes) + Send + Sync); 5] = [
            &|settable| settable.mode = Mode::OWNER_READ,
            &|settable| settable.ownership.user_id = 501,
            &|settable| settable.size = 1,
            &|settable| settable.last_accessed_at = Timestamp(3),
            &|settable| {
                settable.restrictions = Restrictions::empty();
                settable.mode = Mode::OWNER_READ;
            },
        ];
        for update in refused {
            assert!(
                matches!(
                    setattr(&engine, Inode::ROOT, update),
                    Err(SetattrError::OperationNotPermitted)
                ),
                "{restriction:?}"
            );
        }
        let hidden = setattr(&engine, Inode::ROOT, |settable| {
            settable.hints = Hints::HIDDEN
        })
        .unwrap();
        assert_eq!(hidden.settable.hints, Hints::HIDDEN);
        let removed = setattr(&engine, Inode::ROOT, |settable| {
            settable.restrictions = Restrictions::empty();
            settable.hints = Hints::empty();
        })
        .unwrap();
        assert_eq!(removed.settable.restrictions, Restrictions::empty());
        setattr(&engine, Inode::ROOT, |settable| {
            settable.last_accessed_at = Timestamp(4)
        })
        .unwrap();
    }
}

#[test]
fn an_unrestricted_item_takes_a_restriction_together_with_other_changes() {
    for restriction in Restrictions::all().iter() {
        let engine = new_test_engine();
        let restricted = setattr(&engine, Inode::ROOT, |settable| {
            settable.restrictions = restriction;
            settable.mode = Mode::OWNER_READ;
        })
        .unwrap();
        assert_eq!(restricted.settable.restrictions, restriction);
        assert_eq!(restricted.settable.mode, Mode::OWNER_READ);
    }
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
    attributes.settable.restrictions =
        Restrictions::OWNER_IMMUTABLE | Restrictions::ADMINISTRATOR_APPEND_ONLY;
    attributes.settable.hints = Hints::HIDDEN;
    let mut expected = vec![2];
    expected.extend_from_slice(&0o755u16.to_le_bytes());
    expected.extend_from_slice(&[0b1001, 1]);
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
    let mut stored = StoredAttributes::from(&root_created_at(0));
    stored.restrictions = 1 << 4;
    assert!(matches!(
        Attributes::try_from(&stored),
        Err(CorruptValue::UnknownRestrictionBits)
    ));
    let mut stored = StoredAttributes::from(&root_created_at(0));
    stored.hints = 1 << 1;
    assert!(matches!(
        Attributes::try_from(&stored),
        Err(CorruptValue::UnknownHintBits)
    ));
    let stored = Bytes::from(&StoredAttributes::from(&root_created_at(0)));
    assert!(matches!(
        StoredAttributes::view(&stored[..stored.len() - 1]),
        Err(stratafs_serialized::ViewError::WrongLength)
    ));
    assert_eq!(
        StoredAttributes::from(&root_created_at(0)).as_bytes().len(),
        73
    );
}
