use std::convert::Infallible;

use futures_lite::future::block_on;
use stratafs_opendal::OpendalError;
use stratafs_serialized::FINGERPRINT_LENGTH;
use stratafs_store::content::ContentHash;
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

fn getxattr(
    engine: &InMemoryEngine,
    inode: Inode,
    name: &[u8],
) -> Result<Option<Bytes>, GetxattrError<MetadataFailure, OpendalError>> {
    block_on(engine.read_transact(|txn| {
        Box::pin(async move {
            let value = txn.getxattr(inode, name).await?;
            Ok(value.map(|mut value| value.copy_to_bytes(value.remaining())))
        })
    }))
    .map_err(aborted)
}

fn listxattr(
    engine: &InMemoryEngine,
    inode: Inode,
) -> Result<Vec<Bytes>, ListxattrError<MetadataFailure, OpendalError>> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.listxattr(inode).await })))
        .map_err(aborted)
}

fn setxattr(
    engine: &InMemoryEngine,
    inode: Inode,
    name: &[u8],
    value: &[u8],
    mode: SetXattrMode,
) -> Result<(), SetxattrError<MetadataFailure, Infallible, OpendalError>> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.setxattr(inode, name, value, mode).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn removexattr(
    engine: &InMemoryEngine,
    inode: Inode,
    name: &[u8],
) -> Result<(), RemovexattrError<MetadataFailure, Infallible, OpendalError>> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.removexattr(inode, name).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn insert_inode(engine: &InMemoryEngine, inode: Inode, attributes: &Attributes) {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.write_attributes(inode, attributes);
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
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
fn an_extended_attribute_reads_back_and_its_write_records_the_change() {
    set_clock(5);
    let engine = new_test_engine();
    set_clock(9);
    setxattr(
        &engine,
        Inode::ROOT,
        b"user.a",
        b"value",
        SetXattrMode::CreateOrReplace,
    )
    .unwrap();
    assert_eq!(
        getxattr(&engine, Inode::ROOT, b"user.a").unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(getxattr(&engine, Inode::ROOT, b"user.b").unwrap(), None);
    let mut expected = root_created_at(5);
    expected.attribute_changed_at = Timestamp(9);
    expected.change_version = 1;
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), expected);
}

#[test]
fn a_large_extended_attribute_spills_to_the_content_store_and_reads_back() {
    let engine = new_test_engine();
    let value: Vec<u8> = (0..4000).map(|index| index as u8).collect();
    setxattr(
        &engine,
        Inode::ROOT,
        b"user.large",
        &value,
        SetXattrMode::Create,
    )
    .unwrap();
    assert_eq!(
        getxattr(&engine, Inode::ROOT, b"user.large").unwrap(),
        Some(Bytes::from(value.clone()))
    );
    assert!(
        block_on(
            engine
                .content
                .get(&ContentHash::of(&value), 0, value.len() as u64)
        )
        .is_ok()
    );
}

#[test]
fn listxattr_returns_the_inodes_own_names_in_byte_order() {
    let engine = new_test_engine();
    insert_inode(&engine, Inode(1), &root_created_at(0));
    let mut names: Vec<Vec<u8>> = vec![
        b"user.b".to_vec(),
        b"user.a".to_vec(),
        vec![0],
        vec![0, 0],
        vec![1, 2],
        vec![0xff; LARGEST_NAME_LENGTH],
    ];
    for name in &names {
        setxattr(&engine, Inode::ROOT, name, b"x", SetXattrMode::Create).unwrap();
    }
    setxattr(&engine, Inode(1), b"user.other", b"x", SetXattrMode::Create).unwrap();
    names.sort();
    assert_eq!(listxattr(&engine, Inode::ROOT).unwrap(), names);
    assert_eq!(
        listxattr(&engine, Inode(1)).unwrap(),
        vec![Bytes::from_static(b"user.other")]
    );
}

#[test]
fn setxattr_modes_require_the_name_to_exist_or_not() {
    let engine = new_test_engine();
    assert!(matches!(
        setxattr(&engine, Inode::ROOT, b"user.a", b"1", SetXattrMode::Replace),
        Err(SetxattrError::AttributeNotFound)
    ));
    setxattr(&engine, Inode::ROOT, b"user.a", b"1", SetXattrMode::Create).unwrap();
    assert!(matches!(
        setxattr(&engine, Inode::ROOT, b"user.a", b"2", SetXattrMode::Create),
        Err(SetxattrError::AlreadyExists)
    ));
    setxattr(&engine, Inode::ROOT, b"user.a", b"2", SetXattrMode::Replace).unwrap();
    assert_eq!(
        getxattr(&engine, Inode::ROOT, b"user.a").unwrap(),
        Some(Bytes::from_static(b"2"))
    );
    setxattr(
        &engine,
        Inode::ROOT,
        b"user.a",
        b"3",
        SetXattrMode::CreateOrReplace,
    )
    .unwrap();
    assert_eq!(
        getxattr(&engine, Inode::ROOT, b"user.a").unwrap(),
        Some(Bytes::from_static(b"3"))
    );
}

#[test]
fn removexattr_removes_the_name_and_records_the_change() {
    set_clock(5);
    let engine = new_test_engine();
    setxattr(&engine, Inode::ROOT, b"user.a", b"1", SetXattrMode::Create).unwrap();
    set_clock(9);
    removexattr(&engine, Inode::ROOT, b"user.a").unwrap();
    assert_eq!(getxattr(&engine, Inode::ROOT, b"user.a").unwrap(), None);
    assert_eq!(
        listxattr(&engine, Inode::ROOT).unwrap(),
        Vec::<Bytes>::new()
    );
    let attributes = getattr(&engine, Inode::ROOT).unwrap();
    assert_eq!(attributes.attribute_changed_at, Timestamp(9));
    assert_eq!(attributes.change_version, 2);
    assert!(matches!(
        removexattr(&engine, Inode::ROOT, b"user.a"),
        Err(RemovexattrError::AttributeNotFound)
    ));
}

#[test]
fn extended_attribute_names_must_be_one_to_the_largest_name_length_bytes() {
    let engine = new_test_engine();
    let longest = vec![b'a'; LARGEST_NAME_LENGTH];
    let too_long = vec![b'a'; LARGEST_NAME_LENGTH + 1];
    setxattr(&engine, Inode::ROOT, &longest, b"1", SetXattrMode::Create).unwrap();
    assert!(matches!(
        setxattr(&engine, Inode::ROOT, b"", b"1", SetXattrMode::Create),
        Err(SetxattrError::InvalidInput)
    ));
    assert!(matches!(
        setxattr(&engine, Inode::ROOT, &too_long, b"1", SetXattrMode::Create),
        Err(SetxattrError::FileNameTooLong)
    ));
    assert!(matches!(
        getxattr(&engine, Inode::ROOT, b""),
        Err(GetxattrError::InvalidInput)
    ));
    assert!(matches!(
        getxattr(&engine, Inode::ROOT, &too_long),
        Err(GetxattrError::FileNameTooLong)
    ));
    assert!(matches!(
        removexattr(&engine, Inode::ROOT, b""),
        Err(RemovexattrError::InvalidInput)
    ));
    assert!(matches!(
        removexattr(&engine, Inode::ROOT, &too_long),
        Err(RemovexattrError::FileNameTooLong)
    ));
}

#[test]
fn extended_attribute_operations_on_a_missing_inode_are_stale_file_handles() {
    let engine = new_test_engine();
    assert!(matches!(
        getxattr(&engine, MISSING, b"user.a"),
        Err(GetxattrError::StaleFileHandle)
    ));
    assert!(matches!(
        listxattr(&engine, MISSING),
        Err(ListxattrError::StaleFileHandle)
    ));
    assert!(matches!(
        setxattr(
            &engine,
            MISSING,
            b"user.a",
            b"1",
            SetXattrMode::CreateOrReplace
        ),
        Err(SetxattrError::StaleFileHandle)
    ));
    assert!(matches!(
        removexattr(&engine, MISSING, b"user.a"),
        Err(RemovexattrError::StaleFileHandle)
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
