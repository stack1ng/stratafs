use std::convert::Infallible;

use futures_lite::future::block_on;
use stratafs_object_store::ObjectStoreError;
use stratafs_store::content::ContentHash;

use super::*;
use crate::attributes::{Attributes, Restrictions};
use crate::test_support::{
    InMemoryEngine, MetadataFailure, aborted, new_test_engine, restrict, set_clock,
};

fn getattr(engine: &InMemoryEngine, inode: Inode) -> Option<Attributes> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })))
        .map_err(aborted)
        .unwrap()
}

fn get_annotation(
    engine: &InMemoryEngine,
    inode: Inode,
    key: AnnotationKey<'_>,
) -> Result<Option<Bytes>, GetAnnotationError<MetadataFailure, ObjectStoreError>> {
    block_on(engine.read_transact(|txn| {
        Box::pin(async move {
            let value = txn.get_annotation(inode, key).await?;
            Ok(value.map(|mut value| value.copy_to_bytes(value.remaining())))
        })
    }))
    .map_err(aborted)
}

fn list_annotations(
    engine: &InMemoryEngine,
    inode: Inode,
    scope: AnnotationScope<'_>,
) -> Result<Vec<Bytes>, ListAnnotationsError<MetadataFailure, ObjectStoreError>> {
    block_on(
        engine
            .read_transact(|txn| Box::pin(async move { txn.list_annotations(inode, scope).await })),
    )
    .map_err(aborted)
}

fn set_annotation(
    engine: &InMemoryEngine,
    inode: Inode,
    key: AnnotationKey<'_>,
    value: &[u8],
    mode: SetAnnotationMode,
) -> Result<Attributes, SetAnnotationError<MetadataFailure, Infallible, ObjectStoreError>> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.set_annotation(inode, key, value, mode).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn remove_annotation(
    engine: &InMemoryEngine,
    inode: Inode,
    key: AnnotationKey<'_>,
) -> Result<Attributes, RemoveAnnotationError<MetadataFailure, Infallible, ObjectStoreError>> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.remove_annotation(inode, key).await;
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

const MISSING: Inode = Inode(5);

const MACOS: &[u8] = b"macos";

fn portable(name: &[u8]) -> AnnotationKey<'_> {
    AnnotationKey::Portable(name)
}

fn macos(name: &[u8]) -> AnnotationKey<'_> {
    AnnotationKey::Platform {
        platform: MACOS,
        name,
    }
}

#[test]
fn an_annotation_reads_back_and_its_write_records_the_change() {
    set_clock(5);
    let engine = new_test_engine();
    set_clock(9);
    set_annotation(
        &engine,
        Inode::ROOT,
        portable(b"a"),
        b"value",
        SetAnnotationMode::CreateOrReplace,
    )
    .unwrap();
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, portable(b"a")).unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, portable(b"b")).unwrap(),
        None
    );
    let attributes = getattr(&engine, Inode::ROOT).unwrap();
    assert_eq!(attributes.attribute_changed_at, Timestamp(9));
    assert_eq!(attributes.change_version.number, 1);
}

#[test]
fn a_portable_and_a_platform_annotation_of_the_same_name_are_different_keys() {
    let engine = new_test_engine();
    for (key, value) in [
        (portable(b"a"), &b"portable"[..]),
        (macos(b"a"), b"macos"),
        (
            AnnotationKey::Platform {
                platform: b"linux",
                name: b"a",
            },
            b"linux",
        ),
    ] {
        set_annotation(&engine, Inode::ROOT, key, value, SetAnnotationMode::Create).unwrap();
    }
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, portable(b"a")).unwrap(),
        Some(Bytes::from_static(b"portable"))
    );
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, macos(b"a")).unwrap(),
        Some(Bytes::from_static(b"macos"))
    );
}

#[test]
fn a_large_annotation_spills_to_the_content_store_and_reads_back() {
    let engine = new_test_engine();
    let value: Vec<u8> = (0..4000).map(|index| index as u8).collect();
    set_annotation(
        &engine,
        Inode::ROOT,
        macos(b"large"),
        &value,
        SetAnnotationMode::Create,
    )
    .unwrap();
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, macos(b"large")).unwrap(),
        Some(Bytes::from(value.clone()))
    );
    assert!(
        block_on(
            engine
                .content()
                .get(&ContentHash::of(&value), 0, value.len() as u64)
        )
        .is_ok()
    );
}

#[test]
fn list_annotations_returns_the_scopes_own_names_in_byte_order() {
    let engine = new_test_engine();
    insert_inode(&engine, Inode(1), &getattr(&engine, Inode::ROOT).unwrap());
    let mut names: Vec<Vec<u8>> = vec![
        b"b".to_vec(),
        b"a".to_vec(),
        vec![0],
        vec![0, 0],
        vec![1, 2],
        vec![0xff; LARGEST_NAME_LENGTH],
    ];
    for name in &names {
        set_annotation(
            &engine,
            Inode::ROOT,
            portable(name),
            b"x",
            SetAnnotationMode::Create,
        )
        .unwrap();
        set_annotation(
            &engine,
            Inode::ROOT,
            macos(name),
            b"x",
            SetAnnotationMode::Create,
        )
        .unwrap();
    }
    for platform in [&b"maco"[..], b"macos\0", b"macos\x01", b"macosx"] {
        let key = AnnotationKey::Platform {
            platform,
            name: b"neighbour",
        };
        set_annotation(&engine, Inode::ROOT, key, b"x", SetAnnotationMode::Create).unwrap();
    }
    set_annotation(
        &engine,
        Inode(1),
        portable(b"other"),
        b"x",
        SetAnnotationMode::Create,
    )
    .unwrap();
    set_annotation(
        &engine,
        Inode(1),
        macos(b"other"),
        b"x",
        SetAnnotationMode::Create,
    )
    .unwrap();
    names.sort();
    assert_eq!(
        list_annotations(&engine, Inode::ROOT, AnnotationScope::Portable).unwrap(),
        names
    );
    assert_eq!(
        list_annotations(&engine, Inode::ROOT, AnnotationScope::Platform(MACOS)).unwrap(),
        names
    );
    assert_eq!(
        list_annotations(&engine, Inode(1), AnnotationScope::Portable).unwrap(),
        vec![Bytes::from_static(b"other")]
    );
    assert_eq!(
        list_annotations(&engine, Inode::ROOT, AnnotationScope::Platform(b"linux")).unwrap(),
        Vec::<Bytes>::new()
    );
}

#[test]
fn set_annotation_modes_require_the_key_to_exist_or_not() {
    let engine = new_test_engine();
    assert!(matches!(
        set_annotation(
            &engine,
            Inode::ROOT,
            portable(b"a"),
            b"1",
            SetAnnotationMode::Replace
        ),
        Err(SetAnnotationError::AttributeNotFound)
    ));
    set_annotation(
        &engine,
        Inode::ROOT,
        portable(b"a"),
        b"1",
        SetAnnotationMode::Create,
    )
    .unwrap();
    assert!(matches!(
        set_annotation(
            &engine,
            Inode::ROOT,
            portable(b"a"),
            b"2",
            SetAnnotationMode::Create
        ),
        Err(SetAnnotationError::AlreadyExists)
    ));
    set_annotation(
        &engine,
        Inode::ROOT,
        portable(b"a"),
        b"2",
        SetAnnotationMode::Replace,
    )
    .unwrap();
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, portable(b"a")).unwrap(),
        Some(Bytes::from_static(b"2"))
    );
    set_annotation(
        &engine,
        Inode::ROOT,
        portable(b"a"),
        b"3",
        SetAnnotationMode::CreateOrReplace,
    )
    .unwrap();
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, portable(b"a")).unwrap(),
        Some(Bytes::from_static(b"3"))
    );
}

#[test]
fn remove_annotation_removes_the_key_and_records_the_change() {
    set_clock(5);
    let engine = new_test_engine();
    set_annotation(
        &engine,
        Inode::ROOT,
        macos(b"a"),
        b"1",
        SetAnnotationMode::Create,
    )
    .unwrap();
    set_annotation(
        &engine,
        Inode::ROOT,
        portable(b"a"),
        b"1",
        SetAnnotationMode::Create,
    )
    .unwrap();
    set_clock(9);
    let removed = remove_annotation(&engine, Inode::ROOT, macos(b"a")).unwrap();
    assert_eq!(
        get_annotation(&engine, Inode::ROOT, macos(b"a")).unwrap(),
        None
    );
    assert_eq!(
        list_annotations(&engine, Inode::ROOT, AnnotationScope::Platform(MACOS)).unwrap(),
        Vec::<Bytes>::new()
    );
    assert_eq!(
        list_annotations(&engine, Inode::ROOT, AnnotationScope::Portable).unwrap(),
        vec![Bytes::from_static(b"a")]
    );
    let attributes = getattr(&engine, Inode::ROOT).unwrap();
    assert_eq!(attributes.attribute_changed_at, Timestamp(9));
    assert_eq!(attributes.change_version.number, 3);
    assert_eq!(removed, attributes);
    assert!(matches!(
        remove_annotation(&engine, Inode::ROOT, macos(b"a")),
        Err(RemoveAnnotationError::AttributeNotFound)
    ));
}

#[test]
fn annotation_names_must_be_one_to_the_largest_name_length_bytes() {
    let engine = new_test_engine();
    let longest = vec![b'a'; LARGEST_NAME_LENGTH];
    let too_long = vec![b'a'; LARGEST_NAME_LENGTH + 1];
    for key in [portable, macos] {
        set_annotation(
            &engine,
            Inode::ROOT,
            key(&longest),
            b"1",
            SetAnnotationMode::Create,
        )
        .unwrap();
        assert!(matches!(
            set_annotation(
                &engine,
                Inode::ROOT,
                key(b""),
                b"1",
                SetAnnotationMode::Create
            ),
            Err(SetAnnotationError::InvalidInput)
        ));
        assert!(matches!(
            set_annotation(
                &engine,
                Inode::ROOT,
                key(&too_long),
                b"1",
                SetAnnotationMode::Create
            ),
            Err(SetAnnotationError::FileNameTooLong)
        ));
        assert!(matches!(
            get_annotation(&engine, Inode::ROOT, key(b"")),
            Err(GetAnnotationError::InvalidInput)
        ));
        assert!(matches!(
            get_annotation(&engine, Inode::ROOT, key(&too_long)),
            Err(GetAnnotationError::FileNameTooLong)
        ));
        assert!(matches!(
            remove_annotation(&engine, Inode::ROOT, key(b"")),
            Err(RemoveAnnotationError::InvalidInput)
        ));
        assert!(matches!(
            remove_annotation(&engine, Inode::ROOT, key(&too_long)),
            Err(RemoveAnnotationError::FileNameTooLong)
        ));
    }
}

#[test]
fn annotation_operations_on_a_missing_inode_are_stale_file_handles() {
    let engine = new_test_engine();
    assert!(matches!(
        get_annotation(&engine, MISSING, portable(b"a")),
        Err(GetAnnotationError::StaleFileHandle)
    ));
    assert!(matches!(
        list_annotations(&engine, MISSING, AnnotationScope::Portable),
        Err(ListAnnotationsError::StaleFileHandle)
    ));
    assert!(matches!(
        set_annotation(
            &engine,
            MISSING,
            portable(b"a"),
            b"1",
            SetAnnotationMode::CreateOrReplace
        ),
        Err(SetAnnotationError::StaleFileHandle)
    ));
    assert!(matches!(
        remove_annotation(&engine, MISSING, portable(b"a")),
        Err(RemoveAnnotationError::StaleFileHandle)
    ));
}

#[test]
fn a_restricted_item_refuses_annotation_changes() {
    let engine = new_test_engine();
    set_annotation(
        &engine,
        Inode::ROOT,
        portable(b"a"),
        b"1",
        SetAnnotationMode::Create,
    )
    .unwrap();
    for restriction in Restrictions::all() {
        restrict(&engine, Inode::ROOT, restriction);
        assert!(
            matches!(
                set_annotation(
                    &engine,
                    Inode::ROOT,
                    portable(b"b"),
                    b"1",
                    SetAnnotationMode::Create
                ),
                Err(SetAnnotationError::OperationNotPermitted)
            ),
            "{restriction:?}"
        );
        assert!(
            matches!(
                remove_annotation(&engine, Inode::ROOT, portable(b"a")),
                Err(RemoveAnnotationError::OperationNotPermitted)
            ),
            "{restriction:?}"
        );
        assert_eq!(
            get_annotation(&engine, Inode::ROOT, portable(b"a")).unwrap(),
            Some(Bytes::from_static(b"1"))
        );
    }
}
