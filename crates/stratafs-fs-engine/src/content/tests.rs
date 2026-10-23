use futures_lite::future::block_on;
use stratafs_opendal::OpendalError;
use stratafs_redb::RedbFailure;

use super::*;
use crate::Timestamp;
use crate::attributes::{Mode, Ownership, SettableAttributes};
use crate::directories::NewItem;
use crate::test_support::{InMemoryEngine, aborted, new_test_engine};

fn stored(extent: &ExtentValue) -> Bytes {
    let mut stored = extent.to_stored();
    stored.copy_to_bytes(stored.remaining())
}

#[test]
fn an_extent_is_stored_as_its_start_and_offset_then_its_spillable_value() {
    let extent = ExtentValue {
        start: 0x0102,
        offset: 0x0304,
        content: SpillableValue::Inline(Bytes::from_static(b"abc")),
    };
    let mut expected = Vec::new();
    expected.extend_from_slice(&0x0102u64.to_le_bytes());
    expected.extend_from_slice(&0x0304u64.to_le_bytes());
    let mut content = extent.content.to_stored();
    expected.extend_from_slice(&content.copy_to_bytes(content.remaining()));
    assert_eq!(stored(&extent)[..], expected[..]);
}

#[test]
fn stored_extents_decode_to_the_extent_that_was_stored() {
    for extent in [
        ExtentValue {
            start: 0,
            offset: 0,
            content: SpillableValue::Inline(Bytes::from_static(b"inline")),
        },
        ExtentValue {
            start: 4096,
            offset: 40,
            content: SpillableValue::Overflow {
                hash: ContentHash::of(b"blob"),
                length: 1 << 20,
            },
        },
    ] {
        assert_eq!(ExtentValue::from_stored(stored(&extent)).unwrap(), extent);
    }
}

#[test]
fn decoding_rejects_a_header_that_is_too_short_or_a_corrupt_spillable_value() {
    assert!(matches!(
        ExtentValue::from_stored(Bytes::from_static(&[0; 15])),
        Err(CorruptValue::WrongLength)
    ));
    let mut corrupt = vec![0; 16];
    corrupt.push(9);
    assert!(matches!(
        ExtentValue::from_stored(Bytes::from(corrupt)),
        Err(CorruptValue::UnknownSpillableKind)
    ));
}

fn write_extent(engine: &InMemoryEngine, inode: Inode, end: u64, extent: &ExtentValue) {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            txn.metadata
                .set(FsKey::new_extent(inode, end).into(), extent.to_stored())
                .await
        })
    }))
    .unwrap();
}

fn file_of_size(engine: &InMemoryEngine, inode: Inode, size: u64) {
    let attributes = Attributes {
        settable: SettableAttributes {
            mode: Mode::empty(),
            ownership: Ownership {
                user_id: 0,
                group_id: 0,
            },
            size,
            last_accessed_at: Timestamp(0),
            content_changed_at: Timestamp(0),
            created_at: Timestamp(0),
        },
        kind: FileType::File,
        hard_link_count: 1,
        allocated_size: 0,
        attribute_changed_at: Timestamp(0),
        parent: Inode::ROOT,
        change_version: 0,
    };
    block_on(engine.transact(|mut txn| {
        let attributes = attributes.clone();
        Box::pin(async move { txn.write_attributes(inode, &attributes).await })
    }))
    .unwrap();
}

fn three_extents(engine: &InMemoryEngine, inode: Inode) -> ContentHash {
    let blob = ContentHash::of(b"blob");
    write_extent(
        engine,
        inode,
        10,
        &ExtentValue {
            start: 0,
            offset: 0,
            content: SpillableValue::Inline(Bytes::from_static(b"0123456789")),
        },
    );
    write_extent(
        engine,
        inode,
        30,
        &ExtentValue {
            start: 10,
            offset: 5,
            content: SpillableValue::Overflow {
                hash: blob,
                length: 100,
            },
        },
    );
    write_extent(
        engine,
        inode,
        40,
        &ExtentValue {
            start: 30,
            offset: 0,
            content: SpillableValue::Inline(Bytes::from_static(b"abcdefghij")),
        },
    );
    blob
}

fn plan(
    engine: &InMemoryEngine,
    inode: Inode,
    offset: u64,
    size: u64,
) -> Result<ContentLoadPlan, ReadError<RedbFailure, OpendalError>> {
    block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.read(inode, offset, size).await })),
    )
    .map_err(aborted)
}

fn read(engine: &InMemoryEngine, inode: Inode, offset: u64, size: u64) -> (u64, Vec<ContentPiece>) {
    let plan = plan(engine, inode, offset, size).unwrap();
    assert_eq!(plan.offset, offset);
    (plan.size, plan.pieces)
}

#[test]
fn read_plans_the_visible_part_of_each_overlapping_extent() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 40);
    let blob = three_extents(&engine, inode);
    assert_eq!(
        read(&engine, inode, 3, 12),
        (
            12,
            vec![
                ContentPiece::Inline {
                    start: 3,
                    content: Bytes::from_static(b"3456789")
                },
                ContentPiece::Stored {
                    start: 10,
                    hash: blob,
                    offset: 5,
                    length: 5
                },
            ]
        )
    );
    assert_eq!(
        read(&engine, inode, 12, 20),
        (
            20,
            vec![
                ContentPiece::Stored {
                    start: 12,
                    hash: blob,
                    offset: 7,
                    length: 18
                },
                ContentPiece::Inline {
                    start: 30,
                    content: Bytes::from_static(b"ab")
                },
            ]
        )
    );
}

#[test]
fn read_stops_at_the_end_of_the_file() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 40);
    three_extents(&engine, inode);
    assert_eq!(
        read(&engine, inode, 35, 100),
        (
            5,
            vec![ContentPiece::Inline {
                start: 35,
                content: Bytes::from_static(b"fghij")
            }]
        )
    );
    assert_eq!(read(&engine, inode, 40, 10), (0, vec![]));
    assert_eq!(read(&engine, inode, 50, 10), (0, vec![]));
    assert_eq!(read(&engine, inode, u64::MAX, u64::MAX), (0, vec![]));
}

#[test]
fn a_hole_past_the_last_extent_stays_inside_the_planned_size() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 50);
    three_extents(&engine, inode);
    assert_eq!(
        read(&engine, inode, 35, 100),
        (
            15,
            vec![ContentPiece::Inline {
                start: 35,
                content: Bytes::from_static(b"fghij")
            }]
        )
    );
}

#[test]
fn read_needs_an_existing_regular_file() {
    let engine = new_test_engine();
    let link = block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            txn.create(
                Inode::ROOT,
                b"link",
                NewItem::SymbolicLink(Bytes::from_static(b"target")),
                |_| {},
            )
            .await
        })
    }))
    .unwrap()
    .inode;
    assert!(matches!(
        plan(&engine, Inode(7), 0, 10),
        Err(ReadError::StaleFileHandle)
    ));
    assert!(matches!(
        plan(&engine, Inode::ROOT, 0, 10),
        Err(ReadError::IsADirectory)
    ));
    assert!(matches!(
        plan(&engine, link, 0, 10),
        Err(ReadError::InvalidInput)
    ));
}

#[test]
fn a_view_outside_the_stored_bytes_is_an_invalid_extent() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 10);
    write_extent(
        &engine,
        inode,
        10,
        &ExtentValue {
            start: 0,
            offset: 5,
            content: SpillableValue::Inline(Bytes::from_static(b"short")),
        },
    );
    assert!(matches!(
        plan(&engine, inode, 0, 10),
        Err(ReadError::InputOutputError(IoFailure::Corrupt(
            CorruptValue::InvalidExtent
        )))
    ));
}
