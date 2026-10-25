use std::convert::Infallible;

use futures_lite::future::block_on;
use futures_util::future::try_join_all;
use stratafs_opendal::OpendalError;

use super::*;
use crate::Timestamp;
use crate::attributes::error::SetattrError;
use crate::attributes::{Mode, Ownership, SettableAttributes};
use crate::directories::NewItem;
use crate::test_support::{
    InMemoryEngine, MetadataFailure, OneKibInline, aborted, new_test_engine, set_clock,
};

fn stored(extent: &ExtentValue) -> Bytes {
    let mut stored = extent.to_stored();
    stored.copy_to_bytes(stored.remaining())
}

#[test]
fn an_extent_is_stored_as_its_start_and_offset_then_its_spillable_value() {
    let extent = ExtentValue {
        start: 0x0102,
        content_offset: 0x0304,
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
            content_offset: 0,
            content: SpillableValue::Inline(Bytes::from_static(b"inline")),
        },
        ExtentValue {
            start: 4096,
            content_offset: 40,
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
            let result = txn
                .metadata
                .set(FsKey::new_extent(inode, end).into(), extent.to_stored());
            result.map(|result| (result, txn))
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
        attribute_changed_at: Timestamp(0),
        parent: Inode::ROOT,
        change_version: 0,
    };
    block_on(engine.transact(|mut txn| {
        let attributes = attributes.clone();
        Box::pin(async move {
            let result = txn.write_attributes(inode, &attributes);
            result.map(|result| (result, txn))
        })
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
            content_offset: 0,
            content: SpillableValue::Inline(Bytes::from_static(b"0123456789")),
        },
    );
    write_extent(
        engine,
        inode,
        30,
        &ExtentValue {
            start: 10,
            content_offset: 5,
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
            content_offset: 0,
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
) -> Result<ContentLoadPlan, ReadError<MetadataFailure, OpendalError>> {
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
            let result = txn
                .create(
                    Inode::ROOT,
                    b"link",
                    NewItem::SymbolicLink(Bytes::from_static(b"target")),
                    |_| {},
                )
                .await;
            result.map(|result| (result, txn))
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
            content_offset: 5,
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

fn overlapping(engine: &InMemoryEngine, inode: Inode, start: u64, end: u64) -> Vec<(u64, u64)> {
    block_on(engine.read_transact(|txn| {
        Box::pin(async move { txn.overlapping_extents(inode, start, end).await })
    }))
    .map_err(aborted)
    .unwrap()
    .into_iter()
    .map(|(end, extent)| (extent.start, end))
    .collect()
}

#[test]
fn overlapping_extents_returns_whole_extents_touching_the_range() {
    let engine = new_test_engine();
    let inode = Inode(7);
    three_extents(&engine, inode);
    assert_eq!(overlapping(&engine, inode, 12, 20), vec![(10, 30)]);
    assert_eq!(
        overlapping(&engine, inode, 5, 35),
        vec![(0, 10), (10, 30), (30, 40)]
    );
    assert_eq!(overlapping(&engine, inode, 10, 30), vec![(10, 30)]);
    assert_eq!(overlapping(&engine, inode, 40, 50), vec![]);
}

#[test]
fn overlapping_extents_skips_an_extent_that_starts_after_a_hole() {
    let engine = new_test_engine();
    let inode = Inode(7);
    write_extent(
        &engine,
        inode,
        10,
        &ExtentValue {
            start: 0,
            content_offset: 0,
            content: SpillableValue::Inline(Bytes::from_static(b"0123456789")),
        },
    );
    write_extent(
        &engine,
        inode,
        30,
        &ExtentValue {
            start: 20,
            content_offset: 0,
            content: SpillableValue::Inline(Bytes::from_static(b"abcdefghij")),
        },
    );
    assert_eq!(overlapping(&engine, inode, 12, 15), vec![]);
    assert_eq!(overlapping(&engine, inode, 12, 25), vec![(20, 30)]);
}

#[test]
fn slicing_an_inline_extent_shares_its_bytes_and_starts_at_offset_zero() {
    let content = Bytes::from_static(b"__0123456789");
    let extent = ExtentValue {
        start: 100,
        content_offset: 2,
        content: SpillableValue::Inline(content.clone()),
    };
    let slice = extent.slice(103..107).unwrap();
    assert_eq!(slice.start, 103);
    assert_eq!(slice.content_offset, 0);
    let SpillableValue::Inline(sliced) = &slice.content else {
        panic!("an inline extent sliced into an overflow extent");
    };
    assert_eq!(sliced, &Bytes::from_static(b"3456"));
    assert_eq!(sliced.as_ptr(), content[5..].as_ptr());
}

#[test]
fn slicing_an_overflow_extent_moves_its_offset_into_the_same_blob() {
    let content = SpillableValue::Overflow {
        hash: ContentHash::of(b"blob"),
        length: 100,
    };
    let extent = ExtentValue {
        start: 100,
        content_offset: 10,
        content: content.clone(),
    };
    assert_eq!(
        extent.slice(130..150).unwrap(),
        ExtentValue {
            start: 130,
            content_offset: 40,
            content,
        }
    );
}

#[test]
fn slicing_past_the_inline_bytes_is_an_invalid_extent() {
    let extent = ExtentValue {
        start: 0,
        content_offset: 0,
        content: SpillableValue::Inline(Bytes::from_static(b"short")),
    };
    assert!(matches!(
        extent.slice(2..10),
        Err(CorruptValue::InvalidExtent)
    ));
}

type Write = WriteError<MetadataFailure, Infallible, OpendalError>;

fn write(
    engine: &InMemoryEngine,
    inode: Inode,
    offset: u64,
    data: &[u8],
) -> Result<Attributes, Write> {
    let data = Bytes::copy_from_slice(data);
    block_on(engine.transact(|mut txn| {
        let data = data.clone();
        Box::pin(async move {
            let result = txn.write(inode, offset, data).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn getattr(engine: &InMemoryEngine, inode: Inode) -> Option<Attributes> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })))
        .map_err(aborted)
        .unwrap()
}

fn stored_extents(engine: &InMemoryEngine, inode: Inode) -> Vec<(u64, ExtentValue)> {
    block_on(engine.read_transact(|txn| {
        Box::pin(async move { txn.overlapping_extents(inode, 0, u64::MAX).await })
    }))
    .map_err(aborted)
    .unwrap()
}

fn extents(engine: &InMemoryEngine, inode: Inode) -> Vec<(Range<u64>, Bytes)> {
    stored_extents(engine, inode)
        .into_iter()
        .map(|(end, extent)| {
            let length = end - extent.start;
            let content = match extent.content {
                SpillableValue::Inline(content) => content.slice(
                    usize::try_from(extent.content_offset).unwrap()
                        ..usize::try_from(extent.content_offset + length).unwrap(),
                ),
                SpillableValue::Overflow { hash, .. } => {
                    let mut content =
                        block_on(engine.content.get(&hash, extent.content_offset, length)).unwrap();
                    content.copy_to_bytes(content.remaining())
                }
            };
            (extent.start..end, content)
        })
        .collect()
}

fn content_of_length(length: usize) -> Bytes {
    Bytes::from((0..length).map(|index| index as u8).collect::<Vec<_>>())
}

#[test]
fn a_write_stores_one_extent_and_records_the_change() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    set_clock(5);
    let attributes = write(&engine, inode, 0, b"hello").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![(0..5, Bytes::from_static(b"hello"))]
    );
    assert_eq!(attributes.settable.size, 5);
    assert_eq!(attributes.settable.content_changed_at, Timestamp(5));
    assert_eq!(attributes.attribute_changed_at, Timestamp(5));
    assert_eq!(attributes.change_version, 1);
    assert_eq!(getattr(&engine, inode), Some(attributes));
}

#[test]
fn a_write_past_the_end_leaves_a_hole_and_grows_the_file() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 0, b"abc").unwrap();
    let attributes = write(&engine, inode, 10, b"xyz").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![
            (0..3, Bytes::from_static(b"abc")),
            (10..13, Bytes::from_static(b"xyz")),
        ]
    );
    assert_eq!(attributes.settable.size, 13);
}

#[test]
fn a_write_inside_one_extent_keeps_both_of_its_sides() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 0, b"0123456789").unwrap();
    let attributes = write(&engine, inode, 4, b"ab").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![
            (0..4, Bytes::from_static(b"0123")),
            (4..6, Bytes::from_static(b"ab")),
            (6..10, Bytes::from_static(b"6789")),
        ]
    );
    assert_eq!(attributes.settable.size, 10);
}

#[test]
fn a_write_over_the_end_of_an_extent_keeps_its_start() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 0, b"0123456789").unwrap();
    let attributes = write(&engine, inode, 8, b"abcd").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![
            (0..8, Bytes::from_static(b"01234567")),
            (8..12, Bytes::from_static(b"abcd")),
        ]
    );
    assert_eq!(attributes.settable.size, 12);
}

#[test]
fn a_write_over_the_start_of_an_extent_keeps_its_end() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 4, b"0123456789").unwrap();
    let attributes = write(&engine, inode, 2, b"abcd").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![
            (2..6, Bytes::from_static(b"abcd")),
            (6..14, Bytes::from_static(b"23456789")),
        ]
    );
    assert_eq!(attributes.settable.size, 14);
}

#[test]
fn a_write_over_a_whole_extent_replaces_it() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 0, b"0123456789").unwrap();
    let attributes = write(&engine, inode, 0, b"abcdefghij").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![(0..10, Bytes::from_static(b"abcdefghij"))]
    );
    assert_eq!(attributes.settable.size, 10);
}

#[test]
fn a_write_across_extents_trims_the_outer_ones_and_removes_the_inner_ones() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 0, b"0123456789").unwrap();
    write(&engine, inode, 10, b"abcdefghij").unwrap();
    write(&engine, inode, 20, b"ABCDEFGHIJ").unwrap();
    let attributes = write(&engine, inode, 5, b"xxxxxxxxxxxxxxxxxxxx").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![
            (0..5, Bytes::from_static(b"01234")),
            (5..25, Bytes::from_static(b"xxxxxxxxxxxxxxxxxxxx")),
            (25..30, Bytes::from_static(b"FGHIJ")),
        ]
    );
    assert_eq!(attributes.settable.size, 30);
}

#[test]
fn a_write_between_extents_leaves_them_untouched() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 0, b"01234").unwrap();
    write(&engine, inode, 10, b"56789").unwrap();
    let attributes = write(&engine, inode, 5, b"abcde").unwrap();
    assert_eq!(
        extents(&engine, inode),
        vec![
            (0..5, Bytes::from_static(b"01234")),
            (5..10, Bytes::from_static(b"abcde")),
            (10..15, Bytes::from_static(b"56789")),
        ]
    );
    assert_eq!(attributes.settable.size, 15);
}

#[test]
fn a_file_never_sees_the_extents_of_the_next_file() {
    let engine = new_test_engine();
    file_of_size(&engine, Inode(7), 0);
    file_of_size(&engine, Inode(8), 0);
    write(&engine, Inode(8), 0, b"next").unwrap();
    write(&engine, Inode(7), 0, b"this").unwrap();
    assert_eq!(
        extents(&engine, Inode(7)),
        vec![(0..4, Bytes::from_static(b"this"))]
    );
    assert_eq!(
        read(&engine, Inode(7), 0, 10),
        (
            4,
            vec![ContentPiece::Inline {
                start: 0,
                content: Bytes::from_static(b"this")
            }]
        )
    );
}

#[test]
fn a_write_stays_inline_while_its_extent_fits_the_largest_inline_entry() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    let key_length = Bytes::from(FsKey::new_extent(inode, 0)).len();
    let empty_extent_length = stored(&ExtentValue {
        start: 0,
        content_offset: 0,
        content: SpillableValue::Inline(Bytes::new()),
    })
    .len();
    let largest_inline = OneKibInline::LARGEST_INLINE_ENTRY - key_length - empty_extent_length;
    let fits = content_of_length(largest_inline);
    write(&engine, inode, 0, &fits).unwrap();
    assert_eq!(
        stored_extents(&engine, inode),
        vec![(
            largest_inline as u64,
            ExtentValue {
                start: 0,
                content_offset: 0,
                content: SpillableValue::Inline(fits),
            }
        )]
    );
    let spills = content_of_length(largest_inline + 1);
    write(&engine, inode, 0, &spills).unwrap();
    assert_eq!(
        stored_extents(&engine, inode),
        vec![(
            spills.len() as u64,
            ExtentValue {
                start: 0,
                content_offset: 0,
                content: SpillableValue::Overflow {
                    hash: ContentHash::of(&spills),
                    length: spills.len() as u64,
                },
            }
        )]
    );
}

#[test]
fn overwriting_part_of_a_spilled_extent_keeps_pointing_into_its_blob() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    let content = content_of_length(2000);
    write(&engine, inode, 0, &content).unwrap();
    write(&engine, inode, 100, b"ab").unwrap();
    let blob = SpillableValue::Overflow {
        hash: ContentHash::of(&content),
        length: 2000,
    };
    assert_eq!(
        stored_extents(&engine, inode),
        vec![
            (
                100,
                ExtentValue {
                    start: 0,
                    content_offset: 0,
                    content: blob.clone(),
                }
            ),
            (
                102,
                ExtentValue {
                    start: 100,
                    content_offset: 0,
                    content: SpillableValue::Inline(Bytes::from_static(b"ab")),
                }
            ),
            (
                2000,
                ExtentValue {
                    start: 102,
                    content_offset: 102,
                    content: blob,
                }
            ),
        ]
    );
    let mut expected = content.to_vec();
    expected[100..102].copy_from_slice(b"ab");
    assert_eq!(
        extents(&engine, inode)
            .into_iter()
            .flat_map(|(_, content)| content)
            .collect::<Vec<u8>>(),
        expected
    );
}

#[test]
fn an_empty_write_changes_nothing() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 10);
    set_clock(5);
    let before = getattr(&engine, inode).unwrap();
    assert_eq!(write(&engine, inode, 100, b"").unwrap(), before);
    assert_eq!(getattr(&engine, inode), Some(before));
    assert_eq!(extents(&engine, inode), vec![]);
}

#[test]
fn write_needs_an_existing_regular_file() {
    let engine = new_test_engine();
    let link = block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn
                .create(
                    Inode::ROOT,
                    b"link",
                    NewItem::SymbolicLink(Bytes::from_static(b"target")),
                    |_| {},
                )
                .await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap()
    .inode;
    assert!(matches!(
        write(&engine, Inode(7), 0, b"a"),
        Err(WriteError::StaleFileHandle)
    ));
    assert!(matches!(
        write(&engine, Inode::ROOT, 0, b"a"),
        Err(WriteError::IsADirectory)
    ));
    assert!(matches!(
        write(&engine, link, 0, b"a"),
        Err(WriteError::InvalidInput)
    ));
}

#[test]
fn a_write_ending_past_the_largest_offset_is_too_large() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    assert!(matches!(
        write(&engine, inode, u64::MAX, b"a"),
        Err(WriteError::FileTooLarge)
    ));
}

type Setattr = SetattrError<MetadataFailure, Infallible, OpendalError>;

fn setattr(
    engine: &InMemoryEngine,
    inode: Inode,
    update: impl Fn(&mut SettableAttributes) + Send + Sync,
) -> Result<Attributes, Setattr> {
    block_on(engine.transact(|mut txn| {
        let update = &update;
        Box::pin(async move {
            let result = txn.setattr(inode, update).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn resize(engine: &InMemoryEngine, inode: Inode, size: u64) -> Attributes {
    setattr(engine, inode, |settable| settable.size = size).unwrap()
}

fn three_written_extents(engine: &InMemoryEngine, inode: Inode) {
    file_of_size(engine, inode, 0);
    write(engine, inode, 0, b"0123456789").unwrap();
    write(engine, inode, 10, b"abcdefghij").unwrap();
    write(engine, inode, 20, b"klmnopqrst").unwrap();
}

#[test]
fn shrinking_keeps_the_part_of_the_extent_that_crosses_the_new_size() {
    let engine = new_test_engine();
    let inode = Inode(7);
    three_written_extents(&engine, inode);
    let attributes = resize(&engine, inode, 15);
    assert_eq!(
        extents(&engine, inode),
        vec![
            (0..10, Bytes::from_static(b"0123456789")),
            (10..15, Bytes::from_static(b"abcde")),
        ]
    );
    assert_eq!(attributes.settable.size, 15);
}

#[test]
fn shrinking_to_the_end_of_an_extent_keeps_that_extent_whole() {
    let engine = new_test_engine();
    let inode = Inode(7);
    three_written_extents(&engine, inode);
    resize(&engine, inode, 10);
    assert_eq!(
        extents(&engine, inode),
        vec![(0..10, Bytes::from_static(b"0123456789"))]
    );
}

#[test]
fn shrinking_to_zero_removes_every_extent() {
    let engine = new_test_engine();
    let inode = Inode(7);
    three_written_extents(&engine, inode);
    resize(&engine, inode, 0);
    assert_eq!(extents(&engine, inode), vec![]);
}

#[test]
fn growing_after_shrinking_reads_a_hole_instead_of_the_old_content() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    write(&engine, inode, 0, b"0123456789").unwrap();
    resize(&engine, inode, 4);
    resize(&engine, inode, 10);
    assert_eq!(
        read(&engine, inode, 0, 10),
        (
            10,
            vec![ContentPiece::Inline {
                start: 0,
                content: Bytes::from_static(b"0123")
            }]
        )
    );
}

#[test]
fn shrinking_a_spilled_extent_keeps_pointing_into_its_blob() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    let content = content_of_length(2000);
    write(&engine, inode, 0, &content).unwrap();
    resize(&engine, inode, 1000);
    assert_eq!(
        stored_extents(&engine, inode),
        vec![(
            1000,
            ExtentValue {
                start: 0,
                content_offset: 0,
                content: SpillableValue::Overflow {
                    hash: ContentHash::of(&content),
                    length: 2000,
                },
            }
        )]
    );
}

#[test]
fn shrinking_a_file_never_touches_the_extents_of_the_next_file() {
    let engine = new_test_engine();
    three_written_extents(&engine, Inode(7));
    file_of_size(&engine, Inode(8), 0);
    write(&engine, Inode(8), 0, b"next").unwrap();
    resize(&engine, Inode(7), 0);
    assert_eq!(
        extents(&engine, Inode(8)),
        vec![(0..4, Bytes::from_static(b"next"))]
    );
}

#[test]
fn a_resize_records_a_content_change() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 10);
    set_clock(5);
    let attributes = resize(&engine, inode, 4);
    assert_eq!(attributes.settable.content_changed_at, Timestamp(5));
    assert_eq!(attributes.attribute_changed_at, Timestamp(5));
    assert_eq!(attributes.change_version, 1);
    assert_eq!(getattr(&engine, inode), Some(attributes));
}

#[test]
fn a_resize_keeps_a_content_change_time_set_by_the_same_update() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 10);
    set_clock(5);
    let attributes = setattr(&engine, inode, |settable| {
        settable.size = 20;
        settable.content_changed_at = Timestamp(3);
    })
    .unwrap();
    assert_eq!(attributes.settable.content_changed_at, Timestamp(3));
    assert_eq!(attributes.attribute_changed_at, Timestamp(5));
}

fn loaded(engine: &InMemoryEngine, inode: Inode, offset: u64, size: u64) -> Vec<Bytes> {
    let plan = plan(engine, inode, offset, size).unwrap();
    block_on(try_join_all(plan.load(&engine.content)))
        .unwrap()
        .into_iter()
        .map(|mut piece| piece.copy_to_bytes(piece.remaining()))
        .collect()
}

#[test]
fn load_returns_every_piece_in_file_order_with_holes_as_zeros() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    let spilled = content_of_length(2000);
    write(&engine, inode, 0, b"abc").unwrap();
    write(&engine, inode, 10, &spilled).unwrap();
    resize(&engine, inode, 2100);
    let pieces = loaded(&engine, inode, 0, 3000);
    assert_eq!(
        pieces,
        vec![
            Bytes::from_static(b"abc"),
            Bytes::from_static(&[0; 7]),
            spilled,
            Bytes::from(vec![0; 90]),
        ]
    );
}

#[test]
fn load_starts_and_ends_inside_pieces_at_the_read_bounds() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 0);
    let spilled = content_of_length(2000);
    write(&engine, inode, 0, &spilled).unwrap();
    write(&engine, inode, 2000, b"tail").unwrap();
    let pieces = loaded(&engine, inode, 1990, 12);
    assert_eq!(
        pieces,
        vec![spilled.slice(1990..2000), Bytes::from_static(b"ta")]
    );
}

#[test]
fn load_of_a_file_without_content_returns_its_size_in_zeros() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 200_000);
    assert_eq!(
        loaded(&engine, inode, 0, 300_000),
        vec![Bytes::from(vec![0; 200_000])]
    );
}

#[test]
fn load_of_an_empty_read_returns_nothing() {
    let engine = new_test_engine();
    let inode = Inode(7);
    file_of_size(&engine, inode, 10);
    assert_eq!(loaded(&engine, inode, 10, 5), Vec::<Bytes>::new());
}
