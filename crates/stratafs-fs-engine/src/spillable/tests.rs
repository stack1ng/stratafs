use std::collections::BTreeSet;
use std::convert::Infallible;

use futures_lite::future::block_on;
use stratafs_store::content::GetRejection;
use stratafs_store::error::StoreError;

use super::*;
use crate::annotations::{AnnotationKey, SetAnnotationMode};
use crate::directories::NewItem;
use crate::key::FsKey;
use crate::test_support::{InMemoryEngine, OneKibInline, TestVolumes, aborted, new_test_engine};
use crate::{Inode, PurgeWaits};

const KEY: &[u8] = b"key";

fn make_spillable(engine: &InMemoryEngine, prefix_length: usize, content: Bytes) -> SpillableValue {
    block_on(engine.transact(PurgeWaits::No, |txn| {
        let content = content.clone();
        Box::pin(async move {
            let result = txn.make_spillable(KEY, prefix_length, content).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap()
}

fn stored_content(engine: &InMemoryEngine, id: &ContentId, length: usize) -> Option<Bytes> {
    match block_on(engine.content().get(id, 0, length as u64)) {
        Ok(mut content) => Some(content.copy_to_bytes(content.remaining())),
        Err(StoreError::Rejected(GetRejection::UnknownContent(_))) => None,
        Err(error) => panic!("reading the content store failed: {error}"),
    }
}

fn content_of_length(length: usize) -> Bytes {
    Bytes::from((0..length).map(|index| index as u8).collect::<Vec<_>>())
}

#[test]
fn small_content_stays_inline() {
    let engine = new_test_engine();
    let content = content_of_length(10);
    assert_eq!(
        make_spillable(&engine, 0, content.clone()),
        SpillableValue::Inline(content.clone())
    );
    assert_eq!(engine.content().ids(), BTreeSet::new());
}

#[test]
fn content_filling_the_largest_inline_entry_exactly_stays_inline() {
    let engine = new_test_engine();
    let content = content_of_length(
        OneKibInline::LARGEST_INLINE_ENTRY - KEY.len() - SpillableValue::TAG_LENGTH,
    );
    assert_eq!(
        make_spillable(&engine, 0, content.clone()),
        SpillableValue::Inline(content.clone())
    );
    assert_eq!(engine.content().ids(), BTreeSet::new());
}

#[test]
fn content_one_byte_over_the_largest_inline_entry_spills_to_the_content_store() {
    let engine = new_test_engine();
    let content = content_of_length(
        OneKibInline::LARGEST_INLINE_ENTRY - KEY.len() - SpillableValue::TAG_LENGTH + 1,
    );
    let SpillableValue::Overflow {
        reference: ContentReference::New(id),
        length,
        hash,
    } = make_spillable(&engine, 0, content.clone())
    else {
        panic!("content over the largest inline entry stayed inline");
    };
    assert_eq!(length, content.len() as u64);
    assert_eq!(hash, ContentHash::xxh3(&content));
    assert_eq!(engine.content().ids(), BTreeSet::from([id]));
    assert_eq!(stored_content(&engine, &id, content.len()), Some(content));
}

#[test]
fn each_spill_is_kept_under_an_id_of_its_own() {
    let engine = new_test_engine();
    let content = content_of_length(OneKibInline::LARGEST_INLINE_ENTRY);
    let first = make_spillable(&engine, 0, content.clone());
    let second = make_spillable(&engine, 0, content.clone());
    assert_ne!(first, second);
    assert_eq!(engine.content().ids().len(), 2);
}

fn stored(value: &SpillableValue) -> Bytes {
    let mut stored = value.to_stored();
    stored.copy_to_bytes(stored.remaining())
}

#[test]
fn inline_values_are_stored_as_the_inline_kind_then_the_content() {
    let value = SpillableValue::Inline(Bytes::from_static(b"abc"));
    assert_eq!(stored(&value)[..], [0, b'a', b'b', b'c']);
}

#[test]
fn overflow_values_are_stored_as_the_overflow_kind_then_the_id_and_the_length_in_little_endian_then_the_hash()
 {
    let hash = [7; 32];
    let value = SpillableValue::Overflow {
        reference: ContentReference::New(ContentId(0x0102_0304)),
        length: 0x0102,
        hash: ContentHash::from(hash),
    };
    let mut expected = vec![1];
    expected.extend_from_slice(&0x0102_0304u128.to_le_bytes());
    expected.extend_from_slice(&0x0102u64.to_le_bytes());
    expected.extend_from_slice(&hash);
    assert_eq!(stored(&value)[..], expected[..]);
}

#[test]
fn stored_values_decode_to_the_value_that_was_stored() {
    for value in [
        SpillableValue::Inline(Bytes::new()),
        SpillableValue::Inline(content_of_length(100)),
        SpillableValue::Overflow {
            reference: ContentReference::New(ContentId(7)),
            length: 7,
            hash: ContentHash::xxh3(b"content"),
        },
        SpillableValue::Overflow {
            reference: ContentReference::Existing(ContentId(7)),
            length: 7,
            hash: ContentHash::from([7; 32]),
        },
    ] {
        assert_eq!(SpillableValue::from_stored(stored(&value)).unwrap(), value);
    }
}

#[test]
fn decoding_an_inline_value_shares_the_stored_buffer() {
    let stored = stored(&SpillableValue::Inline(content_of_length(100)));
    let SpillableValue::Inline(content) = SpillableValue::from_stored(stored.clone()).unwrap()
    else {
        panic!("an inline value decoded as an overflow value");
    };
    assert_eq!(
        content.as_ptr(),
        stored[SpillableValue::TAG_LENGTH..].as_ptr()
    );
}

#[test]
fn decoding_rejects_corrupt_values() {
    assert!(matches!(
        SpillableValue::from_stored(Bytes::new()),
        Err(CorruptValue::WrongLength)
    ));
    assert!(matches!(
        SpillableValue::from_stored(Bytes::from_static(&[3, 0, 0])),
        Err(CorruptValue::UnknownSpillableKind)
    ));
    assert!(matches!(
        SpillableValue::from_stored(Bytes::from_static(&[1, 0, 0])),
        Err(CorruptValue::WrongLength)
    ));
    // a hash is at most ContentHash::LONGEST bytes
    let mut one_byte_too_long = stored(&SpillableValue::Overflow {
        reference: ContentReference::New(ContentId(0)),
        length: 0,
        hash: ContentHash::from([7; ContentHash::LONGEST]),
    })
    .to_vec();
    one_byte_too_long.push(0);
    assert!(matches!(
        SpillableValue::from_stored(Bytes::from(one_byte_too_long)),
        Err(CorruptValue::WrongLength)
    ));
}

#[test]
fn the_prefix_counts_toward_the_largest_inline_entry() {
    let engine = new_test_engine();
    let prefix_length = 16;
    let fits = content_of_length(
        OneKibInline::LARGEST_INLINE_ENTRY - KEY.len() - prefix_length - SpillableValue::TAG_LENGTH,
    );
    assert_eq!(
        make_spillable(&engine, prefix_length, fits.clone()),
        SpillableValue::Inline(fits)
    );
    let one_over = content_of_length(
        OneKibInline::LARGEST_INLINE_ENTRY - KEY.len() - prefix_length - SpillableValue::TAG_LENGTH
            + 1,
    );
    assert!(matches!(
        make_spillable(&engine, prefix_length, one_over),
        SpillableValue::Overflow { .. }
    ));
}

fn extent_value(content: &SpillableValue) -> Bytes {
    let mut value = Bytes::from([0x0102u64.to_le_bytes(), 0x0304u64.to_le_bytes()].concat())
        .chain(content.to_stored());
    value.copy_to_bytes(value.remaining())
}

#[test]
fn the_referenced_content_is_that_of_a_spilled_extent_annotation_or_symbolic_link_target() {
    let spilled = SpillableValue::Overflow {
        reference: ContentReference::New(ContentId(7)),
        length: 2000,
        hash: ContentHash::xxh3(b"content"),
    };
    let inline = SpillableValue::Inline(Bytes::from_static(b"inline"));
    let extent = Bytes::from(FsKey::new_extent(Inode(9), 2000));
    let annotation = Bytes::from(FsKey::new_annotation(
        Inode(9),
        AnnotationKey::Portable(b"name"),
    ));
    let target = Bytes::from(FsKey::new_symbolic_link_target(Inode(9)));
    assert_eq!(
        referenced_content(&extent, &extent_value(&spilled)),
        Some(ContentReference::New(ContentId(7)))
    );
    assert_eq!(
        referenced_content(&annotation, &stored(&spilled)),
        Some(ContentReference::New(ContentId(7)))
    );
    assert_eq!(
        referenced_content(&target, &stored(&spilled)),
        Some(ContentReference::New(ContentId(7)))
    );
    let piece = SpillableValue::Overflow {
        reference: ContentReference::Existing(ContentId(7)),
        length: 2000,
        hash: ContentHash::xxh3(b"content"),
    };
    assert_eq!(
        referenced_content(&extent, &extent_value(&piece)),
        Some(ContentReference::Existing(ContentId(7)))
    );
    assert_eq!(referenced_content(&extent, &extent_value(&inline)), None);
    assert_eq!(referenced_content(&annotation, &stored(&inline)), None);
    assert_eq!(referenced_content(&target, &stored(&inline)), None);
}

#[test]
fn other_keys_and_values_it_cannot_read_reference_no_content() {
    let spilled = stored(&SpillableValue::Overflow {
        reference: ContentReference::New(ContentId(7)),
        length: 2000,
        hash: ContentHash::xxh3(b"content"),
    });
    let attributes = Bytes::from(FsKey::new_attributes(Inode(9)));
    let extent = Bytes::from(FsKey::new_extent(Inode(9), 2000));
    assert_eq!(referenced_content(&attributes, &spilled), None);
    assert_eq!(referenced_content(b"", &spilled), None);
    // An extent's value starts with its start and offset, so a bare spillable value is too short to be one.
    assert_eq!(referenced_content(&extent, &spilled), None);
    assert_eq!(referenced_content(&extent, &[0; 16]), None);
    assert_eq!(
        referenced_content(&extent, &[[0; 16], [2; 16]].concat()),
        None
    );
}

impl<T: ReadKeyValueTransaction, C: Sync, A: Sync, N: Sync, S: Sync>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    // referenced_ids is the content id each stored entry of the volume references, in key order. It returns `impl
    // Future + Send` for the reason make_spillable does.
    fn referenced_ids(&self) -> impl Future<Output = Result<Vec<ContentId>, T::Failure>> + Send {
        async move {
            let entries = self.metadata.get_range(.., usize::MAX).await?;
            Ok(entries
                .filter_map(|(key, value)| referenced_content(&key, &value))
                .map(ContentReference::id)
                .collect())
        }
    }
}

fn referenced_ids(engine: &InMemoryEngine) -> Vec<ContentId> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.referenced_ids().await })))
        .map_err(aborted)
        .unwrap()
}

#[test]
fn every_blob_the_engine_spills_is_referenced_by_each_entry_naming_it() {
    let engine = new_test_engine();
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let file = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await
                .unwrap()
                .inode;
            txn.write(file, 0, content_of_length(2000)).await.unwrap();
            // Writing over the middle keeps the head and the tail of the first blob, as two extents.
            txn.write(file, 100, Bytes::from_static(b"ab"))
                .await
                .unwrap();
            txn.set_annotation(
                Inode::ROOT,
                AnnotationKey::Portable(b"large"),
                content_of_length(2000),
                SetAnnotationMode::Create,
            )
            .await
            .unwrap();
            let target = Bytes::from(vec![b'x'; 3000]);
            txn.create(Inode::ROOT, b"link", NewItem::SymbolicLink(target), |_| {})
                .await
                .unwrap();
            Ok::<_, Infallible>(((), txn))
        })
    }))
    .unwrap();
    let mut referenced = referenced_ids(&engine);
    referenced.sort_unstable();
    assert_eq!(referenced.len(), 4, "{referenced:?}");
    let mut distinct = referenced.clone();
    distinct.dedup();
    assert_eq!(
        distinct,
        engine.content().ids().into_iter().collect::<Vec<_>>()
    );
}

// create_file creates name in the root directory holding content, and returns its inode.
fn create_file(engine: &InMemoryEngine, name: &'static [u8], content: Bytes) -> Inode {
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
        let content = content.clone();
        Box::pin(async move {
            let file = txn
                .create(Inode::ROOT, name, NewItem::File, |_| {})
                .await
                .unwrap()
                .inode;
            txn.write(file, 0, content).await.unwrap();
            Ok::<_, Infallible>((file, txn))
        })
    }))
    .unwrap()
}

fn write(engine: &InMemoryEngine, file: Inode, offset: u64, content: Bytes) {
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
        let content = content.clone();
        Box::pin(async move {
            txn.write(file, offset, content).await.unwrap();
            Ok::<_, Infallible>(((), txn))
        })
    }))
    .unwrap();
}

// stored_ids is the id of every blob the content store keeps.
fn stored_ids(volumes: &TestVolumes) -> Vec<ContentId> {
    volumes.content.ids().into_iter().collect()
}

// referenced_by is the id of every blob the engines' volumes reference, each once.
fn referenced_by(engines: &[&InMemoryEngine]) -> Vec<ContentId> {
    let referenced: BTreeSet<_> = engines
        .iter()
        .flat_map(|engine| referenced_ids(engine))
        .collect();
    referenced.into_iter().collect()
}

#[test]
fn a_blob_split_into_a_head_and_a_tail_stays_until_both_pieces_go() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let engine = volumes.engine(volume);
    let file = create_file(&engine, b"file", content_of_length(2000));
    let split = stored_ids(&volumes);
    // writing over the middle keeps the head and the tail of the blob, as two extents
    write(&engine, file, 100, Bytes::from_static(b"ab"));
    write(&engine, file, 0, content_of_length(100));
    volumes.collect_garbage_and_delete(volume).unwrap();
    assert_eq!(stored_ids(&volumes), split);
    write(&engine, file, 102, content_of_length(1898));
    volumes.collect_garbage_and_delete(volume).unwrap();
    let stored = stored_ids(&volumes);
    assert!(!stored.contains(&split[0]));
    assert_eq!(stored, referenced_by(&[&engine]));
}

#[test]
fn a_blob_split_by_the_transaction_that_wrote_it_stays_until_both_pieces_go() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let engine = volumes.engine(volume);
    let file = create_file(&engine, b"file", Bytes::new());
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            txn.write(file, 0, content_of_length(2000)).await.unwrap();
            txn.write(file, 100, Bytes::from_static(b"ab"))
                .await
                .unwrap();
            Ok::<_, Infallible>(((), txn))
        })
    }))
    .unwrap();
    let split = stored_ids(&volumes);
    volumes.collect_garbage_and_delete(volume).unwrap();
    assert_eq!(stored_ids(&volumes), split);
    write(&engine, file, 0, content_of_length(2000));
    volumes.collect_garbage_and_delete(volume).unwrap();
    let stored = stored_ids(&volumes);
    assert!(!stored.contains(&split[0]));
    assert_eq!(stored, referenced_by(&[&engine]));
}

#[test]
fn a_read_planned_before_an_overwrite_still_loads_after_garbage_is_collected() {
    let volumes = TestVolumes::new();
    let volume = volumes.create();
    let engine = volumes.engine(volume);
    let content = content_of_length(2000);
    let file = create_file(&engine, b"file", content.clone());
    // a read's plan is loaded once its transaction ends, so a garbage collection pass can run in between
    let plan = block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.read(file, 0, 2000).await })),
    )
    .map_err(aborted)
    .unwrap();
    write(&engine, file, 0, content_of_length(2000));
    volumes.collect_garbage(volume).unwrap();
    let loaded = block_on(futures_util::future::try_join_all(
        plan.load(engine.content()),
    ));
    assert!(
        loaded.is_ok(),
        "the planned read failed: {:?}",
        loaded.err()
    );
}
