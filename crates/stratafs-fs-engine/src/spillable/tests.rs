use futures_lite::future::block_on;
use stratafs_store::content::GetRejection;
use stratafs_store::error::StoreError;

use super::*;
use crate::test_support::{InMemoryEngine, OneKibInline, new_test_engine};

const KEY: &[u8] = b"key";

fn make_spillable(engine: &InMemoryEngine, prefix_length: usize, content: Bytes) -> SpillableValue {
    block_on(engine.transact(|txn| {
        let content = content.clone();
        Box::pin(async move {
            let result = txn.make_spillable(KEY, prefix_length, content).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap()
}

fn stored_content(engine: &InMemoryEngine, hash: &ContentHash, length: usize) -> Option<Bytes> {
    match block_on(engine.content().get(hash, 0, length as u64)) {
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
    assert_eq!(
        stored_content(&engine, &ContentHash::of(&content), content.len()),
        None
    );
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
    assert_eq!(
        stored_content(&engine, &ContentHash::of(&content), content.len()),
        None
    );
}

#[test]
fn content_one_byte_over_the_largest_inline_entry_spills_to_the_content_store() {
    let engine = new_test_engine();
    let content = content_of_length(
        OneKibInline::LARGEST_INLINE_ENTRY - KEY.len() - SpillableValue::TAG_LENGTH + 1,
    );
    let hash = ContentHash::of(&content);
    assert_eq!(
        make_spillable(&engine, 0, content.clone()),
        SpillableValue::Overflow {
            hash,
            length: content.len() as u64,
        }
    );
    assert_eq!(stored_content(&engine, &hash, content.len()), Some(content));
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
fn overflow_values_are_stored_as_the_overflow_kind_then_the_hash_then_the_length_in_little_endian()
{
    let value = SpillableValue::Overflow {
        hash: ContentHash([7; 32]),
        length: 0x0102,
    };
    let mut expected = vec![1];
    expected.extend_from_slice(&[7; 32]);
    expected.extend_from_slice(&0x0102u64.to_le_bytes());
    assert_eq!(stored(&value)[..], expected[..]);
}

#[test]
fn stored_values_decode_to_the_value_that_was_stored() {
    for value in [
        SpillableValue::Inline(Bytes::new()),
        SpillableValue::Inline(content_of_length(100)),
        SpillableValue::Overflow {
            hash: ContentHash::of(b"spilled"),
            length: 7,
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
        SpillableValue::from_stored(Bytes::from_static(&[2, 0, 0])),
        Err(CorruptValue::UnknownSpillableKind)
    ));
    assert!(matches!(
        SpillableValue::from_stored(Bytes::from_static(&[1, 0, 0])),
        Err(CorruptValue::WrongLength)
    ));
    let mut one_byte_too_long = stored(&SpillableValue::Overflow {
        hash: ContentHash([0; 32]),
        length: 0,
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
