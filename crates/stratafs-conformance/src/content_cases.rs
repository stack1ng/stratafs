use std::cell::Cell;
use std::future::poll_fn;
use std::pin::{Pin, pin};
use std::task::Poll;

use bytes::Bytes;
use futures_util::future::join;
use stratafs_store::content::{ContentHash, ContentStore, GetRejection, UnknownContent};
use stratafs_store::error::StoreError;

async fn put<S: ContentStore>(store: &S, bytes: &[u8]) -> ContentHash {
    match store.put(Bytes::copy_from_slice(bytes)).await {
        Ok(hash) => hash,
        Err(error) => panic!("put of {} bytes failed: {error:?}", bytes.len()),
    }
}

async fn get<S: ContentStore>(store: &S, hash: &ContentHash, offset: u64, size: u64) -> Bytes {
    match store.get(hash, offset, size).await {
        Ok(bytes) => bytes,
        Err(error) => panic!("get({hash:?}, {offset}, {size}) failed: {error:?}"),
    }
}

async fn get_out_of_bounds<S: ContentStore>(
    store: &S,
    hash: &ContentHash,
    offset: u64,
    size: u64,
) -> u64 {
    match store.get(hash, offset, size).await {
        Err(StoreError::Rejected(GetRejection::OutOfBounds { len })) => len,
        other => panic!(
            "get({hash:?}, {offset}, {size}) gave {:?} where OutOfBounds was expected",
            other.map(|bytes| bytes.len())
        ),
    }
}

async fn yield_now() {
    let mut yielded = false;
    poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

async fn poll_once<F: Future>(future: &mut Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

fn pattern(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let mut bytes = vec![0; len];
    rng.fill(&mut bytes);
    bytes
}

pub async fn put_returns_blake3_hash<S: ContentStore>(store: &S) {
    for bytes in [&b"hello world"[..], b"", &[0; 1000], &[0xff; 3]] {
        assert_eq!(put(store, bytes).await, ContentHash::of(bytes));
        assert_eq!(put(store, bytes).await.0, *blake3::hash(bytes).as_bytes());
    }
}

pub async fn put_then_get_roundtrip<S: ContentStore>(store: &S) {
    let bytes = pattern(1, 4096);
    let hash = put(store, &bytes).await;
    assert_eq!(get(store, &hash, 0, 4096).await, bytes);
    assert_eq!(get_out_of_bounds(store, &hash, 0, u64::MAX).await, 4096);
}

pub async fn put_is_idempotent<S: ContentStore>(store: &S) {
    let bytes = pattern(2, 300);
    let first = put(store, &bytes).await;
    let second = put(store, &bytes).await;
    let third = put(store, &bytes).await;
    assert_eq!(first, second);
    assert_eq!(second, third);
    assert_eq!(get(store, &first, 0, 300).await, bytes);
}

pub async fn distinct_blobs_are_independent<S: ContentStore>(store: &S) {
    let first = pattern(3, 100);
    let second = pattern(4, 200);
    let first_hash = put(store, &first).await;
    let second_hash = put(store, &second).await;
    assert_ne!(first_hash, second_hash);
    assert_eq!(get(store, &first_hash, 0, 100).await, first);
    assert_eq!(get(store, &second_hash, 0, 200).await, second);
}

pub async fn ranged_get<S: ContentStore>(store: &S) {
    let bytes = pattern(5, 1000);
    let hash = put(store, &bytes).await;
    assert_eq!(get(store, &hash, 10, 20).await, bytes[10..30]);
    assert_eq!(get(store, &hash, 0, 1).await, bytes[..1]);
    assert_eq!(get(store, &hash, 999, 1).await, bytes[999..]);
    assert_eq!(get(store, &hash, 500, 500).await, bytes[500..]);
}

pub async fn get_past_end_is_out_of_bounds<S: ContentStore>(store: &S) {
    let bytes = pattern(6, 100);
    let hash = put(store, &bytes).await;
    for (offset, size) in [
        (90, 20),
        (0, 101),
        (50, u64::MAX),
        (100, 10),
        (100, 1),
        (101, 10),
        (101, 0),
        (1, u64::MAX),
        (u64::MAX, 0),
        (u64::MAX, u64::MAX),
        (u64::MAX - 1, 1),
    ] {
        assert_eq!(
            get_out_of_bounds(store, &hash, offset, size).await,
            100,
            "get({offset}, {size}) of a 100 byte blob"
        );
    }
    assert_eq!(get(store, &hash, 90, 10).await, bytes[90..]);
    assert_eq!(get(store, &hash, 0, 100).await, bytes);
    assert!(get(store, &hash, 100, 0).await.is_empty());
}

pub async fn zero_size_get_returns_nothing<S: ContentStore>(store: &S) {
    let bytes = pattern(7, 50);
    let hash = put(store, &bytes).await;
    assert!(get(store, &hash, 0, 0).await.is_empty());
    assert!(get(store, &hash, 25, 0).await.is_empty());
    assert!(get(store, &hash, 50, 0).await.is_empty());
    assert_eq!(get_out_of_bounds(store, &hash, 51, 0).await, 50);
    assert_eq!(get_out_of_bounds(store, &hash, 500, 0).await, 50);
}

pub async fn unknown_hash_is_rejected<S: ContentStore>(store: &S) {
    put(store, b"something else").await;
    let unknown = ContentHash::of(b"never stored");
    for (offset, size) in [
        (0, 10),
        (0, 0),
        (5, 0),
        (100, 10),
        (0, u64::MAX),
        (u64::MAX, u64::MAX),
    ] {
        let result = store.get(&unknown, offset, size).await;
        assert!(
            matches!(
                result,
                Err(StoreError::Rejected(GetRejection::UnknownContent(
                    UnknownContent
                )))
            ),
            "get({offset}, {size}) of an unknown hash gave {result:?}"
        );
    }
    let empty_unknown = ContentHash::of(b"");
    let result = store.get(&empty_unknown, 0, 0).await;
    assert!(
        matches!(
            result,
            Err(StoreError::Rejected(GetRejection::UnknownContent(
                UnknownContent
            )))
        ),
        "get of the never-stored empty blob gave {result:?}"
    );
}

pub async fn empty_blob_roundtrip<S: ContentStore>(store: &S) {
    let hash = put(store, b"").await;
    assert_eq!(hash, ContentHash::of(b""));
    assert!(get(store, &hash, 0, 0).await.is_empty());
    assert_eq!(get_out_of_bounds(store, &hash, 0, 10).await, 0);
    assert_eq!(get_out_of_bounds(store, &hash, 5, 10).await, 0);
    assert_eq!(get_out_of_bounds(store, &hash, 1, 0).await, 0);
    assert_eq!(put(store, b"").await, hash);
}

pub async fn large_blob_ranged_reads<S: ContentStore>(store: &S) {
    const MIB: usize = 1 << 20;
    let bytes = pattern(8, 5 * MIB + 123);
    let hash = put(store, &bytes).await;
    assert_eq!(hash, ContentHash::of(&bytes));
    let len = bytes.len() as u64;
    for (offset, size) in [
        (0, 16),
        (MIB as u64 - 1, 2),
        (MIB as u64 - 100, 2 * MIB as u64 + 200),
        (3 * MIB as u64 + 7, 4096),
        (4 * MIB as u64, MIB as u64),
        (len - 10, 10),
        (len, 0),
    ] {
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let end = usize::try_from(offset + size).unwrap_or(usize::MAX);
        assert!(
            get(store, &hash, offset, size).await == bytes[start..end],
            "get({offset}, {size}) of a large blob returned the wrong bytes"
        );
    }
    for (offset, size) in [(len - 10, 100), (len - 10, 11), (len, 1), (0, len + 1)] {
        assert_eq!(
            get_out_of_bounds(store, &hash, offset, size).await,
            len,
            "get({offset}, {size}) of a large blob"
        );
    }
    assert!(
        get(store, &hash, 0, len).await == bytes,
        "the whole large blob did not round trip"
    );
    assert_eq!(put(store, &bytes).await, hash);
}

async fn assert_put_repairs<S, Corrupt, Corrupted>(
    store: &S,
    corrupt: Corrupt,
    original: &[u8],
    replacement: &[u8],
) where
    S: ContentStore,
    Corrupt: Fn(&S, ContentHash, Bytes) -> Corrupted,
    Corrupted: Future<Output = ()>,
{
    let hash = put(store, original).await;
    corrupt(store, hash, Bytes::copy_from_slice(replacement)).await;
    let len = original.len() as u64;
    let whole = store.get(&hash, 0, len).await;
    let beyond = store.get(&hash, 0, len + 1).await;
    let whole_looks_original = matches!(&whole, Ok(bytes) if bytes == original);
    let beyond_looks_original = matches!(
        &beyond,
        Err(StoreError::Rejected(GetRejection::OutOfBounds { len: stored })) if *stored == len
    );
    assert!(
        !(whole_looks_original && beyond_looks_original),
        "the corrupt hook left the stored object of {} bytes looking unchanged",
        original.len()
    );
    assert_eq!(put(store, original).await, hash);
    assert!(
        get(store, &hash, 0, len).await == original,
        "put did not repair a stored object of {} bytes replaced by {} bytes",
        original.len(),
        replacement.len()
    );
    assert_eq!(
        get_out_of_bounds(store, &hash, 0, len + 1).await,
        len,
        "the repaired object does not have the original length"
    );
    let middle = len / 2;
    let size = 7.min(len - middle);
    let start = usize::try_from(middle).unwrap_or(usize::MAX);
    let end = usize::try_from(middle + size).unwrap_or(usize::MAX);
    assert!(
        get(store, &hash, middle, size).await == original[start..end],
        "a ranged get after the repair returned the wrong bytes"
    );
    assert_eq!(put(store, original).await, hash);
}

pub async fn put_repairs_shorter_stored_object<S, Corrupt, Corrupted>(store: &S, corrupt: Corrupt)
where
    S: ContentStore,
    Corrupt: Fn(&S, ContentHash, Bytes) -> Corrupted,
    Corrupted: Future<Output = ()>,
{
    let original = pattern(20, 5000);
    assert_put_repairs(store, corrupt, &original, &original[..1234]).await;
}

pub async fn put_repairs_longer_stored_object<S, Corrupt, Corrupted>(store: &S, corrupt: Corrupt)
where
    S: ContentStore,
    Corrupt: Fn(&S, ContentHash, Bytes) -> Corrupted,
    Corrupted: Future<Output = ()>,
{
    let original = pattern(21, 300);
    let mut longer = original.clone();
    longer.extend(pattern(22, 50));
    assert_put_repairs(store, corrupt, &original, &longer).await;
}

pub async fn put_repairs_same_length_stored_object<S, Corrupt, Corrupted>(
    store: &S,
    corrupt: Corrupt,
) where
    S: ContentStore,
    Corrupt: Fn(&S, ContentHash, Bytes) -> Corrupted,
    Corrupted: Future<Output = ()>,
{
    let original = pattern(23, 3 << 20);
    let mut flipped = original.clone();
    flipped[(3 << 20) - 5] ^= 1;
    assert_put_repairs(store, corrupt, &original, &flipped).await;
}

pub async fn put_repairs_corrupt_empty_blob<S, Corrupt, Corrupted>(store: &S, corrupt: Corrupt)
where
    S: ContentStore,
    Corrupt: Fn(&S, ContentHash, Bytes) -> Corrupted,
    Corrupted: Future<Output = ()>,
{
    assert_put_repairs(store, corrupt, b"", b"junk").await;
}

pub async fn empty_placeholder_is_never_served_and_put_repairs_it<S, Corrupt, Corrupted>(
    store: &S,
    corrupt: Corrupt,
) where
    S: ContentStore,
    Corrupt: Fn(&S, ContentHash, Bytes) -> Corrupted,
    Corrupted: Future<Output = ()>,
{
    let original = b"hello world";
    let hash = put(store, original).await;
    corrupt(store, hash, Bytes::new()).await;
    for (offset, size) in [(0_u64, 11_u64), (0, u64::MAX), (3, 4), (0, 0), (20, 5)] {
        let end = offset.checked_add(size).filter(|end| *end <= 11);
        let result = store.get(&hash, offset, size).await;
        assert!(
            match (&result, end) {
                (Ok(bytes), Some(_)) => size == 0 && bytes.is_empty(),
                (Ok(_), None) | (Err(StoreError::Failed(_)), _) => false,
                (Err(StoreError::Rejected(GetRejection::UnknownContent(UnknownContent))), _) => {
                    true
                }
                (Err(StoreError::Rejected(GetRejection::OutOfBounds { len })), end) => {
                    end.is_none() && *len == 11
                }
            },
            "get({offset}, {size}) of a hash whose stored object is an empty placeholder gave {result:?}"
        );
    }
    assert_put_repairs(store, corrupt, original, b"").await;
}

pub async fn get_during_put_sees_absent_or_complete_blob<S: ContentStore>(store: &S) {
    let bytes = pattern(9, (3 << 20) + 17);
    let hash = ContentHash::of(&bytes);
    let len = bytes.len() as u64;
    let put_returned = Cell::new(false);
    let putting = async {
        let stored = put(store, &bytes).await;
        put_returned.set(true);
        stored
    };
    let reading = async {
        loop {
            let returned_before_get = put_returned.get();
            match store.get(&hash, 0, len).await {
                Ok(read) => {
                    assert!(
                        read == bytes,
                        "a get during a put returned {} bytes that are not the blob",
                        read.len()
                    );
                    return;
                }
                Err(StoreError::Rejected(GetRejection::UnknownContent(UnknownContent))) => {
                    assert!(
                        !returned_before_get,
                        "the blob was still unknown after its put returned"
                    );
                }
                Err(StoreError::Rejected(GetRejection::OutOfBounds { len: stored })) => {
                    panic!("a get during a put saw a partial blob of {stored} of {len} bytes")
                }
                Err(StoreError::Failed(error)) => panic!("a get during a put failed: {error:?}"),
            }
            yield_now().await;
        }
    };
    let (stored, ()) = join(putting, reading).await;
    assert_eq!(stored, hash);
    assert!(get(store, &hash, 0, len).await == bytes);
}

pub async fn put_never_exposes_a_partial_object<S, Inspect, Inspected>(store: &S, inspect: Inspect)
where
    S: ContentStore,
    Inspect: Fn(&S, ContentHash) -> Inspected,
    Inspected: Future<Output = Option<u64>>,
{
    let bytes = pattern(10, (5 << 20) + 3);
    let hash = ContentHash::of(&bytes);
    let len = bytes.len() as u64;
    assert_eq!(
        inspect(store, hash).await,
        None,
        "an object is stored under the name of a blob that was never put"
    );
    let mut putting = pin!(store.put(Bytes::copy_from_slice(&bytes)));
    let stored = loop {
        match poll_once(&mut putting).await {
            Poll::Ready(stored) => break stored,
            Poll::Pending => {
                let seen = inspect(store, hash).await;
                assert!(
                    seen.is_none_or(|seen| seen == len),
                    "an unfinished put of {len} bytes exposed an object of {seen:?} bytes under its name"
                );
            }
        }
    };
    match stored {
        Ok(stored) => assert_eq!(stored, hash),
        Err(error) => panic!("a put of {len} bytes failed: {error:?}"),
    }
    assert_eq!(inspect(store, hash).await, Some(len));
    assert!(get(store, &hash, 0, len).await == bytes);
}
