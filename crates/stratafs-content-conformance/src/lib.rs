use std::collections::VecDeque;

use bytes::{Buf, Bytes};
use futures_lite::future::block_on;
use proptest::collection::vec;
use proptest::prelude::any;
use proptest::sample::Index;
use proptest::{prop_assert, prop_assert_eq, proptest};
use stratafs_store::content::{ContentHash, ContentStore, GetRejection};
use stratafs_store::error::StoreError;

pub fn stores_content_under_its_hash<S: ContentStore>(new_store: impl Fn() -> S) {
    proptest!(|(content in vec(any::<u8>(), 0..4096), splits in vec(any::<Index>(), 0..4))| {
        let store = new_store();
        let content = Bytes::from(content);
        let hash = block_on(store.put(pieces(&content, &splits))).unwrap();
        prop_assert_eq!(hash, ContentHash::of(&content));
        prop_assert_eq!(block_on(store.put(content.clone())).unwrap(), hash);
        prop_assert_eq!(read(&store, &hash, 0, content.len() as u64), content);
    });
}

pub fn reads_ranges<S: ContentStore>(new_store: impl Fn() -> S) {
    proptest!(|(content in vec(any::<u8>(), 0..4096), start in any::<Index>(), end in any::<Index>())| {
        let store = new_store();
        let content = Bytes::from(content);
        let hash = block_on(store.put(content.clone())).unwrap();
        let start = start.index(content.len() + 1);
        let end = start + end.index(content.len() - start + 1);
        prop_assert_eq!(
            read(&store, &hash, start as u64, (end - start) as u64),
            content.slice(start..end)
        );
    });
}

pub fn rejects_ranges_beyond_the_content<S: ContentStore>(new_store: impl Fn() -> S) {
    proptest!(|(content in vec(any::<u8>(), 0..4096), offset in any::<Index>(), excess in 1u64..=3)| {
        let len = content.len() as u64;
        let offset = offset.index(content.len() + 1) as u64;
        let size = len - offset + excess;
        let store = new_store();
        let hash = block_on(store.put(Bytes::from(content))).unwrap();
        prop_assert!(
            rejected_as_out_of_bounds(&store, &hash, offset, size, len),
            "reading {size} bytes at {offset} of {len} was not rejected as out of bounds"
        );
    });
    let store = new_store();
    let content = Bytes::from_static(b"content");
    let len = content.len() as u64;
    let hash = block_on(store.put(content)).unwrap();
    for (offset, size) in [
        (u64::MAX, 1),
        (1, u64::MAX),
        (u64::MAX, u64::MAX),
        (len + 1, 0),
    ] {
        assert!(
            rejected_as_out_of_bounds(&store, &hash, offset, size, len),
            "reading {size} bytes at {offset} of {len} was not rejected as out of bounds"
        );
    }
}

pub fn rejects_unknown_content<S: ContentStore>(new_store: impl Fn() -> S) {
    let store = new_store();
    block_on(store.put(Bytes::from_static(b"stored"))).unwrap();
    let unknown = ContentHash::of(b"never stored");
    assert!(
        matches!(
            block_on(store.get(&unknown, 0, 0)),
            Err(StoreError::Rejected(GetRejection::UnknownContent(_)))
        ),
        "reading content that was never stored was not rejected as unknown"
    );
}

fn rejected_as_out_of_bounds<S: ContentStore>(
    store: &S,
    hash: &ContentHash,
    offset: u64,
    size: u64,
    len: u64,
) -> bool {
    matches!(
        block_on(store.get(hash, offset, size)),
        Err(StoreError::Rejected(GetRejection::OutOfBounds { len: reported })) if reported == len
    )
}

fn read<S: ContentStore>(store: &S, hash: &ContentHash, offset: u64, size: u64) -> Bytes {
    let mut content = block_on(store.get(hash, offset, size)).unwrap();
    content.copy_to_bytes(content.remaining())
}

fn pieces(content: &Bytes, splits: &[Index]) -> Pieces {
    let mut boundaries: Vec<usize> = splits
        .iter()
        .map(|split| split.index(content.len() + 1))
        .chain([0, content.len()])
        .collect();
    boundaries.sort_unstable();
    boundaries.dedup();
    Pieces(
        boundaries
            .windows(2)
            .map(|piece| content.slice(piece[0]..piece[1]))
            .collect(),
    )
}

struct Pieces(VecDeque<Bytes>);

impl Buf for Pieces {
    fn remaining(&self) -> usize {
        self.0.iter().map(Bytes::len).sum()
    }

    fn chunk(&self) -> &[u8] {
        self.0.front().map_or(&[], |piece| &piece[..])
    }

    fn advance(&mut self, mut count: usize) {
        while count > 0 {
            let piece = self
                .0
                .front_mut()
                .expect("advanced past the end of the pieces");
            let step = count.min(piece.len());
            piece.advance(step);
            count -= step;
            if piece.is_empty() {
                self.0.pop_front();
            }
        }
    }
}
