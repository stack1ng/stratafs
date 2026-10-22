use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Mutex;

use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use stratafs_redb::{Inline, RedbStore};
use stratafs_store::content::{GetRejection, UnknownContent};
use stratafs_store::error::StoreError;

use super::*;
use crate::{FsEngine, Timestamp};

#[derive(Default)]
struct SimulatedContentStore {
    blobs: Mutex<HashMap<ContentHash, Bytes>>,
}

impl ContentStore for SimulatedContentStore {
    type Failure = Infallible;

    async fn put(&self, bytes: Bytes) -> Result<ContentHash, Infallible> {
        let hash = ContentHash::of(&bytes);
        self.blobs.lock().unwrap().insert(hash, bytes);
        Ok(hash)
    }

    async fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> Result<Bytes, StoreError<GetRejection, Infallible>> {
        let blobs = self.blobs.lock().unwrap();
        let blob = blobs
            .get(hash)
            .ok_or(StoreError::Rejected(GetRejection::UnknownContent(
                UnknownContent,
            )))?;
        let len = blob.len() as u64;
        match offset.checked_add(size) {
            Some(end) if end <= len => Ok(blob.slice(offset as usize..end as usize)),
            _ => Err(StoreError::Rejected(GetRejection::OutOfBounds { len })),
        }
    }
}

struct OneKibInline;

impl FsConfig for OneKibInline {
    const LARGEST_INLINE_ENTRY: usize = 1024;
}

type SimulatedEngine = FsEngine<
    RedbStore<Inline>,
    SimulatedContentStore,
    fn() -> u64,
    fn() -> Timestamp,
    OneKibInline,
>;

fn new_engine() -> SimulatedEngine {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    FsEngine::new(
        RedbStore::new(database, Inline).unwrap(),
        SimulatedContentStore::default(),
        || 0,
        || Timestamp(0),
    )
}

const KEY: &[u8] = b"key";

fn make_spillable(engine: &SimulatedEngine, content: Bytes) -> SpillableValue {
    block_on(engine.transact(|txn| {
        let content = content.clone();
        Box::pin(async move { txn.make_spillable(KEY, content).await })
    }))
    .unwrap()
}

fn content_of_length(length: usize) -> Bytes {
    Bytes::from((0..length).map(|index| index as u8).collect::<Vec<_>>())
}

#[test]
fn small_content_stays_inline() {
    let engine = new_engine();
    let content = content_of_length(10);
    assert_eq!(
        make_spillable(&engine, content.clone()),
        SpillableValue::Inline(content)
    );
    assert!(engine.content.blobs.lock().unwrap().is_empty());
}

#[test]
fn content_filling_the_largest_inline_entry_exactly_stays_inline() {
    let engine = new_engine();
    let content = content_of_length(
        OneKibInline::LARGEST_INLINE_ENTRY - KEY.len() - SpillableValue::TAG_LENGTH,
    );
    assert_eq!(
        make_spillable(&engine, content.clone()),
        SpillableValue::Inline(content)
    );
    assert!(engine.content.blobs.lock().unwrap().is_empty());
}

#[test]
fn content_one_byte_over_the_largest_inline_entry_spills_to_the_content_store() {
    let engine = new_engine();
    let content = content_of_length(
        OneKibInline::LARGEST_INLINE_ENTRY - KEY.len() - SpillableValue::TAG_LENGTH + 1,
    );
    let hash = ContentHash::of(&content);
    assert_eq!(
        make_spillable(&engine, content.clone()),
        SpillableValue::Overflow {
            hash,
            length: content.len() as u64,
        }
    );
    assert_eq!(
        engine.content.blobs.lock().unwrap().get(&hash),
        Some(&content)
    );
}
