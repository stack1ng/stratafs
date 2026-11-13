// Blobs are read whole and checked against the hash their reference holds, and an engine keeps the blobs it checked
// last. A blob never changes, so one kept under its id and hash stays right.

// get returns `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
#![allow(clippy::manual_async_fn)]

use std::marker::PhantomData;

use bytes::{Buf, Bytes};
use quick_cache::Weighter;
use quick_cache::sync::Cache;
use stratafs_store::content::{ContentId, ContentStore};

use crate::FsConfig;
use crate::error::ContentReadError;
use crate::spillable::ContentHash;

// EXPECTED_BLOB_LENGTH is the length the kept blobs are expected to have, which only sizes the table they are kept in.
const EXPECTED_BLOB_LENGTH: u64 = 64 * 1024;

// CheckedBlobs is the blobs an engine checked last, each by its id and the hash it was checked against.
pub(crate) struct CheckedBlobs(Cache<(ContentId, ContentHash), Bytes, ByLength>);

impl CheckedBlobs {
    // new keeps blobs of up to bytes in all.
    pub(crate) fn new(bytes: u64) -> Self {
        let blobs = usize::try_from(bytes / EXPECTED_BLOB_LENGTH).unwrap_or(usize::MAX);
        CheckedBlobs(Cache::with_weighter(blobs.max(1), bytes, ByLength))
    }
}

// ByLength weighs a kept blob by its length.
#[derive(Clone)]
struct ByLength;

impl Weighter<(ContentId, ContentHash), Bytes> for ByLength {
    fn weight(&self, _: &(ContentId, ContentHash), blob: &Bytes) -> u64 {
        blob.len() as u64
    }
}

// CheckedContent is an engine's content store, read through the blobs the engine checked last.
pub struct CheckedContent<'engine, C, S> {
    pub(crate) store: &'engine C,
    pub(crate) checked: &'engine CheckedBlobs,
    pub(crate) _config: PhantomData<S>,
}

impl<C, S> Clone for CheckedContent<'_, C, S> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<C, S> Copy for CheckedContent<'_, C, S> {}

impl<'engine, C, S> CheckedContent<'engine, C, S> {
    // store is the content store itself, which checks nothing.
    pub fn store(&self) -> &'engine C {
        self.store
    }
}

impl<C: ContentStore, S: FsConfig> CheckedContent<'_, C, S> {
    // get is the whole blob of id, which a reference holds to be length bytes with hash.
    pub(crate) fn get(
        &self,
        id: ContentId,
        length: u64,
        hash: ContentHash,
    ) -> impl Future<Output = Result<Bytes, ContentReadError<C::Failure>>> + Send {
        async move {
            self.checked
                .0
                .get_or_insert_async(&(id, hash), async {
                    let mut blob = self
                        .store
                        .get(&id, 0, length)
                        .await
                        .map_err(ContentReadError::Store)?;
                    let blob = blob.copy_to_bytes(blob.remaining());
                    let held = blob.len() as u64 == length
                        && ContentHash::of::<S::HashFunction>(&blob[..]) == hash;
                    held.then_some(blob).ok_or(ContentReadError::Mismatched)
                })
                .await
        }
    }
}
