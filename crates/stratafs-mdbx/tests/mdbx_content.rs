mod common;
mod configuration;

use std::convert::Infallible;

use bytes::{Buf, Bytes};
use futures_lite::future::block_on;
use stratafs_mdbx::MdbxContent;
use stratafs_store::content::{ContentHash, ContentStore, GetRejection, UnknownContent};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};
use stratafs_store::spawn::Inline;

// Content this long fills three chunks and part of a fourth.
const LONG: usize = 3 * 256 * 1024 + 1000;

fn long_content() -> Bytes {
    (0..LONG)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect()
}

fn read(content: &MdbxContent<Inline>, hash: &ContentHash, start: usize, end: usize) -> Bytes {
    let mut read = block_on(content.get(hash, start as u64, (end - start) as u64)).unwrap();
    read.copy_to_bytes(read.remaining())
}

#[test]
fn stores_content_under_its_hash() {
    stratafs_content_conformance::stores_content_under_its_hash(|| {
        MdbxContent::of(&common::open_store())
    });
}

#[test]
fn reads_ranges() {
    stratafs_content_conformance::reads_ranges(|| MdbxContent::of(&common::open_store()));
}

#[test]
fn rejects_ranges_beyond_the_content() {
    stratafs_content_conformance::rejects_ranges_beyond_the_content(|| {
        MdbxContent::of(&common::open_store())
    });
}

#[test]
fn rejects_unknown_content() {
    stratafs_content_conformance::rejects_unknown_content(
        || MdbxContent::of(&common::open_store()),
    );
}

#[test]
fn content_written_to_the_file_reads_back_from_any_range_across_its_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let long = long_content();
    let hash = {
        let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
        let hash = block_on(content.put(long.clone())).unwrap();
        // ensure_durable writes the content no commit has written yet.
        block_on(content.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
        hash
    };
    let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
    let chunk = 256 * 1024;
    for (start, end) in [
        (0, LONG),
        (chunk - 10, chunk + 10),
        (chunk - 1, 3 * chunk + 1),
        (chunk, 2 * chunk),
        (3 * chunk, LONG),
        (LONG - 1, LONG),
        (LONG, LONG),
    ] {
        assert_eq!(
            read(&content, &hash, start, end),
            long.slice(start..end),
            "{start}..{end}"
        );
    }
}

#[test]
fn content_a_transaction_puts_commits_with_the_metadata_naming_it() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let long = long_content();
    {
        let metadata = configuration::open(&path, Inline).unwrap();
        let content = MdbxContent::of(&metadata);
        let (content, long) = (&content, &long);
        block_on(metadata.transact(|mut transaction| {
            Box::pin(async move {
                let hash = content.put(long.clone()).await.unwrap();
                transaction
                    .set(Bytes::from_static(b"file"), &hash.0[..])
                    .unwrap();
                Ok::<_, Infallible>(((), transaction))
            })
        }))
        .unwrap();
    }
    let metadata = configuration::open(&path, Inline).unwrap();
    let content = MdbxContent::of(&metadata);
    let named = block_on(metadata.read_transact(|transaction| {
        Box::pin(async move {
            Ok::<_, Infallible>(
                transaction
                    .get(Bytes::from_static(b"file"))
                    .await
                    .unwrap()
                    .unwrap(),
            )
        })
    }))
    .unwrap();
    let hash = ContentHash(named[..].try_into().unwrap());
    assert_eq!(read(&content, &hash, 0, LONG), long);
}

#[test]
fn content_no_commit_writes_is_gone_after_the_store_closes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let hash = {
        let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
        let hash = block_on(content.put(long_content())).unwrap();
        assert_eq!(read(&content, &hash, 0, LONG), long_content());
        hash
    };
    let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
    assert!(matches!(
        block_on(content.get(&hash, 0, 0)),
        Err(StoreError::Rejected(GetRejection::UnknownContent(
            UnknownContent
        )))
    ));
}

#[test]
fn content_put_again_once_written_reads_back_whole_and_refuses_reads_past_its_end() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let long = long_content();
    let hash = {
        let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
        let mut hash = None;
        // The second put finds the content in the file already.
        for _ in 0..2 {
            hash = Some(block_on(content.put(long.clone())).unwrap());
            block_on(content.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
        }
        hash.unwrap()
    };
    let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
    assert_eq!(read(&content, &hash, 0, LONG), long);
    assert!(matches!(
        block_on(content.get(&hash, LONG as u64, 1)),
        Err(StoreError::Rejected(GetRejection::OutOfBounds { len })) if len == LONG as u64
    ));
}
