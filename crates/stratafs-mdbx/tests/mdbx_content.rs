mod common;
mod configuration;

use std::convert::Infallible;

use bytes::{Buf, Bytes};
use common::Stores;
use futures_lite::future::block_on;
use stratafs_mdbx::MdbxContent;
use stratafs_store::content::{ContentId, ContentStore, GetRejection, UnknownContent};
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

fn read(content: &MdbxContent<Inline>, id: &ContentId, start: usize, end: usize) -> Bytes {
    let mut read = block_on(content.get(id, start as u64, (end - start) as u64)).unwrap();
    read.copy_to_bytes(read.remaining())
}

#[test]
fn stores_content_under_its_id() {
    let stores = Stores::new();
    stratafs_content_conformance::stores_content_under_its_id(|| MdbxContent::of(&stores.open()));
}

#[test]
fn reads_ranges() {
    let stores = Stores::new();
    stratafs_content_conformance::reads_ranges(|| MdbxContent::of(&stores.open()));
}

#[test]
fn rejects_ranges_beyond_the_content() {
    let stores = Stores::new();
    stratafs_content_conformance::rejects_ranges_beyond_the_content(|| {
        MdbxContent::of(&stores.open())
    });
}

#[test]
fn rejects_unknown_content() {
    let stores = Stores::new();
    stratafs_content_conformance::rejects_unknown_content(|| MdbxContent::of(&stores.open()));
}

#[test]
fn keeps_the_same_content_under_each_id_apart() {
    let stores = Stores::new();
    stratafs_content_conformance::keeps_the_same_content_under_each_id_apart(|| {
        MdbxContent::of(&stores.open())
    });
}

#[test]
fn deletes_content() {
    let stores = Stores::new();
    stratafs_content_conformance::deletes_content(|| MdbxContent::of(&stores.open()));
}

#[test]
fn deletes_content_that_is_already_gone() {
    let stores = Stores::new();
    stratafs_content_conformance::deletes_content_that_is_already_gone(|| {
        MdbxContent::of(&stores.open())
    });
}

#[test]
fn content_written_to_the_file_reads_back_from_any_range_across_its_chunks() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let long = long_content();
    let id = ContentId(7);
    {
        let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
        block_on(content.put(&id, long.clone())).unwrap();
        // ensure_durable writes the content no commit has written yet.
        block_on(content.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
    }
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
            read(&content, &id, start, end),
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
                let id = ContentId(7);
                content.put(&id, long.clone()).await.unwrap();
                transaction
                    .set(Bytes::from_static(b"file"), &id.0.to_be_bytes()[..])
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
    let id = ContentId(u128::from_be_bytes(named[..].try_into().unwrap()));
    assert_eq!(read(&content, &id, 0, LONG), long);
}

#[test]
fn content_no_commit_writes_is_gone_after_the_store_closes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let id = ContentId(7);
    {
        let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
        block_on(content.put(&id, long_content())).unwrap();
        assert_eq!(read(&content, &id, 0, LONG), long_content());
    }
    let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
    assert!(matches!(
        block_on(content.get(&id, 0, 0)),
        Err(StoreError::Rejected(GetRejection::UnknownContent(
            UnknownContent
        )))
    ));
}

#[test]
fn content_deleted_once_written_is_gone_from_the_file_and_the_rest_reads_back_whole() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let long = long_content();
    let (deleted, kept) = (ContentId(1), ContentId(2));
    {
        let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
        block_on(content.put(&deleted, long.clone())).unwrap();
        block_on(content.put(&kept, long.clone())).unwrap();
        block_on(content.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
        block_on(content.delete(&deleted)).unwrap();
        block_on(content.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
    }
    let content = MdbxContent::of(&configuration::open(&path, Inline).unwrap());
    assert!(matches!(
        block_on(content.get(&deleted, 0, 0)),
        Err(StoreError::Rejected(GetRejection::UnknownContent(
            UnknownContent
        )))
    ));
    assert_eq!(read(&content, &kept, 0, LONG), long);
    assert!(matches!(
        block_on(content.get(&kept, LONG as u64, 1)),
        Err(StoreError::Rejected(GetRejection::OutOfBounds { len })) if len == LONG as u64
    ));
}
