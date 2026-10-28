#![cfg(disk)]

use bytes::Buf;
use futures_lite::future::block_on;
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_store::content::{ContentHash, ContentStore};
use stratafs_store::durability::DurableAgainst;

fn blob_file(root: &std::path::Path, hash: &ContentHash) -> std::path::PathBuf {
    root.join(format!(
        "blobs/{:02x}/{:032x}",
        hash.0[0],
        u128::from_be_bytes(hash.0)
    ))
}

#[test]
fn a_local_store_keeps_each_blob_as_a_file_that_a_new_store_reads_back() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectStoreContentStore::local(root.path()).unwrap();
    let hash = block_on(store.put(&b"hello world"[..])).unwrap();
    assert_eq!(hash, ContentHash::of(b"hello world"));
    assert!(blob_file(root.path(), &hash).is_file());
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    let reopened = ObjectStoreContentStore::local(root.path()).unwrap();
    let mut read = block_on(reopened.get(&hash, 6, 5)).unwrap();
    assert_eq!(read.copy_to_bytes(read.remaining()), &b"world"[..]);
}

#[test]
fn ensure_durable_that_fails_keeps_its_files_for_the_next_time() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectStoreContentStore::local(root.path()).unwrap();
    let hash = block_on(store.put(&b"hello"[..])).unwrap();
    let file = blob_file(root.path(), &hash);
    let kept = std::fs::read(&file).unwrap();
    std::fs::remove_file(&file).unwrap();
    assert!(block_on(store.ensure_durable(DurableAgainst::PowerFailure)).is_err());
    assert!(block_on(store.ensure_durable(DurableAgainst::PowerFailure)).is_err());
    std::fs::write(&file, kept).unwrap();
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
}
