#![cfg(disk)]

use bytes::Buf;
use futures_lite::future::block_on;
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_store::content::{ContentId, ContentStore};
use stratafs_store::durability::DurableAgainst;

fn blob_file(root: &std::path::Path, id: &ContentId) -> std::path::PathBuf {
    root.join(format!("blobs/{:02x}/{:032x}", id.0 >> 120, id.0))
}

#[test]
fn a_local_store_keeps_each_blob_as_a_file_that_a_new_store_reads_back() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectStoreContentStore::local(root.path()).unwrap();
    let id = ContentId(0xab << 120 | 7);
    block_on(store.put(&id, &b"hello world"[..])).unwrap();
    assert!(blob_file(root.path(), &id).is_file());
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    let reopened = ObjectStoreContentStore::local(root.path()).unwrap();
    let mut read = block_on(reopened.get(&id, 6, 5)).unwrap();
    assert_eq!(read.copy_to_bytes(read.remaining()), &b"world"[..]);
}

#[test]
fn ensure_durable_that_fails_keeps_its_files_for_the_next_time() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectStoreContentStore::local(root.path()).unwrap();
    let id = ContentId(7);
    block_on(store.put(&id, &b"hello"[..])).unwrap();
    let file = blob_file(root.path(), &id);
    let kept = std::fs::read(&file).unwrap();
    std::fs::remove_file(&file).unwrap();
    assert!(block_on(store.ensure_durable(DurableAgainst::PowerFailure)).is_err());
    assert!(block_on(store.ensure_durable(DurableAgainst::PowerFailure)).is_err());
    std::fs::write(&file, kept).unwrap();
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
}

#[test]
fn a_blob_deleted_before_ensure_durable_leaves_nothing_for_it_to_flush() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectStoreContentStore::local(root.path()).unwrap();
    let (deleted, kept) = (ContentId(7), ContentId(8));
    block_on(store.put(&deleted, &b"deleted"[..])).unwrap();
    block_on(store.put(&kept, &b"kept"[..])).unwrap();
    block_on(store.delete(&deleted)).unwrap();
    assert!(!blob_file(root.path(), &deleted).exists());
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    assert!(blob_file(root.path(), &kept).is_file());
}
