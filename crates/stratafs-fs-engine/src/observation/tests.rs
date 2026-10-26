use std::pin::pin;

use futures_lite::future::{block_on, poll_once};

use super::*;
use crate::annotations::{AnnotationKey, SetAnnotationMode};
use crate::directories::NewItem;
use crate::test_support::{InMemoryEngine, new_test_engine};

fn new_file(engine: &InMemoryEngine, name: &'static [u8]) -> (Inode, Attributes) {
    let created = block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.create(Inode::ROOT, name, NewItem::File, |_| {}).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    (created.inode, created.item)
}

fn write(engine: &InMemoryEngine, inode: Inode, data: &'static [u8]) {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.write(inode, 0, Bytes::from_static(data)).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
}

#[test]
fn a_watch_waits_until_its_inode_changes() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"watched");
    let (other, _) = new_file(&engine, b"other");
    let mut changed = pin!(engine.changed(inode, &seen));
    assert!(block_on(poll_once(changed.as_mut())).is_none());
    write(&engine, other, b"elsewhere");
    assert!(block_on(poll_once(changed.as_mut())).is_none());
    write(&engine, inode, b"here");
    assert!(block_on(changed).is_ok());
}

#[test]
fn a_watch_resolves_at_once_when_seen_is_already_stale() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"watched");
    write(&engine, inode, b"before the watch");
    assert!(
        block_on(poll_once(engine.changed(inode, &seen)))
            .unwrap()
            .is_ok()
    );
}

#[test]
fn an_annotation_change_resolves_a_watch() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"watched");
    let changed = engine.changed(inode, &seen);
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn
                .set_annotation(
                    inode,
                    AnnotationKey::Portable(b"a"),
                    &b"1"[..],
                    SetAnnotationMode::Create,
                )
                .await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    assert!(block_on(changed).is_ok());
}

#[test]
fn deleting_the_inode_resolves_a_watch() {
    let engine = new_test_engine();
    let (inode, seen) = new_file(&engine, b"watched");
    let changed = engine.changed(inode, &seen);
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.hard_unlink(Inode::ROOT, b"watched", None).await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    assert!(block_on(changed).is_ok());
}
