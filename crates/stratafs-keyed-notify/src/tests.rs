use std::convert::Infallible;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use futures_lite::future::{block_on, poll_once};

use crate::{KeyedNotify, key_hash};

// waiting waits on key until done is set.
async fn waiting(notify: &KeyedNotify, key: &Bytes, done: &AtomicBool) -> Result<(), Infallible> {
    notify
        .wait_until(key, || async { Ok(done.load(Ordering::SeqCst)) })
        .await
}

#[test]
fn waking_a_hash_wakes_the_waiters_of_its_key() {
    let notify = KeyedNotify::new();
    let (key, done) = (Bytes::from_static(b"key"), AtomicBool::new(false));
    let mut waiter = pin!(waiting(&notify, &key, &done));
    assert!(block_on(poll_once(waiter.as_mut())).is_none());
    done.store(true, Ordering::SeqCst);
    notify.waiters.wake([key_hash(b"other")]);
    assert!(block_on(poll_once(waiter.as_mut())).is_none());
    notify.waiters.wake([key_hash(&key)]);
    assert!(block_on(poll_once(waiter.as_mut())).is_some());
}

#[test]
fn notify_waiters_wakes_only_the_waiters_of_its_keys() {
    let notify = KeyedNotify::new();
    let (key, done) = (Bytes::from_static(b"key"), AtomicBool::new(false));
    let mut waiter = pin!(waiting(&notify, &key, &done));
    assert!(block_on(poll_once(waiter.as_mut())).is_none());
    done.store(true, Ordering::SeqCst);
    notify.notify_waiters(&[Bytes::from_static(b"other")]);
    assert!(block_on(poll_once(waiter.as_mut())).is_none());
    notify.notify_waiters(&[Bytes::from_static(b"key")]);
    assert!(block_on(poll_once(waiter.as_mut())).is_some());
}

#[test]
fn waking_all_wakes_every_waiter() {
    let notify = KeyedNotify::new();
    let (first, second) = (Bytes::from_static(b"first"), Bytes::from_static(b"second"));
    let done = AtomicBool::new(false);
    let mut first_waiter = pin!(waiting(&notify, &first, &done));
    let mut second_waiter = pin!(waiting(&notify, &second, &done));
    assert!(block_on(poll_once(first_waiter.as_mut())).is_none());
    assert!(block_on(poll_once(second_waiter.as_mut())).is_none());
    done.store(true, Ordering::SeqCst);
    notify.waiters.wake_all();
    assert!(block_on(poll_once(first_waiter.as_mut())).is_some());
    assert!(block_on(poll_once(second_waiter.as_mut())).is_some());
}

#[test]
fn a_shared_notify_wakes_the_waiters_of_the_keys_it_notifies_on_another() {
    let directory = tempfile::tempdir().unwrap();
    let watching = KeyedNotify::shared(directory.path()).unwrap();
    let committing = KeyedNotify::shared(directory.path()).unwrap();
    let (key, other) = (Bytes::from_static(b"key"), Bytes::from_static(b"other"));
    let done = AtomicBool::new(false);
    let mut waiter = pin!(waiting(&watching, &key, &done));
    let mut other_waiter = pin!(waiting(&watching, &other, &done));
    assert!(block_on(poll_once(waiter.as_mut())).is_none());
    assert!(block_on(poll_once(other_waiter.as_mut())).is_none());
    done.store(true, Ordering::SeqCst);
    committing.notify_waiters(&[Bytes::from_static(b"unwatched")]);
    committing.notify_waiters(std::slice::from_ref(&other));
    // The reader reads in order, so once other's waiter wakes it has read the unwatched key.
    block_on(other_waiter).unwrap();
    assert!(block_on(poll_once(waiter.as_mut())).is_none());
    committing.notify_waiters(std::slice::from_ref(&key));
    block_on(waiter).unwrap();
}
