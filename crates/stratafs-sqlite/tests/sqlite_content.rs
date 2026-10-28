mod common;
mod stores;

use std::convert::Infallible;
use std::sync::{Barrier, mpsc};
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_lite::future::block_on;
use stores::Stores;
use stratafs_sqlite::SqliteContent;
use stratafs_store::content::{ContentHash, ContentStore, GetRejection, UnknownContent};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, TransactError};

// Content this long fills three chunks and part of a fourth.
const LONG: usize = 3 * 256 * 1024 + 1000;

fn long_content() -> Bytes {
    (0..LONG)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect()
}

#[test]
fn content_reads_back_from_any_range_across_its_chunks() {
    let stores = Stores::new();
    let store = stores.open();
    let content = SqliteContent::of(&store);
    let long = long_content();
    let hash = block_on(content.put(long.clone())).unwrap();
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
        let mut read = block_on(content.get(&hash, start as u64, (end - start) as u64)).unwrap();
        assert_eq!(
            read.copy_to_bytes(read.remaining()),
            long.slice(start..end),
            "{start}..{end}"
        );
    }
}

#[test]
fn content_a_transaction_puts_commits_with_it() {
    let stores = Stores::new();
    let store = stores.open();
    let content = SqliteContent::of(&store);
    let long = long_content();
    let (hash, read_inside) = block_on(store.transact(|mut transaction| {
        let (content, long) = (&content, long.clone());
        Box::pin(async move {
            let hash = content.put(long).await.unwrap();
            let mut read = content.get(&hash, 0, LONG as u64).await.unwrap();
            transaction
                .set(Bytes::from_static(b"file"), &hash.0[..])
                .unwrap();
            Ok::<_, Infallible>(((hash, read.copy_to_bytes(read.remaining())), transaction))
        })
    }))
    .unwrap();
    assert_eq!(read_inside, long);
    let mut read = block_on(content.get(&hash, 0, LONG as u64)).unwrap();
    assert_eq!(read.copy_to_bytes(read.remaining()), long);
}

#[test]
fn content_work_that_aborts_puts_is_rolled_back_with_it() {
    let stores = Stores::new();
    let store = stores.open();
    let content = SqliteContent::of(&store);
    let aborted = block_on(store.transact::<(), _>(|_transaction| {
        let content = &content;
        Box::pin(async move { Err(content.put(&b"aborted"[..]).await.unwrap()) })
    }));
    let Err(TransactError::<Infallible, rusqlite::Error, _>::Aborted(hash)) = aborted else {
        panic!("the work was not aborted");
    };
    assert!(matches!(
        block_on(content.get(&hash, 0, 0)),
        Err(StoreError::Rejected(GetRejection::UnknownContent(
            UnknownContent
        )))
    ));
}

#[test]
fn content_put_while_another_transaction_runs_waits_for_it_and_outlives_its_abort() {
    let stores = Stores::new();
    let store = stores.open();
    let content = SqliteContent::of(&store);
    let (holding, released) = (Barrier::new(2), Barrier::new(2));
    let (put, putting) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            block_on(store.transact::<(), _>(|_transaction| {
                let (holding, released) = (&holding, &released);
                Box::pin(async move {
                    holding.wait();
                    released.wait();
                    Err(())
                })
            }))
        });
        holding.wait();
        scope.spawn(|| {
            put.send(block_on(content.put(&b"put meanwhile"[..])))
                .unwrap()
        });
        assert!(
            putting.recv_timeout(Duration::from_millis(100)).is_err(),
            "the put went into the other transaction"
        );
        released.wait();
    });
    let hash = putting.recv().unwrap().unwrap();
    assert_eq!(hash, ContentHash::of(b"put meanwhile"));
    let mut read = block_on(content.get(&hash, 0, 13)).unwrap();
    assert_eq!(read.copy_to_bytes(read.remaining()), &b"put meanwhile"[..]);
}
