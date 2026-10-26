use std::convert::Infallible;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use redb::{Database, StorageBackend};
use stratafs_redb::{CommitDurability, Inline, RedbStore, SpawnBlocking};
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};

// Event is what happened, in order: the store's flush first step ran, or its file reached the disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Event {
    FlushedFirst,
    Flushed,
}

type Events = Arc<Mutex<Vec<Event>>>;

// RecordedFlushes is an in memory backend that records each flush to the disk.
#[derive(Debug)]
struct RecordedFlushes {
    backend: InMemoryBackend,
    events: Events,
}

impl StorageBackend for RecordedFlushes {
    fn len(&self) -> io::Result<u64> {
        self.backend.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.backend.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.backend.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.events.lock().unwrap().push(Event::Flushed);
        self.backend.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.backend.write(offset, data)
    }
}

// open is a store with durability, and the events its flushes record.
fn open(durability: CommitDurability) -> (RedbStore<Inline>, Events) {
    let events = Events::default();
    let database = Database::builder()
        .create_with_backend(RecordedFlushes {
            backend: InMemoryBackend::new(),
            events: events.clone(),
        })
        .unwrap();
    let flushed_first = events.clone();
    let store = RedbStore::new(database, Inline, durability, move || {
        flushed_first.lock().unwrap().push(Event::FlushedFirst);
        Ok(())
    })
    .unwrap();
    events.lock().unwrap().clear();
    (store, events)
}

// commit sets key to value in a transaction of its own, and returns the events the commit recorded.
fn commit<S: KeyValueStore>(store: &S, events: &Events, key: &'static [u8]) -> Vec<Event> {
    events.lock().unwrap().clear();
    block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction
                .set(Bytes::from_static(key), &b"value"[..])
                .unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
    std::mem::take(&mut *events.lock().unwrap())
}

fn read<S: KeyValueStore>(store: &S, key: &'static [u8]) -> Option<Bytes> {
    block_on(store.read_transact(|transaction| {
        Box::pin(async move {
            Ok::<_, Infallible>(transaction.get(Bytes::from_static(key)).await.unwrap())
        })
    }))
    .unwrap()
}

// flushed_after_flushing_first is whether events are a flush first step followed by at least one flush.
fn flushed_after_flushing_first(events: &[Event]) -> bool {
    matches!(events, [Event::FlushedFirst, rest @ ..] if !rest.is_empty() && rest.iter().all(|event| *event == Event::Flushed))
}

#[test]
fn every_commit_flushes_each_commit_after_flushing_first() {
    let (store, events) = open(CommitDurability::EveryCommit);
    for key in [&b"a"[..], b"b", b"c"] {
        let recorded = commit(&store, &events, key);
        assert!(
            flushed_after_flushing_first(&recorded),
            "{key:?}: {recorded:?}"
        );
    }
}

#[test]
fn never_flushes_no_commit_but_the_close_flushes_first_and_then_them_all() {
    let (store, events) = open(CommitDurability::Never);
    for key in [&b"a"[..], b"b", b"c"] {
        assert_eq!(commit(&store, &events, key), [], "{key:?}");
        assert_eq!(read(&store, key), Some(Bytes::from_static(b"value")));
    }
    drop(store);
    let recorded = events.lock().unwrap().clone();
    assert!(flushed_after_flushing_first(&recorded), "{recorded:?}");
}

#[test]
fn at_most_every_flushes_one_commit_per_interval_after_flushing_first() {
    let (store, events) = open(CommitDurability::AtMostEvery(Duration::from_secs(3600)));
    let recorded = commit(&store, &events, b"a");
    assert!(flushed_after_flushing_first(&recorded), "{recorded:?}");
    for key in [&b"b"[..], b"c"] {
        assert_eq!(commit(&store, &events, key), [], "{key:?}");
    }
    let (store, events) = open(CommitDurability::AtMostEvery(Duration::ZERO));
    for key in [&b"a"[..], b"b", b"c"] {
        let recorded = commit(&store, &events, key);
        assert!(
            flushed_after_flushing_first(&recorded),
            "{key:?}: {recorded:?}"
        );
    }
}

#[test]
fn a_failed_flush_first_fails_the_commit_and_keeps_it_out_of_the_store() {
    let database = Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let store = RedbStore::new(database, Inline, CommitDurability::EveryCommit, || {
        Err(io::Error::other("the content disk is gone"))
    })
    .unwrap();
    let committed = block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction
                .set(Bytes::from_static(b"a"), &b"value"[..])
                .unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }));
    assert!(committed.is_err());
    assert_eq!(read(&store, b"a"), None);
}

// CountedSpawns runs work in place, and counts the work handed to it.
struct CountedSpawns(Arc<AtomicUsize>);

impl SpawnBlocking for CountedSpawns {
    async fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        self.0.fetch_add(1, Ordering::SeqCst);
        work()
    }
}

#[test]
fn only_commits_that_flush_go_to_the_spawner() {
    for (durability, spawned) in [
        (CommitDurability::EveryCommit, 3),
        (CommitDurability::AtMostEvery(Duration::from_secs(3600)), 1),
        (CommitDurability::Never, 0),
    ] {
        let spawns = Arc::new(AtomicUsize::new(0));
        let database = Database::builder()
            .create_with_backend(InMemoryBackend::new())
            .unwrap();
        let store = RedbStore::new(database, CountedSpawns(spawns.clone()), durability, || {
            Ok(())
        })
        .unwrap();
        for key in [&b"a"[..], b"b", b"c"] {
            commit(&store, &Events::default(), key);
            assert_eq!(read(&store, key), Some(Bytes::from_static(b"value")));
        }
        assert_eq!(spawns.load(Ordering::SeqCst), spawned, "{durability:?}");
    }
}
