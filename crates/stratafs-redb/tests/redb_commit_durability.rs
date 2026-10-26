use std::convert::Infallible;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use redb::{Database, StorageBackend};
use stratafs_redb::{RedbStore, SpawnBlocking};
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction};

// CountedFlushes is an in memory backend that counts each flush to the disk.
#[derive(Debug)]
struct CountedFlushes {
    backend: InMemoryBackend,
    flushes: Arc<AtomicUsize>,
}

impl StorageBackend for CountedFlushes {
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
        self.flushes.fetch_add(1, Ordering::SeqCst);
        self.backend.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.backend.write(offset, data)
    }
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

// open is a store that counts its flushes to the disk and the work it hands to its spawner.
fn open() -> (RedbStore<CountedSpawns>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let (flushes, spawns) = (Arc::default(), Arc::default());
    let database = Database::builder()
        .create_with_backend(CountedFlushes {
            backend: InMemoryBackend::new(),
            flushes: Arc::clone(&flushes),
        })
        .unwrap();
    let store = RedbStore::new(database, CountedSpawns(Arc::clone(&spawns))).unwrap();
    flushes.store(0, Ordering::SeqCst);
    (store, flushes, spawns)
}

fn commit<S: KeyValueStore>(store: &S, key: &'static [u8]) {
    block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction
                .set(Bytes::from_static(key), &b"value"[..])
                .unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
}

#[test]
fn ensure_durable_flushes_what_was_committed_once() {
    let (store, flushes, spawns) = open();
    for key in [&b"a"[..], b"b", b"c"] {
        commit(&store, key);
    }
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
    assert_eq!(spawns.load(Ordering::SeqCst), 0);

    block_on(store.ensure_durable()).unwrap();
    assert!(flushes.load(Ordering::SeqCst) > 0);
    assert_eq!(spawns.load(Ordering::SeqCst), 1);

    // Nothing was committed since, so there is nothing to take to the disk.
    flushes.store(0, Ordering::SeqCst);
    block_on(store.ensure_durable()).unwrap();
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
}
