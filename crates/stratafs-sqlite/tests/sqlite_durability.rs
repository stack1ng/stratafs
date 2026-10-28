mod common;
mod crashable;

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Duration;

use bytes::{Buf, Bytes};
use common::SimulatedThreadPool;
use crashable::{CrashableMemory, CrashableStores};
use futures_lite::future::block_on;
use stratafs_kv_conformance::runtime::seeded;
use stratafs_sqlite::{SqliteContent, SqliteMetadata};
use stratafs_store::content::{ContentHash, ContentStore, GetRejection};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, TransactError,
};
use stratafs_store::spawn::{SpawnBlocking, ThreadPool};

// A checkpoint every few commits takes crashes through checkpoints and restarts of the write ahead log.
const CHECKPOINT_OFTEN: u32 = 4;
// A checkpoint this far away never comes in a test, so every sync a test sees is its own.
const CHECKPOINT_NEVER: u32 = u32::MAX;

#[test]
fn is_durable() {
    let stores = Arc::new(CrashableStores::new(CHECKPOINT_OFTEN));
    let reopened = Arc::clone(&stores);
    stratafs_kv_conformance::is_durable(
        seeded(),
        move || stores.open(SimulatedThreadPool),
        move |memory| reopened.reopen_torn(memory, SimulatedThreadPool),
    );
}

#[test]
fn ensure_durable_makes_what_a_killed_process_committed_durable() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (store, memory) = stores.open(SimulatedThreadPool);
    set(&store, b"committed before the kill");
    // A killed process closes nothing.
    std::mem::forget(store);
    let (reopened, left) = stores.reopen_killed(&memory, SimulatedThreadPool);
    block_on(reopened.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    let after_power_failure = stores.reopen_synced(left, SimulatedThreadPool);
    assert_eq!(
        get(&after_power_failure, b"committed before the kill"),
        Some(Bytes::from_static(b"value"))
    );
}

#[test]
fn ensure_durable_makes_what_another_process_committed_durable() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (first, second, memory) = stores.open_two(SimulatedThreadPool, SimulatedThreadPool);
    set(&first, b"committed first");
    block_on(first.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    set(&second, b"committed by the other process");
    block_on(first.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    // The power fails while both processes run.
    std::mem::forget((first, second));
    let after_power_failure = stores.reopen_synced(memory, SimulatedThreadPool);
    assert_eq!(
        get(&after_power_failure, b"committed by the other process"),
        Some(Bytes::from_static(b"value"))
    );
}

#[test]
fn what_ensure_durable_made_durable_survives_a_crash() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (store, memory) = stores.open(SimulatedThreadPool);
    set(&store, b"made durable");
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    set(&store, b"left unsynced");
    let reopened = stores.reopen_synced(memory, SimulatedThreadPool);
    assert_eq!(
        get(&reopened, b"made durable"),
        Some(Bytes::from_static(b"value"))
    );
    // The crash loses the commit nothing made durable, which shows the store really crashed.
    assert_eq!(get(&reopened, b"left unsynced"), None);
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
fn ensure_durable_syncs_what_was_committed_once() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let spawns = Arc::default();
    let (store, memory) = stores.open(CountedSpawns(Arc::clone(&spawns)));
    let synced = || std::mem::take(&mut memory.state().syncs);
    synced();
    for key in [&b"a"[..], b"b", b"c"] {
        set(&store, key);
    }
    assert_eq!(synced(), []);
    assert_eq!(spawns.load(Ordering::SeqCst), 0);

    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    assert_eq!(synced(), [DurableAgainst::PowerFailure]);
    assert_eq!(spawns.load(Ordering::SeqCst), 1);

    // Nothing was committed since, so there is nothing to sync.
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    assert_eq!(synced(), []);
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
}

#[test]
fn ensure_durable_syncs_only_what_is_not_yet_durable_against_the_failure() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (store, memory) = stores.open(SimulatedThreadPool);
    let synced_by = |against| {
        memory.state().syncs.clear();
        block_on(store.ensure_durable(against)).unwrap();
        std::mem::take(&mut memory.state().syncs)
    };
    set(&store, b"a");
    assert_eq!(
        synced_by(DurableAgainst::SoftwareFailure),
        [DurableAgainst::SoftwareFailure]
    );
    assert_eq!(synced_by(DurableAgainst::SoftwareFailure), []);
    // Surviving a software failure does not make the commit survive a power failure.
    assert_eq!(
        synced_by(DurableAgainst::PowerFailure),
        [DurableAgainst::PowerFailure]
    );
    assert_eq!(synced_by(DurableAgainst::SoftwareFailure), []);
    assert_eq!(synced_by(DurableAgainst::PowerFailure), []);
}

#[test]
fn a_commit_that_fills_the_log_checkpoints_it_on_the_spawner_in_order() {
    let stores = CrashableStores::new(CHECKPOINT_OFTEN);
    let spawns = Arc::default();
    let (store, memory) = stores.open(CountedSpawns(Arc::clone(&spawns)));
    memory.state().syncs.clear();
    for key in [&b"a"[..], b"b", b"c", b"d", b"e", b"f"] {
        set(&store, key);
    }
    assert!(spawns.load(Ordering::SeqCst) > 0);
    // A checkpoint only orders the log before the database, and the database before the log restarts.
    let synced = std::mem::take(&mut memory.state().syncs);
    assert!(!synced.is_empty());
    assert!(
        synced
            .iter()
            .all(|against| *against == DurableAgainst::SoftwareFailure)
    );
}

#[test]
fn a_log_that_threads_write_while_it_checkpoints_still_starts_over() {
    const CHECKPOINT_PAGES: u32 = 64;
    const COMMITS: usize = 2000;
    let stores = CrashableStores::new(CHECKPOINT_PAGES);
    let (store, memory) = stores.open(ThreadPool);
    let longest = std::sync::Mutex::new(0);
    std::thread::scope(|scope| {
        for thread in 0..4 {
            let (store, memory, longest) = (&store, &memory, &longest);
            scope.spawn(move || {
                for commit in 0..COMMITS {
                    let key = Bytes::from(format!("{thread} {}", commit % 100));
                    let byte = u8::try_from(commit % 251).unwrap();
                    block_on(store.transact(|mut transaction| {
                        let key = key.clone();
                        Box::pin(async move {
                            transaction.set(key, &[byte; 8 * 1024][..]).unwrap();
                            Ok::<_, Infallible>(((), transaction))
                        })
                    }))
                    .unwrap();
                    let mut longest = longest.lock().unwrap();
                    *longest = memory.length("metadata.sqlite-wal").max(*longest);
                }
            });
        }
    });
    // A frame in the log is a page and its header. The log starts over at about every second checkpoint, and grows
    // a little more while each one copies it.
    let frame = 16 * 1024 + 24;
    let longest = longest.into_inner().unwrap();
    assert!(
        longest < 16 * CHECKPOINT_PAGES as usize * frame,
        "the log grew to {} frames",
        longest / frame
    );
}

#[test]
fn work_that_fails_on_a_read_the_disk_failed_is_aborted_with_that_failure() {
    let (store, memory) = open_uncached();
    let aborted = block_on(store.transact(|mut transaction| {
        let memory = memory.clone();
        Box::pin(async move {
            transaction
                .set(Bytes::from_static(b"first"), &b"value"[..])
                .unwrap();
            memory.state().failing_reads = true;
            let read = transaction.get(Bytes::from_static(b"last")).await;
            memory.state().failing_reads = false;
            read?;
            Ok::<_, rusqlite::Error>(((), transaction))
        })
    }));
    assert!(
        matches!(aborted, Err(TransactError::Aborted(_))),
        "{aborted:?}"
    );
    assert_eq!(get(&store, b"first"), Some(Bytes::from_static(LARGE_VALUE)));
}

#[test]
fn work_that_writes_on_after_a_read_the_disk_failed_commits_nothing() {
    let (store, memory) = open_uncached();
    let committed = block_on(store.transact(|mut transaction| {
        let memory = memory.clone();
        Box::pin(async move {
            transaction
                .set(Bytes::from_static(b"first"), &b"value"[..])
                .unwrap();
            memory.state().failing_reads = true;
            let _failed = transaction.get(Bytes::from_static(b"last")).await;
            memory.state().failing_reads = false;
            let _refused = transaction.set(Bytes::from_static(b"after"), &b"value"[..]);
            Ok::<_, Infallible>(((), transaction))
        })
    }));
    assert!(
        matches!(committed, Err(TransactError::Failed(_))),
        "{committed:?}"
    );
    assert_eq!(get(&store, b"first"), Some(Bytes::from_static(LARGE_VALUE)));
    assert_eq!(get(&store, b"after"), None);
}

#[test]
fn what_ensure_durable_made_durable_survives_a_crash_with_its_content() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (store, memory) = stores.open(SimulatedThreadPool);
    let content = [9; 300 * 1024];
    let made_durable = commit_content(&store, b"made durable", &content);
    make_durable(&store, DurableAgainst::PowerFailure);
    let left_unsynced = commit_content(&store, b"left unsynced", b"unsynced content");
    let reopened = stores.reopen_synced(memory, SimulatedThreadPool);
    assert_eq!(
        get(&reopened, b"made durable"),
        Some(Bytes::copy_from_slice(&made_durable.0))
    );
    assert_eq!(
        read(&reopened, &made_durable, content.len()),
        Some(Bytes::copy_from_slice(&content))
    );
    // The crash loses the commit nothing made durable, and its content, which shows the store really crashed.
    assert_eq!(get(&reopened, b"left unsynced"), None);
    assert_eq!(read(&reopened, &left_unsynced, 16), None);
}

#[test]
fn ensure_durable_does_not_wait_for_a_write_transaction() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (store, memory) = stores.open(ThreadPool);
    set(&store, b"committed before");
    let (holding, released) = (Barrier::new(2), Barrier::new(2));
    let (durable, made_durable) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            block_on(store.transact(|mut transaction| {
                let (holding, released) = (&holding, &released);
                Box::pin(async move {
                    transaction
                        .set(Bytes::from_static(b"held"), &b"value"[..])
                        .unwrap();
                    holding.wait();
                    released.wait();
                    Ok::<_, Infallible>(((), transaction))
                })
            }))
            .unwrap();
        });
        holding.wait();
        // The writer holds its transaction open until ensure_durable returns, or the wait for it times out.
        scope.spawn(|| {
            make_durable(&store, DurableAgainst::PowerFailure);
            durable.send(()).unwrap();
        });
        let returned = made_durable.recv_timeout(Duration::from_secs(10));
        released.wait();
        returned.expect("ensure_durable waited for the write transaction");
    });
    let reopened = stores.reopen_synced(memory, SimulatedThreadPool);
    assert_eq!(
        get(&reopened, b"committed before"),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(get(&reopened, b"held"), None);
}

// HeldSpawns runs work in place, holding each of the first works handed to it until it is released, and counts the
// works it held.
struct HeldSpawns {
    releases: Mutex<VecDeque<Receiver<()>>>,
    held: Arc<AtomicUsize>,
}

impl SpawnBlocking for HeldSpawns {
    async fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        let release = self.releases.lock().unwrap().pop_front();
        if let Some(release) = release {
            self.held.fetch_add(1, Ordering::SeqCst);
            release.recv().unwrap();
        }
        work()
    }
}

#[test]
fn ensure_durable_calls_made_while_a_sync_runs_share_the_next_sync() {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (releases, released): (Vec<_>, VecDeque<_>) = (0..2).map(|_| mpsc::channel()).unzip();
    let holding = Arc::new(AtomicUsize::new(0));
    let (store, memory) = stores.open(HeldSpawns {
        releases: Mutex::new(released),
        held: Arc::clone(&holding),
    });
    let held = |syncs| {
        while holding.load(Ordering::SeqCst) < syncs {
            std::thread::yield_now();
        }
    };
    set(&store, b"a");
    memory.state().syncs.clear();
    std::thread::scope(|scope| {
        let store = &store;
        let first = scope.spawn(move || make_durable(store, DurableAgainst::PowerFailure));
        held(1);
        set(store, b"b");
        let waiting: Vec<_> = (0..3)
            .map(|_| scope.spawn(move || make_durable(store, DurableAgainst::PowerFailure)))
            .collect();
        // The waiting calls are made before the first sync ends, and one of them starts the next.
        std::thread::sleep(Duration::from_millis(100));
        releases[0].send(()).unwrap();
        held(2);
        // A commit made while the next sync runs is not one the waiting calls need durable.
        set(store, b"c");
        releases[1].send(()).unwrap();
        first.join().unwrap();
        for waiting in waiting {
            waiting.join().unwrap();
        }
    });
    assert_eq!(
        std::mem::take(&mut memory.state().syncs),
        [DurableAgainst::PowerFailure, DurableAgainst::PowerFailure]
    );
    let reopened = stores.reopen_synced(memory, SimulatedThreadPool);
    assert_eq!(get(&reopened, b"b"), Some(Bytes::from_static(b"value")));
}

// A value this large keeps most of itself on overflow pages of its own.
const LARGE_VALUE: &[u8] = &[7; 64 * 1024];

// open_uncached is a store opened again on two large values, which it has none of cached, so reading one reads the
// files.
fn open_uncached() -> (SqliteMetadata<SimulatedThreadPool>, CrashableMemory) {
    let stores = CrashableStores::new(CHECKPOINT_NEVER);
    let (store, memory) = stores.open(SimulatedThreadPool);
    for key in [&b"first"[..], b"last"] {
        set_to(&store, key, LARGE_VALUE);
    }
    stores.reopen_killed(&memory, SimulatedThreadPool)
}

fn set<S: KeyValueStore>(store: &S, key: &'static [u8]) {
    set_to(store, key, b"value");
}

fn set_to<S: KeyValueStore>(store: &S, key: &'static [u8], value: &'static [u8]) {
    block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction.set(Bytes::from_static(key), value).unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
}

fn get<S: KeyValueStore>(store: &S, key: &'static [u8]) -> Option<Bytes> {
    block_on(store.read_transact(|transaction| {
        Box::pin(async move {
            Ok::<_, Infallible>(transaction.get(Bytes::from_static(key)).await.unwrap())
        })
    }))
    .unwrap()
}

// commit_content commits key naming content, which the transaction's work puts, as a file system's write does.
fn commit_content<S: SpawnBlocking + 'static>(
    metadata: &SqliteMetadata<S>,
    key: &'static [u8],
    content: &[u8],
) -> ContentHash {
    let stored = SqliteContent::of(metadata);
    let content = Bytes::copy_from_slice(content);
    block_on(metadata.transact(|mut transaction| {
        let (stored, content) = (&stored, content.clone());
        Box::pin(async move {
            let hash = stored.put(content).await.unwrap();
            transaction
                .set(Bytes::from_static(key), &hash.0[..])
                .unwrap();
            Ok::<_, Infallible>((hash, transaction))
        })
    }))
    .unwrap()
}

// read is the whole content of len bytes stored under hash, or None while the store keeps no such content.
fn read<S: SpawnBlocking + 'static>(
    metadata: &SqliteMetadata<S>,
    hash: &ContentHash,
    len: usize,
) -> Option<Bytes> {
    match block_on(SqliteContent::of(metadata).get(hash, 0, len as u64)) {
        Ok(mut read) => Some(read.copy_to_bytes(read.remaining())),
        Err(StoreError::Rejected(GetRejection::UnknownContent(_))) => None,
        Err(error) => panic!("{error}"),
    }
}

fn make_durable<S: SpawnBlocking + 'static>(metadata: &SqliteMetadata<S>, against: DurableAgainst) {
    block_on(metadata.ensure_durable(against)).unwrap();
}
