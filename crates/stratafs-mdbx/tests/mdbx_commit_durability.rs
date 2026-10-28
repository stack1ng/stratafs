mod configuration;

use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use futures_lite::future::block_on;
use stratafs_mdbx::{MdbxMetadata, MdbxOpenError};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection,
};
use stratafs_store::spawn::{Inline, SpawnBlocking};

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

// open is a store in a fresh directory that counts the work it hands to its spawner, which only syncs take.
fn open(directory: &tempfile::TempDir) -> (MdbxMetadata<CountedSpawns>, Arc<AtomicUsize>) {
    let spawns = Arc::default();
    let store = configuration::open(
        directory.path().join("store.mdbx"),
        CountedSpawns(Arc::clone(&spawns)),
    )
    .unwrap();
    (store, spawns)
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
fn ensure_durable_syncs_what_was_committed_once() {
    let directory = tempfile::tempdir().unwrap();
    let (store, spawns) = open(&directory);
    for key in [&b"a"[..], b"b", b"c"] {
        commit(&store, key);
    }
    assert_eq!(spawns.load(Ordering::SeqCst), 0);

    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    assert_eq!(spawns.load(Ordering::SeqCst), 1);

    // Nothing was committed since, so there is nothing to sync.
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    assert_eq!(spawns.load(Ordering::SeqCst), 1);
}

#[test]
fn ensure_durable_syncs_only_what_is_not_yet_durable_against_the_failure() {
    let directory = tempfile::tempdir().unwrap();
    let (store, spawns) = open(&directory);
    let synced_by = |against| {
        let before = spawns.load(Ordering::SeqCst);
        block_on(store.ensure_durable(against)).unwrap();
        spawns.load(Ordering::SeqCst) > before
    };
    commit(&store, b"a");
    assert!(synced_by(DurableAgainst::SoftwareFailure));
    assert!(!synced_by(DurableAgainst::SoftwareFailure));
    // Surviving a software failure does not make the commit survive a power failure.
    assert!(synced_by(DurableAgainst::PowerFailure));
    assert!(!synced_by(DurableAgainst::SoftwareFailure));
    assert!(!synced_by(DurableAgainst::PowerFailure));
}

#[test]
fn a_store_keeps_its_commits_in_its_file_and_refuses_a_second_opening_in_its_process() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let store = configuration::open(&path, Inline).unwrap();
    assert!(matches!(
        configuration::open(&path, Inline),
        Err(MdbxOpenError::AlreadyOpen)
    ));
    commit(&store, b"a");
    block_on(store.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
    commit(&store, b"b");
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    drop(store);
    let reopened = configuration::open(&path, Inline).unwrap();
    for key in [&b"a"[..], b"b"] {
        let value = block_on(reopened.read_transact(|transaction| {
            Box::pin(async move {
                Ok::<_, Infallible>(transaction.get(Bytes::from_static(key)).await.unwrap())
            })
        }))
        .unwrap();
        assert_eq!(value, Some(Bytes::from_static(b"value")), "{key:?}");
    }
}

#[test]
fn a_set_too_large_for_libmdbx_is_rejected_and_one_within_the_largest_entry_is_kept() {
    let directory = tempfile::tempdir().unwrap();
    let (store, _) = open(&directory);
    let largest = <MdbxMetadata<CountedSpawns> as KeyValueStore>::LARGEST_ACCEPTED_ENTRY;
    let accepted = Bytes::from(vec![b'k'; largest]);
    let refused = Bytes::from(vec![b'k'; 64 * 1024]);
    let stored = block_on(store.transact(|mut transaction| {
        let (accepted, refused) = (accepted.clone(), refused.clone());
        Box::pin(async move {
            assert!(matches!(
                transaction.set(refused, &b""[..]),
                Err(StoreError::Rejected(SetRejection::TooLarge))
            ));
            transaction.set(accepted.clone(), &b""[..]).unwrap();
            let stored = transaction.get(accepted).await.unwrap();
            Ok::<_, Infallible>((stored, transaction))
        })
    }))
    .unwrap();
    assert_eq!(stored, Some(Bytes::new()));
    let stored = block_on(store.read_transact(|transaction| {
        let accepted = accepted.clone();
        Box::pin(async move { Ok::<_, Infallible>(transaction.get(accepted).await.unwrap()) })
    }))
    .unwrap();
    assert_eq!(stored, Some(Bytes::new()));
}
