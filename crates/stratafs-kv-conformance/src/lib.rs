pub mod activity;
mod execute;
mod model;
pub mod runtime;
mod simulation;

use std::ops::Bound;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_channel::oneshot;
use futures_lite::future::{block_on, yield_now};
use futures_util::FutureExt;
use proptest::prelude::any;
use proptest::{prop_assert, proptest};
use stateright::semantics::ConsistencyTester;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction,
};

use crate::activity::simulated_client_activity;
use crate::runtime::{Replay, ReplayingRuntime, spawn_with_handle};
use crate::simulation::simulate;

// is_strictly_serializable runs fuzzed transactions against the given store from a fuzzed number
// of concurrent clients, and fails if the resulting history is not strictly serializable,
// using an in memory map as the reference. It can find violations, but cannot prove there are none.
// https://jepsen.io/consistency/models/strict-serializable
pub fn is_strictly_serializable<R: ReplayingRuntime, S: KeyValueStore + 'static>(
    runtime: R,
    new_store: impl Fn() -> S,
) {
    proptest!(|(simulated_client_activity in simulated_client_activity(), seed in any::<u64>())| {
        let tester = simulate(&runtime, new_store(), simulated_client_activity, seed)?;
        prop_assert!(
            tester.is_consistent(),
            "no serial order explains this history: {tester:?}"
        );
    });
}

pub fn panics_on_invalid_ranges<S: KeyValueStore>(new_store: impl Fn() -> S) {
    let store = new_store();
    let invalid_ranges = [
        (
            Bound::Included(Bytes::from_static(b"b")),
            Bound::Included(Bytes::from_static(b"a")),
        ),
        (
            Bound::Excluded(Bytes::from_static(b"a")),
            Bound::Excluded(Bytes::from_static(b"a")),
        ),
    ];
    for range in invalid_ranges {
        let read = block_on(
            AssertUnwindSafe(store.read_transact(|transaction| {
                let range = range.clone();
                Box::pin(async move { transaction.get_range(range, usize::MAX).await })
            }))
            .catch_unwind(),
        );
        assert!(
            read.is_err(),
            "get_range accepted {range:?} in a read transaction"
        );
        let write = block_on(
            AssertUnwindSafe(store.transact(|transaction| {
                let range = range.clone();
                Box::pin(async move { transaction.get_range(range, usize::MAX).await })
            }))
            .catch_unwind(),
        );
        assert!(
            write.is_err(),
            "get_range accepted {range:?} in a write transaction"
        );
    }
}

pub fn changed_waits_for_the_observing_transaction<
    R: ReplayingRuntime,
    S: KeyValueStore + 'static,
>(
    runtime: R,
    new_store: impl Fn() -> S + Send + Sync + 'static,
) {
    let new_store = Arc::new(new_store);
    for in_read_transaction in [false, true] {
        for commits_successfully in [false, true] {
            let new_store = new_store.clone();
            runtime.run(0, move |replay| {
                observe_early(
                    replay,
                    Arc::new(new_store()),
                    in_read_transaction,
                    commits_successfully,
                )
            });
        }
    }
}

#[derive(Debug, PartialEq)]
enum WatchEvent {
    WorkEnded,
    KeyChangeStarted,
    ResolvedChanged,
    ResolvedNotCommitted,
    ResolvedFailed,
}

async fn observe_early<P: Replay, S: KeyValueStore + 'static>(
    replay: P,
    store: Arc<S>,
    in_read_transaction: bool,
    commits_successfully: bool,
) {
    let key = Bytes::from_static(b"observed");
    let log = Arc::new(Mutex::new(Vec::new()));
    let (observation_sender, observation_receiver) = oneshot::channel();
    let observation_sender = Arc::new(Mutex::new(Some(observation_sender)));
    let watcher = spawn_with_handle(&replay, {
        let (store, log) = (store.clone(), log.clone());
        async move {
            let observation = observation_receiver
                .await
                .expect("work sends its observation");
            let event = match store.changed(observation).await {
                Ok(()) => WatchEvent::ResolvedChanged,
                Err(StoreError::Rejected(NotCommitted)) => WatchEvent::ResolvedNotCommitted,
                Err(StoreError::Failed(_)) => WatchEvent::ResolvedFailed,
            };
            log.lock().unwrap().push(event);
        }
    });
    let observing = async {
        if in_read_transaction {
            store
                .read_transact(|transaction| {
                    let (key, sender, log) = (key.clone(), observation_sender.clone(), log.clone());
                    Box::pin(async move {
                        observe_and_send(transaction, key, &sender, &log, commits_successfully)
                            .await
                    })
                })
                .await
        } else {
            store
                .transact(|transaction| {
                    let (key, sender, log) = (key.clone(), observation_sender.clone(), log.clone());
                    Box::pin(async move {
                        observe_and_send(transaction, key, &sender, &log, commits_successfully)
                            .await
                    })
                })
                .await
        }
    };
    let outcome = AssertUnwindSafe(observing).catch_unwind().await;
    if commits_successfully {
        assert!(
            matches!(outcome, Ok(Ok(()))),
            "the observing transaction did not commit"
        );
        yield_now().await;
        log.lock().unwrap().push(WatchEvent::KeyChangeStarted);
        store
            .transact(|transaction| {
                let key = key.clone();
                Box::pin(async move { transaction.set(key, Bytes::from_static(b"changed")) })
            })
            .await
            .expect("the transaction that changes the key failed");
    } else {
        assert!(outcome.is_err(), "the observing transaction did not panic");
    }
    watcher.await;
    let log = std::mem::take(&mut *log.lock().unwrap());
    let expected = if commits_successfully {
        vec![
            WatchEvent::WorkEnded,
            WatchEvent::KeyChangeStarted,
            WatchEvent::ResolvedChanged,
        ]
    } else {
        vec![WatchEvent::WorkEnded, WatchEvent::ResolvedNotCommitted]
    };
    assert_eq!(log, expected, "changed resolved out of order");
}

async fn observe_and_send<T: ReadKeyValueTransaction>(
    transaction: &T,
    key: Bytes,
    observation_sender: &Mutex<Option<oneshot::Sender<T::Observation>>>,
    log: &Mutex<Vec<WatchEvent>>,
    commits_successfully: bool,
) -> Result<(), T::Failure> {
    let observation = transaction.observe(key)?;
    let first_attempt = observation_sender.lock().unwrap().take();
    if let Some(observation_sender) = first_attempt {
        let _ = observation_sender.send(observation);
    }
    yield_now().await;
    log.lock().unwrap().push(WatchEvent::WorkEnded);
    if !commits_successfully {
        resume_unwind(Box::new("simulated panic inside work"));
    }
    Ok(())
}
