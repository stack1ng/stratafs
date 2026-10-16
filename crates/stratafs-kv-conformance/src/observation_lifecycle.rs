use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_channel::oneshot;
use futures_lite::future::yield_now;
use futures_util::FutureExt;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction,
};

use crate::runtime::{Replay, ReplayingRuntime, spawn_with_handle};
use crate::simulation::{cancel_at_nth_suspension, count_suspensions};

const OBSERVED_KEY: &[u8] = b"observed";

#[derive(Clone, Copy)]
pub(crate) enum KeyChange {
    Unchanged,
    Committed,
    CancelledAtSuspension(usize),
}

enum ExpectedObservation {
    NotCommitted,
    Fires,
    StaysPending,
}

#[derive(Debug, PartialEq)]
enum ObservationEvent {
    ObserverWorkEnded,
    ChangerWorkEnded,
    Resolved,
}

pub(crate) fn count_change_suspensions<R, S, N>(runtime: &R, new_store: &Arc<N>) -> usize
where
    R: ReplayingRuntime,
    S: KeyValueStore + 'static,
    N: Fn() -> S + Send + Sync + 'static,
{
    let suspensions = Arc::new(AtomicUsize::new(0));
    let (new_store, counted) = (new_store.clone(), suspensions.clone());
    runtime.run(0, move |_| {
        let (store, counted) = (new_store(), counted.clone());
        async move {
            let change = change_key(&store, Arc::new(Mutex::new(Vec::new())));
            counted.store(count_suspensions(change).await, Ordering::SeqCst);
        }
    });
    suspensions.load(Ordering::SeqCst)
}

async fn change_key<S: KeyValueStore>(
    store: &S,
    log: Arc<Mutex<Vec<ObservationEvent>>>,
) -> Result<(), StoreError<S::Rejection, S::Failure>> {
    store
        .transact(|transaction| {
            let log = log.clone();
            Box::pin(async move {
                transaction.set(
                    Bytes::from_static(OBSERVED_KEY),
                    Bytes::from_static(b"changed"),
                )?;
                log.lock().unwrap().push(ObservationEvent::ChangerWorkEnded);
                Ok(())
            })
        })
        .await
}

pub(crate) async fn run_observation_scenario<P: Replay, S: KeyValueStore + 'static>(
    replay: P,
    store: Arc<S>,
    in_read_transaction: bool,
    observer_commits: bool,
    key_change: KeyChange,
) {
    let key = Bytes::from_static(OBSERVED_KEY);
    let log = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(Mutex::new(None));
    let (observation_sender, observation_receiver) = oneshot::channel();
    let observation_sender = Arc::new(Mutex::new(Some(observation_sender)));
    let watcher = spawn_with_handle(&replay, {
        let (store, log) = (store.clone(), log.clone());
        async move {
            let observation = observation_receiver
                .await
                .expect("work sends its observation");
            let result = store.changed(observation).await;
            log.lock().unwrap().push(ObservationEvent::Resolved);
            result
        }
    });
    let changing = match key_change {
        KeyChange::Unchanged => None,
        KeyChange::Committed | KeyChange::CancelledAtSuspension(_) => {
            Some(spawn_with_handle(&replay, {
                let (store, log) = (store.clone(), log.clone());
                async move {
                    let change = change_key(&*store, log);
                    match key_change {
                        KeyChange::CancelledAtSuspension(n) => {
                            if let Some(result) = cancel_at_nth_suspension(change, n).await {
                                result.expect("the changing transaction failed");
                            }
                        }
                        KeyChange::Unchanged | KeyChange::Committed => {
                            change.await.expect("the changing transaction failed")
                        }
                    }
                }
            }))
        }
    };
    let observing = async {
        if in_read_transaction {
            store
                .read_transact(|transaction| {
                    let (key, sender, seen, log) = (
                        key.clone(),
                        observation_sender.clone(),
                        seen.clone(),
                        log.clone(),
                    );
                    Box::pin(async move {
                        observe_and_send(transaction, key, &sender, &seen, &log, observer_commits)
                            .await
                    })
                })
                .await
        } else {
            store
                .transact(|transaction| {
                    let (key, sender, seen, log) = (
                        key.clone(),
                        observation_sender.clone(),
                        seen.clone(),
                        log.clone(),
                    );
                    Box::pin(async move {
                        observe_and_send(transaction, key, &sender, &seen, &log, observer_commits)
                            .await
                    })
                })
                .await
        }
    };
    let outcome = AssertUnwindSafe(observing).catch_unwind().await;
    if observer_commits {
        assert!(
            matches!(outcome, Ok(Ok(()))),
            "the observing transaction did not commit"
        );
    } else {
        assert!(outcome.is_err(), "the observing transaction did not panic");
    }
    if let Some(changing) = changing {
        changing.await;
    }
    yield_now().await;
    let final_value = store
        .read_transact(|transaction| {
            let key = key.clone();
            Box::pin(async move { transaction.get(key).await })
        })
        .await
        .expect("reading the final value failed");
    let seen = seen
        .lock()
        .unwrap()
        .take()
        .expect("work recorded what it saw");
    let resolved_before = |event| {
        let log = log.lock().unwrap();
        let resolved = log
            .iter()
            .position(|logged| *logged == ObservationEvent::Resolved);
        let event = log.iter().position(|logged| *logged == event);
        matches!((resolved, event), (Some(resolved), Some(event)) if resolved < event)
            || (resolved.is_some() && event.is_none())
    };
    let expected = if !observer_commits {
        ExpectedObservation::NotCommitted
    } else if final_value != seen {
        ExpectedObservation::Fires
    } else {
        ExpectedObservation::StaysPending
    };
    match expected {
        ExpectedObservation::NotCommitted => {
            let result = watcher.await;
            assert!(
                matches!(result, Err(StoreError::Rejected(NotCommitted))),
                "changed did not report NotCommitted although the observing transaction did not commit"
            );
            assert!(
                !resolved_before(ObservationEvent::ObserverWorkEnded),
                "changed resolved before the observing transaction finished"
            );
        }
        ExpectedObservation::Fires => {
            let result = watcher.await;
            assert!(
                result.is_ok(),
                "changed did not resolve although the key changed"
            );
            assert!(
                !resolved_before(ObservationEvent::ObserverWorkEnded),
                "changed resolved before the observing transaction finished"
            );
            assert!(
                !resolved_before(ObservationEvent::ChangerWorkEnded),
                "changed resolved before the key changed"
            );
        }
        ExpectedObservation::StaysPending => {
            assert!(
                !log.lock().unwrap().contains(&ObservationEvent::Resolved),
                "changed resolved although the key did not change"
            );
        }
    }
}

async fn observe_and_send<T: ReadKeyValueTransaction>(
    transaction: &T,
    key: Bytes,
    observation_sender: &Mutex<Option<oneshot::Sender<T::Observation>>>,
    seen: &Mutex<Option<Option<Bytes>>>,
    log: &Mutex<Vec<ObservationEvent>>,
    observer_commits: bool,
) -> Result<(), T::Failure> {
    let value = transaction.get(key.clone()).await?;
    let observation = transaction.observe(key)?;
    let first_attempt = observation_sender.lock().unwrap().take();
    if let Some(observation_sender) = first_attempt {
        *seen.lock().unwrap() = Some(value);
        let _ = observation_sender.send(observation);
    }
    yield_now().await;
    log.lock()
        .unwrap()
        .push(ObservationEvent::ObserverWorkEnded);
    if !observer_commits {
        resume_unwind(Box::new("simulated panic inside work"));
    }
    Ok(())
}
