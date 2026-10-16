use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::future::yield_now;
use futures_util::FutureExt;
use futures_util::future::RemoteHandle;
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
    ObserverWorkEnded { attempt: usize },
    ChangerWorkEnded,
    Resolved { attempt: usize },
}

struct ObservationAttempt<F> {
    seen: Option<Bytes>,
    waiter: RemoteHandle<Result<(), StoreError<NotCommitted, F>>>,
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
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let watch = Arc::new({
        let (replay, store, log, attempts) =
            (replay.clone(), store.clone(), log.clone(), attempts.clone());
        move |seen: Option<Bytes>, observation: S::Observation| {
            let attempt = attempts.lock().unwrap().len();
            let (store, log) = (store.clone(), log.clone());
            let waiter = spawn_with_handle(&replay, async move {
                let result = store.changed(observation).await;
                log.lock()
                    .unwrap()
                    .push(ObservationEvent::Resolved { attempt });
                result
            });
            attempts
                .lock()
                .unwrap()
                .push(ObservationAttempt { seen, waiter });
            attempt
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
                    let (key, watch, log) = (key.clone(), watch.clone(), log.clone());
                    Box::pin(async move {
                        observe_and_watch(transaction, key, &*watch, &log, observer_commits).await
                    })
                })
                .await
        } else {
            store
                .transact(|transaction| {
                    let (key, watch, log) = (key.clone(), watch.clone(), log.clone());
                    Box::pin(async move {
                        observe_and_watch(transaction, key, &*watch, &log, observer_commits).await
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
    let mut attempts = std::mem::take(&mut *attempts.lock().unwrap());
    let last = attempts.pop().expect("work observed the key");
    let last_attempt = attempts.len();
    let resolved_before = |attempt, event| {
        let log = log.lock().unwrap();
        let resolved = log
            .iter()
            .position(|logged| *logged == ObservationEvent::Resolved { attempt });
        let event = log.iter().rposition(|logged| *logged == event);
        matches!((resolved, event), (Some(resolved), Some(event)) if resolved < event)
            || (resolved.is_some() && event.is_none())
    };
    for (attempt, retried) in attempts.into_iter().enumerate() {
        assert!(
            matches!(
                retried.waiter.await,
                Err(StoreError::Rejected(NotCommitted))
            ),
            "changed did not report NotCommitted for an attempt of the observing transaction that the store retried"
        );
        assert!(
            !resolved_before(attempt, ObservationEvent::ObserverWorkEnded { attempt }),
            "changed resolved before a retried attempt of the observing transaction finished"
        );
    }
    let expected = if !observer_commits {
        ExpectedObservation::NotCommitted
    } else if final_value != last.seen {
        ExpectedObservation::Fires
    } else {
        ExpectedObservation::StaysPending
    };
    match expected {
        ExpectedObservation::NotCommitted => {
            let result = last.waiter.await;
            assert!(
                matches!(result, Err(StoreError::Rejected(NotCommitted))),
                "changed did not report NotCommitted although the observing transaction did not commit"
            );
            assert!(
                !resolved_before(
                    last_attempt,
                    ObservationEvent::ObserverWorkEnded {
                        attempt: last_attempt
                    }
                ),
                "changed resolved before the observing transaction finished"
            );
        }
        ExpectedObservation::Fires => {
            let result = last.waiter.await;
            assert!(
                result.is_ok(),
                "changed did not resolve although the key changed"
            );
            assert!(
                !resolved_before(
                    last_attempt,
                    ObservationEvent::ObserverWorkEnded {
                        attempt: last_attempt
                    }
                ),
                "changed resolved before the observing transaction finished"
            );
            assert!(
                !resolved_before(last_attempt, ObservationEvent::ChangerWorkEnded),
                "changed resolved before the key changed"
            );
        }
        ExpectedObservation::StaysPending => {
            let resolved = log.lock().unwrap().contains(&ObservationEvent::Resolved {
                attempt: last_attempt,
            });
            if resolved {
                let value_after_resolving = store
                    .read_transact(|transaction| {
                        let key = key.clone();
                        Box::pin(async move { transaction.get(key).await })
                    })
                    .await
                    .expect("reading the value after changed resolved failed");
                assert!(
                    value_after_resolving != last.seen,
                    "changed resolved although the key did not change"
                );
            }
        }
    }
}

async fn observe_and_watch<T: ReadKeyValueTransaction>(
    transaction: &T,
    key: Bytes,
    watch: &(impl Fn(Option<Bytes>, T::Observation) -> usize + Sync),
    log: &Mutex<Vec<ObservationEvent>>,
    observer_commits: bool,
) -> Result<(), T::Failure> {
    let value = transaction.get(key.clone()).await?;
    let attempt = watch(value, transaction.observe(key)?);
    yield_now().await;
    log.lock()
        .unwrap()
        .push(ObservationEvent::ObserverWorkEnded { attempt });
    if !observer_commits {
        resume_unwind(Box::new("simulated panic inside work"));
    }
    Ok(())
}
