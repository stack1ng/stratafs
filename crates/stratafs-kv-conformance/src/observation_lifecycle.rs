use std::ops::Bound;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::future::yield_now;
use futures_util::FutureExt;
use futures_util::future::{RemoteHandle, join};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction, TransactError,
};

use crate::execute::WorkFailure;
use crate::runtime::{Replay, ReplayingRuntime, spawn_with_handle};
use crate::simulation::{cancel_at_nth_suspension, count_suspensions};

const OBSERVED_KEY: &[u8] = b"observed";

#[derive(Clone, Copy)]
pub(crate) enum ObservingTransaction {
    Read,
    Write,
    WritesObservedKey,
}

#[derive(Clone, Copy)]
pub(crate) enum ObserverOutcome {
    Commits,
    Panics,
    CancelledAtSuspension(usize),
}

#[derive(Clone, Copy)]
pub(crate) enum KeyChange {
    Unchanged,
    Committed,
    CancelledAtSuspension(usize),
    RewritesSameValue,
    ClearedByRange,
}

#[derive(Clone, Copy)]
pub(crate) enum KeyWrite {
    Changes,
    RewritesSameValue,
    ClearsByRange,
}

enum ObserverCommit {
    Committed,
    NotCommitted,
    Unknown,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ObservationEvent {
    ObserverWorkEnded { attempt: usize },
    ChangerWorkEnded,
    Resolved { attempt: usize, observation: usize },
}

pub(crate) type Observations<O> = [(Option<Bytes>, O); 2];

struct ObservationAttempt<F> {
    seen: [Option<Bytes>; 2],
    waiters: [RemoteHandle<Result<(), StoreError<NotCommitted, F>>>; 2],
}

pub(crate) fn count_suspensions_on_a_fresh_store<R, S, N, W, F>(
    runtime: &R,
    new_store: &Arc<N>,
    work: W,
) -> usize
where
    R: ReplayingRuntime,
    S: KeyValueStore + 'static,
    N: Fn() -> S + Send + Sync + 'static,
    W: Fn(Arc<S>) -> F + Send + Sync + 'static,
    F: Future<Output = ()>,
{
    let suspensions = Arc::new(AtomicUsize::new(0));
    let (new_store, counted, work) = (new_store.clone(), suspensions.clone(), Arc::new(work));
    runtime.run(0, move |_| {
        let (store, counted, work) = (Arc::new(new_store()), counted.clone(), work.clone());
        async move {
            counted.store(count_suspensions(work(store)).await, Ordering::SeqCst);
        }
    });
    suspensions.load(Ordering::SeqCst)
}

pub(crate) async fn change_key<S: KeyValueStore>(
    store: &S,
    log: Arc<Mutex<Vec<ObservationEvent>>>,
    write: KeyWrite,
) -> Result<(), TransactError<S::Rejection, S::Failure, WorkFailure<S::Rejection, S::Failure>>> {
    store
        .transact(|mut transaction| {
            let log = log.clone();
            Box::pin(async move {
                let key = Bytes::from_static(OBSERVED_KEY);
                match write {
                    KeyWrite::Changes => {
                        transaction.set(key, Bytes::from_static(b"changed"))?;
                    }
                    KeyWrite::RewritesSameValue => match transaction.get(key.clone()).await? {
                        Some(value) => transaction.set(key, value)?,
                        None => transaction.clear(key)?,
                    },
                    KeyWrite::ClearsByRange => {
                        transaction.clear_range((
                            Bound::Excluded(Bytes::from_static(b"o")),
                            Bound::Included(key),
                        ))?;
                    }
                }
                log.lock().unwrap().push(ObservationEvent::ChangerWorkEnded);
                Ok(((), transaction))
            })
        })
        .await
}

pub(crate) async fn observe<S: KeyValueStore>(
    store: &S,
    observing_transaction: ObservingTransaction,
    watch: Arc<impl Fn(Observations<S::Observation>) -> usize + Send + Sync + 'static>,
    log: Arc<Mutex<Vec<ObservationEvent>>>,
    panics: bool,
) -> Result<(), TransactError<S::Rejection, S::Failure, WorkFailure<S::Rejection, S::Failure>>> {
    let key = Bytes::from_static(OBSERVED_KEY);
    match observing_transaction {
        ObservingTransaction::Read => {
            store
                .read_transact(|transaction| {
                    let (key, watch, log) = (key.clone(), watch.clone(), log.clone());
                    Box::pin(async move {
                        let first = observe_key(&*transaction, &key).await?;
                        let second = observe_key(&*transaction, &key).await?;
                        finish_observer_work(watch([first, second]), &log, panics).await;
                        Ok(())
                    })
                })
                .await
        }
        ObservingTransaction::Write | ObservingTransaction::WritesObservedKey => {
            store
                .transact(|mut transaction| {
                    let (key, watch, log) = (key.clone(), watch.clone(), log.clone());
                    Box::pin(async move {
                        let first = observe_key(&*transaction, &key).await?;
                        if matches!(
                            observing_transaction,
                            ObservingTransaction::WritesObservedKey
                        ) {
                            transaction
                                .set(key.clone(), Bytes::from_static(b"written by the observer"))?;
                        }
                        let second = observe_key(&*transaction, &key).await?;
                        finish_observer_work(watch([first, second]), &log, panics).await;
                        Ok(((), transaction))
                    })
                })
                .await
        }
    }
}

async fn observe_key<T: ReadKeyValueTransaction>(
    transaction: &T,
    key: &Bytes,
) -> Result<(Option<Bytes>, T::Observation), T::Failure> {
    let seen = transaction.get(key.clone()).await?;
    Ok((seen, transaction.observe(key.clone()).await?))
}

async fn finish_observer_work(attempt: usize, log: &Mutex<Vec<ObservationEvent>>, panics: bool) {
    yield_now().await;
    log.lock()
        .unwrap()
        .push(ObservationEvent::ObserverWorkEnded { attempt });
    if panics {
        resume_unwind(Box::new("simulated panic inside work"));
    }
}

pub(crate) async fn run_observation_scenario<P: Replay, S: KeyValueStore + 'static>(
    replay: P,
    store: Arc<S>,
    observing_transaction: ObservingTransaction,
    observer_outcome: ObserverOutcome,
    key_change: KeyChange,
) {
    let key = Bytes::from_static(OBSERVED_KEY);
    let log = Arc::new(Mutex::new(Vec::new()));
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let watch = Arc::new({
        let (replay, store, log, attempts) =
            (replay.clone(), store.clone(), log.clone(), attempts.clone());
        move |[(first_seen, first), (second_seen, second)]: Observations<S::Observation>| {
            let attempt = attempts.lock().unwrap().len();
            let resolve = |observation: usize, observed: S::Observation| {
                let (store, log) = (store.clone(), log.clone());
                async move {
                    let result = store.changed(observed).await;
                    log.lock().unwrap().push(ObservationEvent::Resolved {
                        attempt,
                        observation,
                    });
                    result
                }
                .remote_handle()
            };
            let (first_resolving, first_waiter) = resolve(0, first);
            let (second_resolving, second_waiter) = resolve(1, second);
            replay.spawn(async move {
                join(first_resolving, second_resolving).await;
            });
            attempts.lock().unwrap().push(ObservationAttempt {
                seen: [first_seen, second_seen],
                waiters: [first_waiter, second_waiter],
            });
            attempt
        }
    });
    if let KeyChange::ClearedByRange = key_change {
        store
            .transact(|mut transaction| {
                let key = key.clone();
                Box::pin(async move {
                    let set = transaction.set(key, Bytes::from_static(b"before"));
                    set.map(|()| ((), transaction))
                })
            })
            .await
            .expect("setting the observed key before the scenario failed");
    }
    let changing = match key_change {
        KeyChange::Unchanged => None,
        KeyChange::Committed
        | KeyChange::RewritesSameValue
        | KeyChange::ClearedByRange
        | KeyChange::CancelledAtSuspension(_) => Some(spawn_with_handle(&replay, {
            let (store, log) = (store.clone(), log.clone());
            async move {
                let write = match key_change {
                    KeyChange::RewritesSameValue => KeyWrite::RewritesSameValue,
                    KeyChange::ClearedByRange => KeyWrite::ClearsByRange,
                    KeyChange::Unchanged
                    | KeyChange::Committed
                    | KeyChange::CancelledAtSuspension(_) => KeyWrite::Changes,
                };
                let change = change_key(&*store, log, write);
                let result = match key_change {
                    KeyChange::CancelledAtSuspension(n) => {
                        cancel_at_nth_suspension(change, n).await
                    }
                    KeyChange::Unchanged
                    | KeyChange::Committed
                    | KeyChange::RewritesSameValue
                    | KeyChange::ClearedByRange => Some(change.await),
                };
                if let Some(result) = result {
                    result.expect("the changing transaction failed");
                }
            }
        })),
    };
    let panics = matches!(observer_outcome, ObserverOutcome::Panics);
    let observing = AssertUnwindSafe(observe(
        &*store,
        observing_transaction,
        watch,
        log.clone(),
        panics,
    ))
    .catch_unwind();
    let observed = match observer_outcome {
        ObserverOutcome::CancelledAtSuspension(n) => cancel_at_nth_suspension(observing, n).await,
        ObserverOutcome::Commits | ObserverOutcome::Panics => Some(observing.await),
    };
    let (_, dropped) = store
        .read_transact(|transaction| {
            let key = key.clone();
            Box::pin(async move { observe_key(&*transaction, &key).await })
        })
        .await
        .expect("observing the key again failed");
    assert!(
        store.changed(dropped).now_or_never().is_none(),
        "changed resolved although the key did not change"
    );
    if let Some(changing) = changing {
        changing.await;
    }
    yield_now().await;
    let read_key = || {
        store.read_transact(|transaction| {
            let key = key.clone();
            Box::pin(async move { transaction.get(key).await })
        })
    };
    let final_value = read_key().await.expect("reading the final value failed");
    let mut attempts = std::mem::take(&mut *attempts.lock().unwrap());
    let Some(last) = attempts.pop() else {
        return;
    };
    let last_attempt = attempts.len();
    let logged = |event| log.lock().unwrap().contains(&event);
    let resolved_before = |attempt, observation, event| {
        let log = log.lock().unwrap();
        let resolved = log.iter().position(|logged| {
            *logged
                == ObservationEvent::Resolved {
                    attempt,
                    observation,
                }
        });
        let event = log.iter().rposition(|logged| *logged == event);
        matches!((resolved, event), (Some(resolved), Some(event)) if resolved < event)
            || (resolved.is_some() && event.is_none())
    };
    for (attempt, retried) in attempts.into_iter().enumerate() {
        for (observation, waiter) in retried.waiters.into_iter().enumerate() {
            assert!(
                matches!(waiter.await, Err(StoreError::Rejected(NotCommitted))),
                "changed did not report NotCommitted for an attempt of the observing transaction that the store retried"
            );
            assert!(
                !resolved_before(
                    attempt,
                    observation,
                    ObservationEvent::ObserverWorkEnded { attempt }
                ),
                "changed resolved before a retried attempt of the observing transaction finished"
            );
        }
    }
    let work_ended = logged(ObservationEvent::ObserverWorkEnded {
        attempt: last_attempt,
    });
    let commit = match (observer_outcome, observed) {
        (ObserverOutcome::Panics, Some(Err(_))) => ObserverCommit::NotCommitted,
        (ObserverOutcome::Panics, _) => panic!("the observing transaction did not panic"),
        (_, Some(Ok(Ok(())))) => ObserverCommit::Committed,
        (ObserverOutcome::Commits, _) => panic!("the observing transaction did not commit"),
        (ObserverOutcome::CancelledAtSuspension(_), _) if !work_ended => {
            ObserverCommit::NotCommitted
        }
        (ObserverOutcome::CancelledAtSuspension(_), _) => ObserverCommit::Unknown,
    };
    let writes_observed_key = matches!(
        observing_transaction,
        ObservingTransaction::WritesObservedKey
    );
    for (observation, (seen, waiter)) in last.seen.into_iter().zip(last.waiters).enumerate() {
        let resolved_before_work_ended = resolved_before(
            last_attempt,
            observation,
            ObservationEvent::ObserverWorkEnded {
                attempt: last_attempt,
            },
        );
        let resolved = logged(ObservationEvent::Resolved {
            attempt: last_attempt,
            observation,
        });
        match commit {
            ObserverCommit::NotCommitted => {
                assert!(
                    matches!(waiter.await, Err(StoreError::Rejected(NotCommitted))),
                    "changed did not report NotCommitted although the observing transaction did not commit"
                );
                assert!(
                    !work_ended || !resolved_before_work_ended,
                    "changed resolved before the observing transaction finished"
                );
            }
            ObserverCommit::Committed if final_value != seen => {
                assert!(
                    waiter.await.is_ok(),
                    "changed did not resolve although the key changed"
                );
                assert!(
                    !resolved_before_work_ended,
                    "changed resolved before the observing transaction finished"
                );
                let changed_by_the_observer = writes_observed_key && observation == 0;
                assert!(
                    changed_by_the_observer
                        || !resolved_before(
                            last_attempt,
                            observation,
                            ObservationEvent::ChangerWorkEnded
                        ),
                    "changed resolved before the key changed"
                );
            }
            ObserverCommit::Unknown if final_value != seen => {
                assert!(
                    !matches!(waiter.await, Err(StoreError::Failed(_))),
                    "changed failed for an observation whose transaction was cancelled"
                );
            }
            ObserverCommit::Committed | ObserverCommit::Unknown => {
                if resolved {
                    let result = waiter.await;
                    assert!(
                        matches!(commit, ObserverCommit::Unknown) || result.is_ok(),
                        "changed reported NotCommitted although the observing transaction committed"
                    );
                    if result.is_ok() {
                        let value_after_resolving = read_key()
                            .await
                            .expect("reading the value after changed resolved failed");
                        assert!(
                            value_after_resolving != seen,
                            "changed resolved although the key did not change"
                        );
                    }
                }
            }
        }
    }
}
