use std::ops::Bound;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::future::yield_now;
use futures_util::FutureExt;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
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

#[derive(Debug, PartialEq)]
pub(crate) enum ObservationEvent {
    ChangerWorkEnded,
    Resolved { observation: usize },
}

// ObservedValues are what the observing transaction's latest attempt read from the observed key,
// before and after its own write.
pub(crate) type ObservedValues = [Option<Bytes>; 2];

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
                            Bound::Excluded(Bytes::from_static(b"p")),
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
    observed: Arc<Mutex<Option<ObservedValues>>>,
    panics: bool,
) -> Result<(), TransactError<S::Rejection, S::Failure, WorkFailure<S::Rejection, S::Failure>>> {
    let key = Bytes::from_static(OBSERVED_KEY);
    match observing_transaction {
        ObservingTransaction::Read => {
            store
                .read_transact(|transaction| {
                    let (key, observed) = (key.clone(), observed.clone());
                    Box::pin(async move {
                        let first = transaction.get(key.clone()).await?;
                        let second = transaction.get(key).await?;
                        finish_observer_work([first, second], &observed, panics).await;
                        Ok(())
                    })
                })
                .await
        }
        ObservingTransaction::Write | ObservingTransaction::WritesObservedKey => {
            store
                .transact(|mut transaction| {
                    let (key, observed) = (key.clone(), observed.clone());
                    Box::pin(async move {
                        let first = transaction.get(key.clone()).await?;
                        if matches!(
                            observing_transaction,
                            ObservingTransaction::WritesObservedKey
                        ) {
                            transaction
                                .set(key.clone(), Bytes::from_static(b"written by the observer"))?;
                        }
                        let second = transaction.get(key).await?;
                        finish_observer_work([first, second], &observed, panics).await;
                        Ok(((), transaction))
                    })
                })
                .await
        }
    }
}

// keys_around_the_observed_key are the observed key, the keys the range clear removes around it,
// and keys outside that range on both sides. In a store made of small nodes, the observed key's node
// then sits inside the cleared range and is dropped without being written,
// while the nodes above it stay in place.
fn keys_around_the_observed_key() -> impl Iterator<Item = Bytes> {
    let numbered =
        |prefix: &'static str| (0..24).map(move |index| Bytes::from(format!("{prefix}{index:02}")));
    numbered("a/")
        .chain(numbered("o/"))
        .chain([Bytes::from_static(OBSERVED_KEY)])
        .chain(numbered("observed/"))
        .chain(numbered("z/"))
}

async fn finish_observer_work(
    values: ObservedValues,
    observed: &Mutex<Option<ObservedValues>>,
    panics: bool,
) {
    *observed.lock().unwrap() = Some(values);
    yield_now().await;
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
    let observed = Arc::new(Mutex::new(None));
    if let KeyChange::ClearedByRange = key_change {
        store
            .transact(|mut transaction| {
                Box::pin(async move {
                    for key in keys_around_the_observed_key() {
                        transaction.set(key, Bytes::from_static(b"before"))?;
                    }
                    Ok::<_, StoreError<SetRejection<S::Rejection>, S::Failure>>(((), transaction))
                })
            })
            .await
            .expect("setting the observed key and its neighbours before the scenario failed");
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
        observed.clone(),
        panics,
    ))
    .catch_unwind();
    let observer_result = match observer_outcome {
        ObserverOutcome::CancelledAtSuspension(n) => cancel_at_nth_suspension(observing, n).await,
        ObserverOutcome::Commits | ObserverOutcome::Panics => Some(observing.await),
    };
    match (observer_outcome, observer_result) {
        (ObserverOutcome::Commits, Some(Ok(Ok(()))))
        | (ObserverOutcome::Panics, Some(Err(_)))
        | (ObserverOutcome::CancelledAtSuspension(_), _) => {}
        (ObserverOutcome::Commits, _) => panic!("the observing transaction did not commit"),
        (ObserverOutcome::Panics, _) => panic!("the observing transaction did not panic"),
    }
    let read_key = || {
        store.read_transact(|transaction| {
            let key = key.clone();
            Box::pin(async move { transaction.get(key).await })
        })
    };
    let current = read_key().await.expect("reading the key again failed");
    assert!(
        store
            .changed(key.clone(), current.clone())
            .now_or_never()
            .is_none(),
        "changed resolved although the key did not change"
    );
    let observed: Vec<Option<Bytes>> = observed
        .lock()
        .unwrap()
        .take()
        .into_iter()
        .flatten()
        .collect();
    let waiters: Vec<_> = observed
        .iter()
        .cloned()
        .enumerate()
        .map(|(observation, seen)| {
            let (store, log, key) = (store.clone(), log.clone(), key.clone());
            spawn_with_handle(&replay, async move {
                let result = store.changed(key, seen).await;
                log.lock()
                    .unwrap()
                    .push(ObservationEvent::Resolved { observation });
                result
            })
        })
        .collect();
    if let Some(changing) = changing {
        changing.await;
    }
    yield_now().await;
    let final_value = read_key().await.expect("reading the final value failed");
    let position = |event| {
        log.lock()
            .unwrap()
            .iter()
            .position(|logged| *logged == event)
    };
    for (observation, (seen, waiter)) in observed.into_iter().zip(waiters).enumerate() {
        if final_value == seen {
            assert!(
                position(ObservationEvent::Resolved { observation }).is_none(),
                "changed resolved although the key did not change"
            );
            continue;
        }
        assert!(
            waiter.await.is_ok(),
            "changed did not resolve although the key changed"
        );
        // a value that was committed when the waiter started can only change through the changer
        if seen == current {
            let resolved = position(ObservationEvent::Resolved { observation });
            assert!(
                matches!(
                    (position(ObservationEvent::ChangerWorkEnded), resolved),
                    (Some(changed), Some(resolved)) if changed < resolved
                ),
                "changed resolved before the key changed"
            );
        }
    }
}
