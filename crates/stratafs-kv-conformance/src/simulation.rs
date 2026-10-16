use std::collections::BTreeSet;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use bytes::Bytes;
use futures_util::future::RemoteHandle;
use proptest::test_runner::TestCaseError;
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction,
};

use crate::activity::{SimulatedCall, SimulatedTransaction};
use crate::execute::{Observed, execute};
use crate::model::{BTreeModel, HistoryOperation, Outcome};
use crate::runtime::{Replay, ReplayingRuntime, spawn_with_handle};

pub(crate) type Tester = LinearizabilityTester<HistoryId, BTreeModel>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum HistoryId {
    Client {
        client: usize,
        generation: usize,
    },
    ObservationWaiter {
        client: usize,
        transaction: usize,
        observation: usize,
    },
}

pub(crate) enum Event {
    Invoked(HistoryId, HistoryOperation),
    Returned(HistoryId, Outcome),
}

struct ObservationWaiter {
    key: Bytes,
    seen: Option<Bytes>,
    task: RemoteHandle<()>,
}

pub(crate) fn simulate<R, S, N>(
    runtime: &R,
    new_store: Arc<N>,
    simulated_client_activity: Vec<Vec<SimulatedCall>>,
    seed: u64,
) -> Result<Tester, TestCaseError>
where
    R: ReplayingRuntime,
    S: KeyValueStore + 'static,
    N: Fn() -> S + Send + Sync + 'static,
{
    let result = Arc::new(Mutex::new(None));
    let simulation_result = result.clone();
    runtime.run(seed, move |replay| {
        let new_store = new_store.clone();
        let simulated_client_activity = simulated_client_activity.clone();
        let simulation_result = simulation_result.clone();
        async move {
            let history = run_simulated_client_activity(
                &replay,
                Arc::new(new_store()),
                new_store,
                simulated_client_activity,
            )
            .await;
            *simulation_result.lock().unwrap() = Some(history);
        }
    });
    let history = result.lock().unwrap().take().expect("the simulation ran")?;
    tester_from(BTreeModel::default(), history)
}

pub(crate) async fn run_simulated_client_activity<P, S, N>(
    replay: &P,
    store: Arc<S>,
    new_store: Arc<N>,
    simulated_client_activity: Vec<Vec<SimulatedCall>>,
) -> Result<Vec<Event>, TestCaseError>
where
    P: Replay,
    S: KeyValueStore + 'static,
    N: Fn() -> S + Send + Sync + 'static,
{
    let history = Arc::new(Mutex::new(Vec::new()));
    let clients: Vec<_> = simulated_client_activity
        .into_iter()
        .enumerate()
        .map(|(client, simulated_calls)| {
            spawn_with_handle(
                replay,
                run_client(
                    replay.clone(),
                    store.clone(),
                    new_store.clone(),
                    history.clone(),
                    client,
                    simulated_calls,
                ),
            )
        })
        .collect();
    let mut observation_waiters = Vec::new();
    let mut failure = None;
    for client in clients {
        match client.await {
            Ok(client_observation_waiters) => {
                observation_waiters.extend(client_observation_waiters)
            }
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    let clients_result = match failure {
        Some(failure) => Err(failure),
        None => settle_observation_waiters(&*store, observation_waiters).await,
    };
    let history = std::mem::take(&mut *history.lock().unwrap());
    clients_result.map(|()| history)
}

pub(crate) fn tester_from(
    starting_model: BTreeModel,
    history: Vec<Event>,
) -> Result<Tester, TestCaseError> {
    let fired: BTreeSet<HistoryId> = history
        .iter()
        .filter_map(|event| match event {
            Event::Returned(id @ HistoryId::ObservationWaiter { .. }, _) => Some(*id),
            _ => None,
        })
        .collect();
    let mut tester = LinearizabilityTester::new(starting_model);
    for event in history {
        match event {
            Event::Invoked(id @ HistoryId::ObservationWaiter { .. }, _) if !fired.contains(&id) => {
            }
            Event::Invoked(id, operation) => {
                tester
                    .on_invoke(id, operation)
                    .map_err(TestCaseError::fail)?;
            }
            Event::Returned(id, outcome) => {
                tester.on_return(id, outcome).map_err(TestCaseError::fail)?;
            }
        }
    }
    Ok(tester)
}

async fn run_client<P, S, N>(
    replay: P,
    store: Arc<S>,
    new_store: Arc<N>,
    history: Arc<Mutex<Vec<Event>>>,
    client: usize,
    simulated_calls: Vec<SimulatedCall>,
) -> Result<Vec<ObservationWaiter>, TestCaseError>
where
    P: Replay,
    S: KeyValueStore + 'static,
    N: Fn() -> S,
{
    let mut generation = 0;
    let mut observation_waiters = Vec::new();
    for (transaction, simulated_call) in simulated_calls.into_iter().enumerate() {
        let id = HistoryId::Client { client, generation };
        history.lock().unwrap().push(Event::Invoked(
            id,
            HistoryOperation::Transaction(simulated_call.transaction.clone()),
        ));
        let execution = execute(&*store, simulated_call.transaction.clone());
        let outcome = match simulated_call.cancelled_at_fraction {
            None => Some(execution.await),
            Some(fraction) => {
                let suspensions =
                    measure_suspensions(&*store, &*new_store, simulated_call.transaction).await;
                let n = (fraction * (suspensions + 1) as f64) as usize;
                cancel_at_nth_suspension(execution, n).await
            }
        };
        let Some((result, observed)) = outcome else {
            generation += 1;
            continue;
        };
        match result {
            Ok(Outcome::Panicked) => {
                history
                    .lock()
                    .unwrap()
                    .push(Event::Returned(id, Outcome::Panicked));
                for Observed { observation, .. } in observed {
                    if !matches!(
                        store.changed(observation).await,
                        Err(StoreError::Rejected(NotCommitted))
                    ) {
                        return Err(TestCaseError::fail(
                            "changed did not report NotCommitted for an observation whose transaction panicked",
                        ));
                    }
                }
            }
            Ok(outcome) => {
                history.lock().unwrap().push(Event::Returned(id, outcome));
                for (index, observed) in observed.into_iter().enumerate() {
                    let observation_waiter_id = HistoryId::ObservationWaiter {
                        client,
                        transaction,
                        observation: index,
                    };
                    history.lock().unwrap().push(Event::Invoked(
                        observation_waiter_id,
                        HistoryOperation::ObservationFired {
                            key: observed.key.clone(),
                            seen: observed.seen.clone(),
                        },
                    ));
                    let store = store.clone();
                    let history = history.clone();
                    let task = spawn_with_handle(&replay, async move {
                        match store.changed(observed.observation).await {
                            Ok(()) => history.lock().unwrap().push(Event::Returned(
                                observation_waiter_id,
                                Outcome::Changed(true),
                            )),
                            Err(StoreError::Rejected(NotCommitted)) => panic!(
                                "changed reported NotCommitted for an observation whose transaction committed"
                            ),
                            Err(StoreError::Failed(failure)) => {
                                panic!("changed failed: {failure}")
                            }
                        }
                    });
                    observation_waiters.push(ObservationWaiter {
                        key: observed.key,
                        seen: observed.seen,
                        task,
                    });
                }
            }
            Err(StoreError::Failed(failure)) => {
                return Err(TestCaseError::fail(format!(
                    "the store failed a transaction: {failure}"
                )));
            }
            Err(StoreError::Rejected(rejection)) => {
                return Err(TestCaseError::fail(format!(
                    "a transaction this small was rejected: {rejection}"
                )));
            }
        }
    }
    Ok(observation_waiters)
}

// measure_suspensions measures the number of suspensions in the given transaction,
// by executing the transaction and monitoring what it does at runtime.
// To ensure the borrowed store is not modified, this function makes a complete copy of the store,
// and executes the transaction on it.
async fn measure_suspensions<S: KeyValueStore>(
    store: &S,
    new_store: &impl Fn() -> S,
    transaction: SimulatedTransaction,
) -> usize {
    let copy = new_store();
    let entries = store
        .read_transact(|transaction| {
            Box::pin(async move { transaction.get_range(.., usize::MAX).await })
        })
        .await
        .expect("reading the store to copy failed");
    copy.transact(move |transaction| {
        let entries = entries.clone();
        Box::pin(async move {
            for (key, value) in entries {
                transaction.set(key, value)?;
            }
            Ok(())
        })
    })
    .await
    .expect("writing the copy failed");
    count_suspensions(execute(&copy, transaction)).await
}

pub(crate) async fn count_suspensions<F: Future>(future: F) -> usize {
    let mut future = pin!(future);
    let mut suspensions = 0;
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(_) => Poll::Ready(suspensions),
        Poll::Pending => {
            suspensions += 1;
            Poll::Pending
        }
    })
    .await
}

pub(crate) async fn cancel_at_nth_suspension<F: Future>(future: F, n: usize) -> Option<F::Output> {
    let mut remaining = n;
    let mut future = pin!(future);
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(output) => Poll::Ready(Some(output)),
        Poll::Pending if remaining == 0 => Poll::Ready(None),
        Poll::Pending => {
            remaining -= 1;
            Poll::Pending
        }
    })
    .await
}

// settle_observation_waiters waits for all observations to either get fulfilled (if the key's value changed from the initial value)
// or skipped (if the key's value is the same as the initial value, since the watch isn't required to fire now that no more writes can happen).
async fn settle_observation_waiters<S: KeyValueStore>(
    store: &S,
    observation_waiters: Vec<ObservationWaiter>,
) -> Result<(), TestCaseError> {
    let keys: Vec<Bytes> = observation_waiters
        .iter()
        .map(|observation_waiter| observation_waiter.key.clone())
        .collect();
    let final_values = store
        .read_transact(move |transaction| {
            let keys = keys.clone();
            Box::pin(async move {
                let mut values = Vec::new();
                for key in keys {
                    values.push(transaction.get(key).await?);
                }
                Ok(values)
            })
        })
        .await
        .map_err(|error| {
            TestCaseError::fail(format!("reading the final values failed: {error}"))
        })?;
    for (observation_waiter, final_value) in observation_waiters.into_iter().zip(final_values) {
        if final_value != observation_waiter.seen {
            observation_waiter.task.await;
        }
    }
    Ok(())
}
