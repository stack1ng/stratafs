use std::collections::BTreeSet;
use std::future::poll_fn;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{Either, RemoteHandle, select};
use proptest::test_runner::TestCaseError;
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, NotCommitted, ReadKeyValueTransaction, TransactError,
};

use crate::activity::{SimulatedCall, SimulatedTransaction};
use crate::execute::{Observed, WorkAttempt, WorkFailure, execute};
use crate::model::{BTreeModel, HistoryOperation, ObservationId, Outcome, TransactionId};
use crate::runtime::{Replay, ReplayingRuntime, spawn_with_handle};

pub(crate) type Tester = LinearizabilityTester<HistoryId, BTreeModel>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum HistoryId {
    Client { client: usize, generation: usize },
    ObservationWaiter(ObservationId),
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
) -> Result<(), TestCaseError>
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
            let checked = run_simulated_client_activity(
                &replay,
                Arc::new(new_store()),
                new_store,
                simulated_client_activity,
            )
            .await
            .and_then(|history| {
                check_strictly_serializable(
                    BTreeModel::default(),
                    history,
                    "no serial order explains this history",
                )
            });
            keep_first_failure(&simulation_result, checked);
        }
    });
    result.lock().unwrap().take().expect("the simulation ran")
}

pub(crate) fn keep_first_failure(
    slot: &Mutex<Option<Result<(), TestCaseError>>>,
    result: Result<(), TestCaseError>,
) {
    let mut slot = slot.lock().unwrap();
    if !matches!(*slot, Some(Err(_))) {
        *slot = Some(result);
    }
}

pub(crate) fn check_strictly_serializable(
    starting_model: BTreeModel,
    history: Vec<Event>,
    violation: &str,
) -> Result<(), TestCaseError> {
    let tester = tester_from(starting_model, history)?;
    if tester.is_consistent() {
        Ok(())
    } else {
        Err(TestCaseError::fail(format!("{violation}: {tester:?}")))
    }
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
    let possible_late_commits = simulated_client_activity
        .iter()
        .flatten()
        .filter(|simulated_call| {
            simulated_call.cancelled_at_fraction.is_some()
                && matches!(simulated_call.transaction, SimulatedTransaction::Write(_))
        })
        .count();
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
        None => {
            settle_observation_waiters(&*store, observation_waiters, possible_late_commits).await
        }
    };
    let history = std::mem::take(&mut *history.lock().unwrap());
    clients_result.map(|()| history)
}

fn tester_from(starting_model: BTreeModel, history: Vec<Event>) -> Result<Tester, TestCaseError> {
    let fired: BTreeSet<HistoryId> = history
        .iter()
        .filter_map(|event| match event {
            Event::Returned(id @ HistoryId::ObservationWaiter(_), _) => Some(*id),
            _ => None,
        })
        .collect();
    let mut tester = LinearizabilityTester::new(starting_model);
    for event in history {
        match event {
            Event::Invoked(id @ HistoryId::ObservationWaiter(_), _) if !fired.contains(&id) => {}
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
        let transaction_id = TransactionId {
            client,
            transaction,
        };
        history.lock().unwrap().push(Event::Invoked(
            id,
            HistoryOperation::Transaction(transaction_id, simulated_call.transaction.clone()),
        ));
        let last_attempt = Arc::new(Mutex::new(WorkAttempt::default()));
        let execution = execute(&*store, simulated_call.transaction.clone(), &last_attempt);
        let result = match simulated_call.cancelled_at_fraction {
            None => Some(execution.await),
            Some(fraction) => {
                let suspensions =
                    measure_suspensions(&*store, &*new_store, simulated_call.transaction).await;
                let n = (fraction * (suspensions + 1) as f64) as usize;
                cancel_at_nth_suspension(execution, n).await
            }
        };
        let WorkAttempt { observed, finished } = std::mem::take(&mut *last_attempt.lock().unwrap());
        let known_to_commit = match result {
            None => {
                generation += 1;
                false
            }
            Some(Ok(outcome)) => {
                history.lock().unwrap().push(Event::Returned(id, outcome));
                true
            }
            Some(Err(TransactError::Failed(failure))) => {
                return Err(TestCaseError::fail(format!(
                    "the store failed a transaction: {failure}"
                )));
            }
            Some(Err(TransactError::Aborted(failure))) => {
                return Err(TestCaseError::fail(failure.to_string()));
            }
            Some(Err(TransactError::Rejected(rejection))) => {
                return Err(TestCaseError::fail(format!(
                    "a transaction this small was rejected: {rejection}"
                )));
            }
        };
        if !finished {
            require_not_committed(&*store, observed).await?;
            continue;
        }
        for (index, observed) in observed.into_iter().enumerate() {
            let observation = ObservationId {
                transaction: transaction_id,
                observation: index,
            };
            observation_waiters.push(spawn_observation_waiter(
                &replay,
                &store,
                &history,
                observation,
                observed,
                known_to_commit,
            ));
        }
    }
    Ok(observation_waiters)
}

async fn require_not_committed<S: KeyValueStore>(
    store: &S,
    observed: Vec<Observed<S::Observation>>,
) -> Result<(), TestCaseError> {
    for Observed { observation, .. } in observed {
        if !matches!(
            store.changed(observation).await,
            Err(StoreError::Rejected(NotCommitted))
        ) {
            return Err(TestCaseError::fail(
                "changed did not report NotCommitted for an observation whose transaction did not finish its work",
            ));
        }
    }
    Ok(())
}

fn spawn_observation_waiter<P: Replay, S: KeyValueStore + 'static>(
    replay: &P,
    store: &Arc<S>,
    history: &Arc<Mutex<Vec<Event>>>,
    observation: ObservationId,
    observed: Observed<S::Observation>,
    known_to_commit: bool,
) -> ObservationWaiter {
    let observation_waiter_id = HistoryId::ObservationWaiter(observation);
    history.lock().unwrap().push(Event::Invoked(
        observation_waiter_id,
        HistoryOperation::ObservationFired(observation),
    ));
    let (store, history) = (store.clone(), history.clone());
    let task = spawn_with_handle(replay, async move {
        match store.changed(observed.observation).await {
            Ok(()) => history.lock().unwrap().push(Event::Returned(
                observation_waiter_id,
                Outcome::Changed(true),
            )),
            Err(StoreError::Rejected(NotCommitted)) if !known_to_commit => {}
            Err(StoreError::Rejected(NotCommitted)) => panic!(
                "changed reported NotCommitted for an observation whose transaction committed"
            ),
            Err(StoreError::Failed(failure)) => panic!("changed failed: {failure}"),
        }
    });
    ObservationWaiter {
        key: observed.key,
        seen: observed.seen,
        task,
    }
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
    copy.transact::<_, WorkFailure<S::Rejection, S::Failure>>(move |mut transaction| {
        let entries = entries.clone();
        Box::pin(async move {
            for (key, value) in entries {
                transaction.set(key, value).await?;
            }
            Ok(())
        })
    })
    .await
    .expect("writing the copy failed");
    count_suspensions(execute(&copy, transaction, &Arc::default())).await
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
    let mut parked_at_nth = false;
    let mut future = pin!(future);
    poll_fn(|context| {
        if parked_at_nth {
            return Poll::Ready(None);
        }
        match future.as_mut().poll(context) {
            Poll::Ready(output) => Poll::Ready(Some(output)),
            Poll::Pending if remaining == 0 => {
                parked_at_nth = true;
                Poll::Pending
            }
            Poll::Pending => {
                remaining -= 1;
                Poll::Pending
            }
        }
    })
    .await
}

// settle_observation_waiters waits for all observations to either get fulfilled (if the key's value changed from the initial value)
// or skipped (if the key's value is the same as the initial value, since the watch isn't required to fire now that no more writes can happen).
async fn settle_observation_waiters<S: KeyValueStore>(
    store: &S,
    observation_waiters: Vec<ObservationWaiter>,
    possible_late_commits: usize,
) -> Result<(), TestCaseError> {
    for mut observation_waiter in observation_waiters {
        let mut late_commits_left = possible_late_commits;
        loop {
            let key = observation_waiter.key.clone();
            let (current, observation) = store
                .read_transact::<_, S::Failure>(move |transaction| {
                    let key = key.clone();
                    Box::pin(async move {
                        let current = transaction.get(key.clone()).await?;
                        Ok((current, transaction.observe(key)?))
                    })
                })
                .await
                .map_err(|error| {
                    TestCaseError::fail(format!("reading a watched key failed: {error}"))
                })?;
            if current == observation_waiter.seen {
                observation_waiter.task.now_or_never();
                break;
            }
            let changed_again = pin!(store.changed(observation));
            match select(&mut observation_waiter.task, changed_again).await {
                Either::Left(((), _)) => break,
                Either::Right((Ok(()), _)) => {
                    if late_commits_left == 0 {
                        return Err(TestCaseError::fail(
                            "changed resolved although no transaction could still change the key",
                        ));
                    }
                    late_commits_left -= 1;
                }
                Either::Right((Err(StoreError::Rejected(NotCommitted)), _)) => {
                    return Err(TestCaseError::fail(
                        "changed reported NotCommitted for an observation from a read transaction that finished",
                    ));
                }
                Either::Right((Err(StoreError::Failed(failure)), _)) => {
                    return Err(TestCaseError::fail(format!("changed failed: {failure}")));
                }
            }
        }
    }
    Ok(())
}
