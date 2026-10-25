use std::ops::Bound;
use std::sync::{Arc, Mutex};

use proptest::test_runner::TestCaseError;
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction, TransactError};

use crate::activity::{SimulatedCall, SimulatedRead, SimulatedStop, SimulatedTransaction};
use crate::execute::execute;
use crate::model::{BTreeModel, HistoryOperation, Outcome, Seen, TransactionId};
use crate::runtime::{Replay, ReplayingRuntime, spawn_with_handle};
use crate::simulation::{
    Event, HistoryId, check_strictly_serializable, keep_first_failure,
    run_simulated_client_activity,
};

pub trait CrashableStorage: Clone + Send + Sync + 'static {
    fn storage_operations(&self) -> usize;

    fn crash_after(&self, storage_operations: usize);
}

pub(crate) fn simulate_and_reopen<R, S, C, N, O>(
    runtime: &R,
    new_store: Arc<N>,
    reopen: Arc<O>,
    simulated_client_transactions_before_stop: Vec<Vec<SimulatedTransaction>>,
    simulated_client_activity_after_reopen: Vec<Vec<SimulatedCall>>,
    seed: u64,
    stop: SimulatedStop,
) -> Result<(), TestCaseError>
where
    R: ReplayingRuntime,
    S: KeyValueStore + 'static,
    C: CrashableStorage,
    N: Fn() -> (S, C) + Send + Sync + 'static,
    O: Fn(C) -> S + Send + Sync + 'static,
{
    let crash_point = match stop {
        SimulatedStop::Clean => None,
        SimulatedStop::CrashAtFraction(fraction) => {
            let (activity_start, activity_end) = measure_storage_operations(
                runtime,
                new_store.clone(),
                simulated_client_transactions_before_stop.clone(),
                seed,
            );
            Some(activity_start + (fraction * (activity_end - activity_start + 1) as f64) as usize)
        }
    };
    let result = Arc::new(Mutex::new(None));
    let simulation_result = result.clone();
    runtime.run(seed, move |replay| {
        let new_store = new_store.clone();
        let reopen = reopen.clone();
        let simulated_client_transactions_before_stop =
            simulated_client_transactions_before_stop.clone();
        let simulated_client_activity_after_reopen = simulated_client_activity_after_reopen.clone();
        let simulation_result = simulation_result.clone();
        async move {
            let checked = stop_and_reopen(
                &replay,
                new_store,
                &*reopen,
                crash_point,
                simulated_client_transactions_before_stop,
                simulated_client_activity_after_reopen,
            )
            .await
            .and_then(|(before_reopen, reopened_model, after_reopen)| {
                check_strictly_serializable(
                    BTreeModel::default(),
                    before_reopen,
                    "no serial order explains the history up to reading the reopened store",
                )?;
                check_strictly_serializable(
                    reopened_model,
                    after_reopen,
                    "no serial order explains the history after the reopen, starting from what the reopened store held",
                )
            });
            keep_first_failure(&simulation_result, checked);
        }
    });
    result.lock().unwrap().take().expect("the simulation ran")
}

async fn stop_and_reopen<P, S, C, N>(
    replay: &P,
    new_store: Arc<N>,
    reopen: &impl Fn(C) -> S,
    crash_point: Option<usize>,
    simulated_client_transactions_before_stop: Vec<Vec<SimulatedTransaction>>,
    simulated_client_activity_after_reopen: Vec<Vec<SimulatedCall>>,
) -> Result<(Vec<Event>, BTreeModel, Vec<Event>), TestCaseError>
where
    P: Replay,
    S: KeyValueStore + 'static,
    C: CrashableStorage,
    N: Fn() -> (S, C) + Send + Sync + 'static,
{
    let (store, storage) = new_store();
    if let Some(crash_point) = crash_point {
        storage.crash_after(crash_point);
    }
    let store = Arc::new(store);
    let history = Arc::new(Mutex::new(Vec::new()));
    let final_client = simulated_client_transactions_before_stop.len();
    run_clients_until_crash(
        replay,
        &store,
        &storage,
        crash_point,
        &history,
        simulated_client_transactions_before_stop,
    )
    .await?;
    drop(store);
    let reopened = reopen(storage);
    let reopened_model = read_whole_store(&reopened, &history, final_client).await?;
    let before_reopen = std::mem::take(&mut *history.lock().unwrap());
    let after_reopen = run_simulated_client_activity(
        replay,
        Arc::new(reopened),
        Arc::new(move || new_store().0),
        simulated_client_activity_after_reopen,
    )
    .await?;
    Ok((before_reopen, reopened_model, after_reopen))
}

fn measure_storage_operations<R, S, C, N>(
    runtime: &R,
    new_store: Arc<N>,
    simulated_client_transactions: Vec<Vec<SimulatedTransaction>>,
    seed: u64,
) -> (usize, usize)
where
    R: ReplayingRuntime,
    S: KeyValueStore + 'static,
    C: CrashableStorage,
    N: Fn() -> (S, C) + Send + Sync + 'static,
{
    let measured = Arc::new(Mutex::new((0, 0)));
    let operations = measured.clone();
    runtime.run(seed, move |replay| {
        let new_store = new_store.clone();
        let simulated_client_transactions = simulated_client_transactions.clone();
        let operations = operations.clone();
        async move {
            let (store, storage) = new_store();
            let activity_start = storage.storage_operations();
            let history = Arc::new(Mutex::new(Vec::new()));
            let _ = run_clients_until_crash(
                &replay,
                &Arc::new(store),
                &storage,
                None,
                &history,
                simulated_client_transactions,
            )
            .await;
            *operations.lock().unwrap() = (activity_start, storage.storage_operations());
        }
    });
    *measured.lock().unwrap()
}

async fn run_clients_until_crash<P, S, C>(
    replay: &P,
    store: &Arc<S>,
    storage: &C,
    crash_point: Option<usize>,
    history: &Arc<Mutex<Vec<Event>>>,
    simulated_client_transactions: Vec<Vec<SimulatedTransaction>>,
) -> Result<(), TestCaseError>
where
    P: Replay,
    S: KeyValueStore + 'static,
    C: CrashableStorage,
{
    let clients: Vec<_> = simulated_client_transactions
        .into_iter()
        .enumerate()
        .map(|(client, simulated_transactions)| {
            spawn_with_handle(
                replay,
                run_client_until_crash(
                    store.clone(),
                    storage.clone(),
                    crash_point,
                    history.clone(),
                    client,
                    simulated_transactions,
                ),
            )
        })
        .collect();
    let mut failure = None;
    for client in clients {
        if let Err(error) = client.await {
            failure.get_or_insert(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

async fn run_client_until_crash<S: KeyValueStore, C: CrashableStorage>(
    store: Arc<S>,
    storage: C,
    crash_point: Option<usize>,
    history: Arc<Mutex<Vec<Event>>>,
    client: usize,
    simulated_transactions: Vec<SimulatedTransaction>,
) -> Result<(), TestCaseError> {
    let crashed =
        || crash_point.is_some_and(|crash_point| storage.storage_operations() > crash_point);
    let id = HistoryId::Client {
        client,
        generation: 0,
    };
    for (index, transaction) in simulated_transactions.into_iter().enumerate() {
        if crashed() {
            return Ok(());
        }
        history.lock().unwrap().push(Event::Invoked(
            id,
            HistoryOperation::Transaction(
                TransactionId {
                    client,
                    transaction: index,
                },
                transaction.clone(),
            ),
        ));
        let result = execute(
            &*store,
            transaction,
            // we arent checking the work that was performed here, so just make a throwaway value
            &Arc::default(),
        )
        .await;
        if crashed() {
            return Ok(());
        }
        match result {
            Ok(outcome) => history.lock().unwrap().push(Event::Returned(id, outcome)),
            Err(TransactError::Failed(failure)) => {
                return Err(TestCaseError::fail(format!(
                    "the store failed a transaction: {failure}"
                )));
            }
            Err(TransactError::Aborted(failure)) => {
                return Err(TestCaseError::fail(failure.to_string()));
            }
            Err(TransactError::Rejected(rejection)) => {
                return Err(TestCaseError::fail(format!(
                    "a transaction this small was rejected: {rejection}"
                )));
            }
        }
    }
    Ok(())
}

pub(crate) async fn read_whole_store<S: KeyValueStore>(
    store: &S,
    history: &Mutex<Vec<Event>>,
    final_client: usize,
) -> Result<BTreeModel, TestCaseError> {
    let id = HistoryId::Client {
        client: final_client,
        generation: 0,
    };
    history.lock().unwrap().push(Event::Invoked(
        id,
        HistoryOperation::Transaction(
            TransactionId {
                client: final_client,
                transaction: 0,
            },
            SimulatedTransaction::Read(vec![vec![SimulatedRead::GetRange(
                Bound::Unbounded,
                Bound::Unbounded,
                usize::MAX,
            )]]),
        ),
    ));
    let entries = store
        .read_transact(|transaction| {
            Box::pin(async move {
                transaction
                    .get_range(.., usize::MAX)
                    .await
                    .map(|entries| entries.collect::<Vec<_>>())
            })
        })
        .await
        .map_err(|error| TestCaseError::fail(format!("reading the whole store failed: {error}")))?;
    history.lock().unwrap().push(Event::Returned(
        id,
        Outcome::Completed(vec![Seen::GetRange(entries.clone())]),
    ));
    Ok(entries.into_iter().collect())
}
