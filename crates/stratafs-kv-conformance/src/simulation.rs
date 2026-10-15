use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use proptest::test_runner::TestCaseError;
use shuttle::future::JoinHandle;
use shuttle::scheduler::RandomScheduler;
use shuttle::{Config, Runner};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction};

use crate::activity::SimulatedTransaction;
use crate::execute::{Observed, execute};
use crate::model::{BTreeModel, HistoryOperation, Outcome};

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

enum Event {
    Invoked(HistoryId, HistoryOperation),
    Returned(HistoryId, Outcome),
}

struct ObservationWaiter {
    key: Bytes,
    seen: Option<Bytes>,
    task: JoinHandle<()>,
}

pub(crate) fn simulate<S: KeyValueStore + 'static>(
    store: S,
    simulated_client_activity: Vec<Vec<SimulatedTransaction>>,
    seed: u64,
) -> Result<Tester, TestCaseError> {
    let store = Arc::new(store);
    let mut config = Config::default();
    config.stack_size = 1 << 20;
    let result = Arc::new(Mutex::new(None));
    let simulation_result = result.clone();
    Runner::new(RandomScheduler::new_from_seed(seed, 1), config).run(move || {
        let history = Arc::new(Mutex::new(Vec::new()));
        let clients: Vec<_> = simulated_client_activity
            .iter()
            .cloned()
            .enumerate()
            .map(|(client, simulated_transactions)| {
                shuttle::future::spawn(run_client(
                    store.clone(),
                    history.clone(),
                    client,
                    simulated_transactions,
                ))
            })
            .collect();
        let clients_result = shuttle::future::block_on(async {
            let mut observation_waiters = Vec::new();
            let mut failure = None;
            for client in clients {
                match client.await.expect("client tasks are never cancelled") {
                    Ok(client_observation_waiters) => {
                        observation_waiters.extend(client_observation_waiters)
                    }
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
            }
            match failure {
                Some(failure) => {
                    for observation_waiter in observation_waiters {
                        observation_waiter.task.abort();
                        let _ = observation_waiter.task.await;
                    }
                    Err(failure)
                }
                None => settle_observation_waiters(&*store, observation_waiters).await,
            }
        });
        let history = Arc::into_inner(history)
            .expect("every task has finished")
            .into_inner()
            .expect("no task panicked while recording");
        *simulation_result.lock().unwrap() = Some(clients_result.map(|()| history));
    });
    let history = Arc::into_inner(result)
        .expect("the simulation has finished")
        .into_inner()
        .unwrap()
        .expect("the simulation ran once")?;
    tester_from(history)
}

fn tester_from(history: Vec<Event>) -> Result<Tester, TestCaseError> {
    let fired: BTreeSet<HistoryId> = history
        .iter()
        .filter_map(|event| match event {
            Event::Returned(id @ HistoryId::ObservationWaiter { .. }, _) => Some(*id),
            _ => None,
        })
        .collect();
    let mut tester = LinearizabilityTester::new(BTreeModel::default());
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

async fn run_client<S: KeyValueStore + 'static>(
    store: Arc<S>,
    history: Arc<Mutex<Vec<Event>>>,
    client: usize,
    simulated_transactions: Vec<SimulatedTransaction>,
) -> Result<Vec<ObservationWaiter>, TestCaseError> {
    let mut generation = 0;
    let mut observation_waiters = Vec::new();
    for (transaction, simulated_transaction) in simulated_transactions.into_iter().enumerate() {
        let id = HistoryId::Client { client, generation };
        history.lock().unwrap().push(Event::Invoked(
            id,
            HistoryOperation::Transaction(simulated_transaction.clone()),
        ));
        let (result, observed) = execute(&*store, simulated_transaction).await;
        match result {
            Ok(Outcome::Panicked) => {
                history
                    .lock()
                    .unwrap()
                    .push(Event::Returned(id, Outcome::Panicked));
                for Observed { observation, .. } in observed {
                    if store.changed(observation).await.is_ok() {
                        return Err(TestCaseError::fail(
                            "changed resolved for an observation whose transaction panicked",
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
                        HistoryOperation::Changed {
                            key: observed.key.clone(),
                            seen: observed.seen.clone(),
                        },
                    ));
                    let store = store.clone();
                    let history = history.clone();
                    let task = shuttle::future::spawn(async move {
                        if store.changed(observed.observation).await.is_ok() {
                            history.lock().unwrap().push(Event::Returned(
                                observation_waiter_id,
                                Outcome::Changed(true),
                            ));
                        }
                    });
                    observation_waiters.push(ObservationWaiter {
                        key: observed.key,
                        seen: observed.seen,
                        task,
                    });
                }
            }
            Err(StoreError::Failed(_)) => generation += 1,
            Err(StoreError::Rejected(rejection)) => {
                return Err(TestCaseError::fail(format!(
                    "a transaction this small was rejected: {rejection}"
                )));
            }
        }
    }
    Ok(observation_waiters)
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
        if final_value == observation_waiter.seen {
            observation_waiter.task.abort();
        }
        let _ = observation_waiter.task.await;
    }
    Ok(())
}
