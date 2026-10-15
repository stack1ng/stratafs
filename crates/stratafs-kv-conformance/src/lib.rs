pub mod history;
mod model;

use std::collections::BTreeSet;
use std::ops::Bound;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::future::{block_on, yield_now};
use futures_util::FutureExt;
use proptest::prelude::any;
use proptest::test_runner::TestCaseError;
use proptest::{prop_assert, proptest};
use shuttle::future::JoinHandle;
use shuttle::scheduler::RandomScheduler;
use shuttle::{Config, Runner};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};

use crate::history::{
    SimulatedOperation, SimulatedRead, SimulatedTransaction, simulated_client_activity,
};
use crate::model::{BTreeModel, HistoryOperation, Outcome, Seen};

// is_strictly_serializable runs fuzzed transactions against the given store from a fuzzed number
// of concurrent clients, and fails if the resulting history is not strictly serializable,
// using an in memory map as the reference. It can find violations, but cannot prove there are none.
// https://jepsen.io/consistency/models/strict-serializable
pub fn is_strictly_serializable<S: KeyValueStore + 'static>(new_store: impl Fn() -> S) {
    proptest!(|(simulated_client_activity in simulated_client_activity(), seed in any::<u64>())| {
        let tester = simulate(new_store(), simulated_client_activity, seed)?;
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

type Tester = LinearizabilityTester<HistoryId, BTreeModel>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum HistoryId {
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

struct Observed<O> {
    key: Bytes,
    seen: Option<Bytes>,
    observation: O,
}

struct ObservationWaiter {
    key: Bytes,
    seen: Option<Bytes>,
    task: JoinHandle<()>,
}

fn simulate<S: KeyValueStore + 'static>(
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

async fn execute<S: KeyValueStore>(
    store: &S,
    simulated_transaction: SimulatedTransaction,
) -> (
    Result<Outcome, StoreError<S::Rejection, S::Failure>>,
    Vec<Observed<S::Observation>>,
) {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let run = async {
        match simulated_transaction {
            SimulatedTransaction::Read(reads) => {
                let observed = observed.clone();
                store
                    .read_transact(move |transaction| {
                        let reads = reads.clone();
                        let observed = observed.clone();
                        Box::pin(async move {
                            observed.lock().unwrap().clear();
                            let mut seen = Vec::new();
                            for read in reads {
                                yield_now().await;
                                seen.push(perform_read(transaction, read, &observed).await?);
                            }
                            Ok(seen)
                        })
                    })
                    .await
            }
            SimulatedTransaction::Write(operations) => {
                let observed = observed.clone();
                store
                    .transact(move |transaction| {
                        let operations = operations.clone();
                        let observed = observed.clone();
                        Box::pin(async move {
                            observed.lock().unwrap().clear();
                            let mut seen = Vec::new();
                            for operation in operations {
                                yield_now().await;
                                match operation {
                                    SimulatedOperation::Read(read) => {
                                        seen.push(perform_read(transaction, read, &observed).await?)
                                    }
                                    SimulatedOperation::Set(key, value) => {
                                        transaction.set(key, value)?
                                    }
                                    SimulatedOperation::Clear(key) => transaction.clear(key)?,
                                    SimulatedOperation::Panic => {
                                        resume_unwind(Box::new("simulated panic inside work"))
                                    }
                                }
                            }
                            Ok(seen)
                        })
                    })
                    .await
            }
        }
    };
    let result = match AssertUnwindSafe(run).catch_unwind().await {
        Ok(result) => result.map(Outcome::Completed),
        Err(_) => Ok(Outcome::Panicked),
    };
    let observed = std::mem::take(&mut *observed.lock().unwrap());
    (result, observed)
}

async fn perform_read<T: ReadKeyValueTransaction>(
    transaction: &T,
    read: SimulatedRead,
    observed: &Mutex<Vec<Observed<T::Observation>>>,
) -> Result<Seen, T::Failure> {
    Ok(match read {
        SimulatedRead::Get(key) => Seen::Get(transaction.get(key).await?),
        SimulatedRead::GetRange(start, end, limit) => {
            Seen::GetRange(transaction.get_range((start, end), limit).await?)
        }
        SimulatedRead::Observe(key) => {
            let seen = transaction.get(key.clone()).await?;
            let observation = transaction.observe(key.clone())?;
            observed.lock().unwrap().push(Observed {
                key,
                seen: seen.clone(),
                observation,
            });
            Seen::Observe(seen)
        }
    })
}
