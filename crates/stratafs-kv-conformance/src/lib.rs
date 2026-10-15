pub mod history;
mod model;

use std::ops::Bound;
use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::future::{block_on, yield_now};
use futures_util::FutureExt;
use proptest::prelude::any;
use proptest::test_runner::TestCaseError;
use proptest::{prop_assert, proptest};
use shuttle::scheduler::RandomScheduler;
use shuttle::{Config, Runner};
use stateright::semantics::{ConsistencyTester, LinearizabilityTester};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};

use crate::history::{
    SimulatedOperation, SimulatedRead, SimulatedTransaction, simulated_client_activity,
};
use crate::model::{BTreeModel, Outcome, Seen};

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

type Tester = LinearizabilityTester<(usize, usize), BTreeModel>;

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
        let tester = Arc::new(Mutex::new(
            LinearizabilityTester::new(BTreeModel::default()),
        ));
        let clients: Vec<_> = simulated_client_activity
            .iter()
            .cloned()
            .enumerate()
            .map(|(client, simulated_transactions)| {
                shuttle::future::spawn(run_client(
                    store.clone(),
                    tester.clone(),
                    client,
                    simulated_transactions,
                ))
            })
            .collect();
        let clients_result = shuttle::future::block_on(async {
            let mut clients_result = Ok(());
            for client in clients {
                let client_result = client.await.expect("client tasks are never cancelled");
                if clients_result.is_ok() {
                    clients_result = client_result;
                }
            }
            clients_result
        });
        let tester = Arc::into_inner(tester)
            .expect("every client task has finished")
            .into_inner()
            .expect("no client panicked while recording");
        *simulation_result.lock().unwrap() = Some(clients_result.map(|()| tester));
    });
    Arc::into_inner(result)
        .expect("the simulation has finished")
        .into_inner()
        .unwrap()
        .expect("the simulation ran once")
}

async fn run_client<S: KeyValueStore>(
    store: Arc<S>,
    tester: Arc<Mutex<Tester>>,
    client: usize,
    simulated_transactions: Vec<SimulatedTransaction>,
) -> Result<(), TestCaseError> {
    let mut id = (client, 0);
    for simulated_transaction in simulated_transactions {
        tester
            .lock()
            .unwrap()
            .on_invoke(id, simulated_transaction.clone())
            .map_err(TestCaseError::fail)?;
        match execute(&*store, simulated_transaction).await {
            Ok(outcome) => {
                tester
                    .lock()
                    .unwrap()
                    .on_return(id, outcome)
                    .map_err(TestCaseError::fail)?;
            }
            Err(StoreError::Failed(_)) => id.1 += 1,
            Err(StoreError::Rejected(rejection)) => {
                return Err(TestCaseError::fail(format!(
                    "a transaction this small was rejected: {rejection}"
                )));
            }
        }
    }
    Ok(())
}

async fn execute<S: KeyValueStore>(
    store: &S,
    simulated_transaction: SimulatedTransaction,
) -> Result<Outcome, StoreError<S::Rejection, S::Failure>> {
    let run = async {
        match simulated_transaction {
            SimulatedTransaction::Read(reads) => {
                store
                    .read_transact(move |transaction| {
                        let reads = reads.clone();
                        Box::pin(async move {
                            let mut seen = Vec::new();
                            for read in reads {
                                yield_now().await;
                                seen.push(perform_read(transaction, read).await?);
                            }
                            Ok(seen)
                        })
                    })
                    .await
            }
            SimulatedTransaction::Write(operations) => {
                store
                    .transact(move |transaction| {
                        let operations = operations.clone();
                        Box::pin(async move {
                            let mut seen = Vec::new();
                            for operation in operations {
                                yield_now().await;
                                match operation {
                                    SimulatedOperation::Read(read) => {
                                        seen.push(perform_read(transaction, read).await?)
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
    match AssertUnwindSafe(run).catch_unwind().await {
        Ok(result) => result.map(Outcome::Completed),
        Err(_) => Ok(Outcome::Panicked),
    }
}

async fn perform_read<T: ReadKeyValueTransaction>(
    transaction: &T,
    read: SimulatedRead,
) -> Result<Seen, T::Failure> {
    Ok(match read {
        SimulatedRead::Get(key) => Seen::Get(transaction.get(key).await?),
        SimulatedRead::GetRange(start, end, limit) => {
            Seen::GetRange(transaction.get_range((start, end), limit).await?)
        }
    })
}
