pub mod history;
mod model;

use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use proptest::proptest;
use stateright::semantics::LinearizabilityTester;
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
pub fn is_strictly_serializable<S: KeyValueStore>(new_store: impl Fn() -> S) {
    proptest!(|(_simulated_client_activity in simulated_client_activity())| {
        let _store = new_store();
        let _tester = LinearizabilityTester::<usize, BTreeModel>::new(BTreeModel::default());
    });
}

#[expect(dead_code)]
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
                                match operation {
                                    SimulatedOperation::Read(read) => {
                                        seen.push(perform_read(transaction, read).await?)
                                    }
                                    SimulatedOperation::Set(key, value) => {
                                        transaction.set(key, value)?
                                    }
                                    SimulatedOperation::Clear(key) => transaction.clear(key)?,
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
