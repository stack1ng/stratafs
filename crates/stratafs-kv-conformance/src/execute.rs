use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::future::yield_now;
use futures_util::FutureExt;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};

use crate::activity::{SimulatedOperation, SimulatedRead, SimulatedTransaction};
use crate::model::{Outcome, Seen};

pub(crate) struct Observed<O> {
    pub(crate) key: Bytes,
    pub(crate) seen: Option<Bytes>,
    pub(crate) observation: O,
}

pub(crate) async fn execute<S: KeyValueStore>(
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
