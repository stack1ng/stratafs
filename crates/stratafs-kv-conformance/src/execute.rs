use std::panic::{AssertUnwindSafe, resume_unwind};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_lite::future::yield_now;
use futures_util::FutureExt;
use futures_util::future::try_join_all;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};

use crate::activity::{SimulatedOperation, SimulatedRead, SimulatedTransaction};
use crate::model::{Outcome, Seen};

pub(crate) struct Observed<O> {
    pub(crate) key: Bytes,
    pub(crate) seen: Option<Bytes>,
    pub(crate) observation: O,
}

pub(crate) struct WorkAttempt<O> {
    pub(crate) observed: Vec<Observed<O>>,
    pub(crate) finished: bool,
}

impl<O> Default for WorkAttempt<O> {
    fn default() -> Self {
        WorkAttempt {
            observed: Vec::new(),
            finished: false,
        }
    }
}

pub(crate) async fn execute<S: KeyValueStore>(
    store: &S,
    simulated_transaction: SimulatedTransaction,
    // store implementations are allowed to run the "work" callback more than once,
    // this records only the most recent attempt.
    // The caller owns it, so it survives the transaction being cancelled.
    last_attempt: &Arc<Mutex<WorkAttempt<S::Observation>>>,
) -> Result<Outcome, StoreError<S::Rejection, S::Failure>> {
    let last_attempt = last_attempt.clone();
    let run = async {
        match simulated_transaction {
            SimulatedTransaction::Read(groups) => {
                store
                    .read_transact(move |transaction| {
                        let (groups, last_attempt) = (groups.clone(), last_attempt.clone());
                        Box::pin(async move {
                            *last_attempt.lock().unwrap() = WorkAttempt::default();
                            let mut seen = Vec::new();
                            for reads in groups {
                                yield_now().await;
                                seen.extend(
                                    perform_reads(transaction, reads, &last_attempt).await?,
                                );
                            }
                            last_attempt.lock().unwrap().finished = true;
                            Ok(seen)
                        })
                    })
                    .await
            }
            SimulatedTransaction::Write(operations) => {
                store
                    .transact(move |transaction| {
                        let (operations, last_attempt) = (operations.clone(), last_attempt.clone());
                        Box::pin(async move {
                            *last_attempt.lock().unwrap() = WorkAttempt::default();
                            let mut seen = Vec::new();
                            for operation in operations {
                                yield_now().await;
                                match operation {
                                    SimulatedOperation::Read(reads) => seen.extend(
                                        perform_reads(transaction, reads, &last_attempt).await?,
                                    ),
                                    SimulatedOperation::Set(key, value) => {
                                        transaction.set(key, value)?
                                    }
                                    SimulatedOperation::Clear(key) => transaction.clear(key)?,
                                    SimulatedOperation::Panic => {
                                        resume_unwind(Box::new("simulated panic inside work"))
                                    }
                                }
                            }
                            last_attempt.lock().unwrap().finished = true;
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

async fn perform_reads<T: ReadKeyValueTransaction>(
    transaction: &T,
    reads: Vec<SimulatedRead>,
    last_attempt: &Mutex<WorkAttempt<T::Observation>>,
) -> Result<Vec<Seen>, T::Failure> {
    let performed = try_join_all(
        reads
            .into_iter()
            .map(|read| perform_read(transaction, read)),
    )
    .await?;
    let (seen, observations): (Vec<_>, Vec<_>) = performed.into_iter().unzip();
    last_attempt
        .lock()
        .unwrap()
        .observed
        .extend(observations.into_iter().flatten());
    Ok(seen)
}

async fn perform_read<T: ReadKeyValueTransaction>(
    transaction: &T,
    read: SimulatedRead,
) -> Result<(Seen, Option<Observed<T::Observation>>), T::Failure> {
    Ok(match read {
        SimulatedRead::Get(key) => (Seen::Get(transaction.get(key).await?), None),
        SimulatedRead::GetRange(start, end, limit) => (
            Seen::GetRange(transaction.get_range((start, end), limit).await?),
            None,
        ),
        SimulatedRead::Observe(key) => {
            let observation = transaction.observe(key.clone())?;
            let seen = transaction.get(key.clone()).await?;
            (
                Seen::Observe(seen.clone()),
                Some(Observed {
                    key,
                    seen,
                    observation,
                }),
            )
        }
    })
}
