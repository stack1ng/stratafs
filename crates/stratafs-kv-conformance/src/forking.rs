use std::sync::{Arc, Mutex};

use futures_lite::future::block_on;
use futures_util::future::join;
use proptest::test_runner::TestCaseError;
use stateright::semantics::SequentialSpec;
use stratafs_store::forkable::{ForkableKeyValueStore, ForkingTransaction};
use stratafs_store::kv::KeyValueStore;

use crate::activity::{SimulatedCall, SimulatedForkTransaction, SimulatedTransaction};
use crate::durability::read_whole_store;
use crate::execute::{WorkAttempt, WorkFailure, perform_operations};
use crate::model::{BTreeModel, HistoryOperation, Outcome, TransactionId};
use crate::runtime::{Replay, ReplayingRuntime};
use crate::simulation::{
    Event, HistoryId, check_strictly_serializable, keep_first_failure,
    run_simulated_client_activity,
};

pub(crate) fn simulate_and_fork<R, F, N>(
    runtime: &R,
    new_forkable_store: Arc<N>,
    simulated_client_activity_before_fork: Vec<Vec<SimulatedCall>>,
    simulated_fork_transaction: SimulatedForkTransaction,
    simulated_client_activity_on_source: Vec<Vec<SimulatedCall>>,
    simulated_client_activity_on_fork: Vec<Vec<SimulatedCall>>,
    seed: u64,
) -> Result<(), TestCaseError>
where
    R: ReplayingRuntime,
    F: ForkableKeyValueStore,
    F::Volume: 'static,
    N: Fn() -> F + Send + Sync + 'static,
{
    let result = Arc::new(Mutex::new(None));
    let simulation_result = result.clone();
    runtime.run(seed, move |replay| {
        let new_forkable_store = new_forkable_store.clone();
        let simulated_client_activity_before_fork = simulated_client_activity_before_fork.clone();
        let simulated_fork_transaction = simulated_fork_transaction.clone();
        let simulated_client_activity_on_source = simulated_client_activity_on_source.clone();
        let simulated_client_activity_on_fork = simulated_client_activity_on_fork.clone();
        let simulation_result = simulation_result.clone();
        async move {
            let checked = fork_between_activity(
                &replay,
                new_forkable_store,
                simulated_client_activity_before_fork,
                simulated_fork_transaction,
                simulated_client_activity_on_source,
                simulated_client_activity_on_fork,
            )
            .await
            .and_then(|forked| {
                let Forked {
                    source_history,
                    fork_read,
                    expected_fork,
                    source_model,
                    on_source,
                    on_fork,
                } = forked;
                check_strictly_serializable(
                    BTreeModel::default(),
                    source_history,
                    "no serial order explains the source's history up to and through the forking transaction",
                )?;
                check_strictly_serializable(
                    expected_fork.clone(),
                    fork_read,
                    "the fork does not hold what the source held when the forking transaction forked",
                )?;
                let forked_model = expected_fork;
                check_strictly_serializable(
                    source_model,
                    on_source,
                    "no serial order explains the source's history after the fork, starting from what it held once the forking transaction committed",
                )?;
                check_strictly_serializable(
                    forked_model,
                    on_fork,
                    "no serial order explains the fork's history, starting from what it held when it was forked",
                )
            });
            keep_first_failure(&simulation_result, checked);
        }
    });
    result.lock().unwrap().take().expect("the simulation ran")
}

// Forked is everything a fork simulation records, for checking afterwards.
struct Forked {
    // the source's history, from its first transaction through a read of it after the forking transaction
    source_history: Vec<Event>,
    // a read of the whole fork, right after the forking transaction
    fork_read: Vec<Event>,
    // what the fork should hold: the source before the forking transaction, plus what it did before forking
    expected_fork: BTreeModel,
    // what the source held once the forking transaction committed
    source_model: BTreeModel,
    on_source: Vec<Event>,
    on_fork: Vec<Event>,
}

async fn fork_between_activity<P, F, N>(
    replay: &P,
    new_forkable_store: Arc<N>,
    simulated_client_activity_before_fork: Vec<Vec<SimulatedCall>>,
    simulated_fork_transaction: SimulatedForkTransaction,
    simulated_client_activity_on_source: Vec<Vec<SimulatedCall>>,
    simulated_client_activity_on_fork: Vec<Vec<SimulatedCall>>,
) -> Result<Forked, TestCaseError>
where
    P: Replay,
    F: ForkableKeyValueStore,
    F::Volume: 'static,
    N: Fn() -> F + Send + Sync + 'static,
{
    let forkable_store = new_forkable_store();
    let source = Arc::new(open_new_volume(&forkable_store).await);
    let new_volume = Arc::new(move || block_on(open_new_volume(&new_forkable_store())));
    let reading_before_fork = simulated_client_activity_before_fork.len();
    let (forking, reading_after_fork) = (reading_before_fork + 1, reading_before_fork + 2);
    let source_history = Mutex::new(
        run_simulated_client_activity(
            replay,
            source.clone(),
            new_volume.clone(),
            simulated_client_activity_before_fork,
        )
        .await?,
    );

    let model_before_fork =
        read_whole_store(&*source, &source_history, reading_before_fork).await?;
    let SimulatedForkTransaction {
        before_fork,
        after_fork,
    } = simulated_fork_transaction;
    let forking_id = TransactionId {
        client: forking,
        transaction: 0,
    };
    let history_id = HistoryId::Client {
        client: forking,
        generation: 0,
    };
    source_history.lock().unwrap().push(Event::Invoked(
        history_id,
        HistoryOperation::Transaction(
            forking_id,
            SimulatedTransaction::Write(before_fork.iter().chain(&after_fork).cloned().collect()),
        ),
    ));
    let (fork_id, seen) = source
        .transact::<_, WorkFailure<_, _>>(|mut transaction| {
            let (before_fork, after_fork) = (before_fork.clone(), after_fork.clone());
            Box::pin(async move {
                let attempt = Mutex::new(WorkAttempt::default());
                let mut seen = perform_operations(&mut *transaction, before_fork, &attempt).await?;
                let fork_id = transaction.fork().await?;
                seen.extend(perform_operations(&mut *transaction, after_fork, &attempt).await?);
                Ok(((fork_id, seen), transaction))
            })
        })
        .await
        .map_err(|error| TestCaseError::fail(format!("the forking transaction failed: {error}")))?;
    source_history
        .lock()
        .unwrap()
        .push(Event::Returned(history_id, Outcome::Completed(seen)));
    let source_model = read_whole_store(&*source, &source_history, reading_after_fork).await?;

    let mut expected_fork = model_before_fork;
    expected_fork.invoke(&HistoryOperation::Transaction(
        forking_id,
        SimulatedTransaction::Write(before_fork),
    ));
    let fork = Arc::new(
        forkable_store
            .open(fork_id)
            .await
            .map_err(|error| TestCaseError::fail(format!("opening the fork failed: {error}")))?,
    );
    let fork_read = Mutex::new(Vec::new());
    read_whole_store(&*fork, &fork_read, 0).await?;
    let (on_source, on_fork) = join(
        run_simulated_client_activity(
            replay,
            source,
            new_volume.clone(),
            simulated_client_activity_on_source,
        ),
        run_simulated_client_activity(replay, fork, new_volume, simulated_client_activity_on_fork),
    )
    .await;
    Ok(Forked {
        source_history: source_history.into_inner().unwrap(),
        fork_read: fork_read.into_inner().unwrap(),
        expected_fork,
        source_model,
        on_source: on_source?,
        on_fork: on_fork?,
    })
}

async fn open_new_volume<F: ForkableKeyValueStore>(forkable_store: &F) -> F::Volume {
    let id = forkable_store
        .create()
        .await
        .expect("creating a volume to test should succeed");
    forkable_store
        .open(id)
        .await
        .expect("opening a volume that was just created should succeed")
}
