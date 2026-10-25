use std::sync::{Arc, Mutex};

use futures_lite::future::block_on;
use futures_util::future::join;
use proptest::test_runner::TestCaseError;
use stratafs_store::forkable::{ForkableKeyValueStore, ForkingTransaction};
use stratafs_store::kv::KeyValueStore;

use crate::activity::SimulatedCall;
use crate::durability::read_whole_store;
use crate::model::BTreeModel;
use crate::runtime::{Replay, ReplayingRuntime};
use crate::simulation::{
    Event, check_strictly_serializable, keep_first_failure, run_simulated_client_activity,
};

pub(crate) fn simulate_and_fork<R, F, N>(
    runtime: &R,
    new_forkable_store: Arc<N>,
    simulated_client_activity_before_fork: Vec<Vec<SimulatedCall>>,
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
        let simulated_client_activity_on_source = simulated_client_activity_on_source.clone();
        let simulated_client_activity_on_fork = simulated_client_activity_on_fork.clone();
        let simulation_result = simulation_result.clone();
        async move {
            let checked = fork_between_activity(
                &replay,
                new_forkable_store,
                simulated_client_activity_before_fork,
                simulated_client_activity_on_source,
                simulated_client_activity_on_fork,
            )
            .await
            .and_then(|(before_fork, forked_model, on_source, on_fork)| {
                check_strictly_serializable(
                    BTreeModel::default(),
                    before_fork,
                    "no serial order explains the history up to reading the fork",
                )?;
                check_strictly_serializable(
                    forked_model.clone(),
                    on_source,
                    "no serial order explains the source's history after the fork, starting from what the fork held",
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

async fn fork_between_activity<P, F, N>(
    replay: &P,
    new_forkable_store: Arc<N>,
    simulated_client_activity_before_fork: Vec<Vec<SimulatedCall>>,
    simulated_client_activity_on_source: Vec<Vec<SimulatedCall>>,
    simulated_client_activity_on_fork: Vec<Vec<SimulatedCall>>,
) -> Result<(Vec<Event>, BTreeModel, Vec<Event>, Vec<Event>), TestCaseError>
where
    P: Replay,
    F: ForkableKeyValueStore,
    F::Volume: 'static,
    N: Fn() -> F + Send + Sync + 'static,
{
    let forkable_store = new_forkable_store();
    let source = Arc::new(open_new_volume(&forkable_store).await);
    let new_volume = Arc::new(move || block_on(open_new_volume(&new_forkable_store())));
    let final_client = simulated_client_activity_before_fork.len();
    let before_fork = run_simulated_client_activity(
        replay,
        source.clone(),
        new_volume.clone(),
        simulated_client_activity_before_fork,
    )
    .await?;
    let fork_id = source
        .transact(|mut transaction| {
            Box::pin(async move {
                let fork_id = transaction.fork().await;
                fork_id.map(|fork_id| (fork_id, transaction))
            })
        })
        .await
        .map_err(|error| TestCaseError::fail(format!("forking failed: {error}")))?;
    let fork = Arc::new(
        forkable_store
            .open(fork_id)
            .await
            .map_err(|error| TestCaseError::fail(format!("opening the fork failed: {error}")))?,
    );
    let before_fork = Mutex::new(before_fork);
    let forked_model = read_whole_store(&*fork, &before_fork, final_client).await?;
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
    Ok((
        before_fork.into_inner().unwrap(),
        forked_model,
        on_source?,
        on_fork?,
    ))
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
