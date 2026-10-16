pub mod activity;
pub mod durability;
mod execute;
mod model;
mod observation_lifecycle;
pub mod runtime;
mod simulation;

use std::ops::Bound;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use bytes::Bytes;
use futures_lite::future::block_on;
use futures_util::FutureExt;
use proptest::prelude::any;
use proptest::{prop_assert, proptest};
use stateright::semantics::ConsistencyTester;
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction};

use crate::activity::{simulated_client_activity, simulated_stop};
use crate::durability::{CrashableStorage, simulate_and_reopen};
use crate::observation_lifecycle::{KeyChange, count_change_suspensions, run_observation_scenario};
use crate::runtime::ReplayingRuntime;
use crate::simulation::simulate;

// is_strictly_serializable runs fuzzed transactions against the given store from a fuzzed number
// of concurrent clients, and fails if the resulting history is not strictly serializable,
// using an in memory map as the reference. It can find violations, but cannot prove there are none.
// https://jepsen.io/consistency/models/strict-serializable
// This is exploring an unbounded number of interleavings, so a runtime with bounded exploration is recommended.
pub fn is_strictly_serializable<R: ReplayingRuntime, S: KeyValueStore + 'static>(
    runtime: R,
    new_store: impl Fn() -> S + Send + Sync + 'static,
) {
    let new_store = Arc::new(new_store);
    proptest!(|(simulated_client_activity in simulated_client_activity(), seed in any::<u64>())| {
        let tester = simulate(&runtime, new_store.clone(), simulated_client_activity, seed)?;
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

// observation_lifecycle exercises the lifecycle of an observation,
// which contains a bounded number of interleavings, so more exhaustive runtimes are recommended.
pub fn observation_lifecycle<R: ReplayingRuntime, S: KeyValueStore + 'static>(
    runtime: R,
    new_store: impl Fn() -> S + Send + Sync + 'static,
) {
    let new_store = Arc::new(new_store);
    let change_suspensions = count_change_suspensions(&runtime, &new_store);
    let key_changes: Vec<KeyChange> = [KeyChange::Unchanged, KeyChange::Committed]
        .into_iter()
        .chain((0..change_suspensions).map(KeyChange::CancelledAtSuspension))
        .collect();
    for in_read_transaction in [false, true] {
        for observer_commits in [false, true] {
            for &key_change in &key_changes {
                let new_store = new_store.clone();
                runtime.run(0, move |replay| {
                    run_observation_scenario(
                        replay,
                        Arc::new(new_store()),
                        in_read_transaction,
                        observer_commits,
                        key_change,
                    )
                });
            }
        }
    }
}

// is_durable exercises the durability of the store,
// by simulating a client activity before and after a stop,
// which can either be caused by a crash, or a clean exit.
pub fn is_durable<R, S, C>(
    runtime: R,
    new_store: impl Fn() -> (S, C) + Send + Sync + 'static,
    reopen: impl Fn(C) -> S + Send + Sync + 'static,
) where
    R: ReplayingRuntime,
    S: KeyValueStore + 'static,
    C: CrashableStorage,
{
    let (new_store, reopen) = (Arc::new(new_store), Arc::new(reopen));
    proptest!(|(
        simulated_client_activity_before_stop in simulated_client_activity(),
        simulated_client_activity_after_reopen in simulated_client_activity(),
        seed in any::<u64>(),
        stop in simulated_stop(),
    )| {
        let (before_reopen, after_reopen) = simulate_and_reopen(
            &runtime,
            new_store.clone(),
            reopen.clone(),
            simulated_client_activity_before_stop,
            simulated_client_activity_after_reopen,
            seed,
            stop,
        )?;
        prop_assert!(
            before_reopen.is_consistent(),
            "no serial order explains the history up to reading the reopened store: {before_reopen:?}"
        );
        prop_assert!(
            after_reopen.is_consistent(),
            "no serial order explains the history after the reopen, starting from what the reopened store held: {after_reopen:?}"
        );
    });
}
