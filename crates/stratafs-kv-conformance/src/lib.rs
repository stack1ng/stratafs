pub mod activity;
mod execute;
mod model;
mod simulation;

use std::ops::Bound;
use std::panic::AssertUnwindSafe;

use bytes::Bytes;
use futures_lite::future::block_on;
use futures_util::FutureExt;
use proptest::prelude::any;
use proptest::{prop_assert, proptest};
use stateright::semantics::ConsistencyTester;
use stratafs_store::kv::{KeyValueStore, ReadKeyValueTransaction};

use crate::activity::simulated_client_activity;
use crate::simulation::simulate;

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
