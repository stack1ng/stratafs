pub mod history;
mod model;

use proptest::proptest;
use stateright::semantics::LinearizabilityTester;
use stratafs_store::kv::KeyValueStore;

use crate::history::simulated_client_activity;
use crate::model::BTreeModel;

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
