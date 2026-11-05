// stratafs-consistency-conformance checks the engine's consistencies: several mounts of one volume, each a real engine
// with a model of FSKit's kernel, run fuzzed activity on a replaying runtime. A strong mount's history must be
// linearizable, and eventual mounts must all serve the final version once they settle.

mod activity;
mod faults;
mod kernel;
mod model;
mod mount;
mod simulation;
mod store;

pub use stratafs_runtime::replay as runtime;

use proptest::prelude::{ProptestConfig, any};
use proptest::proptest;
use stratafs_fs_engine::{Eventual, StrongUntilDisconnectCached};
use stratafs_pubsub_conformance::MemoryClient;

use crate::activity::{EVENTUAL_FAULTS, STRONG_FAULTS, simulated_activity};
use crate::runtime::ReplayingRuntime;
use crate::simulation::{Ending, simulate};

// strong_is_linearizable runs fuzzed activity on strong until disconnect, cached mounts, and fails if the history is
// not linearizable, leaving out what a mount cut off from the pub/sub did before it noticed, or a write is left
// pending. It can find violations, but cannot prove there are none.
pub fn strong_is_linearizable<R: ReplayingRuntime>(runtime: R) {
    proptest!(config(), |(activity in simulated_activity(STRONG_FAULTS), seed in any::<u64>())| {
        simulate::<_, StrongUntilDisconnectCached<MemoryClient<u64>>>(
            &runtime,
            Ending::Linearizable,
            activity,
            seed,
        )?;
    });
}

// eventual_converges runs fuzzed activity on eventual mounts, and fails unless every mount serves the final version
// once they settle.
pub fn eventual_converges<R: ReplayingRuntime>(runtime: R) {
    proptest!(config(), |(activity in simulated_activity(EVENTUAL_FAULTS), seed in any::<u64>())| {
        simulate::<_, Eventual>(&runtime, Ending::Converges, activity, seed)?;
    });
}

// CASES is how many activities each check runs, unless PROPTEST_CASES says otherwise.
const CASES: u32 = 1024;

fn config() -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok());
    ProptestConfig::with_cases(cases.unwrap_or(CASES))
}
