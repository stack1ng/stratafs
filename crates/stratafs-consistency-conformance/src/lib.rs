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

use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

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
// UPSTREAM KERNEL BUG PIN: it prints how many tainted reads it left out.
pub fn strong_is_linearizable<R: ReplayingRuntime>(runtime: R) {
    let tainted = AtomicUsize::new(0);
    proptest!(config(), |(activity in simulated_activity(STRONG_FAULTS), seed in any::<u64>())| {
        let left_out = simulate::<_, StrongUntilDisconnectCached<MemoryClient<u64>>>(
            &runtime,
            Ending::Linearizable,
            activity,
            seed,
        )?;
        tainted.fetch_add(left_out, Ordering::Relaxed);
    });
    // UPSTREAM KERNEL BUG PIN: written to stderr directly, so the test harness does not capture it.
    let _ = writeln!(
        std::io::stderr(),
        "left out {} reads answered with a failed write's data",
        tainted.into_inner()
    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::{FaultKind, SimulatedActivity, SimulatedFault, SimulatedOperation};
    use crate::runtime::seeded;

    // UPSTREAM KERNEL BUG PIN: a read right after a write that failed over cached data may get that data before the
    // purge the failure asks is in the kernel, which the random activity seldom reaches. Without that purge the read
    // would not be left out.
    #[test]
    fn strong_leaves_out_reads_of_a_failed_write() {
        let file = ("d", "b");
        let activity = SimulatedActivity {
            mounts: vec![vec![vec![
                SimulatedOperation::Read(file),
                SimulatedOperation::Write(file, b"x"),
                SimulatedOperation::Read(file),
            ]]],
            faults: vec![SimulatedFault {
                kind: FaultKind::StoreFails,
                mount: 0,
                at_fraction: 0.5,
            }],
            slow: false,
        };
        let mut left_out = 0;
        for seed in 0..16 {
            let simulated = simulate::<_, StrongUntilDisconnectCached<MemoryClient<u64>>>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            left_out += simulated.expect("the history is linearizable");
        }
        assert!(left_out > 0, "no read got the failed write's data");
    }
}
