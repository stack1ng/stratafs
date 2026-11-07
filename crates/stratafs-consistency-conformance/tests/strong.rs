use stratafs_consistency_conformance::runtime::{pct, seeded};
use stratafs_consistency_conformance::{Kernel, strong_is_linearizable};

#[test]
fn simulated_fskit_strong_is_linearizable_on_seeded_schedules() {
    strong_is_linearizable(Kernel::SimulatedFsKit, seeded());
}

#[test]
fn simulated_fskit_strong_is_linearizable_on_pct_schedules() {
    strong_is_linearizable(Kernel::SimulatedFsKit, pct(3, 4));
}

#[test]
fn simulated_linux_fuse_strong_is_linearizable_on_seeded_schedules() {
    strong_is_linearizable(Kernel::SimulatedLinuxFuse, seeded());
}

#[test]
fn simulated_linux_fuse_strong_is_linearizable_on_pct_schedules() {
    strong_is_linearizable(Kernel::SimulatedLinuxFuse, pct(3, 4));
}
