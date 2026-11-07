use stratafs_consistency_conformance::runtime::{pct, seeded};
use stratafs_consistency_conformance::{Kernel, eventual_converges};

#[test]
fn simulated_fskit_eventual_converges_on_seeded_schedules() {
    eventual_converges(Kernel::SimulatedFsKit, seeded());
}

#[test]
fn simulated_fskit_eventual_converges_on_pct_schedules() {
    eventual_converges(Kernel::SimulatedFsKit, pct(3, 4));
}

#[test]
fn simulated_linux_fuse_eventual_converges_on_seeded_schedules() {
    eventual_converges(Kernel::SimulatedLinuxFuse, seeded());
}

#[test]
fn simulated_linux_fuse_eventual_converges_on_pct_schedules() {
    eventual_converges(Kernel::SimulatedLinuxFuse, pct(3, 4));
}
