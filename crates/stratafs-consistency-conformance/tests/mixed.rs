use stratafs_consistency_conformance::runtime::{pct, seeded};
use stratafs_consistency_conformance::{Kernel, strong_and_eventual_on_one_volume};

#[test]
fn simulated_fskit_strong_and_eventual_on_one_volume_on_seeded_schedules() {
    strong_and_eventual_on_one_volume(Kernel::SimulatedFsKit, seeded());
}

#[test]
fn simulated_fskit_strong_and_eventual_on_one_volume_on_pct_schedules() {
    strong_and_eventual_on_one_volume(Kernel::SimulatedFsKit, pct(3, 4));
}

#[test]
fn simulated_linux_fuse_strong_and_eventual_on_one_volume_on_seeded_schedules() {
    strong_and_eventual_on_one_volume(Kernel::SimulatedLinuxFuse, seeded());
}

#[test]
fn simulated_linux_fuse_strong_and_eventual_on_one_volume_on_pct_schedules() {
    strong_and_eventual_on_one_volume(Kernel::SimulatedLinuxFuse, pct(3, 4));
}
