use stratafs_consistency_conformance::eventual_converges;
use stratafs_consistency_conformance::runtime::{pct, seeded};

#[test]
fn eventual_converges_on_seeded_schedules() {
    eventual_converges(seeded());
}

#[test]
fn eventual_converges_on_pct_schedules() {
    eventual_converges(pct(3, 4));
}
