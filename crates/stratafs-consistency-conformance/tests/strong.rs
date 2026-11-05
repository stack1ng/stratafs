use stratafs_consistency_conformance::runtime::{pct, seeded};
use stratafs_consistency_conformance::strong_is_linearizable;

#[test]
fn strong_is_linearizable_on_seeded_schedules() {
    strong_is_linearizable(seeded());
}

#[test]
fn strong_is_linearizable_on_pct_schedules() {
    strong_is_linearizable(pct(3, 4));
}
