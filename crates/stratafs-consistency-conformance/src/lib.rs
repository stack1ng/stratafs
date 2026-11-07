// stratafs-consistency-conformance checks the engine's consistencies: several mounts of one volume, each a real engine
// with a model of a kernel, macOS's through FSKit or Linux's through FUSE, run fuzzed activity on a replaying runtime.
// A strong mount's history must be linearizable, and eventual mounts must all serve the final version once they settle.

mod activity;
mod faults;
mod model;
mod mount;
mod simulated_fskit_kernel;
mod simulated_linux_fuse_kernel;
mod simulation;
mod store;

pub use stratafs_runtime::replay as runtime;

use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};

use proptest::prelude::{ProptestConfig, any};
use proptest::proptest;
use proptest::test_runner::FileFailurePersistence;
use stratafs_fs_engine::{Eventual, StrongUntilDisconnectCached};
use stratafs_pubsub_conformance::MemoryClient;

use crate::activity::{EVENTUAL_FAULTS, STRONG_FAULTS, simulated_activity};
use crate::mount::{KernelModel, Mount, Operations};
use crate::runtime::ReplayingRuntime;
use crate::simulation::{Ending, simulate};

// Kernel is the simulated kernel each mount has: macOS's through FSKit, or Linux's through FUSE.
#[derive(Clone, Copy, Debug)]
pub enum Kernel {
    SimulatedFsKit,
    SimulatedLinuxFuse,
}

type Strong = StrongUntilDisconnectCached<MemoryClient<u64>>;

// strong_is_linearizable runs fuzzed activity on strong until disconnect, cached mounts, and fails if the history is
// not linearizable, leaving out what a mount cut off from the pub/sub did before it noticed, or a write is left
// pending. It can find violations, but cannot prove there are none.
// UPSTREAM KERNEL BUG PIN: it prints how many tainted reads it left out.
pub fn strong_is_linearizable<R: ReplayingRuntime>(kernel: Kernel, runtime: R) {
    match kernel {
        Kernel::SimulatedFsKit => linearizable::<_, simulated_fskit_kernel::Kernel>(runtime),
        Kernel::SimulatedLinuxFuse => {
            linearizable::<_, simulated_linux_fuse_kernel::Kernel>(runtime)
        }
    }
}

fn linearizable<R: ReplayingRuntime, K: KernelModel>(runtime: R)
where
    Mount<Strong, K>: Operations,
{
    let tainted = AtomicUsize::new(0);
    proptest!(config(K::REGRESSIONS), |(activity in simulated_activity(STRONG_FAULTS), seed in any::<u64>())| {
        let left_out = simulate::<_, Strong, K>(&runtime, Ending::Linearizable, activity, seed)?;
        tainted.fetch_add(left_out, Ordering::Relaxed);
    });
    // UPSTREAM KERNEL BUG PIN: written to stderr directly, so the test harness does not capture it.
    let _ = writeln!(
        std::io::stderr(),
        "left out {} reads answered with {}",
        tainted.into_inner(),
        K::TAINTED
    );
}

// eventual_converges runs fuzzed activity on eventual mounts, and fails unless every mount serves the final version
// once they settle.
pub fn eventual_converges<R: ReplayingRuntime>(kernel: Kernel, runtime: R) {
    match kernel {
        Kernel::SimulatedFsKit => converges::<_, simulated_fskit_kernel::Kernel>(runtime),
        Kernel::SimulatedLinuxFuse => converges::<_, simulated_linux_fuse_kernel::Kernel>(runtime),
    }
}

fn converges<R: ReplayingRuntime, K: KernelModel>(runtime: R)
where
    Mount<Eventual, K>: Operations,
{
    proptest!(config(K::REGRESSIONS), |(activity in simulated_activity(EVENTUAL_FAULTS), seed in any::<u64>())| {
        simulate::<_, Eventual, K>(&runtime, Ending::Converges, activity, seed)?;
    });
}

// CASES is how many activities each check runs, unless PROPTEST_CASES says otherwise.
const CASES: u32 = 1024;

// config runs CASES activities, keeping the failing ones in regressions.
fn config(regressions: &'static str) -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok());
    ProptestConfig {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(regressions))),
        ..ProptestConfig::with_cases(cases.unwrap_or(CASES))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::activity::{FaultKind, SimulatedActivity, SimulatedFault, SimulatedOperation};
    use crate::runtime::{pct, seeded};
    use crate::simulation::run_once;

    // UPSTREAM KERNEL BUG PIN: a read right after a write that failed over cached data may get that data before the
    // purge the failure asks is in the kernel, which the random activity seldom reaches. Without that purge the read
    // would not be left out.
    #[test]
    fn simulated_fskit_strong_leaves_out_reads_of_a_failed_write() {
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
            let simulated = simulate::<_, Strong, simulated_fskit_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            left_out += simulated.expect("the history is linearizable");
        }
        assert!(left_out > 0, "no read got the failed write's data");
    }

    // UPSTREAM KERNEL BUG PIN: on Linux a read of cached pages sees the data of a write in flight on its mount, which
    // the random activity seldom reaches.
    #[test]
    fn simulated_linux_fuse_strong_leaves_out_reads_of_an_unstored_write() {
        let file = ("d", "b");
        let activity = SimulatedActivity {
            mounts: vec![
                vec![
                    vec![
                        SimulatedOperation::Write(file, b"x"),
                        SimulatedOperation::Read(file),
                        SimulatedOperation::Read(file),
                        SimulatedOperation::Write(file, b"zzz"),
                    ],
                    vec![
                        SimulatedOperation::Read(file),
                        SimulatedOperation::Read(file),
                        SimulatedOperation::Read(file),
                        SimulatedOperation::Read(file),
                    ],
                ],
                vec![vec![
                    SimulatedOperation::Read(file),
                    SimulatedOperation::Read(file),
                    SimulatedOperation::Read(file),
                ]],
            ],
            faults: Vec::new(),
            slow: false,
        };
        let mut left_out = 0;
        for seed in 0..1024 {
            let simulated = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            left_out += simulated.expect("the history is linearizable");
        }
        assert!(left_out > 0, "no read got an unstored write's data");
    }

    // On Linux a lookup on a mount whose own write to the item is in flight replies uncached at once, rather than wait
    // for the write while it holds the directory's lock, which the purge of another mount's rename there needs.
    #[test]
    fn simulated_linux_fuse_strong_lookup_beside_its_own_mounts_write_does_not_stall() {
        let file = ("d", "a");
        let activity = SimulatedActivity {
            mounts: vec![
                vec![vec![
                    SimulatedOperation::Rename(("d", "b"), ("d", "c")),
                    SimulatedOperation::Lookup(file),
                ]],
                vec![
                    vec![SimulatedOperation::Write(file, b"x")],
                    vec![SimulatedOperation::Chmod(file, 0o600)],
                ],
                vec![vec![SimulatedOperation::Lookup(file)]],
            ],
            faults: Vec::new(),
            slow: false,
        };
        for seed in 0..256 {
            let simulated = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            simulated.expect("no operation stalls and the history is linearizable");
        }
    }

    // On Linux a change drops its own mount's cached attributes before it commits, so a stat there after the commit
    // asks the volume, as one on another mount does.
    #[test]
    fn simulated_linux_fuse_strong_a_stat_on_the_changing_mount_sees_its_commit() {
        let file = ("d", "a");
        let activity = SimulatedActivity {
            mounts: vec![
                vec![
                    vec![SimulatedOperation::Chmod(file, 0o600)],
                    vec![SimulatedOperation::Stat(file); 3],
                ],
                vec![vec![SimulatedOperation::Stat(file); 3]],
            ],
            faults: Vec::new(),
            slow: false,
        };
        for seed in 0..1024 {
            let simulated = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            simulated.expect("the history is linearizable");
        }
    }

    // On Linux a change whose own mount's kernel refuses to drop its cached attributes fails before it commits, so no
    // stat there finds them from before a commit.
    #[test]
    fn simulated_linux_fuse_strong_a_change_whose_attributes_its_mount_keeps_fails() {
        let file = ("d", "a");
        let activity = SimulatedActivity {
            mounts: vec![
                vec![
                    vec![SimulatedOperation::Chmod(file, 0o600)],
                    vec![SimulatedOperation::Stat(file); 3],
                ],
                vec![vec![SimulatedOperation::Stat(file); 3]],
            ],
            faults: vec![SimulatedFault {
                kind: FaultKind::Refused,
                mount: 0,
                at_fraction: 0.0,
            }],
            slow: false,
        };
        for seed in 0..256 {
            let simulated = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            simulated.expect("the history is linearizable");
        }
    }

    // UPSTREAM KERNEL BUG PIN: on Linux a lookup on a mount whose own unlink or rename of the name is in flight finds
    // the name in the cache until the change replies, which the random activity seldom reaches.
    #[test]
    fn simulated_linux_fuse_strong_leaves_out_lookups_of_a_name_its_own_change_drops() {
        let (unlinked, renamed) = (("d", "a"), ("d", "b"));
        let lookups = [unlinked, renamed, unlinked, renamed].map(SimulatedOperation::Lookup);
        let activity = SimulatedActivity {
            mounts: vec![
                vec![
                    vec![SimulatedOperation::Unlink(unlinked)],
                    vec![SimulatedOperation::Rename(renamed, ("e", "b"))],
                    lookups.to_vec(),
                ],
                vec![lookups.to_vec()],
            ],
            faults: Vec::new(),
            slow: false,
        };
        let mut left_out = 0;
        for seed in 0..1024 {
            let simulated = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            left_out += simulated.expect("the history is linearizable");
        }
        assert!(left_out > 0, "no lookup found a name its own change drops");
    }

    // UPSTREAM KERNEL BUG PIN: on Linux a read that began before the purge of another mount's write copies up to the
    // size it began with, which the random activity seldom reaches. Only the other mount reads during the writes, so
    // no read gets an unstored write's data.
    #[test]
    fn simulated_linux_fuse_strong_leaves_out_reads_of_a_size_from_before_a_purge() {
        let file = ("d", "b");
        let activity = SimulatedActivity {
            mounts: vec![
                vec![vec![
                    SimulatedOperation::Write(file, b"x"),
                    SimulatedOperation::Write(file, b"yy"),
                    SimulatedOperation::Write(file, b"zzz"),
                ]],
                vec![
                    vec![SimulatedOperation::Read(file); 4],
                    vec![SimulatedOperation::Read(file); 4],
                ],
            ],
            faults: Vec::new(),
            slow: false,
        };
        let mut left_out = 0;
        for seed in 0..1024 {
            let simulated = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            left_out += simulated.expect("the history is linearizable");
        }
        assert!(left_out > 0, "no read got a size from before a purge");
    }

    // UPSTREAM KERNEL BUG PIN: on Linux a stat during another mount's write, or a read's own look at the size, can go
    // uncached with the size from before the write, which a read on the mount copies up to with the write's data, which
    // the random activity seldom reaches.
    #[test]
    fn simulated_linux_fuse_strong_leaves_out_reads_of_a_size_an_uncached_reply_took_in() {
        let file = ("d", "b");
        let activity = SimulatedActivity {
            mounts: vec![
                vec![vec![
                    SimulatedOperation::Write(file, b"x"),
                    SimulatedOperation::Write(file, b"yy"),
                    SimulatedOperation::Write(file, b"zzz"),
                ]],
                vec![
                    vec![SimulatedOperation::Stat(file); 6],
                    vec![SimulatedOperation::Read(file); 4],
                ],
            ],
            faults: Vec::new(),
            slow: false,
        };
        let mut left_out = 0;
        for seed in 0..1024 {
            let on_seeded = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            let on_pct = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            left_out += on_seeded.expect("the history is linearizable");
            left_out += on_pct.expect("the history is linearizable");
        }
        assert!(left_out > 0, "no read got a size from before its data");
    }

    // On Linux a change that would wait for another mount's change fails while any delivery is handled here, since that
    // delivery's purge may wait for the lock of a directory a system call holds while it waits for the lock of a file
    // the change holds, as an unlink of the file does.
    #[test]
    fn simulated_linux_fuse_strong_changes_in_one_directory_on_two_mounts_do_not_stall() {
        let file = ("d", "a");
        let activity = SimulatedActivity {
            mounts: vec![
                vec![
                    vec![SimulatedOperation::Chmod(file, 0o600)],
                    vec![SimulatedOperation::Unlink(file)],
                ],
                vec![vec![SimulatedOperation::Rename(file, ("e", "c"))]],
            ],
            faults: Vec::new(),
            slow: false,
        };
        for seed in 0..256 {
            let on_seeded = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            let on_pct = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            on_seeded.expect("no operation stalls and the history is linearizable");
            on_pct.expect("no operation stalls and the history is linearizable");
        }
    }

    // On Linux an item's purge takes no directory's lock. Mount 0's unlink holds the directory as it waits on mount 1,
    // whose purge of it waits for an unlink there, which waits for the lock of a file a write holds while it waits for
    // mount 0 to purge the file, which then waits for nothing.
    #[test]
    fn simulated_linux_fuse_strong_unlinks_beside_writes_in_one_directory_on_two_mounts_do_not_stall()
     {
        let activity = SimulatedActivity {
            mounts: vec![
                vec![vec![SimulatedOperation::Unlink(("d", "a"))]],
                vec![
                    vec![SimulatedOperation::Write(("d", "b"), b"x")],
                    vec![SimulatedOperation::Unlink(("d", "b"))],
                ],
                vec![vec![SimulatedOperation::Lookup(("d", "a")); 2]],
            ],
            faults: Vec::new(),
            slow: false,
        };
        for seed in 0..256 {
            let on_seeded = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            let on_pct = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            on_seeded.expect("no operation stalls and the history is linearizable");
            on_pct.expect("no operation stalls and the history is linearizable");
        }
    }

    // On Linux a purge drops nothing of an item the kernel does not hold yet, and the kernel takes a create's or a
    // link's attributes in whatever purge came since, so it keeps the attributes only a lookup or a listing hands over
    // of an item it asked about, which it holds.
    #[test]
    fn simulated_linux_fuse_strong_keeps_only_attributes_a_purge_drops() {
        let (file, other) = (("d", "a"), ("e", "c"));
        let activities = [
            vec![
                vec![vec![SimulatedOperation::Chmod(file, 0o600)]],
                vec![vec![SimulatedOperation::Stat(file); 3]],
            ],
            vec![
                vec![vec![SimulatedOperation::Chmod(file, 0o600)]],
                vec![vec![
                    SimulatedOperation::List("d"),
                    SimulatedOperation::Stat(file),
                    SimulatedOperation::Stat(file),
                ]],
            ],
            vec![
                vec![vec![
                    SimulatedOperation::Chmod(file, 0o600),
                    SimulatedOperation::Chmod(file, 0o644),
                ]],
                vec![vec![
                    SimulatedOperation::Link(file, other),
                    SimulatedOperation::Stat(other),
                    SimulatedOperation::Stat(other),
                ]],
            ],
            vec![
                vec![vec![
                    SimulatedOperation::Create(other),
                    SimulatedOperation::Stat(other),
                    SimulatedOperation::Stat(other),
                ]],
                vec![vec![
                    SimulatedOperation::Chmod(other, 0o600),
                    SimulatedOperation::Chmod(other, 0o644),
                ]],
            ],
        ];
        for mounts in activities {
            let activity = SimulatedActivity {
                mounts,
                faults: Vec::new(),
                slow: false,
            };
            for seed in 0..512 {
                let on_seeded = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                    &seeded(),
                    Ending::Linearizable,
                    activity.clone(),
                    seed,
                );
                let on_pct = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                    &pct(3, 4),
                    Ending::Linearizable,
                    activity.clone(),
                    seed,
                );
                on_seeded.expect("the history is linearizable");
                on_pct.expect("the history is linearizable");
            }
        }
    }

    // On Linux a rename renames nothing when its two lookups find one file under both names. A lookup on one mount can
    // find the file under the source as another mount renames it onto the target, and the next under the target, so a
    // lookup refuses to hand over a file an uncached lookup found older since.
    #[test]
    fn simulated_linux_fuse_strong_a_rename_finds_its_two_names_at_one_moment() {
        let (from, to) = (("d", "b"), ("e", "a"));
        let activity = SimulatedActivity {
            mounts: vec![
                vec![vec![
                    SimulatedOperation::Lookup(to),
                    SimulatedOperation::Rename(from, to),
                ]],
                vec![vec![SimulatedOperation::Rename(from, to)]],
            ],
            faults: Vec::new(),
            slow: false,
        };
        for seed in 0..512 {
            let on_seeded = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            let on_pct = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            on_seeded.expect("the history is linearizable");
            on_pct.expect("the history is linearizable");
        }
    }

    // On Linux a lookup, listing or stat older than a delivery being handled goes uncached rather than have the
    // delivery purge again, so steady reads of a file on one mount let each write of it on another purge it there once.
    #[test]
    fn simulated_linux_fuse_strong_a_write_beside_steady_reads_on_another_mount_purges_once() {
        let file = ("d", "b");
        let writes = [
            SimulatedOperation::Write(file, b"x"),
            SimulatedOperation::Write(file, b"yy"),
            SimulatedOperation::Write(file, b"zzz"),
        ];
        let reads = [
            SimulatedOperation::Lookup(file),
            SimulatedOperation::List("d"),
            SimulatedOperation::Stat(file),
        ];
        let activity = SimulatedActivity {
            mounts: vec![vec![writes.to_vec()], vec![reads.repeat(8); 2]],
            faults: Vec::new(),
            slow: false,
        };
        let planned = vec![Vec::new(); activity.mounts.len()];
        for seed in 0..256 {
            let ran = run_once::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &seeded(),
                Ending::Linearizable,
                &activity,
                &planned,
                seed,
            );
            let (counted, _) = ran.expect("no operation stalls and the history is linearizable");
            let purges = counted[1].invalidations;
            assert!(
                purges <= writes.len(),
                "seed {seed}: mount 1 purged {purges} times for {} writes",
                writes.len()
            );
        }
    }

    // On Linux a listing shows an app no attributes, as getdents does. A listing's reply may come long after its read,
    // and hands the kernel the newest attributes handed out for each entry, so its own for some and newer for others.
    #[test]
    fn simulated_linux_fuse_strong_a_listing_shows_no_attributes() {
        let (file, other) = (("d", "a"), ("d", "b"));
        let activity = SimulatedActivity {
            mounts: vec![
                vec![
                    vec![SimulatedOperation::List("d"); 2],
                    vec![SimulatedOperation::Stat(file); 3],
                ],
                vec![
                    vec![
                        SimulatedOperation::Touch(other, 1),
                        SimulatedOperation::Touch(other, 2),
                    ],
                    vec![
                        SimulatedOperation::Touch(file, 1),
                        SimulatedOperation::Touch(file, 2),
                    ],
                ],
            ],
            faults: Vec::new(),
            slow: false,
        };
        for seed in 0..1024 {
            let simulated = simulate::<_, Strong, simulated_linux_fuse_kernel::Kernel>(
                &pct(3, 4),
                Ending::Linearizable,
                activity.clone(),
                seed,
            );
            simulated.expect("the history is linearizable");
        }
    }
}
