use std::ops::RangeInclusive;

use proptest::collection::vec;
use proptest::prelude::*;
use proptest::sample::select;

// Path is a file's name in one of the volume's DIRECTORIES, each with the NAMES the activity gives files, where files
// at MADE exist when it begins.
pub type Path = (&'static str, &'static str);
// UPSTREAM KERNEL BUG PIN: the activity stays out of the volume root, as FSKit cannot purge the root (FB25243414).
pub(crate) static DIRECTORIES: [&str; 2] = ["d", "e"];
pub(crate) static NAMES: [&str; 3] = ["a", "b", "c"];
pub(crate) static MADE: [Path; 2] = [("d", "a"), ("d", "b")];
static CONTENTS: [&[u8]; 3] = [b"x", b"yy", b"zzz"];
static TIMES: [i64; 3] = [1, 2, 3];
static MODES: [u16; 2] = [0o600, 0o644];

#[derive(Clone, Debug)]
pub struct SimulatedActivity {
    // mounts is the apps of each mount, each with the operations it asks one after another.
    pub mounts: Vec<Vec<Vec<SimulatedOperation>>>,
    pub faults: Vec<SimulatedFault>,
    // slow says whether the clock also moves now and then while tasks could run.
    pub slow: bool,
}

// SimulatedOperation is what an app asks of its mount. Each but Lookup, List and Create first looks up the paths it
// works on, as the kernel does. Reclaim is the kernel letting go of a file, as it also does once a file is unlinked.
#[derive(Clone, Copy, Debug)]
pub enum SimulatedOperation {
    Lookup(Path),
    List(&'static str),
    Stat(Path),
    Read(Path),
    Write(Path, &'static [u8]),
    Touch(Path, i64),
    Chmod(Path, u16),
    Create(Path),
    Unlink(Path),
    Rename(Path, Path),
    Link(Path, Path),
    Reclaim(Path),
}

// SimulatedFault is a fault of one mount, at a fraction of the run counted in its store transactions, in its kernel's
// invalidations for a refusal, or in its connects to the pub/sub for a failed connect.
#[derive(Clone, Copy, Debug)]
pub struct SimulatedFault {
    pub kind: FaultKind,
    pub mount: usize,
    pub at_fraction: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultKind {
    // Cut cuts the mount's pub/sub connection as the transaction begins.
    Cut,
    // StoreFails fails the transaction.
    StoreFails,
    // Refused makes the kernel refuse the invalidation and the next REFUSALS - 1, so a delivery's second try fails too,
    // and so does the purge that follows.
    Refused,
    // ConnectFails fails the connect.
    ConnectFails,
    // Dies stops the mount for good once the transaction ends: every later transaction fails, its pub/sub connection
    // is cut, and its apps ask nothing more.
    Dies,
}

pub(crate) const REFUSALS: usize = 3;

// Slow runs never time a publish out early, and no kind does: that fails its write and cuts holders, as Cut does.
pub(crate) const STRONG_FAULTS: &[FaultKind] = &[
    FaultKind::Cut,
    FaultKind::StoreFails,
    FaultKind::Refused,
    FaultKind::ConnectFails,
    FaultKind::Dies,
];
// An eventual mount whose kernel refuses an invalidation keeps its copy until the kernel's own cache expires, which the
// model kernel's never does.
pub(crate) const EVENTUAL_FAULTS: &[FaultKind] = &[
    FaultKind::Cut,
    FaultKind::StoreFails,
    FaultKind::ConnectFails,
    FaultKind::Dies,
];

// simulated_activity is the activity of as many mounts as mounts says, each with one or two apps, and faults of kinds.
pub(crate) fn simulated_activity(
    kinds: &'static [FaultKind],
    mounts: RangeInclusive<usize>,
) -> impl Strategy<Value = SimulatedActivity> {
    (
        vec(vec(vec(operation(), 1..=4), 1..=2), mounts),
        prop_oneof![
            2 => Just(Vec::new()),
            1 => vec(fault(kinds), 1..=2),
        ],
        any::<bool>(),
    )
        .prop_map(|(mounts, faults, slow)| SimulatedActivity {
            mounts,
            faults,
            slow,
        })
}

fn fault(kinds: &'static [FaultKind]) -> impl Strategy<Value = SimulatedFault> {
    (select(kinds), 0..3usize, 0.0..1.0).prop_map(|(kind, mount, at_fraction)| SimulatedFault {
        kind,
        mount,
        at_fraction,
    })
}

fn operation() -> impl Strategy<Value = SimulatedOperation> {
    prop_oneof![
        2 => path().prop_map(SimulatedOperation::Lookup),
        1 => select(DIRECTORIES.as_slice()).prop_map(SimulatedOperation::List),
        3 => path().prop_map(SimulatedOperation::Stat),
        2 => path().prop_map(SimulatedOperation::Read),
        3 => (path(), select(CONTENTS.as_slice()))
            .prop_map(|(path, content)| SimulatedOperation::Write(path, content)),
        1 => (path(), select(TIMES.as_slice()))
            .prop_map(|(path, at)| SimulatedOperation::Touch(path, at)),
        1 => (path(), select(MODES.as_slice()))
            .prop_map(|(path, mode)| SimulatedOperation::Chmod(path, mode)),
        3 => path().prop_map(SimulatedOperation::Create),
        1 => path().prop_map(SimulatedOperation::Unlink),
        1 => (path(), path())
            .prop_filter("two paths", |(from, to)| from != to)
            .prop_map(|(from, to)| SimulatedOperation::Rename(from, to)),
        1 => (path(), path())
            .prop_filter("two paths", |(from, to)| from != to)
            .prop_map(|(existing, new)| SimulatedOperation::Link(existing, new)),
        1 => path().prop_map(SimulatedOperation::Reclaim),
    ]
}

fn path() -> impl Strategy<Value = Path> {
    (select(DIRECTORIES.as_slice()), select(NAMES.as_slice()))
}
