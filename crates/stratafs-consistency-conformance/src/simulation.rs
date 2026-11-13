use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use bytes::Bytes;
use proptest::test_runner::TestCaseError;
use stratafs_fs_engine::attributes::Attributes;
use stratafs_fs_engine::directories::NewItem;
use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::{Inode, MountId};
use stratafs_runtime::replay::{Replay, ReplayingRuntime, spawn_with_handle};

use crate::activity::{
    DIRECTORIES, FaultKind, MADE, NAMES, Path, SimulatedActivity, SimulatedOperation,
};
use crate::faults::Counted;
use crate::model::{HistoryOperation, Namespace, Outcome, Seen};
use crate::mount::{Consistency, Failure, KernelModel, Mount, Operations, Reference, Volume};

// SETTLED is how long mounts are given to settle once the activity is done, longer than a publish's timeout and the
// engine's settle margin after it. The clock moves only once every task is blocked, so nothing is left to do after.
const SETTLED: Duration = Duration::from_secs(3);

// LONGEST is the most virtual time an operation of a strict run may take. Its clock moves only once every task is
// blocked, so one that took longer waited on a timer meant for faults, such as a publish's timeout.
const LONGEST: Duration = Duration::from_millis(100);

// OpId names one operation of one app.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OpId {
    app: (usize, usize),
    operation: usize,
}

pub(crate) enum Event {
    Invoked(OpId, HistoryOperation),
    Returned(OpId, Outcome),
    // Unknown is an operation that failed and may have committed, and Unchanged one that failed committing nothing.
    Unknown(OpId, PosixError),
    Unchanged(OpId, PosixError),
    // Stalled is an operation that took longer than LONGEST.
    Stalled(OpId, Duration),
    // UPSTREAM KERNEL BUG PIN: Tainted is a read the kernel answered with what an upstream bug excuses, which the check
    // leaves out and counts.
    Tainted(OpId),
    // Cut and Connected are when a mount's pub/sub connection was cut, or its kernel refused an invalidation, which it
    // drops its connection for, and when it next connected, and Died when it stopped for good.
    Cut(usize),
    Connected(usize),
    Died(usize),
}

#[derive(Clone, Default)]
pub(crate) struct History(Arc<Mutex<Vec<Event>>>);

impl History {
    pub(crate) fn push(&self, event: Event) -> usize {
        let mut events = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        events.push(event);
        events.len() - 1
    }

    fn set(&self, at: usize, event: Event) {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)[at] = event;
    }

    fn take(&self) -> Vec<Event> {
        std::mem::take(&mut self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

// Ending is whether a run must show a linearizable history of its strong mounts once its activity is done. Every
// eventual mount must serve the final version once the mounts settle either way.
#[derive(Clone, Copy)]
pub(crate) enum Ending {
    Linearizable,
    Converges,
}

// Planned is each mount's faults, each at its count.
type Planned = Vec<Vec<(FaultKind, usize)>>;

// simulate runs the activity, first without faults to count where to place them when it has any, and is how many
// tainted reads the checks left out.
pub(crate) fn simulate<R: ReplayingRuntime, C: Consistency, K: KernelModel>(
    runtime: &R,
    ending: Ending,
    activity: SimulatedActivity,
    seed: u64,
) -> Result<usize, TestCaseError>
where
    Mount<C, K>: Operations,
{
    let mut planned = vec![Vec::new(); activity.mounts.len()];
    // UPSTREAM KERNEL BUG PIN: the tainted reads left out in both runs.
    let mut tainted = 0;
    if !activity.faults.is_empty() {
        let (counted, left_out) = run_once::<R, C, K>(runtime, ending, &activity, &planned, seed)?;
        tainted += left_out;
        for fault in &activity.faults {
            let mount = fault.mount % activity.mounts.len();
            // An eventual mount whose kernel refuses an invalidation keeps its copy, which the model kernel never
            // drops, so only a strong mount's kernel refuses.
            if fault.kind == FaultKind::Refused && C::eventual(mount) {
                continue;
            }
            let count = counted[mount].of(fault.kind);
            planned[mount].push((
                fault.kind,
                (fault.at_fraction * (count + 1) as f64) as usize,
            ));
        }
    }
    let (_, left_out) = run_once::<R, C, K>(runtime, ending, &activity, &planned, seed)?;
    Ok(tainted + left_out)
}

// run_once runs the activity with planned faults and checks each execution's ending, and returns what each mount
// counted in the last, and how many tainted reads the checks left out in all.
// UPSTREAM KERNEL BUG PIN: the tainted count.
pub(crate) fn run_once<R: ReplayingRuntime, C: Consistency, K: KernelModel>(
    runtime: &R,
    ending: Ending,
    activity: &SimulatedActivity,
    planned: &Planned,
    seed: u64,
) -> Result<(Vec<Counted>, usize), TestCaseError>
where
    Mount<C, K>: Operations,
{
    let result = Arc::new(Mutex::new(None));
    let run_result = Arc::clone(&result);
    let (activity, planned) = (activity.clone(), planned.clone());
    runtime.run(seed, move |replay| {
        let (activity, planned, run_result) =
            (activity.clone(), planned.clone(), Arc::clone(&run_result));
        async move {
            let strict = activity.faults.is_empty() && !activity.slow;
            let ran = run::<_, C, K>(&replay, ending, activity, planned).await;
            let checked = ran.and_then(|(made, history, counted)| {
                if strict {
                    check_strict(&history)?;
                }
                let tainted = match ending {
                    Ending::Linearizable => check_linearizable::<C, K>(made, history)?,
                    Ending::Converges => 0,
                };
                Ok((counted, tainted))
            });
            let mut result = run_result.lock().unwrap_or_else(PoisonError::into_inner);
            let before = match &*result {
                Some(Err(_)) => return,
                Some(Ok((_, tainted))) => *tainted,
                None => 0,
            };
            *result = Some(checked.map(|(counted, tainted)| (counted, before + tainted)));
        }
    });
    let ran = result.lock().unwrap_or_else(PoisonError::into_inner).take();
    ran.expect("the simulation ran")
}

async fn run<P: Replay, C: Consistency, K: KernelModel>(
    replay: &P,
    ending: Ending,
    activity: SimulatedActivity,
    planned: Planned,
) -> Result<(Namespace, Vec<Event>, Vec<Counted>), TestCaseError>
where
    Mount<C, K>: Operations,
{
    let history = History::default();
    let volume = Volume::new(replay.runtime(activity.slow), &history).await;
    let mut mounts = Vec::new();
    for index in 0..activity.mounts.len() {
        mounts.push(Arc::new(volume.mount::<C, K>(index).await));
    }
    let mut directories = Vec::new();
    for name in DIRECTORIES {
        match mounts[0]
            .create(Inode::ROOT, name, NewItem::Directory)
            .await
        {
            (_, Ok((directory, _))) => directories.push(directory),
            (_, Err(_)) => panic!("making the volume's directories failed"),
        }
    }
    // Every mount looks the directories up, as its kernel does to reach them.
    for mount in &mounts {
        for name in DIRECTORIES {
            if mount.resolve(Inode::ROOT, name).await.is_err() {
                panic!("looking up the volume's directories failed");
            }
        }
    }
    let directories: Arc<[Inode]> = directories.into();
    let mut made = Namespace::new(K::FINDS_NAMES, K::LISTS_ATTRIBUTES, K::STATS_BEFORE_COPYING);
    for path in MADE {
        match mounts[0]
            .create(directory(&directories, path.0), path.1, NewItem::File)
            .await
        {
            (_, Ok((inode, _))) => made.made(path, inode.0),
            (_, Err(_)) => panic!("making the volume's files failed"),
        }
    }
    for (mount, planned) in mounts.iter().zip(planned) {
        mount.faults.plan(planned);
    }
    let mut apps = Vec::new();
    for (index, (mount, operations)) in mounts.iter().zip(activity.mounts).enumerate() {
        for (app, operations) in operations.into_iter().enumerate() {
            let mut client = Client::new(mount, &history, (index, app), &directories);
            apps.push(spawn_with_handle(replay, async move {
                for operation in operations {
                    if client.mount.faults.dead() {
                        break;
                    }
                    client.perform(operation).await;
                }
            }));
        }
    }
    for app in apps {
        app.await;
    }
    // Faults stop with the activity, so what each mount counted is the activity's.
    let counted = mounts.iter().map(|mount| mount.faults.counted()).collect();
    for mount in &mounts {
        mount.faults.plan(Vec::new());
    }
    volume.runtime.sleep(SETTLED).await;
    let live: Vec<_> = mounts
        .iter()
        .enumerate()
        .filter(|(_, mount)| !mount.faults.dead())
        .map(|(index, mount)| Client::new(mount, &history, (index, usize::MAX), &directories))
        .collect();
    if let Some(client) = live.first() {
        announcements_settled(client, &mounts, &volume.inodes()).await?;
    }
    let (eventual, mut strong): (Vec<_>, Vec<_>) = live
        .into_iter()
        .partition(|client| C::eventual(client.app.0));
    // The eventual mounts are checked first, as a strong mount's read may settle a write whose writer died.
    converged(&eventual).await?;
    match ending {
        Ending::Linearizable => {
            for client in &mut strong {
                client.read_everything().await;
            }
        }
        Ending::Converges => {}
    }
    Ok((made, history.take(), counted))
}

// announcements_settled fails if a write left an inode's pending change version ahead of its change version, but for
// a write of a mount a store transaction of which failed on purpose, which may have been its settle.
async fn announcements_settled<C: Consistency, K>(
    client: &Client<C, K>,
    mounts: &[Arc<Mount<C, K>>],
    inodes: &[Inode],
) -> Result<(), TestCaseError> {
    let Ok(live) = client.mount.live_pending(inodes).await else {
        return Err(TestCaseError::fail(
            "reading pending change versions failed",
        ));
    };
    let failed = |MountId(id): MountId| mounts[id as usize - 1].faults.failed();
    match live.into_iter().find(|(_, mount)| !failed(*mount)) {
        Some((inode, mount)) => Err(TestCaseError::fail(format!(
            "{inode:?} is still pending a write of {mount:?} once the activity is done"
        ))),
        None => Ok(()),
    }
}

// converged fails unless every live mount serves what the store holds now.
async fn converged<C: Consistency, K: KernelModel>(
    live: &[Client<C, K>],
) -> Result<(), TestCaseError>
where
    Mount<C, K>: Operations,
{
    let Some(first) = live.first() else {
        return Ok(());
    };
    for path in paths() {
        let stored = first.stored(path).await;
        for client in live {
            let (served, stale) = client.served(path).await;
            // UPSTREAM KERNEL BUG PIN: attributes a reply brought in after the kernel let go of the item stay stale, so
            // only the rest is compared.
            let mount = MountId(client.app.0 as u64 + 1);
            let same = if stale {
                without_attributes(&served) == without_attributes(&stored)
            } else {
                served == stored || settled_own_write(&served, &stored, mount)
            };
            if stored.is_none() || !same {
                return Err(TestCaseError::fail(format!(
                    "mount {} serves {path:?} as {served:?}, but the store holds {stored:?}",
                    client.app.0
                )));
            }
        }
    }
    Ok(())
}

// without_attributes is the file served or stored as without its attributes.
fn without_attributes(
    file: &Option<Option<(Inode, Attributes, Bytes)>>,
) -> Option<Option<(Inode, &Bytes)>> {
    file.as_ref()
        .map(|file| file.as_ref().map(|(inode, _, content)| (*inode, content)))
}

// settled_own_write says whether the file served differs from the one stored only in an older change version, where
// the stored one is mount's. A settle of mount's own write moves the change version and nothing else, which mount's auto
// invalidator takes for its own change, so it keeps its copy, through as many settles in a row as mount's writes fail.
fn settled_own_write(
    served: &Option<Option<(Inode, Attributes, Bytes)>>,
    stored: &Option<Option<(Inode, Attributes, Bytes)>>,
    mount: MountId,
) -> bool {
    let (Some(Some((served_inode, served, served_content))), Some(Some((inode, stored, content)))) =
        (served, stored)
    else {
        return false;
    };
    let with_stored_version = |attributes: &Attributes| Attributes {
        change_version: stored.change_version,
        ..attributes.clone()
    };
    served_inode == inode
        && served_content == content
        && served.change_version.number < stored.change_version.number
        && stored.change_version.mount == mount
        && with_stored_version(served) == *stored
}

// directory is the inode of the directory named name, of directories, the inode of each of DIRECTORIES.
fn directory(directories: &[Inode], name: &'static str) -> Inode {
    let index = DIRECTORIES.iter().position(|directory| *directory == name);
    directories[index.expect("only known directories are named")]
}

// paths is every path the activity gives files.
fn paths() -> impl Iterator<Item = Path> {
    DIRECTORIES
        .into_iter()
        .flat_map(|directory| NAMES.map(|name| (directory, name)))
}

// Client is the app on one mount, which asks it one operation at a time, and records each in the history.
struct Client<C: Consistency, K> {
    mount: Arc<Mount<C, K>>,
    history: History,
    // app is the mount and the app on it.
    app: (usize, usize),
    operations: usize,
    // directories is the inode of each of DIRECTORIES.
    directories: Arc<[Inode]>,
    // referred is every file the operation running looked up, which the kernel keeps until it ends.
    referred: Vec<Reference>,
}

impl<C: Consistency, K: KernelModel> Client<C, K>
where
    Mount<C, K>: Operations,
{
    fn new(
        mount: &Arc<Mount<C, K>>,
        history: &History,
        app: (usize, usize),
        directories: &Arc<[Inode]>,
    ) -> Self {
        Client {
            mount: Arc::clone(mount),
            history: History::clone(history),
            app,
            operations: 0,
            directories: Arc::clone(directories),
            referred: Vec::new(),
        }
    }

    async fn perform(&mut self, operation: SimulatedOperation) {
        self.referred.clear();
        match operation {
            SimulatedOperation::Lookup(path) => {
                self.lookup(path).await;
            }
            SimulatedOperation::List(directory) => {
                let invoked = self.invoke(HistoryOperation::List(directory));
                let listed = self.mount.list(self.directory(directory)).await;
                self.end(invoked, listed, |listed| {
                    let mut listed: Vec<_> = listed
                        .iter()
                        .map(|(name, inode, attributes)| {
                            let name = NAMES.iter().find(|known| known.as_bytes() == name);
                            (
                                *name.expect("only known names are given"),
                                inode.0,
                                K::LISTS_ATTRIBUTES.then(|| Seen::from(attributes)),
                            )
                        })
                        .collect();
                    listed.sort_by_key(|(name, ..)| *name);
                    Outcome::Listed(listed)
                });
            }
            SimulatedOperation::Stat(path) => {
                if let Some(Some(inode)) = self.lookup(path).await {
                    self.stat(inode).await;
                }
            }
            SimulatedOperation::Read(path) => {
                if let Some(Some(inode)) = self.lookup(path).await {
                    self.read(inode).await;
                }
            }
            SimulatedOperation::Write(path, data) => {
                if let Some(Some(inode)) = self.lookup(path).await {
                    let invoked = self.invoke(HistoryOperation::Write(inode.0, data));
                    let written = self.mount.write(inode, data).await;
                    self.end(invoked, written, |written| {
                        Outcome::Seen(Seen::from(written))
                    });
                }
            }
            SimulatedOperation::Touch(path, at) => {
                if let Some(Some(inode)) = self.lookup(path).await {
                    let invoked = self.invoke(HistoryOperation::Touch(inode.0, at));
                    let touched = self.mount.touch(inode, at).await;
                    self.end(invoked, touched, |touched| {
                        Outcome::Seen(Seen::from(touched))
                    });
                }
            }
            SimulatedOperation::Chmod(path, mode) => {
                if let Some(Some(inode)) = self.lookup(path).await {
                    let invoked = self.invoke(HistoryOperation::Chmod(inode.0, mode));
                    let changed = self.mount.chmod(inode, mode).await;
                    self.end(invoked, changed, |changed| {
                        Outcome::Seen(Seen::from(changed))
                    });
                }
            }
            SimulatedOperation::Create(path) => {
                let invoked = self.invoke(HistoryOperation::Create(path, 0));
                let directory = self.directory(path.0);
                let (made, created) = self.mount.create(directory, path.1, NewItem::File).await;
                // A create that made no inode created nothing.
                match made {
                    Some(inode) => {
                        let creating = HistoryOperation::Create(path, inode.0);
                        self.history
                            .set(invoked.1, Event::Invoked(invoked.0, creating));
                        self.end(invoked, created, |(_, created)| {
                            Outcome::Seen(Seen::from(created))
                        });
                    }
                    None => {
                        self.end(invoked, created.map_err(Failure::committed_nothing), |_| {
                            unreachable!("a create that made nothing created nothing")
                        });
                    }
                }
            }
            SimulatedOperation::Unlink(path) => {
                if let Some(Some(inode)) = self.lookup(path).await {
                    let invoked = self.invoke(HistoryOperation::Unlink(path, inode.0));
                    let removed = self
                        .mount
                        .unlink(self.directory(path.0), path.1, inode)
                        .await;
                    self.end(invoked, removed, |removed| {
                        Outcome::Seen(Seen::from(removed))
                    });
                    self.reclaim(inode).await;
                }
            }
            SimulatedOperation::Rename(from, to) => {
                let Some(Some(inode)) = self.lookup(from).await else {
                    return;
                };
                // The kernel renames nothing when both names refer to one file.
                let Some(replaced) = self
                    .lookup(to)
                    .await
                    .filter(|replaced| *replaced != Some(inode))
                else {
                    return;
                };
                let invoked = self.invoke(HistoryOperation::Rename(
                    from,
                    to,
                    inode.0,
                    replaced.map(|replaced| replaced.0),
                ));
                let at = |(directory, name): Path| (self.directory(directory), name);
                let renamed = self.mount.rename(at(from), at(to), inode, replaced).await;
                // The volume may replace another file than the one the app found at the destination.
                if let Ok((_, found)) = &renamed
                    && *found != replaced
                {
                    let found = found.map(|found| found.0);
                    let renaming = HistoryOperation::Rename(from, to, inode.0, found);
                    self.history
                        .set(invoked.1, Event::Invoked(invoked.0, renaming));
                }
                self.end(invoked, renamed, |(renamed, _)| {
                    Outcome::Seen(Seen::from(renamed))
                });
            }
            SimulatedOperation::Reclaim(path) => {
                if let Some(Some(inode)) = self.lookup(path).await {
                    self.reclaim(inode).await;
                }
            }
            SimulatedOperation::Link(existing, new) => {
                if let Some(Some(inode)) = self.lookup(existing).await {
                    let invoked = self.invoke(HistoryOperation::Link(inode.0, new));
                    let linked = self.mount.link(inode, self.directory(new.0), new.1).await;
                    self.end(invoked, linked, |linked| Outcome::Seen(Seen::from(linked)));
                }
            }
        }
    }

    // read_everything looks up every path, and reads the file of each it finds.
    async fn read_everything(&mut self) {
        for path in paths() {
            if let Some(Some(inode)) = self.lookup(path).await {
                self.stat(inode).await;
                self.read(inode).await;
            }
        }
    }

    fn directory(&self, name: &'static str) -> Inode {
        directory(&self.directories, name)
    }

    // lookup is the inode path refers to, or None when the lookup failed. As the kernel does, it finds the file, then
    // stats it, so the attributes may be of a later moment than the path.
    async fn lookup(&mut self, path: Path) -> Option<Option<Inode>> {
        let invoked = self.invoke(HistoryOperation::Lookup(path));
        let resolved = self.mount.resolve(self.directory(path.0), path.1).await;
        let inode = match resolved {
            Ok((Some(inode), tainted)) => {
                // UPSTREAM KERNEL BUG PIN: a lookup answered with what an upstream bug excuses.
                if tainted {
                    self.history.push(Event::Tainted(invoked.0));
                }
                inode
            }
            resolved => {
                let resolved = resolved.map(|(found, _)| found);
                return self.end(invoked, resolved, |_| Outcome::Found(None));
            }
        };
        let referred = self.mount.refer(inode);
        // A stat failing with its work's error fails the lookup, which returns that error with the file it found.
        let stat = match self.mount.stat(inode).await {
            Ok((attributes, tainted)) => {
                // UPSTREAM KERNEL BUG PIN: a stat answered with what an upstream bug excuses.
                if tainted {
                    self.history.push(Event::Tainted(invoked.0));
                }
                Ok(Outcome::Seen(Seen::from(&attributes)))
            }
            Err(Failure::Posix(posix)) => Ok(Outcome::Failed(posix)),
            Err(failure) => Err(failure),
        };
        let stat = self.end(invoked, stat, |stat| {
            Outcome::Found(Some((inode.0, Box::new(stat.clone()))))
        })?;
        matches!(stat, Outcome::Seen(_)).then(|| {
            self.referred.push(referred);
            Some(inode)
        })
    }

    async fn stat(&mut self, inode: Inode) {
        let invoked = self.invoke(HistoryOperation::Stat(inode.0));
        let attributes = self.mount.stat(inode).await;
        // UPSTREAM KERNEL BUG PIN: a stat answered with what an upstream bug excuses.
        if let Ok((_, true)) = attributes {
            self.history.push(Event::Tainted(invoked.0));
        }
        self.end(invoked, attributes, |(attributes, _)| {
            Outcome::Seen(Seen::from(attributes))
        });
    }

    // reclaim lets go of inode, once this app is done with it, unless another operation still refers to it.
    async fn reclaim(&mut self, inode: Inode) {
        self.referred.clear();
        if self.mount.let_go(inode) {
            let invoked = self.invoke(HistoryOperation::Reclaim(inode.0));
            let deleted = self.mount.delete_if_unlinked(inode).await;
            self.end(invoked, deleted, |()| Outcome::Done);
        }
    }

    async fn read(&mut self, inode: Inode) {
        let invoked = self.invoke(HistoryOperation::Read(inode.0));
        let read = self.mount.read(inode).await;
        // UPSTREAM KERNEL BUG PIN: a read answered with what an upstream bug excuses.
        if let Ok((.., true)) = read {
            self.history.push(Event::Tainted(invoked.0));
        }
        self.end(invoked, read, |(content, attributes, _)| {
            Outcome::Read(content.to_vec(), Seen::from(attributes))
        });
    }

    // stored is the file path refers to, with its attributes and content, read past the kernel and the consistency,
    // or None when reading it failed.
    async fn stored(&self, path: Path) -> Option<Option<(Inode, Attributes, Bytes)>> {
        let stored = async {
            let directory = self.directory(path.0);
            let Some(inode) = self.mount.stored_lookup(directory, path.1).await? else {
                return Ok(None);
            };
            let (content, attributes) = self.mount.stored_read(inode).await?;
            Ok::<_, Failure>(Some((inode, attributes, content)))
        };
        stored.await.ok()
    }

    // served is the file path refers to, with its attributes and content, as the mount serves them, or None when an
    // operation failed, and whether an upstream bug excuses its attributes.
    async fn served(&self, path: Path) -> (Option<Option<(Inode, Attributes, Bytes)>>, bool) {
        let mut stale = false;
        let served = async {
            let (Some(inode), _) = self.mount.resolve(self.directory(path.0), path.1).await? else {
                return Ok(None);
            };
            let attributes;
            (attributes, stale) = self.mount.stat(inode).await?;
            let (content, ..) = self.mount.read(inode).await?;
            Ok::<_, Failure>(Some((inode, attributes, content)))
        };
        let served = served.await.ok();
        (served, stale)
    }

    // invoke records operation as invoked, and is its id, its place in the history, and when it was invoked.
    fn invoke(&mut self, operation: HistoryOperation) -> (OpId, usize, Instant) {
        let id = OpId {
            app: self.app,
            operation: self.operations,
        };
        self.operations += 1;
        let at = self.history.push(Event::Invoked(id, operation));
        (id, at, self.mount.runtime.now())
    }

    // end records how the operation invoked ended, and is what it returned.
    fn end<T>(
        &mut self,
        (id, _, invoked): (OpId, usize, Instant),
        result: Result<T, Failure>,
        outcome: impl FnOnce(&T) -> Outcome,
    ) -> Option<T> {
        let took = self.mount.runtime.now() - invoked;
        if took > LONGEST {
            self.history.push(Event::Stalled(id, took));
        }
        let event = match &result {
            Ok(returned) => Event::Returned(id, outcome(returned)),
            Err(Failure::Posix(posix)) => Event::Returned(id, Outcome::Failed(*posix)),
            Err(Failure::Unchanged(posix)) => Event::Unchanged(id, *posix),
            Err(Failure::Unknown(posix)) => Event::Unknown(id, *posix),
        };
        self.history.push(event);
        result.ok()
    }
}

impl Failure {
    // committed_nothing is this failure of an operation known to have committed nothing.
    fn committed_nothing(self) -> Failure {
        match self {
            Failure::Unknown(posix) => Failure::Unchanged(posix),
            failure => failure,
        }
    }
}

// check_strict fails a strict run, with no fault and time moving only when idle, in which an operation failed with an
// I/O error or took longer than LONGEST, or a mount connected again.
fn check_strict(history: &[Event]) -> Result<(), TestCaseError> {
    let mut connects = HashMap::<usize, usize>::new();
    for event in history {
        let failure = match event {
            Event::Unknown(id, PosixError::EIO) | Event::Unchanged(id, PosixError::EIO) => {
                format!("operation {id:?} failed with an I/O error")
            }
            Event::Stalled(id, took) => format!("operation {id:?} took {took:?}"),
            Event::Connected(mount) => {
                let connects = connects.entry(*mount).or_default();
                *connects += 1;
                if *connects == 1 {
                    continue;
                }
                format!("mount {mount} connected again")
            }
            _ => continue,
        };
        return Err(TestCaseError::fail(format!(
            "{failure} with no fault and time moving only when idle"
        )));
    }
    Ok(())
}

// check_linearizable fails unless some order of the operations' steps, each between its operation's invocation and end,
// explains what each returned. It leaves out the reads and failed changes of an eventual mount, and what its changes
// returned, the reads of a mount cut off until it connected again, or dead, and what its writes returned, and a write
// that failed in a way that may have committed may or may not take effect.
fn check_linearizable<C: Consistency, K: KernelModel>(
    made: Namespace,
    history: Vec<Event>,
) -> Result<usize, TestCaseError> {
    let mut ends = HashMap::new();
    // UPSTREAM KERNEL BUG PIN: the reads answered with what an upstream bug excuses, left out below.
    let mut tainted = HashSet::new();
    let mut excused: Vec<(usize, usize, usize)> = Vec::new();
    let mut cut = HashMap::new();
    for (at, event) in history.iter().enumerate() {
        match event {
            Event::Returned(id, _) | Event::Unknown(id, _) | Event::Unchanged(id, _) => {
                ends.insert(*id, (at, event));
            }
            Event::Cut(mount) => {
                cut.entry(*mount).or_insert(at);
            }
            Event::Connected(mount) => {
                if let Some(start) = cut.remove(mount) {
                    excused.push((*mount, start, at));
                }
            }
            Event::Died(mount) => excused.push((*mount, at, usize::MAX)),
            Event::Tainted(id) => {
                tainted.insert(*id);
            }
            Event::Invoked(..) | Event::Stalled(..) => {}
        }
    }
    excused.extend(
        cut.into_iter()
            .map(|(mount, start)| (mount, start, usize::MAX)),
    );
    // An app's operations are one thread, in order.
    let mut threads: BTreeMap<(usize, usize), Vec<Step>> = BTreeMap::new();
    for (at, event) in history.iter().enumerate() {
        let Event::Invoked(id, operation) = event else {
            continue;
        };
        if tainted.contains(id) {
            continue;
        }
        let (end, ended) = ends[id];
        // An eventual mount's kernel may answer its reads, fail its changes, and hand the attributes a change returns,
        // from what it caches.
        let eventual = C::eventual(id.app.0);
        if eventual
            && (operation.reads() || matches!(ended, Event::Returned(_, Outcome::Failed(_))))
        {
            continue;
        }
        let not_held = eventual
            || excused
                .iter()
                .any(|(mount, start, stop)| *mount == id.app.0 && at < *stop && end > *start);
        // A failure that committed nothing is left out, and so is a write's outcome the check does not hold it to.
        let (returned, committed) = match ended {
            Event::Returned(_, outcome) if !not_held => (Some(outcome.clone()), true),
            Event::Unchanged(..) | Event::Returned(_, Outcome::Failed(_)) => continue,
            _ if operation.reads() => continue,
            Event::Returned(..) => (None, true),
            _ => (None, false),
        };
        let steps = operation
            .clone()
            .steps(returned, K::FINDS_NAMES, K::STATS_BEFORE_COPYING);
        threads
            .entry(id.app)
            .or_default()
            .extend(steps.into_iter().map(|(operation, returned)| Step {
                operation,
                invoked: at,
                ended: end,
                returned,
                committed,
            }));
    }
    let threads: Vec<Vec<Step>> = threads.into_values().collect();
    match linearizable(made, &threads) {
        true => Ok(tainted.len()),
        false => Err(TestCaseError::fail(format!(
            "no order of the operations explains this history: {threads:#?}"
        ))),
    }
}

// Step is one step of an operation as the check places it: when the operation was invoked and when it ended, what the
// step returned when the check holds it to that, and whether it took effect, which one that may have committed may not.
#[derive(Debug)]
struct Step {
    operation: HistoryOperation,
    invoked: usize,
    ended: usize,
    returned: Option<Outcome>,
    committed: bool,
}

// linearizable says whether some order of every thread's steps, each placed before any step invoked after it ended,
// explains what each returned, starting from model. A step that took effect must succeed in the model, and one that may
// not have may be left out. It explores each state once, as orders of operations that commute reach the same one,
// which a search without memory explores again and again.
fn linearizable(model: Namespace, threads: &[Vec<Step>]) -> bool {
    let mut explored = HashSet::new();
    let mut pending = vec![(vec![0; threads.len()], model)];
    while let Some((placed, model)) = pending.pop() {
        let heads: Vec<(usize, &Step)> = threads
            .iter()
            .zip(&placed)
            .enumerate()
            .filter_map(|(thread, (steps, at))| Some((thread, steps.get(*at)?)))
            .collect();
        let Some(deadline) = heads.iter().map(|(_, step)| step.ended).min() else {
            return true;
        };
        for (thread, step) in heads
            .into_iter()
            .filter(|(_, step)| step.invoked < deadline)
        {
            let mut next = model.clone();
            let valid = match &step.returned {
                Some(returned) => next.invoke(&step.operation) == *returned,
                None => !matches!(next.invoke(&step.operation), Outcome::Failed(_)),
            };
            let left_out = (!step.committed).then(|| model.clone());
            for next in valid.then_some(next).into_iter().chain(left_out) {
                let mut placed = placed.clone();
                placed[thread] += 1;
                if explored.insert((placed.clone(), next.clone())) {
                    pending.push((placed, next));
                }
            }
        }
    }
    false
}
