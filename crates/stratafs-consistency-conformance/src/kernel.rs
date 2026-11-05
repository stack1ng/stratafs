// A model of one mount's kernel and FSKit's reply path, as probed on macOS 27.2. A file's stat waits for a times
// change, an extending write and an unlink or rename of any name of it, a read or write for a write, its purge for a
// stat, read, write or times change running when asked, later ones for the purge. In a directory, lookups run side by
// side, a change waits for lookups and changes, a lookup or listing for a change. Replies take effect in order.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use event_listener::Event;
use futures_lite::future::yield_now;
use futures_util::future::BoxFuture;
use stratafs_fs_engine::Inode;
use stratafs_fs_engine::attributes::Attributes;
use stratafs_fs_engine::observation::{Answered, Asked, KernelCache, Uninvalidated};

use crate::faults::Faults;

pub(crate) struct Kernel {
    state: Mutex<State>,
    changed: Event,
    faults: Arc<Faults>,
}

#[derive(Default)]
struct State {
    // newest is the newest attributes handed out for each item, which every later reply hands out instead of older
    // ones, as the FSKit volume's Items does.
    newest: HashMap<Inode, Attributes>,
    // cached is the attributes the kernel caches for each item, and names what each name it caches in a directory
    // refers to, None for a name known to be free.
    cached: HashMap<Inode, Attributes>,
    names: HashMap<(Inode, &'static str), Option<Inode>>,
    // content is the data the kernel caches for each file as a read's reply hands it over, with a write's data put over
    // it as the write begins, until a purge or the write's reply: a write that fails leaves it. written is the number
    // of the last write begun on each file, so a read begun before it caches nothing.
    content: HashMap<Inode, Bytes>,
    written: HashMap<Inode, u64>,
    // operations is each operation running on an item that holds something back, by its number, with what it holds.
    operations: HashMap<Inode, BTreeMap<u64, Holding>>,
    // asking is each purge of an item that asked and has not purged, oldest first, with the operations it waits for,
    // and deferred each update of a reply about an item that takes effect after its next purge.
    asking: HashMap<Inode, VecDeque<(u64, BTreeSet<u64>)>>,
    deferred: HashMap<Inode, Vec<(u64, Update)>>,
    // references counts the operations referring to each item, which the kernel does not reclaim meanwhile.
    references: HashMap<Inode, usize>,
    // lookups counts the lookups running in each directory, and changing is each directory a change runs in.
    lookups: HashMap<Inode, usize>,
    changing: HashSet<Inode>,
    numbered: u64,
}

// Replied is what a reply handed over, by item, and the reply, which its operation waits for to take effect.
pub(crate) struct Replied {
    pub(crate) handed: HashMap<Inode, Attributes>,
    pub(crate) reply: u64,
}

// Update is what a reply has the kernel cache about an item: its attributes, what a name in it refers to, or its data.
#[derive(Clone)]
pub(crate) enum Update {
    Attributes(Attributes),
    Name(&'static str, Option<Inode>),
    Content(Bytes),
}

// Access is what an operation does to a file: stat it, read it, write data at its start, change its times, or unlink
// or rename one of its names.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Access {
    Stat,
    Read,
    Write(&'static [u8]),
    Times,
    Unlink,
}

// Holding is what an operation running on a file holds back: reads and writes, as a write does; stats, as a write that
// extends the file, a change of times and an unlink do; and purges, as all but an unlink do.
#[derive(Clone, Copy)]
struct Holding {
    reads: bool,
    stats: bool,
    purges: bool,
}

// Operation is an operation running on a file, until it is dropped.
pub(crate) struct Operation<'a> {
    kernel: &'a Kernel,
    inode: Inode,
    number: u64,
}

// Work is what an operation does in directories: look a name up in one, change entries of one or two, or list one.
pub(crate) enum Work {
    Lookup(Inode),
    Change(Vec<Inode>),
    Listing(Inode),
}

// InDirectory is an operation working in directories, until it is dropped.
pub(crate) struct InDirectory<'a> {
    kernel: &'a Kernel,
    work: Work,
}

// Reference marks an operation referring to an item, until it is dropped.
pub(crate) struct Reference {
    kernel: Arc<Kernel>,
    inode: Inode,
}

// KernelHandle is the kernel as the engine reaches it.
pub(crate) struct KernelHandle(pub(crate) Arc<Kernel>);

impl KernelCache for KernelHandle {
    fn invalidate(&self, inode: Inode) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        let kernel = Arc::clone(&self.0);
        Box::pin(async move { kernel.invalidate(inode, None).await })
    }

    fn invalidate_noting(
        &self,
        inode: Inode,
        asked: Asked,
        answered: Answered,
    ) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        let kernel = Arc::clone(&self.0);
        Box::pin(async move { kernel.invalidate(inode, Some((asked, answered))).await })
    }
}

impl Kernel {
    pub(crate) fn new(faults: &Arc<Faults>) -> Self {
        Kernel {
            state: Mutex::default(),
            changed: Event::new(),
            faults: Arc::clone(faults),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // cached is the attributes the kernel caches for inode, once nothing holds stats of it back.
    pub(crate) async fn cached(&self, inode: Inode) -> Option<Attributes> {
        let mut cached = None;
        self.until(|state| {
            cached = state.cached.get(&inode).cloned();
            !state.holds(inode, |held| held.stats)
        })
        .await;
        cached
    }

    // bound is what the kernel caches name in directory as referring to once no change runs there: a file, or None for
    // a free name. It is None when the kernel has to ask the volume.
    pub(crate) async fn bound(
        &self,
        directory: Inode,
        name: &'static str,
    ) -> Option<Option<Inode>> {
        let mut bound = None;
        self.until(|state| {
            bound = state.names.get(&(directory, name)).copied();
            !state.changing.contains(&directory)
        })
        .await;
        bound
    }

    // cached_content is the data the kernel caches for inode, a file, with its attributes, once no write of it runs
    // and no purge of it is asking, as a read the kernel answers itself waits for both.
    pub(crate) async fn cached_content(&self, inode: Inode) -> Option<(Bytes, Attributes)> {
        let mut cached = None;
        self.until(|state| {
            cached = state
                .content
                .get(&inode)
                .cloned()
                .zip(state.cached.get(&inode).cloned());
            !state.holds(inode, |held| held.reads) && !state.asking.contains_key(&inode)
        })
        .await;
        cached
    }

    // begin marks an operation running on inode, a file, until the Operation it returns is dropped, once nothing holds
    // the operation back, and but for an unlink, no purge of inode has asked and not purged.
    pub(crate) async fn begin(&self, inode: Inode, access: Access) -> Operation<'_> {
        let mut number = 0;
        self.until(|state| {
            let held = match access {
                Access::Stat => state.holds(inode, |held| held.stats),
                Access::Read | Access::Write(_) => state.holds(inode, |held| held.reads),
                Access::Times | Access::Unlink => false,
            };
            let purging = access != Access::Unlink && state.asking.contains_key(&inode);
            if purging || held {
                return false;
            }
            let size = state
                .newest
                .get(&inode)
                .map_or(0, |newest| newest.settable.size);
            let holding = Holding {
                reads: matches!(access, Access::Write(_)),
                stats: match access {
                    Access::Stat | Access::Read => false,
                    Access::Write(data) => data.len() as u64 > size,
                    Access::Times | Access::Unlink => true,
                },
                purges: access != Access::Unlink,
            };
            state.numbered += 1;
            number = state.numbered;
            if let Access::Write(data) = access {
                // The model pages nothing in, so data goes only over content the kernel caches.
                if let Some(cached) = state.content.get(&inode) {
                    let content = [data, cached.get(data.len()..).unwrap_or_default()].concat();
                    state.content.insert(inode, content.into());
                }
                state.written.insert(inode, number);
            }
            state
                .operations
                .entry(inode)
                .or_default()
                .insert(number, holding);
            true
        })
        .await;
        Operation {
            kernel: self,
            inode,
            number,
        }
    }

    // enter begins work once the work running in its directories lets it.
    pub(crate) async fn enter(&self, work: Work) -> InDirectory<'_> {
        self.until(|state| match &work {
            Work::Lookup(directory) | Work::Listing(directory)
                if state.changing.contains(directory) =>
            {
                false
            }
            Work::Lookup(directory) => {
                *state.lookups.entry(*directory).or_default() += 1;
                true
            }
            Work::Listing(_) => true,
            Work::Change(directories) => {
                let free = directories.iter().all(|directory| {
                    !state.changing.contains(directory) && !state.lookups.contains_key(directory)
                });
                if free {
                    state.changing.extend(directories);
                }
                free
            }
        })
        .await;
        InDirectory { kernel: self, work }
    }

    // reply asks vouch while every item of reads is locked, as the FSKit volume's Items does, and if it vouches hands
    // over each read, or a newer one handed out before, with updates of items. It is what it handed over, or None when
    // vouch did not vouch. Data goes only with the operation's own file, if no write of it began since.
    pub(crate) fn reply(
        &self,
        reads: &[(Inode, Attributes)],
        updates: &[(Inode, Update)],
        operation: Option<&Operation<'_>>,
        vouch: &dyn Fn() -> bool,
    ) -> Option<Replied> {
        let mut state = self.state();
        if !vouch() {
            return None;
        }
        state.numbered += 1;
        let reply = state.numbered;
        for (inode, read) in reads {
            let newest = state.newest.entry(*inode).or_insert_with(|| read.clone());
            if read.change_version.number > newest.change_version.number {
                *newest = read.clone();
            }
        }
        let handed: HashMap<Inode, Attributes> = reads
            .iter()
            .map(|(inode, _)| (*inode, state.newest[inode].clone()))
            .collect();
        for (inode, attributes) in &handed {
            let waited = operation.filter(|operation| operation.inode == *inode);
            // The volume took the write the reply is for, so the kernel drops its data and the next read asks for it.
            if waited.is_some_and(|operation| state.written.get(inode) == Some(&operation.number)) {
                state.content.remove(inode);
            }
            let unwritten = waited.is_some_and(|operation| {
                state
                    .written
                    .get(inode)
                    .is_none_or(|written| *written < operation.number)
            });
            let applied = [Update::Attributes(attributes.clone())]
                .into_iter()
                .chain(
                    updates
                        .iter()
                        .filter(|(item, update)| {
                            item == inode && (unwritten || !matches!(update, Update::Content(_)))
                        })
                        .map(|(_, update)| update.clone()),
                )
                .collect();
            state.apply(*inode, reply, applied, waited.is_some());
        }
        Some(Replied { handed, reply })
    }

    // reply_listed asks vouch while every item of reads is locked, and if it vouches hands over each read, or a newer
    // one handed out before, telling cached of each item the kernel holds, which is all the kernel keeps of a listing.
    pub(crate) fn reply_listed(
        &self,
        reads: &[(Inode, Attributes)],
        vouch: &dyn Fn() -> bool,
        cached: impl Fn(Inode, &Attributes),
    ) -> Option<(Vec<Attributes>, u64)> {
        let mut state = self.state();
        if !vouch() {
            return None;
        }
        state.numbered += 1;
        let reply = state.numbered;
        let handed = reads
            .iter()
            .map(|(inode, read)| {
                let Some(newest) = state.newest.get_mut(inode) else {
                    return read.clone();
                };
                if read.change_version.number > newest.change_version.number {
                    *newest = read.clone();
                }
                let newest = newest.clone();
                cached(*inode, &newest);
                state.apply(
                    *inode,
                    reply,
                    vec![Update::Attributes(newest.clone())],
                    false,
                );
                newest
            })
            .collect();
        Some((handed, reply))
    }

    // took_effect resolves once every update of reply took effect.
    pub(crate) async fn took_effect(&self, reply: u64) {
        self.until(|state| {
            state
                .deferred
                .values()
                .flatten()
                .all(|(deferred, _)| *deferred != reply)
        })
        .await;
    }

    pub(crate) fn refer(self: &Arc<Self>, inode: Inode) -> Reference {
        *self.state().references.entry(inode).or_default() += 1;
        Reference {
            kernel: Arc::clone(self),
            inode,
        }
    }

    // reclaim lets go of inode unless an operation refers to it, dropping what the kernel caches for it and every name
    // in it or of it, and running forget under the item's lock, as the FSKit volume's reclaim does, and says whether
    // it did. XNU purges a reclaimed vnode's name cache entries.
    pub(crate) fn reclaim(&self, inode: Inode, forget: impl FnOnce()) -> bool {
        let mut state = self.state();
        if state.references.contains_key(&inode) {
            return false;
        }
        forget();
        state.newest.remove(&inode);
        state.cached.remove(&inode);
        state.content.remove(&inode);
        state.deferred.remove(&inode);
        state
            .names
            .retain(|(directory, _), bound| *directory != inode && *bound != Some(inode));
        true
    }

    // invalidate drops the kernel's copy of inode once the operations on it at the ask have ended and the purges asked
    // before have purged, or fails, purging nothing, when the kernel refuses. FSKit invalidates neither the root nor
    // an item it was never handed.
    async fn invalidate(
        &self,
        inode: Inode,
        noting: Option<(Asked, Answered)>,
    ) -> Result<(), Uninvalidated> {
        // FSKit invalidates on a thread of its own, so the caller goes on first.
        yield_now().await;
        let (asked, answered) = noting.unzip();
        let handed = inode != Inode::ROOT && self.state().newest.contains_key(&inode);
        let refused = handed && self.faults.refuses();
        let number = (handed && !refused).then(|| {
            let mut state = self.state();
            state.numbered += 1;
            let waits = state.operations.get(&inode).into_iter().flatten();
            let waits = waits
                .filter(|(_, held)| held.purges)
                .map(|(number, _)| *number);
            let (number, waits) = (state.numbered, waits.collect());
            state
                .asking
                .entry(inode)
                .or_default()
                .push_back((number, waits));
            number
        });
        if let Some(asked) = asked {
            asked();
        }
        if let Some(number) = number {
            self.until(|state| {
                let asking = &state.asking[&inode];
                let (first, waits) = asking
                    .front()
                    .expect("a purge asking is kept until it purged");
                *first == number
                    && state
                        .operations
                        .get(&inode)
                        .is_none_or(|running| running.keys().all(|number| !waits.contains(number)))
            })
            .await;
            self.state().purge(inode);
            self.changed.notify(usize::MAX);
            // The kernel answers once it purged, after a while.
            yield_now().await;
        }
        if let Some(answered) = answered {
            answered(!refused);
        }
        match refused {
            true => Err(Uninvalidated("the kernel refused".to_owned())),
            false => Ok(()),
        }
    }

    // until resolves once done holds of the state, which it may change when it does.
    async fn until(&self, mut done: impl FnMut(&mut State) -> bool) {
        loop {
            let listener = self.changed.listen();
            if done(&mut self.state()) {
                return;
            }
            listener.await;
        }
    }
}

impl State {
    // holds says whether an operation running on inode holds back what held picks.
    fn holds(&self, inode: Inode, held: impl Fn(&Holding) -> bool) -> bool {
        self.operations
            .get(&inode)
            .is_some_and(|running| running.values().any(held))
    }

    // apply has the kernel cache updates about inode, after the next purge of it when one is asking and the reply is
    // not one the purge waits for, and after any updates deferred until then.
    fn apply(&mut self, inode: Inode, reply: u64, updates: Vec<Update>, waited: bool) {
        let updates = updates.into_iter().map(|update| (reply, update));
        if self.asking.contains_key(&inode) && !waited {
            self.deferred.entry(inode).or_default().extend(updates);
            return;
        }
        let deferred = self.deferred.remove(&inode).unwrap_or_default();
        for (_, update) in deferred.into_iter().chain(updates) {
            match update {
                Update::Attributes(attributes) => {
                    self.cached.insert(inode, attributes);
                }
                Update::Name(name, bound) => {
                    self.names.insert((inode, name), bound);
                }
                Update::Content(content) => {
                    self.content.insert(inode, content);
                }
            }
        }
    }

    // purge drops what the kernel caches for inode for its oldest purge asking, and applies the updates deferred until
    // then.
    fn purge(&mut self, inode: Inode) {
        self.cached.remove(&inode);
        self.content.remove(&inode);
        self.names.retain(|(directory, _), _| *directory != inode);
        let asking = self
            .asking
            .get_mut(&inode)
            .expect("a purge asking is kept until it purged");
        asking.pop_front();
        if asking.is_empty() {
            self.asking.remove(&inode);
        }
        self.apply(inode, 0, Vec::new(), true);
    }
}

impl Drop for Operation<'_> {
    fn drop(&mut self) {
        {
            let mut state = self.kernel.state();
            let operations = state
                .operations
                .get_mut(&self.inode)
                .expect("an operation is kept until it ends");
            operations.remove(&self.number);
            if operations.is_empty() {
                state.operations.remove(&self.inode);
            }
        }
        self.kernel.changed.notify(usize::MAX);
    }
}

impl Drop for Reference {
    fn drop(&mut self) {
        let mut state = self.kernel.state();
        let references = state
            .references
            .get_mut(&self.inode)
            .expect("a reference is counted until it is dropped");
        *references -= 1;
        if *references == 0 {
            state.references.remove(&self.inode);
        }
    }
}

impl Drop for InDirectory<'_> {
    fn drop(&mut self) {
        {
            let mut state = self.kernel.state();
            match &self.work {
                Work::Lookup(directory) => {
                    let lookups = state
                        .lookups
                        .get_mut(directory)
                        .expect("a lookup is counted until it ends");
                    *lookups -= 1;
                    if *lookups == 0 {
                        state.lookups.remove(directory);
                    }
                }
                Work::Change(directories) => {
                    for directory in directories {
                        state.changing.remove(directory);
                    }
                }
                Work::Listing(_) => {}
            }
        }
        self.kernel.changed.notify(usize::MAX);
    }
}
