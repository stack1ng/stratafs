// A model of one mount's Linux kernel through FUSE, as probed on Linux 6.12 with fuser 0.18, and of what stratafs-fuse
// keeps of what it handed the kernel. A purge drops the names in the item under its lock, then its attributes at once
// and its pages once the read or write holding them ends. A reply about an item the kernel holds keeps its attributes
// only if no change to them came since the request began, so the volume caches no others.

mod mount;
mod purges;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use event_listener::Event;
use futures_lite::future::yield_now;
use futures_util::future::BoxFuture;
use stratafs_fs_engine::attributes::Attributes;
use stratafs_fs_engine::kernel_cache::{Answered, Asked, KernelCache, Uninvalidated};
use stratafs_fs_engine::{Inode, ResultHandling};

use crate::faults::Faults;
use crate::mount::{KernelModel, References};

pub(crate) struct Kernel {
    state: Mutex<State>,
    changed: Event,
    faults: Arc<Faults>,
    references: Arc<References>,
}

#[derive(Default)]
struct State {
    // items is what the volume keeps of each inode it handed the kernel, as stratafs-fuse's Items does.
    items: HashMap<Inode, Item>,
    // held is each inode the kernel holds, and entries each name it keeps in a directory, never a free one: the volume
    // refuses a name it does not find. A name from an uncached reply it keeps for no time, and the next lookup asks the
    // volume about it again, without the directory's lock. Each entry has a number, as a revalidation is of one entry.
    held: HashMap<Inode, Held>,
    entries: HashMap<(Inode, &'static str), (Inode, bool, u64)>,
    // locks is each item's lock a request holds. Lookups and listings in a directory share its lock, as the volume
    // asks for parallel directory operations.
    locks: HashMap<Inode, Taken>,
    // paging is the read or write holding each file's pages, by its number. numbered is the last number given to one or
    // to an entry.
    paging: HashMap<Inode, u64>,
    // changes counts the changes to attributes the kernel took in, numbering each, as its attr_version does.
    changes: u64,
    numbered: u64,
    // replies numbers the volume's replies, as stratafs-fuse's Items does.
    replies: u64,
    // UPSTREAM KERNEL BUG PIN: unnaming is each name an unlink or rename in flight on this mount drops at its reply.
    // Lookups find it in the cache until then, as only a notify that takes the directory's lock could drop it sooner.
    unnaming: HashSet<(Inode, &'static str)>,
}

// Item is what the volume keeps of an inode it handed the kernel: the lookups the kernel holds, the newest attributes
// handed out, and the names in it handed out with the inode each named and the reply that handed it. asked and
// found_uncached are as stratafs-fuse's ItemState has them.
#[derive(Default)]
struct Item {
    lookups: u64,
    newest: Option<Attributes>,
    names: HashMap<&'static str, (Inode, u64)>,
    asked: bool,
    found_uncached: Option<u64>,
}

// Held is what the kernel keeps of an inode it holds.
#[derive(Default)]
struct Held {
    lookups: u64,
    // attributes are the attributes it caches, None once they are out of date, and size the size it takes the file to
    // have, which only attributes it takes in and writes move. changed numbers the last change to them.
    attributes: Option<Attributes>,
    size: u64,
    changed: u64,
    // pages is the file's data it caches, which it keeps from one open to the next.
    pages: Option<Bytes>,
    // UPSTREAM KERNEL BUG PIN: unstored is the write in flight whose data is in pages, which reads see before the
    // volume stores it, and whether or not it then does.
    unstored: Option<u64>,
    // UPSTREAM KERNEL BUG PIN: a read copies up to the size the kernel has once it has the pages, which can be older
    // than they are. purged numbers the last change that left the size out of date: a purge, which keeps the size, or a
    // reply it took none of while a write grew the file. uncached says whether the size came from an uncached reply.
    purged: u64,
    uncached: bool,
    // UPSTREAM KERNEL BUG PIN: growing is the write in flight that grows the file, until which the kernel takes no
    // attributes in, as FUSE_I_SIZE_UNSTABLE has it.
    growing: Option<u64>,
}

// Mode is how a request takes an item's lock.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    Shared,
    Exclusive,
}

enum Taken {
    Shared(usize),
    Exclusive,
}

// Kept is what the kernel keeps of an entry a reply hands it, taking the rest in for no time: nothing, the name alone,
// or the name and the item's attributes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kept {
    Nothing,
    Name,
    NameAndAttributes,
}

// Update is what a reply has the kernel take in, naming each item as the kernel found it, whatever the volume found.
// The attributes of an item the kernel already holds go only if no change to them came since sent, when there is one.
// cached says whether the reply's result handling was Cache.
pub(crate) enum Update {
    Entries {
        directory: Inode,
        entries: Vec<(&'static str, Inode, Attributes, Kept)>,
        sent: Option<u64>,
        cached: bool,
    },
    Attributes {
        inode: Inode,
        attributes: Attributes,
        sent: Option<u64>,
        cached: bool,
    },
    // Revalidated is the reply to a lookup of a name the kernel keeps for no time, as the entry revalidated, naming an
    // inode and numbered. The kernel keeps that entry while the reply names its inode, and drops it otherwise,
    // forgetting what the reply handed over. A purge may have dropped it meanwhile, and a lookup put another in place.
    Revalidated {
        directory: Inode,
        name: &'static str,
        revalidated: (Inode, u64),
        found: Option<(Inode, Attributes)>,
        sent: u64,
        kept: Kept,
        cached: bool,
    },
    // Set is a setattr's reply, which the kernel takes in whatever changed the attributes meanwhile, but keeps for no
    // time, as the volume asks. With a size other than the one it has, it drops every page, as fuse_do_setattr does.
    Set {
        inode: Inode,
        attributes: Attributes,
    },
    Data {
        inode: Inode,
        data: Bytes,
    },
    Written {
        inode: Inode,
        end: u64,
    },
    Unlinked {
        directory: Inode,
        name: &'static str,
        inode: Inode,
    },
    Renamed {
        from: (Inode, &'static str),
        to: (Inode, &'static str),
        inode: Inode,
        replaced: Option<Inode>,
    },
}

// Handing is the volume's record of a reply, numbered reply, as it hands the kernel what it read, and whether the
// kernel may cache it.
pub(crate) struct Handing<'a> {
    state: &'a mut State,
    reply: u64,
    pub(crate) cached: bool,
}

// Locked is the locks a request holds, until it is dropped.
pub(crate) struct Locked<'a> {
    kernel: &'a Kernel,
    items: Vec<(Inode, Mode)>,
}

// UPSTREAM KERNEL BUG PIN: Unnaming is an unlink or rename in flight, which drops its names at its reply, until it is
// dropped.
pub(crate) struct Unnaming<'a> {
    kernel: &'a Kernel,
    names: Vec<(Inode, &'static str)>,
}

// Paging is a read or write holding a file's pages, until it is dropped.
pub(crate) struct Paging<'a> {
    kernel: &'a Kernel,
    inode: Inode,
    number: u64,
}

// KernelHandle is the kernel as the engine reaches it.
struct KernelHandle(Arc<Kernel>);

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

    fn invalidate_attributes(&self, inode: Inode) -> Result<(), Uninvalidated> {
        if self.0.faults.refuses_attributes() {
            return Err(Uninvalidated("the kernel refused".to_owned()));
        }
        self.0.state().invalidate_attributes(inode);
        Ok(())
    }

    // A directory's purge takes its lock, which an unlink there holds while it waits for the lock of a file another
    // change holds.
    fn purge_waits_on_others(&self) -> bool {
        true
    }
}

impl KernelModel for Kernel {
    // UPSTREAM KERNEL BUG PIN: Linux shows reads of cached pages a write's data before the volume stores it, a read
    // copies up to a size the kernel took in before the data it copies, and a mount's lookups find the names its own
    // change drops until that change replies.
    const TAINTED: &'static str =
        "an unstored write's data, a size older than its data, or a name its own change drops";
    // Linux finds a rename's source and a link's new name under the directories' locks, as fs/namei.c does.
    const FINDS_NAMES: bool = true;
    // getdents shows an app each entry's name and inode alone, as fs/readdir.c does. readdirplus hands the attributes
    // to the kernel, which a stat may find later.
    const LISTS_ATTRIBUTES: bool = false;
    const REGRESSIONS: &'static str = "proptest-regressions/linux_fuse.txt";

    // new is the kernel of a volume just mounted, which holds the root from the mount on, without a lookup.
    fn new(faults: &Arc<Faults>, references: &Arc<References>) -> Self {
        let root = Item {
            lookups: 1,
            asked: true,
            ..Item::default()
        };
        let held = Held {
            lookups: 1,
            ..Held::default()
        };
        Kernel {
            state: Mutex::new(State {
                items: HashMap::from([(Inode::ROOT, root)]),
                held: HashMap::from([(Inode::ROOT, held)]),
                ..State::default()
            }),
            changed: Event::new(),
            faults: Arc::clone(faults),
            references: Arc::clone(references),
        }
    }

    fn cache(self: &Arc<Self>) -> impl KernelCache {
        KernelHandle(Arc::clone(self))
    }
}

impl Kernel {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // lock takes each of items' locks as its mode says, all at once, once no request holds one in a way that keeps it
    // out, until the Locked it returns is dropped.
    pub(crate) async fn lock(&self, items: Vec<(Inode, Mode)>) -> Locked<'_> {
        self.until(|state| {
            let free = items
                .iter()
                .all(|(item, mode)| match state.locks.get(item) {
                    None => true,
                    Some(Taken::Shared(_)) => *mode == Mode::Shared,
                    Some(Taken::Exclusive) => false,
                });
            if free {
                for (item, mode) in &items {
                    let taken = state.locks.entry(*item).or_insert(Taken::Shared(0));
                    match (mode, taken) {
                        (Mode::Shared, Taken::Shared(sharing)) => *sharing += 1,
                        (Mode::Exclusive, taken) => *taken = Taken::Exclusive,
                        (Mode::Shared, Taken::Exclusive) => unreachable!("a shared lock is free"),
                    }
                }
            }
            free
        })
        .await;
        Locked {
            kernel: self,
            items,
        }
    }

    // entry is the item name refers to in directory, if the kernel keeps the name, which it finds without a lock,
    // whether it caches it, and the entry's number.
    pub(crate) fn entry(&self, directory: Inode, name: &'static str) -> Option<(Inode, bool, u64)> {
        self.state().entries.get(&(directory, name)).copied()
    }

    // forget lets go of one lookup of inode the kernel was handed but did not take in, as it forgets a revalidation's
    // reply. Once the kernel holds none, the volume runs forget and drops what it keeps of it, as for reclaim.
    pub(crate) fn forget(&self, inode: Inode, forget: impl FnOnce()) {
        let mut state = self.state();
        let Some(item) = state.items.get_mut(&inode) else {
            return;
        };
        item.lookups = item.lookups.saturating_sub(1);
        item.asked = false;
        if item.lookups == 0 {
            forget();
            state.items.remove(&inode);
        }
    }

    // UPSTREAM KERNEL BUG PIN: unnaming marks names, which an unlink or rename drops at its reply, until the Unnaming
    // it returns is dropped.
    pub(crate) fn unnaming(&self, names: Vec<(Inode, &'static str)>) -> Unnaming<'_> {
        self.state().unnaming.extend(names.iter().copied());
        Unnaming {
            kernel: self,
            names,
        }
    }

    // UPSTREAM KERNEL BUG PIN: unnamed_at_reply says whether a change in flight on this mount drops name in directory
    // at its reply.
    pub(crate) fn unnamed_at_reply(&self, directory: Inode, name: &'static str) -> bool {
        self.state().unnaming.contains(&(directory, name))
    }

    // attributes are the attributes the kernel caches for inode, unless they are out of date.
    pub(crate) fn attributes(&self, inode: Inode) -> Option<Attributes> {
        let state = self.state();
        state.held.get(&inode)?.attributes.clone()
    }

    // size is the size the kernel takes inode to have.
    pub(crate) fn size(&self, inode: Inode) -> u64 {
        self.state().held.get(&inode).map_or(0, |held| held.size)
    }

    // named is the inode the volume last handed the kernel name in directory as, if it handed the name.
    pub(crate) fn named(&self, directory: Inode, name: &'static str) -> Option<Inode> {
        let state = self.state();
        let item = state.items.get(&directory)?;
        item.names.get(name).map(|(inode, _)| *inode)
    }

    // sent numbers a request as it begins, by the last change to attributes the kernel took in.
    pub(crate) fn sent(&self) -> u64 {
        self.state().changes
    }

    // pages is the data the kernel caches for inode, which a read takes without waiting, and whether it holds a write's
    // data the volume has not stored.
    pub(crate) fn pages(&self, inode: Inode) -> Option<(Bytes, bool)> {
        let state = self.state();
        let held = state.held.get(&inode)?;
        // UPSTREAM KERNEL BUG PIN: whether it is a write's data the read is excused for.
        Some((held.pages.clone()?, held.unstored.is_some()))
    }

    // UPSTREAM KERNEL BUG PIN: size_out_of_date says whether the size the kernel has for inode may be older than the
    // data a read begun at the change numbered began copies: it went out of date since, or an uncached reply set it.
    pub(crate) fn size_out_of_date(&self, inode: Inode, began: u64) -> bool {
        let state = self.state();
        let held = state.held.get(&inode);
        held.is_some_and(|held| held.purged > began || held.uncached)
    }

    // page takes inode's pages for a read or a write, once no other holds them, until the Paging it returns is dropped.
    // A write puts its data over the pages the kernel caches as it takes them.
    pub(crate) async fn page(&self, inode: Inode, write: Option<&'static [u8]>) -> Paging<'_> {
        let mut number = 0;
        self.until(|state| {
            if state.paging.contains_key(&inode) {
                return false;
            }
            state.numbered += 1;
            number = state.numbered;
            state.paging.insert(inode, number);
            if let Some(data) = write
                && let Some(held) = state.held.get_mut(&inode)
            {
                // UPSTREAM KERNEL BUG PIN: the kernel takes no attributes in until a write past the size ends.
                if data.len() as u64 > held.size {
                    held.growing = Some(number);
                }
                if let Some(pages) = &held.pages {
                    let written = [data, pages.get(data.len()..).unwrap_or_default()].concat();
                    held.pages = Some(written.into());
                    // UPSTREAM KERNEL BUG PIN: reads see this data from now on.
                    held.unstored = Some(number);
                }
            }
            true
        })
        .await;
        Paging {
            kernel: self,
            inode,
            number,
        }
    }

    // reply asks for the result handling while every item of reads is locked, as stratafs-fuse's Items does. Unless it
    // is Retry, reply keeps each read newer than the newest handed out and calls reply; otherwise it returns None. An
    // item left with no lookups goes, as the kernel holds nothing of it.
    pub(crate) fn reply<R>(
        &self,
        reads: &[(Inode, Attributes)],
        result_handling: &dyn Fn() -> ResultHandling,
        reply: impl FnOnce(&mut Handing<'_>) -> R,
    ) -> Option<R> {
        let mut state = self.state();
        for (inode, _) in reads {
            state.items.entry(*inode).or_default();
        }
        let handling = result_handling();
        let replied =
            (handling != ResultHandling::Retry).then(|| {
                for (inode, read) in reads {
                    let item = state
                        .items
                        .get_mut(inode)
                        .expect("every read's item is kept");
                    if item.newest.as_ref().is_none_or(|newest| {
                        read.change_version.number > newest.change_version.number
                    }) {
                        item.newest = Some(read.clone());
                    }
                }
                state.replies += 1;
                let reply_number = state.replies;
                reply(&mut Handing {
                    state: &mut state,
                    reply: reply_number,
                    cached: handling == ResultHandling::Cache,
                })
            });
        for (inode, _) in reads {
            if state.items.get(inode).is_some_and(|item| item.lookups == 0) {
                state.items.remove(inode);
            }
        }
        replied
    }

    // apply has the kernel take update in, a while after the volume replied.
    pub(crate) async fn apply(&self, update: Update) {
        yield_now().await;
        self.state().apply(update);
        self.changed.notify(usize::MAX);
    }

    // write_failed drops inode's pages as the failure of the write holding them arrives, as the kernel does without the
    // writeback cache.
    pub(crate) async fn write_failed(&self, inode: Inode) {
        yield_now().await;
        if let Some(held) = self.state().held.get_mut(&inode) {
            held.pages = None;
            held.unstored = None;
        }
        self.changed.notify(usize::MAX);
    }

    // reclaim lets go of inode unless an operation refers to it: the kernel drops what it keeps of it and every name of
    // it or in it, and forgets its lookups. Once the kernel holds none, the volume runs forget and drops what it keeps
    // of it, as stratafs-fuse's Items does, and reclaim says whether it did.
    pub(crate) fn reclaim(&self, inode: Inode, forget: impl FnOnce()) -> bool {
        if self.references.referred(inode) {
            return false;
        }
        let mut state = self.state();
        let Some(held) = state.held.remove(&inode) else {
            return false;
        };
        state
            .entries
            .retain(|(directory, _), (item, ..)| *directory != inode && *item != inode);
        let Some(item) = state.items.get_mut(&inode) else {
            return false;
        };
        item.lookups = item.lookups.saturating_sub(held.lookups);
        item.asked = false;
        if item.lookups > 0 {
            return false;
        }
        forget();
        state.items.remove(&inode);
        true
    }

    // invalidate drops what the kernel was handed of inode, as stratafs-fuse's FuseCache does: each name in it the
    // kernel was handed, then its attributes and pages. It fails, dropping nothing, when the kernel refuses.
    pub(crate) async fn invalidate(
        &self,
        inode: Inode,
        noting: Option<(Asked, Answered)>,
    ) -> Result<(), Uninvalidated> {
        // The volume invalidates on a thread of its own, so the caller goes on first.
        yield_now().await;
        let (asked, answered) = noting.unzip();
        if let Some(asked) = asked {
            asked();
        }
        let invalidated = self.invalidate_now(inode).await;
        if let Some(answered) = answered {
            answered(invalidated.is_ok());
        }
        invalidated
    }

    // invalidate_now keeps each name until it is dropped, unless a later reply handed it again, so a purge running
    // beside another drops them too.
    async fn invalidate_now(&self, inode: Inode) -> Result<(), Uninvalidated> {
        let mut names: Vec<(&'static str, u64)> = {
            let state = self.state();
            let Some(item) = state.items.get(&inode) else {
                return Ok(());
            };
            item.names
                .iter()
                .map(|(name, (_, reply))| (*name, *reply))
                .collect()
        };
        // They go in order, so a purge runs the same way each time.
        names.sort();
        if self.faults.refuses() {
            return Err(Uninvalidated("the kernel refused".to_owned()));
        }
        for (name, reply) in names {
            self.invalidate_entry(inode, name).await;
            if let Some(item) = self.state().items.get_mut(&inode)
                && item
                    .names
                    .get(name)
                    .is_some_and(|(_, handed)| *handed == reply)
            {
                item.names.remove(name);
            }
        }
        self.invalidate_inode(inode).await;
        Ok(())
    }

    // invalidate_entry is the kernel's inval_entry: it takes directory's lock once no request holds it, then drops
    // name. A directory it does not hold it leaves alone.
    async fn invalidate_entry(&self, directory: Inode, name: &'static str) {
        // The kernel is asked a while after the volume asks it.
        yield_now().await;
        self.until(|state| {
            if state.held.contains_key(&directory) && state.locks.contains_key(&directory) {
                return false;
            }
            state.entries.remove(&(directory, name));
            true
        })
        .await;
    }

    // invalidate_inode is the kernel's inval_inode over all of inode's data: it drops its attributes at once, and its
    // pages once the read or write holding them as it was asked ends. An item it does not hold it leaves alone.
    async fn invalidate_inode(&self, inode: Inode) {
        yield_now().await;
        let paging = {
            let mut state = self.state();
            if !state.held.contains_key(&inode) {
                return;
            }
            let changed = state.change();
            let held = state.held.get_mut(&inode).expect("the kernel holds it");
            held.changed = changed;
            held.attributes = None;
            // UPSTREAM KERNEL BUG PIN: a read that began before this goes on with the size it had.
            held.purged = changed;
            state.paging.get(&inode).copied()
        };
        self.until(|state| {
            if paging.is_some_and(|paging| state.paging.get(&inode) == Some(&paging)) {
                return false;
            }
            if let Some(held) = state.held.get_mut(&inode) {
                held.pages = None;
                // UPSTREAM KERNEL BUG PIN: the purge dropped it.
                held.unstored = None;
            }
            true
        })
        .await;
        // The kernel answers a while after it purged.
        yield_now().await;
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
    // change numbers a change to attributes the kernel takes in.
    fn change(&mut self) -> u64 {
        self.changes += 1;
        self.changes
    }

    // invalidate_attributes is the kernel's inval_inode over none of inode's data: it drops its attributes and waits
    // for nothing, and a reply to a request sent before takes none in. An item it does not hold it leaves alone.
    fn invalidate_attributes(&mut self, inode: Inode) {
        let changed = self.change();
        if let Some(held) = self.held.get_mut(&inode) {
            held.changed = changed;
            held.attributes = None;
            // UPSTREAM KERNEL BUG PIN: a read that began before this goes on with the size it had.
            held.purged = changed;
        }
    }

    fn apply(&mut self, update: Update) {
        match update {
            Update::Entries {
                directory,
                entries,
                sent,
                cached,
            } => {
                for (name, inode, attributes, kept) in entries {
                    self.numbered += 1;
                    let entry = (inode, kept != Kept::Nothing, self.numbered);
                    self.entries.insert((directory, name), entry);
                    // A reply bringing in an item the kernel does not hold gives it these attributes, however old.
                    let sent = match self.held.get_mut(&inode) {
                        Some(held) => {
                            held.lookups += 1;
                            sent
                        }
                        None => {
                            let held = Held {
                                lookups: 1,
                                ..Held::default()
                            };
                            self.held.insert(inode, held);
                            None
                        }
                    };
                    let kept = kept == Kept::NameAndAttributes;
                    self.take(inode, attributes, sent, kept, cached);
                }
            }
            Update::Attributes {
                inode,
                attributes,
                sent,
                cached,
            } => self.take(inode, attributes, sent, cached, cached),
            Update::Revalidated {
                directory,
                name,
                revalidated: (revalidated, number),
                found,
                sent,
                kept,
                cached,
            } => {
                // A purge that dropped the entry meanwhile unhashed it, so the kernel keeps it no longer, and keeps the
                // one a lookup since put in its place.
                let entry = (directory, name);
                let current = self.entries.get(&entry).copied();
                let still = current.is_some_and(|(_, _, numbered)| numbered == number);
                match found {
                    Some((inode, attributes)) if inode == revalidated => {
                        self.apply(Update::Entries {
                            directory,
                            entries: vec![(name, inode, attributes, kept)],
                            sent: Some(sent),
                            cached,
                        });
                        if !still {
                            match current {
                                Some(current) => self.entries.insert(entry, current),
                                None => self.entries.remove(&entry),
                            };
                        }
                    }
                    _ if still => {
                        self.entries.remove(&entry);
                    }
                    _ => {}
                }
            }
            Update::Set { inode, attributes } => {
                if let Some(held) = self.held.get_mut(&inode)
                    && held.size != attributes.settable.size
                {
                    held.pages = None;
                }
                self.take(inode, attributes, None, false, false)
            }
            // A read makes the next stat ask for the attributes, as its access time changed.
            Update::Data { inode, data } => {
                if let Some(held) = self.held.get_mut(&inode) {
                    held.pages = Some(data);
                    held.attributes = None;
                }
            }
            Update::Written { inode, end } => {
                let changed = self.change();
                if let Some(held) = self.held.get_mut(&inode) {
                    // UPSTREAM KERNEL BUG PIN: a size the write moves is its own.
                    if end > held.size {
                        held.size = end;
                        held.uncached = false;
                    }
                    held.changed = changed;
                    held.attributes = None;
                }
            }
            Update::Unlinked {
                directory,
                name,
                inode,
            } => {
                self.entries.remove(&(directory, name));
                self.unlinked(inode);
                self.out_of_date(directory);
            }
            Update::Renamed {
                from,
                to,
                inode,
                replaced,
            } => {
                // The kernel moves the name over, kept as long as it was.
                let cached = self
                    .entries
                    .remove(&from)
                    .is_none_or(|(_, cached, _)| cached);
                self.numbered += 1;
                self.entries.insert(to, (inode, cached, self.numbered));
                if let Some(replaced) = replaced {
                    self.unlinked(replaced);
                }
                for item in [inode, from.0, to.0] {
                    self.out_of_date(item);
                }
            }
        }
    }

    // take has the kernel take attributes in for inode, from a reply handled with Cache or NoCache, keeping them when
    // kept says so, unless they are a reply's for a request sent before a change to them it took in. A size smaller
    // than the one it had only cuts its pages short, as the kernel drops data only when the volume asks.
    fn take(
        &mut self,
        inode: Inode,
        attributes: Attributes,
        sent: Option<u64>,
        kept: bool,
        cached: bool,
    ) {
        let changed = self.changes + 1;
        let Some(held) = self.held.get_mut(&inode) else {
            return;
        };
        if sent.is_some_and(|sent| held.changed > sent) {
            return;
        }
        self.changes = changed;
        // UPSTREAM KERNEL BUG PIN: a write growing the file keeps the kernel from taking any attributes in.
        if held.growing.is_some() {
            held.purged = changed;
            return;
        }
        held.changed = changed;
        let size = attributes.settable.size;
        if let Some(pages) = &held.pages
            && size < held.size
            && pages.len() as u64 > size
        {
            held.pages = Some(pages.slice(..size as usize));
        }
        held.size = size;
        // UPSTREAM KERNEL BUG PIN: an uncached reply's size may be from before a write in flight.
        held.uncached = !cached;
        held.attributes = kept.then_some(attributes);
    }

    // unlinked marks a change to inode's attributes as one of its names goes.
    fn unlinked(&mut self, inode: Inode) {
        let changed = self.change();
        if let Some(held) = self.held.get_mut(&inode) {
            held.changed = changed;
            held.attributes = None;
        }
    }

    fn out_of_date(&mut self, inode: Inode) {
        if let Some(held) = self.held.get_mut(&inode) {
            held.attributes = None;
        }
    }
}

impl Handing<'_> {
    // attr is inode's newest attributes, of an inode the reply read, as the kernel takes them.
    pub(crate) fn attr(&self, inode: Inode) -> Attributes {
        let item = self.state.items.get(&inode);
        let newest = item.and_then(|item| item.newest.clone());
        newest.expect("a reply keeps a read for every inode it read")
    }

    // hand records that the reply hands the kernel a lookup of inode, under name in directory.
    pub(crate) fn hand(&mut self, directory: Inode, name: &'static str, inode: Inode) {
        self.name(directory, name, inode);
        self.item(inode).lookups += 1;
    }

    // look_up hands inode as hand does, for a lookup that found it at change number found, as stratafs-fuse's Locked
    // does, and says whether it did.
    pub(crate) fn look_up(
        &mut self,
        directory: Inode,
        name: &'static str,
        inode: Inode,
        found: u64,
    ) -> bool {
        let cached = self.cached;
        let item = self.item(inode);
        if item.found_uncached.is_some_and(|uncached| uncached < found) {
            item.found_uncached = None;
            return false;
        }
        if !cached {
            item.found_uncached = Some(item.found_uncached.unwrap_or(found).min(found));
        }
        self.hand(directory, name, inode);
        true
    }

    // keeps is what the kernel keeps of a lookup of inode the reply hands it, as stratafs-fuse's lookup has it.
    pub(crate) fn keeps(&self, inode: Inode) -> Kept {
        let asked = self.state.items.get(&inode).is_some_and(|item| item.asked);
        match (self.cached, asked) {
            (false, _) => Kept::Nothing,
            (true, false) => Kept::Name,
            (true, true) => Kept::NameAndAttributes,
        }
    }

    // ask records that the kernel asked for inode's attributes, which it does only for an item it holds.
    pub(crate) fn ask(&mut self, inode: Inode) {
        self.item(inode).asked = true;
    }

    // name records that the reply hands the kernel name in directory, naming inode.
    pub(crate) fn name(&mut self, directory: Inode, name: &'static str, inode: Inode) {
        let reply = self.reply;
        self.item(directory).names.insert(name, (inode, reply));
    }

    // unname records that the kernel drops name in directory on the reply.
    pub(crate) fn unname(&mut self, directory: Inode, name: &'static str) {
        self.item(directory).names.remove(name);
    }

    fn item(&mut self, inode: Inode) -> &mut Item {
        self.state
            .items
            .get_mut(&inode)
            .expect("a reply only hands over inodes it read")
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        {
            let mut state = self.kernel.state();
            for (item, _) in &self.items {
                let taken = state
                    .locks
                    .get_mut(item)
                    .expect("a lock is kept until it is let go");
                match taken {
                    Taken::Shared(sharing) if *sharing > 1 => *sharing -= 1,
                    Taken::Shared(_) | Taken::Exclusive => {
                        state.locks.remove(item);
                    }
                }
            }
        }
        self.kernel.changed.notify(usize::MAX);
    }
}

impl Drop for Unnaming<'_> {
    fn drop(&mut self) {
        let mut state = self.kernel.state();
        for name in &self.names {
            state.unnaming.remove(name);
        }
    }
}

impl Drop for Paging<'_> {
    fn drop(&mut self) {
        {
            let mut state = self.kernel.state();
            state.paging.remove(&self.inode);
            if let Some(held) = state.held.get_mut(&self.inode) {
                // UPSTREAM KERNEL BUG PIN: a write that ended either stored its data or dropped it.
                if held.unstored == Some(self.number) {
                    held.unstored = None;
                }
                // UPSTREAM KERNEL BUG PIN: and the kernel takes attributes in again.
                if held.growing == Some(self.number) {
                    held.growing = None;
                }
            }
        }
        self.kernel.changed.notify(usize::MAX);
    }
}
