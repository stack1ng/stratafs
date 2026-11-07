use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;
use fuser::{FileAttr, Notifier};
use stratafs_fs_engine::attributes::Attributes;
use stratafs_fs_engine::{Inode, ResultHandling};

use super::item::file_attr;

// Items keeps what the kernel was handed of each inode it holds, each under the inode's lock.
// The engine forgets an inode only once the kernel forgot every lookup, with no reply handing it out meanwhile.
// A reply that read older attributes than the newest handed out never hands them out last.
pub(crate) struct Items {
    states: Mutex<HashMap<Inode, Arc<Mutex<ItemState>>>>,
    // replies numbers each reply, so an invalidation lets go only of what no reply handed over again since it began.
    replies: AtomicU64,
    // notifier is the session's, set once the session starts. The kernel holds nothing to invalidate before then.
    pub(crate) notifier: OnceLock<Notifier>,
}

// ItemState is what the volume keeps about an inode the kernel has been handed.
#[derive(Default)]
struct ItemState {
    // lookups is how many times the kernel was handed the inode, less how many it forgot.
    lookups: u64,
    newest: Option<Attributes>,
    // names are the names in this directory the kernel was handed, which it keeps until they are invalidated, each
    // with the inode it named and the reply that handed it.
    names: HashMap<Bytes, (Inode, u64)>,
    // asked says whether the kernel asked for the item's attributes since it last forgot a lookup of it. It asks only
    // about an item it holds, for which it drops attributes replied to a request sent before a purge.
    asked: bool,
    // found_uncached is the item's change number as the oldest uncached lookup found it, since look_up last refused it.
    found_uncached: Option<u64>,
    // retired marks a state taken out of the map. An operation that reached it first starts over.
    retired: bool,
}

// Locked is every inode an operation replies about, each locked until the reply returns, the reply's number, and
// whether the kernel may cache what it hands over.
pub(crate) struct Locked<'a> {
    states: BTreeMap<u64, MutexGuard<'a, ItemState>>,
    reply: u64,
    pub(crate) cached: bool,
}

impl Items {
    // new is the items of a volume just mounted: the kernel holds the root from the mount on, without a lookup.
    pub(crate) fn new() -> Self {
        let root = ItemState {
            lookups: 1,
            asked: true,
            ..ItemState::default()
        };
        Items {
            states: Mutex::new(HashMap::from([(Inode::ROOT, Arc::new(Mutex::new(root)))])),
            replies: AtomicU64::new(0),
            notifier: OnceLock::new(),
        }
    }

    // reply locks every inode in reads, in inode order, and asks for the result handling. Unless it is Retry, reply
    // keeps each read newer than the newest handed out and calls reply under the locks; otherwise it returns None. A
    // state left with no lookups is one the kernel holds nothing of, so it goes, and an operation waiting on it starts
    // over.
    pub(crate) fn reply<R>(
        &self,
        reads: impl IntoIterator<Item = (Inode, Attributes)>,
        result_handling: &dyn Fn() -> ResultHandling,
        reply: impl FnOnce(&mut Locked<'_>) -> R,
    ) -> Option<R> {
        // A reply can read one inode twice, such as a rename within one directory, so keep the newer read.
        let mut reads_by_inode: BTreeMap<u64, Attributes> = BTreeMap::new();
        for (inode, read) in reads {
            match reads_by_inode.entry(inode.0) {
                Entry::Occupied(mut kept) => {
                    if read.change_version.number > kept.get().change_version.number {
                        kept.insert(read);
                    }
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(read);
                }
            }
        }
        loop {
            let states: Vec<(u64, Arc<Mutex<ItemState>>)> = {
                let mut map = self.lock_states();
                reads_by_inode
                    .keys()
                    .map(|&inode| (inode, map.entry(Inode(inode)).or_default().clone()))
                    .collect()
            };
            let mut locked = Locked {
                states: states
                    .iter()
                    .map(|(inode, state)| {
                        (*inode, state.lock().unwrap_or_else(PoisonError::into_inner))
                    })
                    .collect(),
                reply: self.replies.fetch_add(1, Ordering::Relaxed),
                cached: false,
            };
            if locked.states.values().any(|state| state.retired) {
                continue;
            }
            let handling = result_handling();
            locked.cached = handling == ResultHandling::Cache;
            let replies = handling != ResultHandling::Retry;
            for (inode, read) in std::mem::take(&mut reads_by_inode)
                .into_iter()
                .filter(|_| replies)
            {
                let state = locked
                    .states
                    .get_mut(&inode)
                    .expect("every read's inode is locked");
                if state
                    .newest
                    .as_ref()
                    .is_none_or(|newest| read.change_version.number > newest.change_version.number)
                {
                    state.newest = Some(read);
                }
            }
            let replied = replies.then(|| reply(&mut locked));
            let mut map = self.lock_states();
            for (inode, state) in &mut locked.states {
                if state.lookups == 0 {
                    state.retired = true;
                    map.remove(&Inode(*inode));
                }
            }
            return replied;
        }
    }

    // forget lets go of count lookups of inode, and once the kernel holds none, runs forget and says whether the inode
    // has no name, by the newest attributes handed out for it. It returns None while the kernel still holds a lookup.
    // The inode's lock is held throughout, so no reply can hand the inode out before forget has run.
    pub(crate) fn forget(&self, inode: Inode, count: u64, forget: impl FnOnce()) -> Option<bool> {
        let state = self.state(inode)?;
        let mut locked = state.lock().unwrap_or_else(PoisonError::into_inner);
        locked.lookups = locked.lookups.saturating_sub(count);
        locked.asked = false;
        if locked.retired || locked.lookups > 0 {
            return None;
        }
        forget();
        locked.retired = true;
        self.lock_states().remove(&inode);
        Some(
            locked
                .newest
                .as_ref()
                .is_some_and(|newest| newest.hard_link_count == 0),
        )
    }

    // release runs forget unless the kernel holds a lookup of inode, for a read the engine took as handed to the
    // kernel that the reply left out. The inode's lock is held throughout, as for forget.
    pub(crate) fn release(&self, inode: Inode, forget: impl FnOnce()) {
        loop {
            let state = self.lock_states().entry(inode).or_default().clone();
            let mut locked = state.lock().unwrap_or_else(PoisonError::into_inner);
            if locked.retired {
                continue;
            }
            if locked.lookups == 0 {
                forget();
                locked.retired = true;
                self.lock_states().remove(&inode);
            }
            return;
        }
    }

    // to_invalidate is the names in inode that invalidating it drops, each with the last reply that handed it, kept
    // until dropped_name says each went, so an invalidation running beside another drops them too. It is None when the
    // kernel holds nothing of inode.
    pub(crate) fn to_invalidate(&self, inode: Inode) -> Option<Vec<(Bytes, u64)>> {
        let state = self.state(inode)?;
        let locked = state.lock().unwrap_or_else(PoisonError::into_inner);
        let names = locked
            .names
            .iter()
            .map(|(name, (_, reply))| (name.clone(), *reply));
        Some(names.collect())
    }

    // dropped_name lets go of name in directory, handed by reply, once the kernel dropped it, unless a later reply
    // handed it again.
    pub(crate) fn dropped_name(&self, directory: Inode, name: &[u8], reply: u64) {
        if let Some(state) = self.state(directory) {
            let mut locked = state.lock().unwrap_or_else(PoisonError::into_inner);
            if locked
                .names
                .get(name)
                .is_some_and(|(_, handed)| *handed == reply)
            {
                locked.names.remove(name);
            }
        }
    }

    // named is the inode the kernel was last handed name in directory as, if it was handed the name.
    pub(crate) fn named(&self, directory: Inode, name: &[u8]) -> Option<Inode> {
        let state = self.state(directory)?;
        let locked = state.lock().unwrap_or_else(PoisonError::into_inner);
        locked.names.get(name).map(|(inode, _)| *inode)
    }

    // unlinked are the inodes the kernel holds whose newest attributes handed out say no name refers to them.
    pub(crate) fn unlinked(&self) -> Vec<Inode> {
        let states: Vec<(Inode, Arc<Mutex<ItemState>>)> = self
            .lock_states()
            .iter()
            .map(|(inode, state)| (*inode, Arc::clone(state)))
            .collect();
        states
            .into_iter()
            .filter(|(_, state)| {
                let locked = state.lock().unwrap_or_else(PoisonError::into_inner);
                !locked.retired
                    && locked
                        .newest
                        .as_ref()
                        .is_some_and(|newest| newest.hard_link_count == 0)
            })
            .map(|(inode, _)| inode)
            .collect()
    }

    fn state(&self, inode: Inode) -> Option<Arc<Mutex<ItemState>>> {
        self.lock_states().get(&inode).cloned()
    }

    fn lock_states(&self) -> MutexGuard<'_, HashMap<Inode, Arc<Mutex<ItemState>>>> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Locked<'_> {
    // attr is inode's newest attributes, one of the inodes locked, as the kernel takes them.
    pub(crate) fn attr(&self, inode: Inode) -> FileAttr {
        let newest = self
            .state(inode)
            .newest
            .as_ref()
            .expect("reply keeps a read for every inode it locks");
        file_attr(inode, newest)
    }

    // hand records that the reply hands the kernel a lookup of inode, under name in directory.
    pub(crate) fn hand(&mut self, directory: Inode, name: &Bytes, inode: Inode) {
        self.name(directory, name, inode);
        self.state_mut(inode).lookups += 1;
    }

    // look_up hands inode as hand does, for a lookup that found it at change number found, unless an uncached lookup
    // found it at an older one since. Then it hands nothing and says so: the system call that asked may use both
    // lookups' names, as a rename does, which renames nothing when both name one file, and the older may be gone.
    pub(crate) fn look_up(
        &mut self,
        directory: Inode,
        name: &Bytes,
        inode: Inode,
        found: u64,
    ) -> bool {
        let cached = self.cached;
        let state = self.state_mut(inode);
        if state
            .found_uncached
            .is_some_and(|uncached| uncached < found)
        {
            state.found_uncached = None;
            return false;
        }
        if !cached {
            state.found_uncached = Some(state.found_uncached.unwrap_or(found).min(found));
        }
        self.hand(directory, name, inode);
        true
    }

    // asked says whether the kernel asked for inode's attributes since it last forgot a lookup of it.
    pub(crate) fn asked(&self, inode: Inode) -> bool {
        self.state(inode).asked
    }

    // ask records that the kernel asked for inode's attributes, which it does only for an item it holds.
    pub(crate) fn ask(&mut self, inode: Inode) {
        self.state_mut(inode).asked = true;
    }

    // name records that the reply hands the kernel name in directory, naming inode.
    pub(crate) fn name(&mut self, directory: Inode, name: &Bytes, inode: Inode) {
        let reply = self.reply;
        self.state_mut(directory)
            .names
            .insert(name.clone(), (inode, reply));
    }

    // unname records that the kernel drops name in directory on the reply.
    pub(crate) fn unname(&mut self, directory: Inode, name: &[u8]) {
        self.state_mut(directory).names.remove(name);
    }

    fn state(&self, inode: Inode) -> &ItemState {
        self.states
            .get(&inode.0)
            .expect("an operation only replies about inodes it locked")
    }

    fn state_mut(&mut self, inode: Inode) -> &mut ItemState {
        self.states
            .get_mut(&inode.0)
            .expect("an operation only replies about inodes it locked")
    }
}

#[cfg(test)]
mod tests;
