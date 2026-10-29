use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use block2::RcBlock;
use objc2::rc::Retained;
use objc2_fs_kit::{FSDataCacheMode, FSItemAttributes, FSKernelCacheCoherencyType};
use stratafs_fs_engine::Inode;
use stratafs_fs_engine::attributes::{Attributes, FileType};

use super::item::{StrataItem, item_attributes};

// Items keeps, for each inode FSKit holds, its one StrataItem and the newest attributes handed out for it,
// both under that inode's lock:
// - FSKit must get the same item for a file every time, and must not get it back from a lookup while it
//   reclaims it (FSItem's tryReclaim).
// - FSKit numbers every FSItemAttributes as it is filled in, and the kernel keeps the highest number.
//   Operations run in parallel, so one that read older attributes could otherwise fill its snapshot last
//   and win.
#[derive(Default)]
pub(crate) struct Items {
    states: Mutex<HashMap<Inode, Arc<Mutex<ItemState>>>>,
}

// ItemState is what the volume keeps about an inode FSKit has been handed, as an item or in a directory listing.
#[derive(Default)]
struct ItemState {
    // item is the inode's one StrataItem, made the first time FSKit is handed the item. A directory listing hands
    // FSKit only an entry's attributes, so an inode can have a state and no item yet.
    item: Option<Retained<StrataItem>>,
    newest: Option<Attributes>,
    // retired marks a state that reclaim took out of the map. An operation that reached it first starts over.
    retired: bool,
    // granted is the caching the kernel holds for the item's contents, from its last open or upgrade until it
    // closes.
    granted: Granted,
}

// Granted is the caching the kernel holds for an item's contents.
#[derive(Clone, Copy, Default)]
pub(crate) struct Granted {
    pub(crate) mode: FSDataCacheMode,
    pub(crate) coherency: FSKernelCacheCoherencyType,
}

// Locked is every inode an operation replies about, each locked until the reply returns.
pub(crate) struct Locked<'a> {
    states: BTreeMap<u64, MutexGuard<'a, ItemState>>,
}

impl Items {
    // reply locks every inode in reads, in inode order so operations never wait on each other in a cycle,
    // keeps each read that is newer than the newest attributes already handed out, and calls reply while
    // the locks are held.
    pub(crate) fn reply<R>(
        &self,
        reads: impl IntoIterator<Item = (Inode, Attributes)>,
        reply: impl FnOnce(&mut Locked<'_>) -> R,
    ) -> R {
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
                let mut map = self.states.lock().unwrap_or_else(PoisonError::into_inner);
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
            };
            if locked.states.values().any(|state| state.retired) {
                continue;
            }
            for (inode, read) in std::mem::take(&mut reads_by_inode) {
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
            return reply(&mut locked);
        }
    }

    // item is inode's one StrataItem, for a reply that hands FSKit the item without its attributes.
    pub(crate) fn item(&self, inode: Inode, kind: FileType) -> Retained<StrataItem> {
        loop {
            let state = self
                .states
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(inode)
                .or_default()
                .clone();
            let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
            if !state.retired {
                return state
                    .item
                    .get_or_insert_with(|| StrataItem::new(inode, kind))
                    .clone();
            }
        }
    }

    // reclaim lets go of item, unless FSKit handed it out again since asking for the reclaim, and says whether
    // the item it let go of has no name, by the newest attributes handed out for it, or None when it kept the item.
    // The inode's lock is held throughout, so no lookup can hand the item out meanwhile, as FSItem's tryReclaim
    // requires.
    pub(crate) fn reclaim(&self, item: &StrataItem) -> Option<bool> {
        let inode = *item.inode();
        let state = self.state(inode)?;
        let mut locked = state.lock().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: the reclaim block is a plain block, and the inode's lock keeps lookups out.
        let reclaimed = unsafe { item.tryReclaimWithBlock(&RcBlock::new(|| {})) };
        if !reclaimed {
            return None;
        }
        locked.retired = true;
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&inode);
        Some(
            locked
                .newest
                .as_ref()
                .is_some_and(|newest| newest.hard_link_count == 0),
        )
    }

    // grant records the caching the kernel holds for inode's contents.
    pub(crate) fn grant(&self, inode: Inode, granted: Granted) {
        if let Some(state) = self.state(inode) {
            state.lock().unwrap_or_else(PoisonError::into_inner).granted = granted;
        }
    }

    // item_to_invalidate is inode's item and the caching the kernel holds for it, which invalidating its kernel copy
    // takes. It is None when FSKit holds no item for inode, or the item is a symbolic link, since the kernel
    // invalidates only files and directories.
    pub(crate) fn item_to_invalidate(
        &self,
        inode: Inode,
    ) -> Option<(Retained<StrataItem>, Granted)> {
        let state = self.state(inode)?;
        let state = state.lock().unwrap_or_else(PoisonError::into_inner);
        let item = state
            .item
            .clone()
            .filter(|item| *item.kind() != FileType::SymbolicLink)?;
        Some((item, state.granted))
    }

    fn state(&self, inode: Inode) -> Option<Arc<Mutex<ItemState>>> {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&inode)
            .cloned()
    }
}

impl Locked<'_> {
    // attributes fills a snapshot of inode's newest attributes.
    pub(crate) fn attributes(&self, inode: Inode) -> Retained<FSItemAttributes> {
        let newest = self.states[&inode.0]
            .newest
            .as_ref()
            .expect("reply keeps a read for every inode it locks");
        item_attributes(inode, newest)
    }

    // item is inode's one StrataItem, made the first time FSKit is handed it.
    pub(crate) fn item(&mut self, inode: Inode) -> Retained<StrataItem> {
        let state = self
            .states
            .get_mut(&inode.0)
            .expect("an operation only asks for items it locked");
        let kind = state
            .newest
            .as_ref()
            .expect("reply keeps a read for every inode it locks")
            .kind;
        state
            .item
            .get_or_insert_with(|| StrataItem::new(inode, kind))
            .clone()
    }
}
