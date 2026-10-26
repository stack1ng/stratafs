use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use block2::RcBlock;
use objc2::rc::Retained;
use objc2_fs_kit::FSItemAttributes;
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
    slots: Mutex<HashMap<Inode, Arc<Mutex<Slot>>>>,
}

#[derive(Default)]
struct Slot {
    item: Option<Retained<StrataItem>>,
    newest: Option<Attributes>,
    // retired marks a slot that reclaim took out of the map. An operation that reached it first starts over.
    retired: bool,
}

// Locked is every inode an operation replies about, each locked until the reply returns.
pub(crate) struct Locked<'a> {
    slots: BTreeMap<u64, MutexGuard<'a, Slot>>,
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
                    if read.change_version > kept.get().change_version {
                        kept.insert(read);
                    }
                }
                Entry::Vacant(slot) => {
                    slot.insert(read);
                }
            }
        }
        loop {
            let slots: Vec<(u64, Arc<Mutex<Slot>>)> = {
                let mut map = self.slots.lock().unwrap_or_else(PoisonError::into_inner);
                reads_by_inode
                    .keys()
                    .map(|&inode| (inode, map.entry(Inode(inode)).or_default().clone()))
                    .collect()
            };
            let mut locked = Locked {
                slots: slots
                    .iter()
                    .map(|(inode, slot)| {
                        (*inode, slot.lock().unwrap_or_else(PoisonError::into_inner))
                    })
                    .collect(),
            };
            if locked.slots.values().any(|slot| slot.retired) {
                continue;
            }
            for (inode, read) in std::mem::take(&mut reads_by_inode) {
                let slot = locked
                    .slots
                    .get_mut(&inode)
                    .expect("every read's inode is locked");
                if slot
                    .newest
                    .as_ref()
                    .is_none_or(|newest| read.change_version > newest.change_version)
                {
                    slot.newest = Some(read);
                }
            }
            return reply(&mut locked);
        }
    }

    // item is inode's one StrataItem, for a reply that hands FSKit the item without its attributes.
    pub(crate) fn item(&self, inode: Inode, kind: FileType) -> Retained<StrataItem> {
        loop {
            let slot = self
                .slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(inode)
                .or_default()
                .clone();
            let mut slot = slot.lock().unwrap_or_else(PoisonError::into_inner);
            if !slot.retired {
                return slot
                    .item
                    .get_or_insert_with(|| StrataItem::new(inode, kind))
                    .clone();
            }
        }
    }

    // reclaim lets go of item, unless FSKit handed it out again since asking for the reclaim. The inode's
    // lock is held throughout, so no lookup can hand the item out meanwhile, as FSItem's tryReclaim requires.
    pub(crate) fn reclaim(&self, item: &StrataItem) {
        let inode = *item.inode();
        let Some(slot) = self
            .slots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&inode)
            .cloned()
        else {
            return;
        };
        let mut locked = slot.lock().unwrap_or_else(PoisonError::into_inner);
        // SAFETY: the reclaim block is a plain block, and the inode's lock keeps lookups out.
        let reclaimed = unsafe { item.tryReclaimWithBlock(&RcBlock::new(|| {})) };
        if reclaimed {
            locked.retired = true;
            self.slots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&inode);
        }
    }
}

impl Locked<'_> {
    // attributes fills a snapshot of inode's newest attributes.
    pub(crate) fn attributes(&self, inode: Inode) -> Retained<FSItemAttributes> {
        let newest = self.slots[&inode.0]
            .newest
            .as_ref()
            .expect("reply keeps a read for every inode it locks");
        item_attributes(inode, newest)
    }

    // item is inode's one StrataItem, made the first time FSKit is handed it.
    pub(crate) fn item(&mut self, inode: Inode) -> Retained<StrataItem> {
        let slot = self
            .slots
            .get_mut(&inode.0)
            .expect("an operation only asks for items it locked");
        let kind = slot
            .newest
            .as_ref()
            .expect("reply keeps a read for every inode it locks")
            .kind;
        slot.item
            .get_or_insert_with(|| StrataItem::new(inode, kind))
            .clone()
    }
}
