use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex, PoisonError};

use objc2_fs_kit::FSItemAttributes;
use stratafs_fs_engine::Inode;
use stratafs_fs_engine::attributes::Attributes;

use super::item::item_attributes;

// Snapshots keeps, for each inode, the newest attributes this volume has handed to FSKit.
// FSKit numbers every FSItemAttributes as it is filled in, and the kernel keeps the highest number as
// the item's attributes. Operations run in parallel, so an operation that read older attributes could
// otherwise fill its snapshot last and win.
#[derive(Default)]
pub(crate) struct Snapshots {
    newest: Mutex<HashMap<Inode, Arc<Mutex<Attributes>>>>,
}

impl Snapshots {
    // reply fills inode's snapshot from whichever is newer, read or the newest already handed out,
    // and replies with it. The inode's lock is held from the comparison until reply returns, so the
    // snapshots of one inode are filled and handed out in the order of the attributes they hold.
    pub(crate) fn reply(
        &self,
        inode: Inode,
        read: Attributes,
        reply: impl FnOnce(&FSItemAttributes),
    ) {
        let (slot, unrecorded) = match self
            .newest
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(inode)
        {
            Entry::Occupied(slot) => (slot.get().clone(), Some(read)),
            Entry::Vacant(slot) => (slot.insert(Arc::new(Mutex::new(read))).clone(), None),
        };
        let mut newest = slot.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(read) = unrecorded
            && read.change_version > newest.change_version
        {
            *newest = read;
        }
        reply(&item_attributes(inode, &newest));
    }
}
