use std::sync::Arc;
use std::time::SystemTime;

use fuser::TimeOrNow;
use stratafs_fs_engine::attributes::{Mode, SettableAttributes};

use super::item::timestamp;

// AttributeChange changes an item's settable attributes. The store can retry a transaction, so it can run
// more than once.
pub(crate) type AttributeChange = Arc<dyn Fn(&mut SettableAttributes) + Send + Sync>;

// Requested is what a setattr asks to change. The kernel reports the time it changed an item at itself, which the
// engine keeps instead.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Requested {
    pub(crate) mode: Option<u32>,
    pub(crate) user_id: Option<u32>,
    pub(crate) group_id: Option<u32>,
    pub(crate) size: Option<u64>,
    pub(crate) accessed: Option<TimeOrNow>,
    pub(crate) modified: Option<TimeOrNow>,
}

impl Requested {
    // change is the change as the engine applies it, with "now" read once, as the request arrives.
    pub(crate) fn change(self) -> AttributeChange {
        let now = SystemTime::now();
        let at = |time: Option<TimeOrNow>| {
            time.map(|time| match time {
                TimeOrNow::SpecificTime(time) => timestamp(time),
                TimeOrNow::Now => timestamp(now),
            })
        };
        let (accessed, modified) = (at(self.accessed), at(self.modified));
        let mode = self.mode.map(permissions);
        Arc::new(move |settable| {
            settable.mode = mode.unwrap_or(settable.mode);
            settable.ownership.user_id = self.user_id.unwrap_or(settable.ownership.user_id);
            settable.ownership.group_id = self.group_id.unwrap_or(settable.ownership.group_id);
            settable.size = self.size.unwrap_or(settable.size);
            settable.last_accessed_at = accessed.unwrap_or(settable.last_accessed_at);
            settable.content_changed_at = modified.unwrap_or(settable.content_changed_at);
        })
    }
}

// made is the change that gives an item a new request makes its mode and owner. The kernel has already applied the
// caller's umask to mode.
pub(crate) fn made(mode: u32, user_id: u32, group_id: u32) -> AttributeChange {
    Requested {
        mode: Some(mode),
        user_id: Some(user_id),
        group_id: Some(group_id),
        ..Requested::default()
    }
    .change()
}

// permissions are a mode's permission bits: the type bits are fixed when an item is made.
fn permissions(mode: u32) -> Mode {
    Mode::from_bits_truncate((mode & 0o7777) as u16)
}

#[cfg(test)]
mod tests;
