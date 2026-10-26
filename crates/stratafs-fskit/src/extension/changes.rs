use std::sync::Arc;

use objc2::msg_send;
use objc2::rc::Retained;
use objc2_foundation::NSError;
use objc2_fs_kit::{FSItemAttribute, FSItemSetAttributesRequest};
use stratafs_fs_engine::attributes::{Mode, SettableAttributes};

use super::item::{restrictions_and_hints, timestamp};
use super::posix_error;

// AttributeChange changes an item's settable attributes. The store can retry a transaction, so it can run
// more than once.
pub(crate) type AttributeChange = Arc<dyn Fn(&mut SettableAttributes) + Send + Sync>;

// APPLIED are the attributes a change can set. The rest belong to the engine.
const APPLIED: FSItemAttribute = FSItemAttribute::Mode
    .union(FSItemAttribute::UID)
    .union(FSItemAttribute::GID)
    .union(FSItemAttribute::Size)
    .union(FSItemAttribute::AccessTime)
    .union(FSItemAttribute::ModifyTime)
    .union(FSItemAttribute::BirthTime)
    .union(FSItemAttribute::Flags);

// attribute_change reads what request asks to change on FSKit's thread, tells FSKit which of those it
// applies, and returns the change for the operation to apply on its own thread. It refuses flags the volume
// does not store.
pub(crate) fn attribute_change(
    request: &FSItemSetAttributesRequest,
) -> Result<AttributeChange, Retained<NSError>> {
    // SAFETY: isValid and the getters read plain values, and the times are timespecs.
    unsafe {
        let wants = |attribute| request.isValid(attribute);
        // Only the permission bits: the type bits are fixed when an item is made.
        let mode = wants(FSItemAttribute::Mode)
            .then(|| Mode::from_bits_truncate((request.mode() & 0o7777) as u16));
        let user_id = wants(FSItemAttribute::UID).then(|| request.uid());
        let group_id = wants(FSItemAttribute::GID).then(|| request.gid());
        let size = wants(FSItemAttribute::Size).then(|| request.size());
        let accessed =
            wants(FSItemAttribute::AccessTime).then(|| timestamp(msg_send![request, accessTime]));
        let modified =
            wants(FSItemAttribute::ModifyTime).then(|| timestamp(msg_send![request, modifyTime]));
        let created =
            wants(FSItemAttribute::BirthTime).then(|| timestamp(msg_send![request, birthTime]));
        let flags = if wants(FSItemAttribute::Flags) {
            let flags = restrictions_and_hints(request.flags()).ok_or_else(|| {
                posix_error(
                    libc::ENOTSUP,
                    "only the immutable, append only and hidden flags can be set here",
                )
            })?;
            Some(flags)
        } else {
            None
        };
        let (restrictions, hints) = flags.unzip();
        request.setConsumedAttributes(
            APPLIED
                .iter()
                .filter(|attribute| wants(*attribute))
                .collect(),
        );
        Ok(Arc::new(move |settable| {
            settable.mode = mode.unwrap_or(settable.mode);
            settable.ownership.user_id = user_id.unwrap_or(settable.ownership.user_id);
            settable.ownership.group_id = group_id.unwrap_or(settable.ownership.group_id);
            settable.size = size.unwrap_or(settable.size);
            settable.last_accessed_at = accessed.unwrap_or(settable.last_accessed_at);
            settable.content_changed_at = modified.unwrap_or(settable.content_changed_at);
            settable.created_at = created.unwrap_or(settable.created_at);
            settable.restrictions = restrictions.unwrap_or(settable.restrictions);
            settable.hints = hints.unwrap_or(settable.hints);
        }))
    }
}
