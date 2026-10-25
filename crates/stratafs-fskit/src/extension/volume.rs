use std::any::Any;

use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::{AllocAnyThread, define_class, msg_send};
use objc2_foundation::{NSObject, NSString};
use objc2_fs_kit::{FSFileName, FSVolume, FSVolumeIdentifier};

use super::{OpenedVolume, identity};

define_class!(
    // StrataVolume is the volume FSKit mounts. It holds the engine opened for it.
    #[unsafe(super(FSVolume, NSObject))]
    #[name = "StrataVolume"]
    #[ivars = Box<dyn Any + Send + Sync>]
    pub(crate) struct StrataVolume;

    unsafe impl NSObjectProtocol for StrataVolume {}
);

impl StrataVolume {
    pub(crate) fn new(opened: OpenedVolume) -> Retained<Self> {
        // SAFETY: FSVolumeIdentifier and FSFileName are plain value objects made from valid arguments.
        let (volume_id, volume_name) = unsafe {
            (
                FSVolumeIdentifier::initWithUUID(
                    FSVolumeIdentifier::alloc(),
                    &identity(&opened.id),
                ),
                FSFileName::nameWithString(&NSString::from_str(&hex::encode(&opened.id))),
            )
        };
        let this = Self::alloc().set_ivars(opened.engine);
        // SAFETY: initWithVolumeID:volumeName: is FSVolume's designated initializer.
        unsafe { msg_send![super(this), initWithVolumeID: &*volume_id, volumeName: &*volume_name] }
    }
}
