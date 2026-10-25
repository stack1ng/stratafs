use std::ptr::null_mut;
use std::sync::{Arc, Mutex, PoisonError};

use block2::SendableBlock;
use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::{AllocAnyThread, Ivars, define_class, msg_send};
use objc2_foundation::{NSError, NSInteger, NSObject, NSString};
use objc2_fs_kit::{
    FSActivateResult, FSContext, FSDeactivateOptions, FSFileName, FSGetAttributesResult, FSItem,
    FSItemGetAttributesRequest, FSTaskOptions, FSVolume, FSVolumeCommonOperations, FSVolumeHandler,
    FSVolumeIdentifier, FSVolumePathConfOperations,
};
use stratafs_fs_engine::{Inode, LARGEST_NAME_LENGTH};

use super::item::StrataItem;
use super::operations::VolumeOperations;
use super::{OpenedVolume, identity, posix_error};

define_class!(
    // StrataVolume is the volume FSKit mounts. It forwards each operation to the volume's operations,
    // which it holds until deactivate.
    #[unsafe(super(FSVolume, NSObject))]
    #[name = "StrataVolume"]
    pub(crate) struct StrataVolume {
        operations: Mutex<Option<Arc<dyn VolumeOperations>>>,
    }

    unsafe impl NSObjectProtocol for StrataVolume {}

    unsafe impl FSVolumePathConfOperations for StrataVolume {
        #[unsafe(method(maximumLinkCount))]
        fn maximum_link_count(&self) -> NSInteger {
            NSInteger::try_from(u32::MAX).expect("a u32 fits in an NSInteger")
        }

        #[unsafe(method(maximumNameLength))]
        fn maximum_name_length(&self) -> NSInteger {
            NSInteger::try_from(LARGEST_NAME_LENGTH).expect("the longest name fits in an NSInteger")
        }

        #[unsafe(method(restrictsOwnershipChanges))]
        fn restricts_ownership_changes(&self) -> bool {
            false
        }

        #[unsafe(method(truncatesLongNames))]
        fn truncates_long_names(&self) -> bool {
            false
        }
    }

    unsafe impl FSVolumeCommonOperations for StrataVolume {}

    unsafe impl FSVolumeHandler for StrataVolume {
        // activate hands FSKit the root directory.
        #[unsafe(method(activateVolumeWithOptions:replyHandler:))]
        fn activate(
            &self,
            _options: &FSTaskOptions,
            reply: &SendableBlock<'static, fn(*mut FSActivateResult, *mut NSError)>,
        ) {
            let root = StrataItem::new(Inode::ROOT);
            // SAFETY: the root is a live item.
            let result =
                unsafe { FSActivateResult::initWithRootItem(FSActivateResult::alloc(), &root) }
                    .expect("an activate result holds any root item");
            reply.call(Retained::as_ptr(&result).cast_mut(), null_mut());
        }

        // deactivate is the last call FSKit makes before it kills the process, without releasing the
        // volume first, so the engine is dropped here or not at all.
        #[unsafe(method(deactivateVolumeWithOptions:replyHandler:))]
        fn deactivate(
            &self,
            _options: FSDeactivateOptions,
            reply: &SendableBlock<'static, fn(*mut NSError)>,
        ) {
            drop(
                self.operations()
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take(),
            );
            reply.call(null_mut());
        }

        #[unsafe(method(getAttributes:ofItem:context:replyHandler:))]
        fn get_attributes(
            &self,
            _wanted: &FSItemGetAttributesRequest,
            item: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSGetAttributesResult, *mut NSError)>,
        ) {
            let reply = reply.copy();
            match self.active_operations() {
                Some(operations) => operations.get_attributes(StrataItem::inode_of(item), reply),
                None => reply.call(null_mut(), deactivated().cast_mut()),
            }
        }
    }
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
        let this = Self::alloc().set_ivars(Ivars::<Self> {
            operations: Mutex::new(Some(opened.operations)),
        });
        // SAFETY: initWithVolumeID:volumeName: is FSVolume's designated initializer.
        unsafe { msg_send![super(this), initWithVolumeID: &*volume_id, volumeName: &*volume_name] }
    }

    // active_operations are the volume's operations, until deactivate drops them and the engine with them.
    fn active_operations(&self) -> Option<Arc<dyn VolumeOperations>> {
        self.operations()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

// deactivated is the error an operation gets once deactivate has dropped the engine.
fn deactivated() -> *const NSError {
    Retained::autorelease_ptr(posix_error(libc::ENXIO, "the volume has been deactivated"))
}
