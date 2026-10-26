use std::ptr::null_mut;
use std::sync::{Arc, Mutex, PoisonError};

use block2::SendableBlock;
use bytes::Bytes;
use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::{AllocAnyThread, Ivars, Message, define_class, msg_send};
use objc2_foundation::{NSData, NSError, NSInteger, NSObject, NSString};
use objc2_fs_kit::{
    FSActivateResult, FSContext, FSCreateItemResult, FSCreateLinkResult, FSCreateSymlinkResult,
    FSDeactivateOptions, FSDirectoryCookie, FSDirectoryEntryPacker, FSDirectoryVerifier,
    FSEnumerateDirectoryResult, FSFileName, FSGetAttributesResult, FSGetXattrResult, FSItem,
    FSItemGetAttributesRequest, FSItemSetAttributesRequest, FSItemType, FSListXattrsResult,
    FSLookupItemResult, FSMutableFileDataBuffer, FSReadFileResult, FSReadSymlinkResult,
    FSRemoveItemResult, FSRenameItemResult, FSSetAttributesResult, FSSetXattrPolicy,
    FSSetXattrResult, FSStatFSResult, FSSyncFlags, FSTaskOptions, FSVolume, FSVolumeCaseFormat,
    FSVolumeCommonOperations, FSVolumeHandler, FSVolumeIdentifier, FSVolumePathConfOperations,
    FSVolumeReadWriteHandler, FSVolumeSupportedCapabilities, FSVolumeXattrHandler,
    FSWriteFileResult,
};
use stratafs_fs_engine::attributes::FileType;
use stratafs_fs_engine::directories::NewItem;
use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::{Inode, LARGEST_NAME_LENGTH};

use super::changes::attribute_change;
use super::item::{StrataItem, name_bytes};
use super::operations::{Handed, Reply, VolumeOperations};
use super::{OpenedVolume, errno, identity, posix_error};

// FILE_SYSTEM_TYPE is the type name statfs reports for a mounted volume.
const FILE_SYSTEM_TYPE: &str = "stratafs";

// REPORTED_BYTES is the size, free space and file count a volume reports. The engine has no capacity of
// its own, so these only need to be large enough that nothing refuses to write.
const REPORTED_BYTES: u64 = 1 << 50;
const BLOCK_SIZE: u64 = 4096;

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

    // FSVolumeHandler re-declares the methods it shares with FSVolumeCommonOperations, so they are defined
    // here, and objc2 counts them toward FSVolumeHandler.
    unsafe impl FSVolumeHandler for StrataVolume {
        #[unsafe(method(supportedVolumeCapabilities))]
        fn supported_volume_capabilities(&self) -> Retained<FSVolumeSupportedCapabilities> {
            // SAFETY: the capabilities are a plain value object, and every setter takes a plain value.
            unsafe {
                let capabilities: Retained<FSVolumeSupportedCapabilities> =
                    msg_send![FSVolumeSupportedCapabilities::alloc(), init];
                capabilities.setSupportsPersistentObjectIDs(true);
                capabilities.setSupports64BitObjectIDs(true);
                capabilities.setSupportsHardLinks(true);
                capabilities.setSupportsSymbolicLinks(true);
                capabilities.setSupportsSparseFiles(true);
                capabilities.setSupportsHiddenFiles(true);
                capabilities.setSupports2TBFiles(true);
                capabilities.setSupportsFastStatFS(true);
                capabilities.setCaseFormat(FSVolumeCaseFormat::Sensitive);
                capabilities
            }
        }

        #[unsafe(method(volumeStatistics))]
        fn volume_statistics(&self) -> Retained<FSStatFSResult> {
            // SAFETY: the statistics are a plain value object, and every setter takes a plain value.
            unsafe {
                let statistics = FSStatFSResult::initWithFileSystemTypeName(
                    FSStatFSResult::alloc(),
                    &NSString::from_str(FILE_SYSTEM_TYPE),
                );
                statistics.setBlockSize(BLOCK_SIZE as NSInteger);
                statistics.setIoSize(BLOCK_SIZE as NSInteger);
                statistics.setTotalBlocks(REPORTED_BYTES / BLOCK_SIZE);
                statistics.setAvailableBlocks(REPORTED_BYTES / BLOCK_SIZE);
                statistics.setFreeBlocks(REPORTED_BYTES / BLOCK_SIZE);
                statistics.setTotalBytes(REPORTED_BYTES);
                statistics.setAvailableBytes(REPORTED_BYTES);
                statistics.setFreeBytes(REPORTED_BYTES);
                statistics.setTotalFiles(REPORTED_BYTES);
                statistics.setFreeFiles(REPORTED_BYTES);
                statistics
            }
        }

        #[unsafe(method(mountWithOptions:replyHandler:))]
        fn mount(
            &self,
            _options: &FSTaskOptions,
            reply: &SendableBlock<'static, fn(*mut NSError)>,
        ) {
            reply.call(null_mut());
        }

        #[unsafe(method(unmountWithReplyHandler:))]
        fn unmount(&self, reply: &SendableBlock<'static, fn()>) {
            reply.call();
        }

        // synchronize has nothing to flush: every operation is durable once its transaction commits.
        #[unsafe(method(synchronizeWithFlags:replyHandler:))]
        fn synchronize(
            &self,
            _flags: FSSyncFlags,
            reply: &SendableBlock<'static, fn(*mut NSError)>,
        ) {
            reply.call(null_mut());
        }

        #[unsafe(method(reclaimItem:replyHandler:))]
        fn reclaim(&self, item: &FSItem, reply: &SendableBlock<'static, fn(*mut NSError)>) {
            if let Some(operations) = self.active_operations() {
                operations.items().reclaim(StrataItem::of(item));
            }
            reply.call(null_mut());
        }

        // activate hands FSKit the root directory.
        #[unsafe(method(activateVolumeWithOptions:replyHandler:))]
        fn activate(
            &self,
            _options: &FSTaskOptions,
            reply: &SendableBlock<'static, fn(*mut FSActivateResult, *mut NSError)>,
        ) {
            let Some(operations) = self.active_operations() else {
                return reply.call(null_mut(), deactivated().cast_mut());
            };
            let root = operations.items().item(Inode::ROOT, FileType::Directory);
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
            let inode = *StrataItem::of(item).inode();
            self.forward(reply, |operations, reply| {
                operations.get_attributes(inode, reply)
            });
        }

        #[unsafe(method(setAttributes:onItem:context:replyHandler:))]
        fn set_attributes(
            &self,
            request: &FSItemSetAttributesRequest,
            item: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSSetAttributesResult, *mut NSError)>,
        ) {
            let inode = *StrataItem::of(item).inode();
            let change = match attribute_change(request) {
                Ok(change) => change,
                Err(error) => return reply.call(null_mut(), Retained::as_ptr(&error).cast_mut()),
            };
            self.forward(reply, |operations, reply| {
                operations.set_attributes(inode, change, reply)
            });
        }

        #[unsafe(method(lookupItemNamed:inDirectory:context:replyHandler:))]
        fn lookup(
            &self,
            name: &FSFileName,
            directory: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSLookupItemResult, *mut NSError)>,
        ) {
            let (directory, name) = (*StrataItem::of(directory).inode(), name_bytes(name));
            self.forward(reply, |operations, reply| {
                operations.lookup(directory, name, reply)
            });
        }

        #[unsafe(method(readSymbolicLink:context:replyHandler:))]
        fn read_symbolic_link(
            &self,
            item: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSReadSymlinkResult, *mut NSError)>,
        ) {
            let inode = *StrataItem::of(item).inode();
            self.forward(reply, |operations, reply| {
                operations.read_symbolic_link(inode, reply)
            });
        }

        #[unsafe(method(createItemNamed:type:inDirectory:attributes:context:replyHandler:))]
        fn create(
            &self,
            name: &FSFileName,
            item_type: FSItemType,
            directory: &FSItem,
            attributes: &FSItemSetAttributesRequest,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSCreateItemResult, *mut NSError)>,
        ) {
            let item = match item_type {
                FSItemType::File => NewItem::File,
                FSItemType::Directory => NewItem::Directory,
                _ => {
                    let error =
                        posix_error(libc::ENOTSUP, "only files and directories can be made here");
                    return reply.call(null_mut(), Retained::as_ptr(&error).cast_mut());
                }
            };
            let (directory, name) = (*StrataItem::of(directory).inode(), name_bytes(name));
            let change = match attribute_change(attributes) {
                Ok(change) => change,
                Err(error) => return reply.call(null_mut(), Retained::as_ptr(&error).cast_mut()),
            };
            self.forward(reply, |operations, reply| {
                operations.create(directory, name, item, change, reply)
            });
        }

        #[unsafe(method(createSymbolicLinkNamed:inDirectory:attributes:linkContents:context:replyHandler:))]
        fn create_symbolic_link(
            &self,
            name: &FSFileName,
            directory: &FSItem,
            attributes: &FSItemSetAttributesRequest,
            contents: &FSFileName,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSCreateSymlinkResult, *mut NSError)>,
        ) {
            let (directory, name) = (*StrataItem::of(directory).inode(), name_bytes(name));
            let target = name_bytes(contents);
            let change = match attribute_change(attributes) {
                Ok(change) => change,
                Err(error) => return reply.call(null_mut(), Retained::as_ptr(&error).cast_mut()),
            };
            self.forward(reply, |operations, reply| {
                operations.create_symbolic_link(directory, name, target, change, reply)
            });
        }

        #[unsafe(method(createLinkToItem:named:inDirectory:context:replyHandler:))]
        fn create_link(
            &self,
            item: &FSItem,
            name: &FSFileName,
            directory: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSCreateLinkResult, *mut NSError)>,
        ) {
            let inode = *StrataItem::of(item).inode();
            let (directory, name) = (*StrataItem::of(directory).inode(), name_bytes(name));
            self.forward(reply, |operations, reply| {
                operations.create_link(inode, directory, name, reply)
            });
        }

        #[unsafe(method(removeItem:named:fromDirectory:context:replyHandler:))]
        fn remove(
            &self,
            item: &FSItem,
            name: &FSFileName,
            directory: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSRemoveItemResult, *mut NSError)>,
        ) {
            let item = StrataItem::of(item);
            let (inode, kind) = (*item.inode(), *item.kind());
            let (directory, name) = (*StrataItem::of(directory).inode(), name_bytes(name));
            self.forward(reply, |operations, reply| {
                operations.remove(inode, kind, directory, name, reply)
            });
        }

        // The parameters are FSKit's own selector.
        #[allow(clippy::too_many_arguments)]
        #[unsafe(method(renameItem:inDirectory:named:toNewName:inDirectory:overItem:context:replyHandler:))]
        fn rename(
            &self,
            item: &FSItem,
            source_directory: &FSItem,
            source_name: &FSFileName,
            destination_name: &FSFileName,
            destination_directory: &FSItem,
            over_item: Option<&FSItem>,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSRenameItemResult, *mut NSError)>,
        ) {
            let inode = *StrataItem::of(item).inode();
            let from = (
                *StrataItem::of(source_directory).inode(),
                name_bytes(source_name),
            );
            let to = (
                *StrataItem::of(destination_directory).inode(),
                name_bytes(destination_name),
            );
            let replaced = over_item.map(|replaced| *StrataItem::of(replaced).inode());
            self.forward(reply, |operations, reply| {
                operations.rename(inode, from, to, replaced, reply)
            });
        }

        // The parameters are FSKit's own selector.
        #[allow(clippy::too_many_arguments)]
        #[unsafe(method(enumerateDirectory:startingAtCookie:verifier:providingAttributes:usingPacker:context:replyHandler:))]
        fn enumerate_directory(
            &self,
            directory: &FSItem,
            cookie: FSDirectoryCookie,
            verifier: FSDirectoryVerifier,
            attributes: Option<&FSItemGetAttributesRequest>,
            packer: &FSDirectoryEntryPacker,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSEnumerateDirectoryResult, *mut NSError)>,
        ) {
            let directory = *StrataItem::of(directory).inode();
            let (with_attributes, packer) = (attributes.is_some(), Handed(packer.retain()));
            self.forward(reply, |operations, reply| {
                operations.enumerate_directory(
                    directory,
                    cookie,
                    verifier,
                    with_attributes,
                    packer,
                    reply,
                )
            });
        }
    }

    unsafe impl FSVolumeReadWriteHandler for StrataVolume {
        #[unsafe(method(readFromFile:offset:length:intoBuffer:replyHandler:))]
        fn read(
            &self,
            item: &FSItem,
            offset: libc::off_t,
            length: usize,
            buffer: &FSMutableFileDataBuffer,
            reply: &SendableBlock<'static, fn(*mut FSReadFileResult, *mut NSError)>,
        ) {
            let inode = *StrataItem::of(item).inode();
            let Ok(offset) = u64::try_from(offset) else {
                return invalid_offset(reply);
            };
            let buffer = Handed(buffer.retain());
            self.forward(reply, |operations, reply| {
                operations.read(inode, offset, length, buffer, reply)
            });
        }

        #[unsafe(method(writeContents:toFile:atOffset:replyHandler:))]
        fn write(
            &self,
            contents: &NSData,
            item: &FSItem,
            offset: libc::off_t,
            reply: &SendableBlock<'static, fn(*mut FSWriteFileResult, *mut NSError)>,
        ) {
            let inode = *StrataItem::of(item).inode();
            let Ok(offset) = u64::try_from(offset) else {
                return invalid_offset(reply);
            };
            let contents = Bytes::from(contents.to_vec());
            self.forward(reply, |operations, reply| {
                operations.write(inode, offset, contents, reply)
            });
        }
    }

    unsafe impl FSVolumeXattrHandler for StrataVolume {
        #[unsafe(method(getXattrNamed:ofItem:context:replyHandler:))]
        fn get_xattr(
            &self,
            name: &FSFileName,
            item: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSGetXattrResult, *mut NSError)>,
        ) {
            let (inode, name) = (*StrataItem::of(item).inode(), name_bytes(name));
            self.forward(reply, |operations, reply| {
                operations.get_xattr(inode, name, reply)
            });
        }

        #[unsafe(method(setXattrNamed:toData:onItem:policy:context:replyHandler:))]
        fn set_xattr(
            &self,
            name: &FSFileName,
            value: Option<&NSData>,
            item: &FSItem,
            policy: FSSetXattrPolicy,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSSetXattrResult, *mut NSError)>,
        ) {
            let (inode, name) = (*StrataItem::of(item).inode(), name_bytes(name));
            let value = value.map(|value| Bytes::from(value.to_vec()));
            self.forward(reply, |operations, reply| {
                operations.set_xattr(inode, name, value, policy, reply)
            });
        }

        #[unsafe(method(listXattrsOfItem:context:replyHandler:))]
        fn list_xattrs(
            &self,
            item: &FSItem,
            _context: &FSContext,
            reply: &SendableBlock<'static, fn(*mut FSListXattrsResult, *mut NSError)>,
        ) {
            let inode = *StrataItem::of(item).inode();
            self.forward(reply, |operations, reply| {
                operations.list_xattrs(inode, reply)
            });
        }
    }
);

// SAFETY: StrataVolume defines every FSVolumeCommonOperations method as part of FSVolumeHandler, which
// re-declares them all. Conforming to FSVolumeHandler also conforms it to FSVolumeCommonOperations.
unsafe impl FSVolumeCommonOperations for StrataVolume {}

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

    // forward hands an operation the volume's operations and a copy of its reply, or replies that the
    // volume has been deactivated.
    fn forward<R: Message>(
        &self,
        reply: &SendableBlock<'static, fn(*mut R, *mut NSError)>,
        operation: impl FnOnce(Arc<dyn VolumeOperations>, Reply<R>),
    ) {
        let reply = reply.copy();
        match self.active_operations() {
            Some(operations) => operation(operations, reply),
            None => reply.call(null_mut(), deactivated().cast_mut()),
        }
    }
}

// deactivated is the error an operation gets once deactivate has dropped the engine.
fn deactivated() -> *const NSError {
    Retained::autorelease_ptr(posix_error(libc::ENXIO, "the volume has been deactivated"))
}

fn invalid_offset<R: Message>(reply: &SendableBlock<'static, fn(*mut R, *mut NSError)>) {
    let error = posix_error(errno(PosixError::EINVAL), "the offset is negative");
    reply.call(null_mut(), Retained::as_ptr(&error).cast_mut());
}
