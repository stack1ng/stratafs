use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::ptr;

use block2::SendableBlock;
use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::{AllocAnyThread, define_class};
use objc2_foundation::{NSError, NSObject, NSString, NSURL};
use objc2_fs_kit::{
    FSContainerIdentifier, FSContainerStatus, FSFileSystemBase, FSPathURLResource, FSProbeResult,
    FSResource, FSTaskOptions, FSUnaryFileSystem, FSUnaryFileSystemOperations, FSVolume,
};

use super::volume::{StoreAccess, StrataVolume, store_name};
use super::{LoadError, identity, posix_error};

define_class!(
    // StrataFileSystem is the principal class the extension's Info.plist names. FSKit makes one per process.
    #[unsafe(super(FSUnaryFileSystem, NSObject))]
    #[name = "StrataFileSystem"]
    pub(crate) struct StrataFileSystem;

    unsafe impl NSObjectProtocol for StrataFileSystem {}

    unsafe impl FSUnaryFileSystemOperations for StrataFileSystem {
        // probeResource says whether a resource is a path a store directory can be at. The store's
        // contents are the extension binary's to judge, when activate opens it.
        #[unsafe(method(probeResource:replyHandler:))]
        fn probe_resource(
            &self,
            resource: &FSResource,
            reply: &SendableBlock<'static, fn(*mut FSProbeResult, *mut NSError)>,
        ) {
            // SAFETY: the probe results are made from valid arguments.
            let result = unsafe {
                match store_of(resource) {
                    Ok((_, store)) => FSProbeResult::usableProbeResultWithName_containerID(
                        &NSString::from_str(&store_name(&store).to_string_lossy()),
                        &FSContainerIdentifier::initWithUUID(
                            FSContainerIdentifier::alloc(),
                            &identity(store.as_os_str().as_bytes()),
                        ),
                    ),
                    Err(_) => FSProbeResult::notRecognizedProbeResult(),
                }
            };
            reply.call(Retained::as_ptr(&result).cast_mut(), ptr::null_mut());
        }

        // loadResource takes the store directory mount was given, and replies with the volume. mount hands
        // its options to activate, not here, so the volume opens its store when FSKit activates it.
        #[unsafe(method(loadResource:options:replyHandler:))]
        fn load_resource(
            &self,
            resource: &FSResource,
            _options: &FSTaskOptions,
            reply: &SendableBlock<'static, fn(*mut FSVolume, *mut NSError)>,
        ) {
            match store_of(resource) {
                Ok((url, store)) => {
                    let volume =
                        Retained::into_super(StrataVolume::new(store, StoreAccess::start(url)));
                    // SAFETY: setting the container status to ready is what FSKit expects before the reply.
                    unsafe { self.setContainerStatus(&FSContainerStatus::ready()) };
                    reply.call(Retained::as_ptr(&volume).cast_mut(), ptr::null_mut());
                }
                Err(error) => {
                    reply.call(
                        ptr::null_mut(),
                        Retained::as_ptr(&ns_error(&error)).cast_mut(),
                    );
                }
            }
        }

        #[unsafe(method(unloadResource:options:replyHandler:))]
        fn unload_resource(
            &self,
            _resource: &FSResource,
            _options: &FSTaskOptions,
            reply: &SendableBlock<'static, fn(*mut NSError)>,
        ) {
            // we implement "unload" behavior in the idiomatic rust way through a Drop trait on the engine.
            reply.call(ptr::null_mut());
        }
    }
);

// store_of is the store directory a path resource names, as its URL and as a path.
fn store_of(resource: &FSResource) -> Result<(Retained<NSURL>, PathBuf), LoadError> {
    let resource = resource
        .downcast_ref::<FSPathURLResource>()
        .ok_or(LoadError::NotAPath)?;
    // SAFETY: a path URL resource always has a URL.
    let url = unsafe { resource.url() };
    let store = url.to_file_path().ok_or(LoadError::NotAPath)?;
    Ok((url, store))
}

fn ns_error(error: &LoadError) -> Retained<NSError> {
    let errno = match error {
        LoadError::NotAPath => libc::EINVAL,
    };
    posix_error(errno, &error.to_string())
}
