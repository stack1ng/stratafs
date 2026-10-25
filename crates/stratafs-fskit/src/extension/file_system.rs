use std::ptr;

use block2::SendableBlock;
use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::{AllocAnyThread, define_class};
use objc2_foundation::{NSError, NSObject, NSString};
use objc2_fs_kit::{
    FSContainerIdentifier, FSContainerStatus, FSFileSystemBase, FSGenericURLResource,
    FSProbeResult, FSResource, FSTaskOptions, FSUnaryFileSystem, FSUnaryFileSystemOperations,
    FSVolume,
};

use super::volume::StrataVolume;
use super::{LoadError, identity, open_volume, posix_error};
use crate::volume_url::VolumeUrl;

define_class!(
    // StrataFileSystem is the principal class the extension's Info.plist names. FSKit makes one per process.
    #[unsafe(super(FSUnaryFileSystem, NSObject))]
    #[name = "StrataFileSystem"]
    pub(crate) struct StrataFileSystem;

    unsafe impl NSObjectProtocol for StrataFileSystem {}

    unsafe impl FSUnaryFileSystemOperations for StrataFileSystem {
        // probeResource says whether a resource is a volume URL. Mounting a URL skips the probe,
        // so this only answers tools that ask.
        #[unsafe(method(probeResource:replyHandler:))]
        fn probe_resource(
            &self,
            resource: &FSResource,
            reply: &SendableBlock<'static, fn(*mut FSProbeResult, *mut NSError)>,
        ) {
            let volume_id = url_of(resource).and_then(|url| {
                url.parse::<VolumeUrl<bytes::Bytes>>()
                    .map(|url| url.volume)
                    .map_err(|error| LoadError::InvalidUrl(Box::new(error)))
            });
            // SAFETY: the probe results are made from valid arguments.
            let result = unsafe {
                match volume_id {
                    Ok(volume_id) => FSProbeResult::usableProbeResultWithName_containerID(
                        &NSString::from_str(&hex::encode(&volume_id)),
                        &FSContainerIdentifier::initWithUUID(
                            FSContainerIdentifier::alloc(),
                            &identity(&volume_id),
                        ),
                    ),
                    Err(_) => FSProbeResult::notRecognizedProbeResult(),
                }
            };
            reply.call(Retained::as_ptr(&result).cast_mut(), ptr::null_mut());
        }

        // loadResource opens the volume the mount URL names, and replies with it.
        #[unsafe(method(loadResource:options:replyHandler:))]
        fn load_resource(
            &self,
            resource: &FSResource,
            _options: &FSTaskOptions,
            reply: &SendableBlock<'static, fn(*mut FSVolume, *mut NSError)>,
        ) {
            match url_of(resource).and_then(|url| open_volume(&url)) {
                Ok(opened) => {
                    let volume = Retained::into_super(StrataVolume::new(opened));
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

fn url_of(resource: &FSResource) -> Result<String, LoadError> {
    let resource = resource
        .downcast_ref::<FSGenericURLResource>()
        .ok_or(LoadError::NoUrl)?;
    // SAFETY: a generic URL resource always has a URL.
    let url = unsafe { resource.url() };
    url.absoluteString()
        .map(|url| url.to_string())
        .ok_or(LoadError::NoUrl)
}

fn ns_error(error: &LoadError) -> Retained<NSError> {
    let errno = match error {
        LoadError::NoUrl | LoadError::InvalidUrl(_) => libc::EINVAL,
        LoadError::OpenFailed(_) => libc::EIO,
    };
    posix_error(errno, &error.to_string())
}
