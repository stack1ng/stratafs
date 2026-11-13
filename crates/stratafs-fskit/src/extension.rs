#![allow(unsafe_code)]

mod changes;
mod file_system;
mod item;
mod items;
mod operations;
mod volume;

use std::error::Error;
use std::ffi::{CString, c_char, c_int};
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use futures_lite::future::block_on;
use objc2::ClassType;
use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2_foundation::{
    NSDictionary, NSError, NSInteger, NSLocalizedDescriptionKey, NSPOSIXErrorDomain, NSString,
    NSUUID,
};
use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::{Cached, EngineSpecification, FsConfig, FsEngine, Timestamp};
use stratafs_pubsub::PubSub;
use stratafs_store::store::Store;

use file_system::StrataFileSystem;
use items::Items;
use operations::{Volume, VolumeOperations, make_durable_on_cadence};
pub use volume::FsKitCache;
use volume::StrataVolume;

// OpenedVolume is an activated volume: the bytes that name it to FSKit, and its operations on its engine.
pub(crate) struct OpenedVolume {
    id: Bytes,
    operations: Arc<dyn VolumeOperations>,
}

// Request is a mount as FSKit hands it over: the store directory mount was given, and the arguments it passed
// activate, such as each -o option. The library reads none of it.
pub struct Request {
    pub resource: PathBuf,
    pub arguments: Vec<String>,
}

// Opened is what run's open returns for a request: the specification of the volume's engine, of whichever store,
// consistency and configuration, with the bytes that name the volume to FSKit. It becomes the volume's operations
// once it has the kernel's cache.
#[allow(clippy::type_complexity)]
pub struct Opened(Box<dyn FnOnce(FsKitCache) -> Result<OpenedVolume, ActivateError>>);

impl Opened {
    pub fn new<St, P, A, N, S, C>(
        name: Bytes,
        specification: EngineSpecification<St, P, A, N, S, C>,
    ) -> Self
    where
        St: Store,
        P: PubSub<Message = u64>,
        A: Fn() -> u64 + Send + Sync + 'static,
        N: Fn() -> Timestamp + Send + Sync + 'static,
        S: FsConfig,
        C: Cached<St, P>,
    {
        Opened(Box::new(move |kernel| {
            let engine =
                block_on(FsEngine::new(specification, kernel.clone())).map_err(|error| {
                    ActivateError::OpenFailed {
                        posix: PosixError::EIO,
                        source: Box::new(error),
                    }
                })?;
            let (stops_durability_cadence, durability_cadence_stopped) = mpsc::channel();
            let volume = Arc::new(Volume {
                runtime: Arc::clone(engine.runtime()),
                engine,
                items: Items::default(),
                kernel,
                stops_durability_cadence,
            });
            make_durable_on_cadence(
                Arc::downgrade(&volume),
                durability_cadence_stopped,
                DURABILITY_CADENCE,
            );
            Ok(OpenedVolume {
                id: name,
                operations: volume,
            })
        }))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LoadError {
    #[error("the resource is not the path of a store directory")]
    NotAPath,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActivateError {
    #[error("the volume could not be opened: {source}")]
    OpenFailed {
        posix: PosixError,
        source: Box<dyn Error + Send + Sync>,
    },
}

type OpenVolume =
    Box<dyn Fn(&Path, &[String], FsKitCache) -> Result<OpenedVolume, ActivateError> + Send + Sync>;

// OPEN_VOLUME is how activate opens a volume. It is the one static: NSExtensionMain creates
// StrataFileSystem itself, with no arguments, so this is the only way run's arguments reach it.
// Everything loadResource builds from here on gets what it needs passed in.
static OPEN_VOLUME: OnceLock<OpenVolume> = OnceLock::new();

// run starts the FSKit extension and never returns. The extension binary's main calls it.
// When FSKit activates a mounted volume, run's open gets the request and returns the specification of the volume's
// engine, which then keeps the kernel's cache in line. Its errors reach mount as the POSIX errors they stand for.
// FSKit hands every operation over on one thread and does not wait for the reply. Each operation runs on
// that thread until it finishes or yields, as a store does while it waits for the disk to make commits durable, and then
// finishes on the specification's runtime, so FSKit can hand over the next one. Store work that blocks without
// yielding, such as file IO on this machine's disk, holds up that thread. The runtime must run on threads of its own:
// NSExtensionMain keeps the main thread.
pub fn run<F>(open: impl AsyncFn(Request) -> Result<Opened, F> + Send + Sync + 'static) -> !
where
    F: Error + Send + Sync + 'static,
    for<'e> PosixError: From<&'e F>,
{
    let open: OpenVolume = Box::new(move |store, arguments, kernel| {
        let request = Request {
            resource: store.to_owned(),
            arguments: arguments.to_vec(),
        };
        let opened = block_on(open(request)).map_err(|error| ActivateError::OpenFailed {
            posix: PosixError::from(&error),
            source: Box::new(error),
        })?;
        (opened.0)(kernel)
    });
    if OPEN_VOLUME.set(open).is_err() {
        panic!("run is called once per extension process");
    }
    // NSExtensionMain finds our classes by the names in Info.plist, so they must exist first.
    StrataFileSystem::class();
    StrataVolume::class();
    let arguments: Vec<CString> = std::env::args_os()
        .map(|argument| CString::new(argument.into_vec()).expect("an argument has no NUL byte"))
        .collect();
    let mut argv: Vec<*mut c_char> = arguments
        .iter()
        .map(|argument| argument.as_ptr().cast_mut())
        .chain([std::ptr::null_mut()])
        .collect();
    let argc = c_int::try_from(arguments.len()).expect("the argument count fits in a C int");
    // SAFETY: argv holds argc NUL terminated strings and a final null, all alive until the process exits.
    let status = unsafe { NSExtensionMain(argc, argv.as_mut_ptr()) };
    std::process::exit(status)
}

// DURABILITY_CADENCE is how often a volume makes every committed operation durable, the period FSKit's own periodic
// sync uses. FSKit gives no usable signal for when an application needs a sync, so the volume keeps to this.
const DURABILITY_CADENCE: Duration = Duration::from_secs(30);

pub(crate) fn open_volume(
    store: &Path,
    arguments: &[String],
    kernel: FsKitCache,
) -> Result<OpenedVolume, ActivateError> {
    let open = OPEN_VOLUME
        .get()
        .expect("run stores the volume opener before FSKit can call in");
    open(store, arguments, kernel)
}

// errno is a POSIX error's number on macOS.
pub(crate) fn errno(error: PosixError) -> c_int {
    match error {
        PosixError::EBUSY => libc::EBUSY,
        PosixError::EEXIST => libc::EEXIST,
        PosixError::EFBIG => libc::EFBIG,
        PosixError::EINVAL => libc::EINVAL,
        PosixError::EIO => libc::EIO,
        PosixError::EISDIR => libc::EISDIR,
        PosixError::EMLINK => libc::EMLINK,
        PosixError::ENAMETOOLONG => libc::ENAMETOOLONG,
        PosixError::ENOATTR => libc::ENOATTR,
        PosixError::ENOENT => libc::ENOENT,
        PosixError::ENOSPC => libc::ENOSPC,
        PosixError::ENOTDIR => libc::ENOTDIR,
        PosixError::ENOTEMPTY => libc::ENOTEMPTY,
        PosixError::EPERM => libc::EPERM,
        PosixError::ESTALE => libc::ESTALE,
    }
}

// posix_error is the error FSKit reports for errno, with description as its message.
pub(crate) fn posix_error(errno: c_int, description: &str) -> Retained<NSError> {
    let description = NSString::from_str(description);
    let description: &AnyObject = &description;
    // SAFETY: the domain and key are Foundation's own constants.
    unsafe {
        let user_info = NSDictionary::from_slices(&[NSLocalizedDescriptionKey], &[description]);
        NSError::errorWithDomain_code_userInfo(
            NSPOSIXErrorDomain,
            errno as NSInteger,
            Some(&user_info),
        )
    }
}

// identity is the UUID FSKit knows a volume or its store by, made from bytes that name it, the same for
// every mount.
// It goes through the UUID's string form, since NSUUID::from_bytes fails objc2's argument checks
// on current macOS (see its documentation).
pub(crate) fn identity(name: &[u8]) -> Retained<NSUUID> {
    let hash = blake3::hash(name);
    let (uuid, _) = hash
        .as_bytes()
        .split_first_chunk::<16>()
        .expect("a hash is longer than a UUID");
    NSUUID::from_string(&NSString::from_str(
        &uuid::Uuid::from_bytes(*uuid).to_string(),
    ))
    .expect("a formatted UUID parses")
}

#[cfg(test)]
mod tests;

#[link(name = "Foundation", kind = "framework")]
unsafe extern "C" {
    fn NSExtensionMain(argc: c_int, argv: *mut *mut c_char) -> c_int;
}
