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
use std::path::Path;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use clap::Parser;
use futures_lite::future::block_on;
use futures_util::task::Spawn;
use objc2::ClassType;
use objc2::rc::Retained;
use objc2::runtime::AnyObject;
use objc2_foundation::{
    NSDictionary, NSError, NSInteger, NSLocalizedDescriptionKey, NSPOSIXErrorDomain, NSString,
    NSUUID,
};
use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::{FsConfig, FsEngine, Timestamp};
use stratafs_store::content::ContentStore;
use stratafs_store::kv::KeyValueStore;

use crate::mount_options::MountOptions;
use file_system::StrataFileSystem;
use items::Items;
use operations::{Volume, VolumeOperations};
use volume::StrataVolume;

// OpenedVolume is an activated volume: its id, and its operations on the engine the extension binary opened.
pub(crate) struct OpenedVolume {
    id: Bytes,
    operations: Arc<dyn VolumeOperations>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LoadError {
    #[error("the resource is not the path of a store directory")]
    NotAPath,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ActivateError {
    #[error("the mount options could not be read: {0}")]
    UnreadableOptions(#[source] clap::Error),
    #[error(transparent)]
    InvalidOptions(Box<dyn Error + Send + Sync>),
    #[error("the volume could not be opened: {source}")]
    OpenFailed {
        posix: PosixError,
        source: Box<dyn Error + Send + Sync>,
    },
}

// ActivateArguments are the options FSKit hands activate: each -o mount was given.
#[derive(Parser)]
#[command(no_binary_name = true)]
struct ActivateArguments {
    #[arg(short = 'o')]
    options: Vec<String>,
}

type OpenVolume =
    Box<dyn Fn(&Path, &[String]) -> Result<OpenedVolume, ActivateError> + Send + Sync>;

// OPEN_VOLUME is how activate opens a volume. It is the one static: NSExtensionMain creates
// StrataFileSystem itself, with no arguments, so this is the only way run's arguments reach it.
// Everything loadResource builds from here on gets what it needs passed in.
static OPEN_VOLUME: OnceLock<OpenVolume> = OnceLock::new();

// run starts the FSKit extension and never returns. The extension binary's main calls it.
// When FSKit activates a mounted volume, run's open_volume opens it from the store directory mount was
// given and the volume its -o volume option names. Its errors reach mount as the POSIX errors they stand for.
// FSKit hands every operation over on one thread and does not wait for the reply. Each operation runs on
// that thread until it finishes or yields, as a store does while it waits for the disk to make commits durable, and then
// finishes on spawner, so FSKit can hand over the next one. Store work that blocks without yielding, such as
// file IO on this machine's disk, holds up that thread. spawner must run on threads of its own:
// NSExtensionMain keeps the main thread.
pub fn run<V, M, C, A, N, S, F>(
    spawner: impl Spawn + Send + Sync + 'static,
    open_volume: impl AsyncFn(&Path, V) -> Result<FsEngine<M, C, A, N, S>, F> + Send + Sync + 'static,
) -> !
where
    V: Clone + Into<Bytes> + TryFrom<Bytes, Error: Error + Send + Sync + 'static>,
    M: KeyValueStore,
    C: ContentStore + 'static,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    F: Error + Send + Sync + 'static,
    for<'e> PosixError: From<&'e F>,
{
    if OPEN_VOLUME.set(opener(spawner, open_volume)).is_err() {
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

// opener wraps open_volume so it takes the store directory and activate's options, and returns the volume
// as FSKit's classes hold it, running its operations on spawner.
fn opener<V, M, C, A, N, S, F>(
    spawner: impl Spawn + Send + Sync + 'static,
    open_volume: impl AsyncFn(&Path, V) -> Result<FsEngine<M, C, A, N, S>, F> + Send + Sync + 'static,
) -> OpenVolume
where
    V: Clone + Into<Bytes> + TryFrom<Bytes, Error: Error + Send + Sync + 'static>,
    M: KeyValueStore,
    C: ContentStore + 'static,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    F: Error + Send + Sync + 'static,
    for<'e> PosixError: From<&'e F>,
{
    let spawner: Arc<dyn Spawn + Send + Sync> = Arc::new(spawner);
    Box::new(move |store, arguments| {
        let arguments = ActivateArguments::try_parse_from(arguments)
            .map_err(ActivateError::UnreadableOptions)?;
        let options: MountOptions<V> = arguments
            .options
            .join(",")
            .parse()
            .map_err(|error| ActivateError::InvalidOptions(Box::new(error)))?;
        let id = options.volume.clone().into();
        let engine = block_on(open_volume(store, options.volume)).map_err(|error| {
            ActivateError::OpenFailed {
                posix: PosixError::from(&error),
                source: Box::new(error),
            }
        })?;
        Ok(OpenedVolume {
            id,
            operations: Arc::new(Volume {
                engine,
                spawner: spawner.clone(),
                items: Items::default(),
            }),
        })
    })
}

pub(crate) fn open_volume(
    store: &Path,
    arguments: &[String],
) -> Result<OpenedVolume, ActivateError> {
    let open = OPEN_VOLUME
        .get()
        .expect("run stores the volume opener before FSKit can call in");
    open(store, arguments)
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
