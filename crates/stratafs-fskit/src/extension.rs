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
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
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

use crate::volume_url::VolumeUrl;
use file_system::StrataFileSystem;
use items::Items;
use operations::{Volume, VolumeOperations};
use volume::StrataVolume;

// OpenedVolume is a loaded volume: its id, and its operations on the engine the extension binary opened.
pub(crate) struct OpenedVolume {
    id: Bytes,
    operations: Arc<dyn VolumeOperations>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum LoadError {
    #[error("the resource is not a URL")]
    NoUrl,
    #[error("the URL does not name a volume: {0}")]
    InvalidUrl(Box<dyn Error + Send + Sync>),
    #[error("the volume could not be opened: {0}")]
    OpenFailed(Box<dyn Error + Send + Sync>),
}

type OpenVolume = Box<dyn Fn(&str) -> Result<OpenedVolume, LoadError> + Send + Sync>;

// OPEN_VOLUME is how loadResource opens a volume. It is the one static: NSExtensionMain creates
// StrataFileSystem itself, with no arguments, so this is the only way run's arguments reach it.
// Everything loadResource builds from here on gets what it needs passed in.
static OPEN_VOLUME: OnceLock<OpenVolume> = OnceLock::new();

// run starts the FSKit extension and never returns. The extension binary's main calls it.
// When FSKit loads a mounted volume, run's open_volume opens it from its volume id.
// FSKit hands every operation over on one thread and does not wait for the reply, so each operation
// runs on spawner, which must run on threads of its own: NSExtensionMain keeps the main thread.
pub fn run<V, M, C, A, N, S, F>(
    spawner: impl Spawn + Send + Sync + 'static,
    open_volume: impl AsyncFn(V) -> Result<FsEngine<M, C, A, N, S>, F> + Send + Sync + 'static,
) -> !
where
    V: Clone + Into<Bytes> + TryFrom<Bytes, Error: Error + Send + Sync + 'static>,
    M: KeyValueStore,
    C: ContentStore + 'static,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    F: Error + Send + Sync + 'static,
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

// opener wraps open_volume so it takes a mount URL and returns the volume as FSKit's classes hold it,
// running its operations on spawner.
fn opener<V, M, C, A, N, S, F>(
    spawner: impl Spawn + Send + Sync + 'static,
    open_volume: impl AsyncFn(V) -> Result<FsEngine<M, C, A, N, S>, F> + Send + Sync + 'static,
) -> OpenVolume
where
    V: Clone + Into<Bytes> + TryFrom<Bytes, Error: Error + Send + Sync + 'static>,
    M: KeyValueStore,
    C: ContentStore + 'static,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    F: Error + Send + Sync + 'static,
{
    let spawner: Arc<dyn Spawn + Send + Sync> = Arc::new(spawner);
    Box::new(move |url| {
        let url: VolumeUrl<V> = url
            .parse()
            .map_err(|error| LoadError::InvalidUrl(Box::new(error)))?;
        let id = url.volume.clone().into();
        let engine = block_on(open_volume(url.volume))
            .map_err(|error| LoadError::OpenFailed(Box::new(error)))?;
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

pub(crate) fn open_volume(url: &str) -> Result<OpenedVolume, LoadError> {
    let open = OPEN_VOLUME
        .get()
        .expect("run stores the volume opener before FSKit can call in");
    open(url)
}

// errno is a POSIX error's number on macOS.
pub(crate) fn errno(error: PosixError) -> c_int {
    match error {
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

// identity is the UUID FSKit knows a volume by, the same for every mount of it.
// It goes through the UUID's string form, since NSUUID::from_bytes fails objc2's argument checks
// on current macOS (see its documentation).
pub(crate) fn identity(volume_id: &[u8]) -> Retained<NSUUID> {
    let hash = blake3::hash(volume_id);
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
