#![allow(unsafe_code)]

mod file_system;
mod volume;

use std::any::Any;
use std::error::Error;
use std::ffi::{CString, c_char, c_int};
use std::os::unix::ffi::OsStringExt;
use std::sync::OnceLock;

use bytes::Bytes;
use futures_lite::future::block_on;
use objc2::ClassType;
use objc2::rc::Retained;
use objc2_foundation::{NSString, NSUUID};

use crate::volume_url::VolumeUrl;
use file_system::StrataFileSystem;
use volume::StrataVolume;

// OpenedVolume is a loaded volume: its id, and the engine the extension binary opened for it.
pub(crate) struct OpenedVolume {
    id: Bytes,
    engine: Box<dyn Any + Send + Sync>,
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

static OPEN_VOLUME: OnceLock<OpenVolume> = OnceLock::new();

// run starts the FSKit extension and never returns. The extension binary's main calls it.
// When FSKit loads a mounted volume, run's open_volume opens it from its volume id.
pub fn run<V, E, F>(open_volume: impl AsyncFn(V) -> Result<E, F> + Send + Sync + 'static) -> !
where
    V: Clone + Into<Bytes> + TryFrom<Bytes, Error: Error + Send + Sync + 'static>,
    E: Send + Sync + 'static,
    F: Error + Send + Sync + 'static,
{
    if OPEN_VOLUME.set(opener(open_volume)).is_err() {
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

// opener wraps open_volume so it takes a mount URL and returns the volume as FSKit's classes hold it.
fn opener<V, E, F>(
    open_volume: impl AsyncFn(V) -> Result<E, F> + Send + Sync + 'static,
) -> OpenVolume
where
    V: Clone + Into<Bytes> + TryFrom<Bytes, Error: Error + Send + Sync + 'static>,
    E: Send + Sync + 'static,
    F: Error + Send + Sync + 'static,
{
    Box::new(move |url| {
        let url: VolumeUrl<V> = url
            .parse()
            .map_err(|error| LoadError::InvalidUrl(Box::new(error)))?;
        let id = url.volume.clone().into();
        let engine = block_on(open_volume(url.volume))
            .map_err(|error| LoadError::OpenFailed(Box::new(error)))?;
        Ok(OpenedVolume {
            id,
            engine: Box::new(engine),
        })
    })
}

pub(crate) fn open_volume(url: &str) -> Result<OpenedVolume, LoadError> {
    let open = OPEN_VOLUME
        .get()
        .expect("run stores the volume opener before FSKit can call in");
    open(url)
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
