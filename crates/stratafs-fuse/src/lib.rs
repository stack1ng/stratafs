// stratafs-fuse serves a stratafs volume to the Linux kernel through FUSE. mount answers the kernel's requests with
// the engine until the volume is unmounted; stores opens an engine on the store and consistency a mount names.
#![cfg(target_os = "linux")]

mod changes;
mod file_system;
mod item;
mod items;
mod operations;
mod purges;
pub mod stores;
mod volume;

use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use fuser::{Config, MountOption, Session, SessionACL, SessionUnmounter};
use futures_lite::future::block_on;
use nix::mount::{MntFlags, umount2};
use nix::sys::signal::{SigSet, Signal};
use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::{Cached, FsConfig, FsEngine, Timestamp};
use stratafs_pubsub::PubSub;
use stratafs_runtime::Runtime;
use stratafs_store::store::Store;

use file_system::StrataFileSystem;
use items::Items;
use operations::{Volume, make_durable_on_cadence};
pub use volume::FuseCache;

// FILE_SYSTEM_TYPE names a mounted volume in the mount table.
const FILE_SYSTEM_TYPE: &str = "stratafs";

#[derive(Debug, thiserror::Error)]
pub enum MountError<F> {
    #[error("the volume could not be opened")]
    Open(#[source] F),
    #[error("the volume could not be mounted")]
    Mount(#[source] io::Error),
    #[error("serving the volume failed")]
    Serve(#[source] io::Error),
    #[error("the volume's last commits could not be made durable on unmount")]
    Durable(#[source] Box<dyn Error + Send + Sync>),
}

// mount opens a volume with open_volume and serves it on mount_point until it is unmounted and its last operation
// ends, making commits durable each durability_cadence, on fsync and on unmount, to every user when allow_other says
// so and otherwise to the one mounting it. A request that waits finishes on runtime, which needs threads of its own.
// SIGINT and SIGTERM unmount, so the caller blocks them in every thread it starts, as the daemon's main does first.
pub fn mount<St, P, A, N, S, C, F>(
    runtime: Arc<dyn Runtime>,
    mount_point: &Path,
    durability_cadence: Duration,
    allow_other: bool,
    open_volume: impl AsyncFnOnce(FuseCache, Arc<dyn Runtime>) -> Result<FsEngine<St, P, A, N, S, C>, F>,
) -> Result<(), MountError<F>>
where
    St: Store,
    P: PubSub<Message = u64>,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St, P>,
    F: Error,
{
    let items = Arc::new(Items::new());
    let kernel = FuseCache::new(Arc::clone(&items));
    let engine =
        block_on(open_volume(kernel.clone(), Arc::clone(&runtime))).map_err(MountError::Open)?;
    let (stops_durability_cadence, durability_cadence_stopped) = mpsc::channel();
    let (closes, closed) = mpsc::channel();
    let volume = Arc::new(Volume {
        engine,
        runtime,
        items,
        kernel: kernel.clone(),
        stops_durability_cadence,
        closes,
    });
    make_durable_on_cadence(
        Arc::downgrade(&volume),
        durability_cadence_stopped,
        durability_cadence,
    );
    let mut config = Config::default();
    // The kernel checks permissions against the attributes the volume hands it, which the engine leaves to it.
    config.mount_options = vec![
        MountOption::FSName(FILE_SYSTEM_TYPE.to_owned()),
        MountOption::Subtype(FILE_SYSTEM_TYPE.to_owned()),
        MountOption::DefaultPermissions,
    ];
    if allow_other {
        config.acl = SessionACL::All;
    }
    let mut session = Session::new(StrataFileSystem::new(volume), mount_point, &config)
        .map_err(MountError::Mount)?;
    kernel.connect(session.notifier());
    unmount_on_signal(session.unmount_callable(), mount_point.to_owned());
    let served = session.run().map_err(MountError::Serve);
    let durable = closed
        .recv()
        .expect("a volume says how it closed when it is dropped");
    served?;
    durable.map_err(|failure| MountError::Durable(Box::new(failure)))
}

// unmount_on_signal unmounts the volume, which ends the session, once the process gets SIGINT or SIGTERM. When the
// mount is in use, it detaches it instead, which ends the session once nothing uses it.
fn unmount_on_signal(unmounter: SessionUnmounter, mount_point: PathBuf) {
    std::thread::Builder::new()
        .name("unmount on signal".to_owned())
        .spawn(move || {
            let signals = SigSet::from_iter([Signal::SIGINT, Signal::SIGTERM]);
            let mut unmounting = Some(unmounter);
            loop {
                match signals.wait() {
                    Ok(signal) => tracing::info!(%signal, "unmounting"),
                    Err(error) => return tracing::error!(%error, "waiting for a signal failed"),
                }
                // fuser unmounts once, and only detaching is left after that fails.
                if let Some(mut unmounter) = unmounting.take() {
                    match unmounter.unmount() {
                        Ok(()) => return,
                        Err(error) => {
                            tracing::warn!(%error, "unmounting failed, so detaching the mount")
                        }
                    }
                }
                match umount2(&mount_point, MntFlags::MNT_DETACH) {
                    Ok(()) => return,
                    Err(error) => tracing::error!(%error, "detaching the mount failed"),
                }
            }
        })
        .expect("the operating system starts the signal thread");
}

// error_chain is an error with each of its sources, the way a person needs to read it.
pub fn error_chain(error: &dyn Error) -> String {
    let mut chain = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        chain.push_str(": ");
        chain.push_str(&cause.to_string());
        source = cause.source();
    }
    chain
}

// errno is a POSIX error's number on Linux, which has ENODATA where POSIX names ENOATTR.
pub(crate) fn errno(error: PosixError) -> i32 {
    match error {
        PosixError::EBUSY => libc::EBUSY,
        PosixError::EEXIST => libc::EEXIST,
        PosixError::EFBIG => libc::EFBIG,
        PosixError::EINVAL => libc::EINVAL,
        PosixError::EIO => libc::EIO,
        PosixError::EISDIR => libc::EISDIR,
        PosixError::EMLINK => libc::EMLINK,
        PosixError::ENAMETOOLONG => libc::ENAMETOOLONG,
        PosixError::ENOATTR => libc::ENODATA,
        PosixError::ENOENT => libc::ENOENT,
        PosixError::ENOSPC => libc::ENOSPC,
        PosixError::ENOTDIR => libc::ENOTDIR,
        PosixError::ENOTEMPTY => libc::ENOTEMPTY,
        PosixError::EPERM => libc::EPERM,
        PosixError::ESTALE => libc::ESTALE,
    }
}
