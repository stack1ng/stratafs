use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use stratafs_store::durability::DurableAgainst;

use crate::{ObjectStoreContentStore, ObjectStoreError};

// Unflushed is what a store on this machine's disk has written but not yet made durable.
#[derive(Debug)]
pub(crate) struct Unflushed {
    root: PathBuf,
    pending: Mutex<Pending>,
}

#[derive(Debug, Default)]
struct Pending {
    // files are the files no flush has handed to the disk yet.
    files: Vec<PathBuf>,
    // in_disk_cache is whether a flush handed files to the disk that its own cache may still hold.
    in_disk_cache: bool,
}

impl Unflushed {
    // record notes the file a put wrote at path, under the store's root.
    pub(crate) fn record(&self, path: &object_store::path::Path) {
        self.pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .files
            .push(self.root.join(path.as_ref()));
    }

    // flush makes every file put since the last flush durable against a failure, along with the directories
    // that name them.
    pub(crate) fn flush(&self, against: DurableAgainst) -> io::Result<()> {
        let (files, in_disk_cache) = {
            let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            (std::mem::take(&mut pending.files), pending.in_disk_cache)
        };
        let flushed = flush_files(&self.root, &files, against, in_disk_cache);
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        match (&flushed, against) {
            // The files stay for the next flush to try again.
            (Err(_), _) => pending.files.extend(files),
            (Ok(()), DurableAgainst::SoftwareFailure) => pending.in_disk_cache |= !files.is_empty(),
            (Ok(()), DurableAgainst::PowerFailure) => pending.in_disk_cache = false,
        }
        flushed
    }
}

// Dropping the last handle of a store makes every put durable against a power failure, as closing a metadata
// store does.
impl Drop for Unflushed {
    fn drop(&mut self) {
        if let Err(error) = self.flush(DurableAgainst::PowerFailure) {
            tracing::warn!(%error, "making the content durable on close failed");
        }
    }
}

impl ObjectStoreContentStore {
    // local keeps content as files under root on this machine's disk. Putting a blob does not flush it to the
    // disk; ensure_durable does, for every blob put since the last time.
    pub fn local(root: impl Into<PathBuf>) -> Result<Self, ObjectStoreError> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(ObjectStoreError::Directory)?;
        let store = object_store::local::LocalFileSystem::new_with_prefix(&root)?;
        Ok(Self {
            store: Arc::new(store),
            unflushed: Some(Arc::new(Unflushed {
                root,
                pending: Mutex::default(),
            })),
        })
    }
}

// flush_files hands files to the disk, then every directory from each file's up to root, so the names a put gave
// them are handed over too. Against a software failure a barrier then keeps whatever is written next from
// reaching the disk first. Against a power failure the disk then flushes its own cache, which also covers files
// an earlier flush handed over while it was in_disk_cache.
fn flush_files(
    root: &Path,
    files: &[PathBuf],
    against: DurableAgainst,
    in_disk_cache: bool,
) -> io::Result<()> {
    let mut directories = BTreeSet::new();
    for file in files {
        fsync(file)?;
        for directory in file.ancestors().skip(1) {
            directories.insert(directory);
            if directory == root {
                break;
            }
        }
    }
    for directory in directories {
        fsync(directory)?;
    }
    match against {
        DurableAgainst::SoftwareFailure if files.is_empty() => Ok(()),
        DurableAgainst::SoftwareFailure => sync_in_order(&std::fs::File::open(root)?),
        DurableAgainst::PowerFailure if files.is_empty() && !in_disk_cache => Ok(()),
        DurableAgainst::PowerFailure => std::fs::File::open(root)?.sync_all(),
    }
}

// fsync is the plain fsync system call. Rust's sync_all and sync_data also flush the disk's own cache on Apple's
// systems, once per call, which flush_files does at most once at the end instead.
fn fsync(path: &Path) -> io::Result<()> {
    let file = std::fs::File::open(path)?;
    nix::unistd::fsync(&file).map_err(io::Error::from)
}

// sync_in_order hands file to the disk with a barrier, so nothing written after it reaches the disk before it,
// without waiting for the disk to flush its own cache. Elsewhere than Apple's systems a sync always flushes it.
#[cfg(target_vendor = "apple")]
fn sync_in_order(file: &std::fs::File) -> io::Result<()> {
    nix::fcntl::fcntl(file, nix::fcntl::FcntlArg::F_BARRIERFSYNC)
        .map(|_| ())
        .map_err(io::Error::from)
}

#[cfg(not(target_vendor = "apple"))]
fn sync_in_order(file: &std::fs::File) -> io::Result<()> {
    file.sync_data()
}
