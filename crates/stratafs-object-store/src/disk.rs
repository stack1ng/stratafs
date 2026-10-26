use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use crate::{ObjectStoreContentStore, ObjectStoreError};

// Unflushed are the files a store on this machine's disk has written since its last flush.
#[derive(Debug)]
pub(crate) struct Unflushed {
    root: PathBuf,
    files: Mutex<Vec<PathBuf>>,
}

impl Unflushed {
    // record notes the file a put wrote at path, under the store's root.
    pub(crate) fn record(&self, path: &object_store::path::Path) {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(self.root.join(path.as_ref()));
    }

    // flush makes every file put since the last flush durable, along with the directories that name them.
    pub(crate) fn flush(&self) -> io::Result<()> {
        let files = std::mem::take(&mut *self.files.lock().unwrap_or_else(PoisonError::into_inner));
        let flushed = flush_files(&self.root, &files);
        if flushed.is_err() {
            // The files stay for the next flush to try again.
            self.files
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(files);
        }
        flushed
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
                files: Mutex::default(),
            })),
        })
    }
}

// flush_files flushes files to the disk, then every directory from each file's up to root, so the names a put
// gave them are flushed too.
fn flush_files(root: &Path, files: &[PathBuf]) -> io::Result<()> {
    if files.is_empty() {
        return Ok(());
    }
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
    // One flush of the disk's own cache then covers every file above. On macOS that is what sync_all adds to a
    // plain fsync.
    std::fs::File::open(root)?.sync_all()
}

// fsync is the plain fsync system call. Rust's sync_all and sync_data also flush the disk's own cache on macOS,
// once per call, which flush_files does once at the end instead.
fn fsync(path: &Path) -> io::Result<()> {
    let file = std::fs::File::open(path)?;
    rustix::fs::fsync(&file).map_err(io::Error::from)
}
