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
}

impl ObjectStoreContentStore {
    // local keeps content as files under root on this machine's disk. Putting a blob does not flush it to the
    // disk; flush does, for every blob put since the last flush.
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

    // flush writes every blob put since the last flush to the disk, along with the directories that name them.
    // On macOS that stops short of the disk's own cache, so it is durable once a flush of the disk cache follows,
    // such as the metadata store's next durable commit; elsewhere it is durable on its own. A store that is not
    // on this machine's disk has nothing to flush.
    pub fn flush(&self) -> io::Result<()> {
        let Some(unflushed) = &self.unflushed else {
            return Ok(());
        };
        let files = std::mem::take(
            &mut *unflushed
                .files
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        let flushed = flush_files(&unflushed.root, &files);
        if flushed.is_err() {
            // The files stay for the next flush to try again.
            unflushed
                .files
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(files);
        }
        flushed
    }
}

// flush_files flushes files to the disk, then every directory from each file's up to root, so the names a put
// gave them are flushed too.
fn flush_files(root: &Path, files: &[PathBuf]) -> io::Result<()> {
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
    Ok(())
}

// fsync is the plain fsync system call. Rust's sync_all and sync_data instead flush the disk's own cache on macOS,
// once per call, which flush leaves to one durable commit after it.
fn fsync(path: &Path) -> io::Result<()> {
    let file = std::fs::File::open(path)?;
    rustix::fs::fsync(&file).map_err(io::Error::from)
}
