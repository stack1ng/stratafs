use std::sync::atomic::{AtomicUsize, Ordering};

use stratafs_sqlite::SqliteStore;
use tempfile::TempDir;

use crate::common::SimulatedThreadPool;

// Stores is a temporary directory that gives each store opened in it a fresh directory of its own.
pub struct Stores {
    directory: TempDir,
    opened: AtomicUsize,
}

impl Stores {
    pub fn new() -> Self {
        Self {
            directory: tempfile::tempdir().unwrap(),
            opened: AtomicUsize::new(0),
        }
    }

    pub fn open(&self) -> SqliteStore<SimulatedThreadPool> {
        let directory = self
            .directory
            .path()
            .join(self.opened.fetch_add(1, Ordering::Relaxed).to_string());
        std::fs::create_dir(&directory).unwrap();
        SqliteStore::local(directory.join("metadata.sqlite"), SimulatedThreadPool).unwrap()
    }
}
