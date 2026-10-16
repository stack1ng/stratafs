mod common;

use std::io;
use std::sync::{Arc, Mutex};

use common::SimulatedThreadPool;
use redb::StorageBackend;
use stratafs_kv_conformance::durability::CrashableStorage;
use stratafs_kv_conformance::runtime::seeded;
use stratafs_redb::RedbStore;

#[test]
fn is_durable() {
    stratafs_kv_conformance::is_durable(seeded(), new_crashable_store, reopen_crashed_store);
}

#[derive(Clone, Debug, Default)]
struct CrashableMemory(Arc<Mutex<CrashableMemoryState>>);

#[derive(Debug, Default)]
struct CrashableMemoryState {
    current: Vec<u8>,
    synced: Vec<u8>,
    storage_operations: usize,
    crash_after: Option<usize>,
}

impl CrashableMemory {
    fn perform<T>(
        &self,
        operation: impl FnOnce(&mut CrashableMemoryState) -> io::Result<T>,
    ) -> io::Result<T> {
        let mut state = self.0.lock().unwrap();
        state.storage_operations += 1;
        if state
            .crash_after
            .is_some_and(|crash_after| state.storage_operations > crash_after)
        {
            return Err(io::Error::other("crashed"));
        }
        operation(&mut state)
    }

    fn synced_copy(&self) -> CrashableMemory {
        let synced = self.0.lock().unwrap().synced.clone();
        CrashableMemory(Arc::new(Mutex::new(CrashableMemoryState {
            current: synced.clone(),
            synced,
            ..CrashableMemoryState::default()
        })))
    }
}

fn out_of_range() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "out of range")
}

impl StorageBackend for CrashableMemory {
    fn len(&self) -> io::Result<u64> {
        Ok(self.0.lock().unwrap().current.len() as u64)
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        let state = self.0.lock().unwrap();
        let start = usize::try_from(offset).map_err(|_| out_of_range())?;
        let read = state
            .current
            .get(start..start + out.len())
            .ok_or_else(out_of_range)?;
        out.copy_from_slice(read);
        Ok(())
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.perform(|state| {
            let len = usize::try_from(len).map_err(|_| out_of_range())?;
            state.current.resize(len, 0);
            Ok(())
        })
    }

    fn sync_data(&self) -> io::Result<()> {
        self.perform(|state| {
            state.synced = state.current.clone();
            Ok(())
        })
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.perform(|state| {
            let start = usize::try_from(offset).map_err(|_| out_of_range())?;
            state
                .current
                .get_mut(start..start + data.len())
                .ok_or_else(out_of_range)?
                .copy_from_slice(data);
            Ok(())
        })
    }
}

impl CrashableStorage for CrashableMemory {
    fn storage_operations(&self) -> usize {
        self.0.lock().unwrap().storage_operations
    }

    fn crash_after(&self, storage_operations: usize) {
        self.0.lock().unwrap().crash_after = Some(storage_operations);
    }
}

fn new_crashable_store() -> (RedbStore<SimulatedThreadPool>, CrashableMemory) {
    let storage = CrashableMemory::default();
    (common::open_store(storage.clone()), storage)
}

fn reopen_crashed_store(storage: CrashableMemory) -> RedbStore<SimulatedThreadPool> {
    common::open_store(storage.synced_copy())
}
