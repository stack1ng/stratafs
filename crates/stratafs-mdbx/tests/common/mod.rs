use futures_lite::future::yield_now;
use stratafs_mdbx::MdbxMetadata;
use stratafs_store::spawn::SpawnBlocking;

use crate::configuration;

pub struct SimulatedThreadPool;

// SimulatedThreadPool is a spawner that simulates a thread pool,
// by simply yielding before and after the work is done, like how a real thread pool would.
impl SpawnBlocking for SimulatedThreadPool {
    async fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        yield_now().await;
        let output = work();
        yield_now().await;
        output
    }
}

// open_store opens a store in a fresh directory, and removes the directory at once:
// the store keeps its files open, and they go when it closes.
pub fn open_store() -> MdbxMetadata<SimulatedThreadPool> {
    let directory = tempfile::tempdir().unwrap();
    configuration::open(directory.path().join("store.mdbx"), SimulatedThreadPool).unwrap()
}
