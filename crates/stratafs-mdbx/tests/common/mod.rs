use futures_lite::future::yield_now;
use stratafs_mdbx::MdbxMetadata;
use stratafs_store::spawn::SpawnBlocking;
use tempfile::TempDir;

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

// Stores is a temporary directory that gives each store opened in it a fresh directory of its own. They last as long
// as Stores, since a store whose change log is removed reads Gaps, which wake its watches from outside a simulation.
pub struct Stores(TempDir);

impl Stores {
    pub fn new() -> Self {
        Self(tempfile::tempdir().unwrap())
    }

    pub fn open(&self) -> MdbxMetadata<SimulatedThreadPool> {
        let directory = tempfile::tempdir_in(self.0.path()).unwrap().keep();
        configuration::open(directory.join("store.mdbx"), SimulatedThreadPool).unwrap()
    }
}
