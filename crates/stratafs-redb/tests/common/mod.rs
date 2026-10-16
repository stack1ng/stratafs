use futures_lite::future::yield_now;
use redb::{Database, StorageBackend};
use stratafs_redb::{RedbStore, SpawnBlocking};

pub struct SimulatedThreadPool;

// SimulatedThreadPool is a spawner the simulates a thread pool,
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

pub fn open_store(backend: impl StorageBackend) -> RedbStore<SimulatedThreadPool> {
    RedbStore::new(
        Database::builder().create_with_backend(backend).unwrap(),
        SimulatedThreadPool,
    )
    .unwrap()
}
