pub trait SpawnBlocking: Send + Sync {
    fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> + Send;
}

#[cfg(threads)]
pub struct ThreadPool;

#[cfg(threads)]
impl SpawnBlocking for ThreadPool {
    fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> impl Future<Output = T> + Send {
        blocking::unblock(work)
    }
}

pub struct Inline;

impl SpawnBlocking for Inline {
    async fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        work()
    }
}
