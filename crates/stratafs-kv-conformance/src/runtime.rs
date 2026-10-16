use futures_util::FutureExt;
use futures_util::future::RemoteHandle;
use shuttle::scheduler::{DfsScheduler, RandomScheduler, Scheduler};
use shuttle::{Config, Runner};

// ReplayingRuntime is an async runtime that is expected to replay the run body
// for the purpose of exercising unique code branches.
pub trait ReplayingRuntime {
    type Replay: Replay;

    // run runs the given body any number of times
    fn run<B, F>(&self, seed: u64, body: B)
    where
        B: Fn(Self::Replay) -> F + Send + Sync + 'static,
        F: Future<Output = ()>;
}

pub trait Replay: Clone + Send + Sync + 'static {
    // spawn gives a task to the runtime's scheduler, scoped to this Replay
    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static);
}

#[derive(Clone)]
pub struct Shuttle<N> {
    new_scheduler: N,
}

impl<N, S> ReplayingRuntime for Shuttle<N>
where
    N: Fn(u64) -> S + Clone + Send + Sync + 'static,
    S: Scheduler + 'static,
{
    type Replay = ShuttleReplay;

    fn run<B, F>(&self, seed: u64, body: B)
    where
        B: Fn(ShuttleReplay) -> F + Send + Sync + 'static,
        F: Future<Output = ()>,
    {
        let mut config = Config::default();
        config.stack_size = 1 << 20;
        Runner::new((self.new_scheduler)(seed), config)
            .run(move || shuttle::future::block_on(body(ShuttleReplay(()))));
    }
}

#[derive(Clone)]
pub struct ShuttleReplay(());

impl Replay for ShuttleReplay {
    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        shuttle::future::spawn(task);
    }
}

pub fn seeded() -> Shuttle<impl Fn(u64) -> RandomScheduler + Clone + Send + Sync + 'static> {
    Shuttle {
        new_scheduler: |seed| RandomScheduler::new_from_seed(seed, 1),
    }
}

pub fn exhaustive_dfs() -> Shuttle<impl Fn(u64) -> DfsScheduler + Clone + Send + Sync + 'static> {
    Shuttle {
        new_scheduler: |_| DfsScheduler::new(None, false),
    }
}

pub(crate) fn spawn_with_handle<P: Replay, T: Send + 'static>(
    replay: &P,
    task: impl Future<Output = T> + Send + 'static,
) -> RemoteHandle<T> {
    let (task, handle) = task.remote_handle();
    replay.spawn(task);
    handle
}
