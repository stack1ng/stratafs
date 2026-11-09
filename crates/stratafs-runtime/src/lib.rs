// stratafs-runtime is the async runtime the engine is handed: where its tasks run, and the clock its timers follow,
// which a test may move as it likes to simulate the flow of time.

#[cfg(feature = "replay")]
pub mod replay;

use std::pin::pin;
use std::time::{Duration, Instant};

use futures_core::future::BoxFuture;
use futures_executor::ThreadPool;
use futures_timer::Delay;
use futures_util::future::{Either, select};

pub trait Runtime: Send + Sync + 'static {
    // spawn runs task on its own until it finishes.
    fn spawn(&self, task: BoxFuture<'static, ()>);

    // now is the time on the runtime's monotonic clock.
    fn now(&self) -> Instant;

    // sleep_until resolves once the runtime's clock reaches deadline.
    fn sleep_until(&self, deadline: Instant) -> BoxFuture<'static, ()>;
}

impl dyn Runtime {
    // sleep resolves once duration passes on the runtime's clock.
    pub fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        self.sleep_until(self.now() + duration)
    }

    // timeout is what future resolves to, or None when duration passes on the runtime's clock first.
    pub async fn timeout<T>(
        &self,
        duration: Duration,
        future: impl Future<Output = T>,
    ) -> Option<T> {
        match select(pin!(future), self.sleep(duration)).await {
            Either::Left((output, _)) => Some(output),
            Either::Right(_) => None,
        }
    }
}

// ThreadPoolRuntime runs tasks on a thread pool, and its clock is the system's monotonic clock.
pub struct ThreadPoolRuntime(pub ThreadPool);

impl Runtime for ThreadPoolRuntime {
    fn spawn(&self, task: BoxFuture<'static, ()>) {
        self.0.spawn_ok(task);
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, deadline: Instant) -> BoxFuture<'static, ()> {
        Box::pin(Delay::new(
            deadline.saturating_duration_since(Instant::now()),
        ))
    }
}

// TaskDroppingRuntime drops each task it is given and never fires a timer, for tests that run no background task.
pub struct TaskDroppingRuntime;

impl Runtime for TaskDroppingRuntime {
    fn spawn(&self, _task: BoxFuture<'static, ()>) {}

    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep_until(&self, _deadline: Instant) -> BoxFuture<'static, ()> {
        Box::pin(std::future::pending())
    }
}
