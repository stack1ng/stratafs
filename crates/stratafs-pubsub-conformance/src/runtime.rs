// TokioRuntime runs the tasks and timers of the volumes and mounts the checks make on the Tokio runtime the checks
// run on.

use std::time::Instant;

use futures_util::future::BoxFuture;
use stratafs_runtime::Runtime;
use tokio::runtime::Handle;

pub struct TokioRuntime(Handle);

impl TokioRuntime {
    // current is the Tokio runtime the caller runs on.
    pub fn current() -> Self {
        TokioRuntime(Handle::current())
    }
}

impl Runtime for TokioRuntime {
    fn spawn(&self, task: BoxFuture<'static, ()>) {
        self.0.spawn(task);
    }

    fn now(&self) -> Instant {
        Instant::now()
    }

    // sleep_until makes its timer on the runtime, so it may be polled from any thread.
    fn sleep_until(&self, deadline: Instant) -> BoxFuture<'static, ()> {
        let _entered = self.0.enter();
        Box::pin(tokio::time::sleep_until(deadline.into()))
    }
}
