// Replaying runtimes run a test body many times, each time in another interleaving of its tasks. A replay's Runtime
// keeps a virtual clock that moves only once no other task can run, as Go's synctest does.

use std::collections::BTreeMap;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::future::RemoteHandle;
use shuttle::scheduler::{
    DfsScheduler, PctScheduler, RandomScheduler, Schedule, Scheduler, Task, TaskId,
};
use shuttle::{Config, Runner};

use crate::Runtime;

// EARLY_ODDS is the odds, one in it, that a slow run's clock moves at a step where another task could run, and
// EARLY_MOST is the most it moves then.
const EARLY_ODDS: u64 = 256;
const EARLY_MOST: Duration = Duration::from_millis(10);

// TIME_LIMIT is the most virtual time a run may take.
const TIME_LIMIT: Duration = Duration::from_secs(60);

// ReplayingRuntime is an async runtime that is expected to replay the run body
// for the purpose of exercising unique interleavings of tasks.
pub trait ReplayingRuntime {
    type Replay: Replay;

    // run runs the given body one or more times, each time with a unique Replay
    fn run<B, F>(&self, seed: u64, body: B)
    where
        B: Fn(Self::Replay) -> F + Send + Sync + 'static,
        F: Future<Output = ()>;
}

pub trait Replay: Clone + Send + Sync + 'static {
    // spawn gives a task to the runtime's scheduler, scoped to this Replay
    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static);

    // runtime runs its tasks in this Replay, and its timers on the virtual clock. A slow run's clock also moves a
    // little now and then while other tasks could run, as on a machine too busy to run them.
    fn runtime(&self, slow: bool) -> Arc<dyn Runtime>;
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
        let clock = Arc::new(Clock {
            base: Instant::now(),
            state: Mutex::default(),
        });
        let scheduler = Timed {
            scheduler: (self.new_scheduler)(seed),
            clock: Arc::clone(&clock),
        };
        Runner::new(scheduler, config).run(move || {
            clock.set_back();
            shuttle::future::block_on(body(ShuttleReplay {
                clock: Arc::clone(&clock),
            }));
            // The clock's task outlives the body, so Timed still picks it only once no other task can run.
            let task = clock.state().task;
            clock.set_back();
            clock.state().task = task;
        });
    }
}

#[derive(Clone)]
pub struct ShuttleReplay {
    clock: Arc<Clock>,
}

impl Replay for ShuttleReplay {
    fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        shuttle::future::spawn(task);
    }

    fn runtime(&self, slow: bool) -> Arc<dyn Runtime> {
        let start = {
            let mut state = self.clock.state();
            state.slow = slow;
            !std::mem::replace(&mut state.started, true)
        };
        if start {
            shuttle::future::spawn(keep_time(Arc::clone(&self.clock)));
            shuttle::future::spawn(start_spawned(Arc::clone(&self.clock)));
        }
        Arc::new(ReplayRuntime(Arc::clone(&self.clock)))
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

// pct runs the oldest task first, then iterations - 1 seeded schedules that each change task priorities at depth - 1
// steps anywhere in the run, as probabilistic concurrency testing does.
pub fn pct(
    depth: usize,
    iterations: usize,
) -> Shuttle<impl Fn(u64) -> PctScheduler + Clone + Send + Sync + 'static> {
    Shuttle {
        new_scheduler: move |seed| PctScheduler::new_from_seed(seed, depth, iterations),
    }
}

pub fn spawn_with_handle<P: Replay, T: Send + 'static>(
    replay: &P,
    task: impl Future<Output = T> + Send + 'static,
) -> RemoteHandle<T> {
    let (task, handle) = task.remote_handle();
    replay.spawn(task);
    handle
}

// TODO: consider replacing this clock with shuttle's time models if https://github.com/awslabs/shuttle/pull/217 merges.
// Clock is the virtual clock of a run, set back as each execution begins and ends.
struct Clock {
    base: Instant,
    state: Mutex<ClockState>,
}

#[derive(Default)]
struct ClockState {
    // now is how far the clock has moved past base.
    now: Duration,
    // timers wakes each pending timer, by its deadline and then by when it was made.
    timers: BTreeMap<(Duration, u64), Waker>,
    made: u64,
    // task is the task moving the clock, which waker wakes once a timer is pending.
    task: Option<TaskId>,
    waker: Option<Waker>,
    // spawned is each task the Runtime was given and has not spawned yet, and spawner wakes the task spawning them.
    spawned: Vec<BoxFuture<'static, ()>>,
    spawner: Option<Waker>,
    started: bool,
    // slow says whether the clock may move early, and early that the scheduler just picked its task to.
    slow: bool,
    early: bool,
}

impl Clock {
    fn state(&self) -> MutexGuard<'_, ClockState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // set_back starts the clock afresh, inside an execution, since what the last one left may wake tasks as it is
    // dropped, and once the lock is let go of, which a timer dropped takes.
    fn set_back(&self) {
        let left = std::mem::take(&mut *self.state());
        drop(left);
    }
}

// keep_time moves the clock to the next deadline and wakes the timers it reaches, each time the scheduler picks it.
async fn keep_time(clock: Arc<Clock>) {
    // Timed picks this task like any other until it knows it, so its first poll only makes it known.
    clock.state().task = Some(shuttle::current::me());
    shuttle::future::yield_now().await;
    poll_fn(|context| {
        let mut state = clock.state();
        let Some(&(deadline, _)) = state.timers.keys().next() else {
            state.waker = Some(context.waker().clone());
            return Poll::Pending;
        };
        let to = match std::mem::take(&mut state.early) {
            true => deadline.min(state.now + EARLY_MOST),
            false => deadline,
        };
        assert!(
            to <= TIME_LIMIT,
            "the run took longer than {TIME_LIMIT:?} of virtual time"
        );
        state.now = to;
        let later = state.timers.split_off(&(to, u64::MAX));
        for (_, waker) in std::mem::replace(&mut state.timers, later) {
            waker.wake();
        }
        context.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}

// start_spawned spawns the tasks the Runtime was given. Spawning switches tasks, which the Runtime's caller may not
// expect, such as while it holds a lock.
async fn start_spawned(clock: Arc<Clock>) {
    loop {
        let spawned = poll_fn(|context| {
            let mut state = clock.state();
            if state.spawned.is_empty() {
                state.spawner = Some(context.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(std::mem::take(&mut state.spawned))
        })
        .await;
        for task in spawned {
            shuttle::future::spawn(task);
        }
    }
}

struct ReplayRuntime(Arc<Clock>);

impl Runtime for ReplayRuntime {
    fn spawn(&self, task: BoxFuture<'static, ()>) {
        let mut state = self.0.state();
        state.spawned.push(task);
        if let Some(spawner) = state.spawner.take() {
            spawner.wake();
        }
    }

    fn now(&self) -> Instant {
        self.0.base + self.0.state().now
    }

    fn sleep_until(&self, deadline: Instant) -> BoxFuture<'static, ()> {
        Box::pin(Sleep {
            clock: Arc::clone(&self.0),
            deadline: deadline.saturating_duration_since(self.0.base),
            made: None,
        })
    }
}

// Sleep is a timer, pending in its clock from its first poll until it is done or dropped.
struct Sleep {
    clock: Arc<Clock>,
    deadline: Duration,
    made: Option<u64>,
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        let clock = Arc::clone(&self.clock);
        let mut state = clock.state();
        if state.now >= self.deadline {
            if let Some(made) = self.made.take() {
                state.timers.remove(&(self.deadline, made));
            }
            return Poll::Ready(());
        }
        let made = *self.made.get_or_insert_with(|| {
            state.made += 1;
            state.made
        });
        state
            .timers
            .insert((self.deadline, made), context.waker().clone());
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(made) = self.made {
            self.clock.state().timers.remove(&(self.deadline, made));
        }
    }
}

// Timed picks the clock's task only once no other task can run or, on a slow run, when a seeded coin says so, and
// leaves every other choice to its scheduler.
struct Timed<S> {
    scheduler: S,
    clock: Arc<Clock>,
}

impl<S: Scheduler> Scheduler for Timed<S> {
    fn new_execution(&mut self) -> Option<Schedule> {
        self.scheduler.new_execution()
    }

    fn next_task(
        &mut self,
        runnable: &[&Task],
        current: Option<TaskId>,
        is_yielding: bool,
    ) -> Option<TaskId> {
        let (clock, slow) = {
            let state = self.clock.state();
            (state.task, state.slow)
        };
        let others: Vec<&Task>;
        let choices = match clock.filter(|clock| runnable.iter().any(|task| task.id() == *clock)) {
            None => runnable,
            Some(clock) => {
                others = runnable
                    .iter()
                    .copied()
                    .filter(|task| task.id() != clock)
                    .collect();
                if others.is_empty() || slow && self.scheduler.next_u64().is_multiple_of(EARLY_ODDS)
                {
                    self.clock.state().early = !others.is_empty();
                    return Some(clock);
                }
                &others
            }
        };
        self.scheduler.next_task(choices, current, is_yielding)
    }

    fn next_u64(&mut self) -> u64 {
        self.scheduler.next_u64()
    }
}
