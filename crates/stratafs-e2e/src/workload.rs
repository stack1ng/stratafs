use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use nix::errno::Errno;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

use crate::operations::Operation;
use crate::snapshot::{Snapshot, difference, snapshot};
use crate::{ConsistencyKind, Mount, Progress};

// SHARED is the directory every thread changes when more than one runs.
pub const SHARED: &str = "shared";

// SHARED_ERRORS are the errors an operation on the shared directory may fail with, as another thread may have changed
// what it names. EPERM is macOS's answer to removing a directory as a file, and EBUSY the engine's to two mounts
// changing one directory's name at once.
const SHARED_ERRORS: [Errno; 7] = [
    Errno::ENOENT,
    Errno::EEXIST,
    Errno::ENOTEMPTY,
    Errno::ENOTDIR,
    Errno::EISDIR,
    Errno::EPERM,
    Errno::EBUSY,
];

// directory is the name of the directory only thread changes.
pub fn directory(thread: usize) -> String {
    format!("thread-{thread}")
}

// Workload is the operations a run makes: each thread's own, from seed, on mounts picked at random.
pub struct Workload<'a> {
    pub mounts: &'a [Mount],
    // native is the directory of the native file system that gets each thread's operations too.
    pub native: &'a Path,
    pub seed: u64,
    pub operations: usize,
    pub threads: usize,
    pub converges_within: Duration,
}

impl Workload<'_> {
    // run runs every thread's operations, and returns what the mounts agree the shared directory holds afterwards, if
    // the threads shared one.
    pub fn run(&self, progress: &Progress) -> Result<Option<Snapshot>, String> {
        let shared = self.threads > 1;
        if shared {
            self.make(SHARED)?;
        }
        let failed = AtomicBool::new(false);
        let ran: Vec<Result<(), String>> = std::thread::scope(|scope| {
            let running: Vec<_> = (0..self.threads)
                .map(|thread| {
                    let failed = &failed;
                    scope.spawn(move || {
                        let ran = self.thread(thread, shared, failed, progress);
                        if ran.is_err() {
                            failed.store(true, Ordering::Relaxed);
                        }
                        ran
                    })
                })
                .collect();
            running
                .into_iter()
                .map(|running| {
                    running
                        .join()
                        .unwrap_or_else(|_| Err("a thread panicked".to_owned()))
                })
                .collect()
        });
        ran.into_iter().collect::<Result<Vec<()>, String>>()?;
        if !shared {
            return Ok(None);
        }
        progress.now(0, format!("waiting for the mounts to agree on {SHARED}"));
        self.agreed(SHARED).map(Some)
    }

    // make makes the directory name through the first mount, and checks every mount shows it.
    fn make(&self, name: &str) -> Result<(), String> {
        let first = &self.mounts[0];
        fs::create_dir(first.root.join(name))
            .map_err(|error| format!("making {name} on {first}: {error}"))?;
        for mount in self.mounts {
            shows(mount, name, &Snapshot::new(), self.lag(mount, first)).map_err(|difference| {
                format!("after making {name} on {first}, on {mount}, {name}/{difference}")
            })?;
        }
        Ok(())
    }

    // lag is how long mount may take to show a change made through changed: no time when it is strong or made the
    // change itself.
    fn lag(&self, mount: &Mount, changed: &Mount) -> Option<Duration> {
        let at_once = mount.consistency == ConsistencyKind::Strong || mount.index == changed.index;
        (!at_once).then_some(self.converges_within)
    }

    // thread runs one thread's operations in its own directory, each through a mount picked at random and on the native
    // file system, and checks after each that the two answered alike and every mount shows what the native one holds.
    fn thread(
        &self,
        thread: usize,
        shared: bool,
        failed: &AtomicBool,
        progress: &Progress,
    ) -> Result<(), String> {
        let name = directory(thread);
        let mut rng = StdRng::seed_from_u64(self.seed.wrapping_add(thread as u64));
        let native = self.native.join(&name);
        fs::create_dir(&native).map_err(|error| format!("making the native {name}: {error}"))?;
        self.make(&name)?;
        let mut entries = Snapshot::new();
        for step in 0..self.operations {
            if failed.load(Ordering::Relaxed) {
                return Ok(());
            }
            let mount = &self.mounts[rng.random_range(0..self.mounts.len())];
            if shared && rng.random_range(0..5) == 0 {
                let operation = Operation::generate_shared(&mut rng);
                let doing =
                    format!("thread {thread} step {step}: {operation:?} in {SHARED} on {mount}");
                progress.now(thread, doing.clone());
                if let Err(error) = operation.apply(&mount.root.join(SHARED))
                    && !SHARED_ERRORS.contains(&Errno::from_raw(error))
                {
                    return Err(format!(
                        "{doing}: the mount answered {}",
                        answer(Err(error))
                    ));
                }
                continue;
            }
            let operation = Operation::generate(&mut rng, &entries);
            let doing = format!("thread {thread} step {step}: {operation:?} on {mount}");
            progress.now(thread, doing.clone());
            let expected = operation.apply(&native);
            let found = operation.apply(&mount.root.join(&name));
            if found != expected {
                return Err(format!(
                    "{doing}: the mount answered {}, and the native file system {}",
                    answer(found),
                    answer(expected)
                ));
            }
            entries =
                snapshot(&native).map_err(|error| format!("reading the native {name}: {error}"))?;
            for other in self.mounts {
                shows(other, &name, &entries, self.lag(other, mount)).map_err(|difference| {
                    format!("{doing}: afterwards on {other}, {name}/{difference}")
                })?;
            }
        }
        Ok(())
    }

    // agreed is what every mount shows in the directory name, now that no operation runs on it: every strong mount at
    // once, and every eventual mount within its time.
    fn agreed(&self, name: &str) -> Result<Snapshot, String> {
        let strong = |mount: &&Mount| mount.consistency == ConsistencyKind::Strong;
        let first = self.mounts.iter().find(strong).unwrap_or(&self.mounts[0]);
        let deadline = Instant::now() + self.converges_within;
        loop {
            let agreed = snapshot(&first.root.join(name))
                .map_err(|error| format!("reading {name} on {first}: {error}"))?;
            let disagreeing = self.mounts.iter().find_map(|mount| {
                let lag = self
                    .lag(mount, mount)
                    .or(strong(&first).then_some(self.converges_within));
                shows(mount, name, &agreed, lag).err().map(|difference| {
                    format!("{mount} and {first} disagree: on {mount}, {name}/{difference}")
                })
            });
            match disagreeing {
                None => return Ok(agreed),
                // With no strong mount, the first one may be the one behind, so it is read again.
                Some(_) if !strong(&first) && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Some(disagreeing) => return Err(disagreeing),
            }
        }
    }
}

// shows checks that mount's directory name holds exactly entries, and says how it departs otherwise. A mount given a
// lag has that long to catch up.
pub fn shows(
    mount: &Mount,
    name: &str,
    entries: &Snapshot,
    lag: Option<Duration>,
) -> Result<(), String> {
    let began = Instant::now();
    let mut pause = Duration::from_millis(2);
    loop {
        let difference = match snapshot(&mount.root.join(name)) {
            Ok(found) => match difference(entries, &found) {
                None => return Ok(()),
                Some(difference) => difference,
            },
            Err(error) => format!(" could not be read: {error}"),
        };
        let Some(lag) = lag else {
            return Err(difference);
        };
        if began.elapsed() >= lag {
            return Err(format!("{difference}, still after {} s", lag.as_secs()));
        }
        // Nothing tells a process when an eventual mount has caught up, so it is read again after a pause.
        std::thread::sleep(pause);
        pause = (pause * 2).min(Duration::from_millis(200));
    }
}

// answer is an operation's result in words.
fn answer(result: Result<(), i32>) -> String {
    match result {
        Ok(()) => "success".to_owned(),
        Err(error) => io::Error::from_raw_os_error(error).to_string(),
    }
}
