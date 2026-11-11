// stratafs-e2e mounts one volume several times through the real kernel, runs random file operations on the mounts, and
// checks every mount against the same operations run on a directory of the native file system.

mod coordinator;
mod operations;
mod platform;
mod snapshot;
mod workload;

use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Mutex;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use clap::Parser;

use coordinator::Coordinator;
use platform::{Mounted, Store};
use snapshot::snapshot;
use workload::{SHARED, Workload, shows};

#[derive(Parser)]
#[command(
    about = "Run random file operations on strong and eventual mounts of one stratafs volume, made through the kernel, and check every mount against the native file system"
)]
struct Arguments {
    #[arg(
        long,
        default_value_t = 0,
        help = "How many strong mounts the volume gets"
    )]
    strong: usize,
    #[arg(
        long,
        default_value_t = 0,
        help = "How many eventual mounts the volume gets"
    )]
    eventual: usize,
    #[arg(
        long,
        default_value_t = 1,
        help = "How many threads run operations at once, each in a directory of its own; more than one also share a directory"
    )]
    threads: usize,
    #[arg(
        long,
        default_value_t = 500,
        help = "How many operations each thread runs"
    )]
    operations: usize,
    #[arg(
        long,
        help = "The seed the operations come from; a random one when left out"
    )]
    seed: Option<u64>,
    #[arg(
        long,
        value_parser = seconds,
        default_value = "30",
        help = "How long an eventual mount may take to show another mount's change, in whole seconds"
    )]
    converges_within: Duration,
    #[arg(
        long,
        value_parser = seconds,
        default_value = "120",
        help = "How long the run may go without finishing a step before it counts as hung, in whole seconds"
    )]
    hangs_after: Duration,
    #[arg(
        long,
        help = "The directory the run makes for its store, mount points and native copy, removed again when the run passes; one in the temporary directory when left out"
    )]
    directory: Option<PathBuf>,
    #[command(flatten)]
    platform: platform::Arguments,
}

fn seconds(seconds: &str) -> Result<Duration, std::num::ParseIntError> {
    seconds.parse().map(Duration::from_secs)
}

// ConsistencyKind is what a mount's reads promise about other mounts' writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsistencyKind {
    Strong,
    Eventual,
}

impl Display for ConsistencyKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            ConsistencyKind::Strong => "strong",
            ConsistencyKind::Eventual => "eventual",
        })
    }
}

// Mount is one mount of the volume: root is the directory it is mounted on.
pub struct Mount {
    pub index: usize,
    pub consistency: ConsistencyKind,
    pub root: PathBuf,
}

impl Display for Mount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "mount {} ({})", self.index, self.consistency)
    }
}

// Progress is how the run tells the thread watching for a hang that it finished a step, and what each thread does now.
pub struct Progress {
    // finished gets false for each step, and true once the run ended.
    finished: mpsc::Sender<bool>,
    doing: Mutex<BTreeMap<usize, String>>,
}

impl Progress {
    // now notes that thread starts on doing, having finished what it did before.
    pub fn now(&self, thread: usize, doing: String) {
        self.doing
            .lock()
            .expect("no thread panics holding the lock")
            .insert(thread, doing);
        // The watching thread only stops listening when the run ended.
        let _ = self.finished.send(false);
    }
}

// Ended tells the watching thread the run ended, however it ended.
struct Ended<'a>(&'a Progress);

impl Drop for Ended<'_> {
    fn drop(&mut self) {
        let _ = self.0.finished.send(true);
    }
}

fn main() -> ExitCode {
    let arguments = Arguments::parse();
    let seed = arguments.seed.unwrap_or_else(rand::random);
    println!("seed {seed}");
    let began = Instant::now();
    let (finished, steps) = mpsc::channel();
    let progress = Progress {
        finished,
        doing: Mutex::new(BTreeMap::new()),
    };
    let ran = std::thread::scope(|scope| {
        let running = scope.spawn(|| {
            let _ended = Ended(&progress);
            run(&arguments, seed, &progress)
        });
        loop {
            match steps.recv_timeout(arguments.hangs_after) {
                Ok(false) => {}
                Ok(true) | Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => {
                    eprintln!(
                        "HUNG with seed {seed}: no step finished in {} s",
                        arguments.hangs_after.as_secs()
                    );
                    let doing = progress
                        .doing
                        .lock()
                        .expect("no thread panics holding the lock");
                    for doing in doing.values() {
                        eprintln!("  {doing}");
                    }
                    // A thread stuck in the kernel cannot be stopped, so the process ends with the mounts left behind.
                    std::process::exit(2);
                }
            }
        }
        running.join()
    });
    match ran {
        Ok(Ok(())) => {
            println!("passed in {} s", began.elapsed().as_secs());
            ExitCode::SUCCESS
        }
        Ok(Err(error)) => {
            eprintln!("FAILED with seed {seed}: {error}");
            ExitCode::FAILURE
        }
        Err(_) => {
            eprintln!("FAILED with seed {seed}: the run panicked");
            ExitCode::FAILURE
        }
    }
}

// run makes the volume and its mounts, runs the workload on them, and checks a fresh mount shows what they ended with.
// A run that fails leaves its directory behind.
fn run(arguments: &Arguments, seed: u64, progress: &Progress) -> Result<(), String> {
    if arguments.strong + arguments.eventual == 0 {
        return Err("the volume needs a mount: give --strong or --eventual".to_owned());
    }
    if arguments.threads == 0 {
        return Err("the run needs a thread".to_owned());
    }
    let directory = match &arguments.directory {
        Some(directory) => directory.clone(),
        None => std::env::temp_dir().join(format!("stratafs-e2e-{seed}")),
    };
    let made = |error| format!("making a directory in {}: {error}", directory.display());
    fs::create_dir(&directory).map_err(made)?;
    // A mount point named through a link, as the temporary directory is on macOS, would not match the mount table.
    let directory = fs::canonicalize(&directory).map_err(made)?;
    let native = directory.join("native");
    fs::create_dir(&native).map_err(made)?;
    progress.now(0, "making the volume".to_owned());
    let coordinator =
        Coordinator::start().map_err(|error| format!("starting the coordinator: {error}"))?;
    let store = Store::create(
        &arguments.platform,
        &directory.join("store"),
        &coordinator.url,
    )?;

    let kinds = std::iter::repeat_n(ConsistencyKind::Strong, arguments.strong).chain(
        std::iter::repeat_n(ConsistencyKind::Eventual, arguments.eventual),
    );
    let mut mounts = Vec::new();
    let mut mounted = Vec::new();
    for (index, consistency) in kinds.enumerate() {
        let mount = Mount {
            index,
            consistency,
            root: directory.join(format!("mount-{index}-{consistency}")),
        };
        progress.now(0, format!("mounting {mount}"));
        let mounting = fs::create_dir(&mount.root)
            .map_err(made)
            .and_then(|()| store.mount(index, consistency, &mount.root));
        match mounting {
            Ok(mounting) => mounted.push(mounting),
            Err(error) => {
                let _ = unmount(mounted, progress);
                return Err(error);
            }
        }
        mounts.push(mount);
    }

    let workload = Workload {
        mounts: &mounts,
        native: &native,
        seed,
        operations: arguments.operations,
        threads: arguments.threads,
        converges_within: arguments.converges_within,
    };
    let worked = workload.run(progress);
    let unmounted = unmount(mounted, progress);
    let shared = worked?;
    unmounted?;

    // A mount made now has nothing cached, so it shows what the store holds.
    let again = Mount {
        index: mounts.len(),
        consistency: mounts[0].consistency,
        root: directory.join("mount-again"),
    };
    progress.now(0, format!("mounting {again}, a fresh mount"));
    fs::create_dir(&again.root).map_err(made)?;
    let mounted = store.mount(again.index, again.consistency, &again.root)?;
    let stored = stored(
        &again,
        &native,
        arguments.threads,
        shared.as_ref(),
        progress,
    );
    let unmounted = unmount(vec![mounted], progress);
    stored?;
    unmounted?;
    fs::remove_dir_all(&directory)
        .map_err(|error| format!("removing {}: {error}", directory.display()))
}

// stored checks a fresh mount shows each thread's directory as the native file system has it, and the shared directory
// as the mounts agreed on it.
fn stored(
    again: &Mount,
    native: &Path,
    threads: usize,
    shared: Option<&snapshot::Snapshot>,
    progress: &Progress,
) -> Result<(), String> {
    progress.now(0, format!("reading everything back on {again}"));
    for thread in 0..threads {
        let name = workload::directory(thread);
        let entries = snapshot(&native.join(&name))
            .map_err(|error| format!("reading the native {name}: {error}"))?;
        shows(again, &name, &entries, None).map_err(|difference| {
            format!("after unmounting every mount, on {again}, {name}/{difference}")
        })?;
    }
    if let Some(shared) = shared {
        shows(again, SHARED, shared, None).map_err(|difference| {
            format!("after unmounting every mount, on {again}, {SHARED}/{difference}")
        })?;
    }
    Ok(())
}

// unmount unmounts every mount, and returns the first failure once it tried them all.
fn unmount(mounted: Vec<Mounted>, progress: &Progress) -> Result<(), String> {
    progress.now(0, "unmounting".to_owned());
    let mut unmounted = Ok(());
    for mounted in mounted {
        unmounted = unmounted.and(mounted.unmount());
    }
    unmounted
}
