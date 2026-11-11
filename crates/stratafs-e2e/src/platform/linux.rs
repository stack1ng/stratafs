// Mounts through FUSE, each served by a stratafs-fuse daemon of its own. The store must be mdbx or sqlite when the
// volume gets more than one mount.

use std::fs::{self, File};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use clap::Args;
use nix::poll::{PollFd, PollFlags, PollTimeout, poll};

use crate::ConsistencyKind;

// MOUNT_TIMEOUT is how long a daemon gets to mount its volume.
const MOUNT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Args)]
pub struct Arguments {
    #[arg(
        long,
        help = "The stratafs-fuse binary; the one beside this binary when left out"
    )]
    daemon: Option<PathBuf>,
    #[arg(
        long,
        default_value = "sqlite",
        help = "The store's database: mdbx, redb or sqlite"
    )]
    store: String,
}

// Store is a store directory with one volume in it, and what a mount of it is started with.
pub struct Store {
    daemon: PathBuf,
    database: String,
    directory: PathBuf,
    volume: String,
    coordinator: String,
}

impl Store {
    // create makes the store in directory with one volume, whose mounts reach the coordinator at its URL.
    pub fn create(
        arguments: &Arguments,
        directory: &Path,
        coordinator: &str,
    ) -> Result<Store, String> {
        let daemon = match &arguments.daemon {
            Some(daemon) => daemon.clone(),
            None => super::beside("stratafs-fuse")?,
        };
        let created = Command::new(&daemon)
            .arg("create")
            .arg(directory)
            .args(["--store", &arguments.store])
            .output()
            .map_err(|error| format!("starting {}: {error}", daemon.display()))?;
        if !created.status.success() {
            return Err(format!(
                "creating the volume failed: {}",
                String::from_utf8_lossy(&created.stderr)
            ));
        }
        Ok(Store {
            daemon,
            database: arguments.store.clone(),
            directory: directory.to_owned(),
            volume: String::from_utf8_lossy(&created.stdout).trim().to_owned(),
            coordinator: coordinator.to_owned(),
        })
    }

    // mount mounts the volume on mount_point with consistency, and returns once the kernel has it mounted.
    pub fn mount(
        &self,
        _index: usize,
        consistency: ConsistencyKind,
        mount_point: &Path,
    ) -> Result<Mounted, String> {
        let daemon = Command::new(&self.daemon)
            .arg("mount")
            .arg(&self.directory)
            .arg(mount_point)
            .args(["--volume", &self.volume])
            .args(["--store", &self.database])
            .args(["--consistency", &consistency.to_string()])
            .args(["--coordinator", &self.coordinator])
            .stdin(Stdio::null())
            // The daemon writes nothing to its output, so the pipe's one event is closing when the daemon exits.
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|error| format!("starting {}: {error}", self.daemon.display()))?;
        let mut mounted = Mounted {
            daemon,
            mount_point: mount_point.to_owned(),
        };
        mounted.wait_until_mounted()?;
        Ok(mounted)
    }
}

// Mounted is a daemon serving the volume on mount_point, until it is unmounted.
pub struct Mounted {
    daemon: Child,
    mount_point: PathBuf,
}

impl Mounted {
    // wait_until_mounted blocks until the mount table has the mount, waking at each change to the table, and fails if
    // the daemon exits first or MOUNT_TIMEOUT passes.
    fn wait_until_mounted(&mut self) -> Result<(), String> {
        let failed = |error| format!("waiting for the mount table: {error}");
        // Opened before the first check, so a change after it wakes the poll.
        let table = File::open("/proc/self/mountinfo").map_err(failed)?;
        let deadline = Instant::now() + MOUNT_TIMEOUT;
        while !is_mounted(&self.mount_point).map_err(failed)? {
            let output = self
                .daemon
                .stdout
                .as_ref()
                .expect("the daemon's output is piped");
            let mut events = [
                PollFd::new(table.as_fd(), PollFlags::POLLPRI),
                PollFd::new(output.as_fd(), PollFlags::empty()),
            ];
            let left = deadline.saturating_duration_since(Instant::now());
            let left = PollTimeout::try_from(left).expect("the mount timeout fits a poll");
            let ready = poll(&mut events, left).map_err(|error| failed(error.into()))?;
            let mount_point = self.mount_point.display();
            if ready == 0 {
                return Err(format!("the mount table never showed {mount_point}"));
            }
            if events[1].revents().is_some_and(|events| !events.is_empty()) {
                let status = self.daemon.wait().map_err(failed)?;
                return Err(format!(
                    "the daemon exited with {status} before mounting {mount_point}"
                ));
            }
        }
        Ok(())
    }

    // unmount unmounts the volume, and waits for the daemon to exit cleanly.
    pub fn unmount(mut self) -> Result<(), String> {
        let mount_point = self.mount_point.display();
        let unmounted = Command::new("umount")
            .arg(&self.mount_point)
            .output()
            .map_err(|error| format!("starting umount: {error}"))?;
        if !unmounted.status.success() {
            return Err(format!(
                "unmounting {mount_point}: {}",
                String::from_utf8_lossy(&unmounted.stderr)
            ));
        }
        let status = self
            .daemon
            .wait()
            .map_err(|error| format!("waiting for the daemon of {mount_point}: {error}"))?;
        if !status.success() {
            return Err(format!("the daemon of {mount_point} exited with {status}"));
        }
        Ok(())
    }
}

// is_mounted says whether the mount table has a stratafs FUSE volume on mount_point.
fn is_mounted(mount_point: &Path) -> std::io::Result<bool> {
    let table = fs::read_to_string("/proc/self/mountinfo")?;
    Ok(table.lines().any(|line| {
        let Some((mount, source)) = line.split_once(" - ") else {
            return false;
        };
        let mut source = source.split(' ');
        mount.split(' ').nth(4).map(Path::new) == Some(mount_point)
            && source.next().is_some_and(|kind| kind.starts_with("fuse"))
            && source.next() == Some("stratafs")
    }))
}
