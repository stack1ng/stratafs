// Mounts a real volume with the daemon, through the kernel's FUSE, once per store and consistency, and drives it with
// ordinary file system calls. It needs /dev/fuse and the right to mount, as cargo xtask fuse-test's container has.
#![cfg(target_os = "linux")]

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::fd::AsFd;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

use nix::poll::{PollFd, PollFlags, PollTimeout, poll};
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use stratafs_ed25519_signer::Ed25519Signer;
use tempfile::TempDir;

const DAEMON: &str = env!("CARGO_BIN_EXE_stratafs-local");

// NOBODY is the user and group of the tests' other user, who did not mount the volume.
const NOBODY: u32 = 65534;

// MOUNT_TIMEOUT is how long a mount may take to appear in the mount table.
const MOUNT_TIMEOUT: Duration = Duration::from_secs(30);

// DEADLOCK_TIMEOUT is how long a mount may be up before the test takes it for deadlocked and aborts its connection.
// That fails the requests the kernel waits on, which no signal can, so the test fails instead of leaving the container
// stuck.
const DEADLOCK_TIMEOUT: Duration = Duration::from_secs(120);

// Store is a store directory with one volume in it, the coordinator its mounts share, and the flags a mount of it takes.
struct Store {
    directory: TempDir,
    volume: String,
    flags: Vec<String>,
    _coordinator: Coordinator,
}

impl Store {
    // new makes a store of database with one volume, which is not signed.
    fn new(database: &str) -> Store {
        Store::with(database, &[])
    }

    // signed makes a store of database with one volume, signed with a new key.
    fn signed(database: &str) -> Store {
        Store::with(database, &["--signing-key".to_owned(), new_key()])
    }

    // with makes a store of database with one volume, which create and every mount take signing for.
    fn with(database: &str, signing: &[String]) -> Store {
        let directory = tempfile::tempdir().unwrap();
        let created = Command::new(DAEMON)
            .arg("create")
            .arg(directory.path())
            .args(["--store", database])
            .args(signing)
            .output()
            .unwrap();
        assert!(
            created.status.success(),
            "create failed: {}",
            String::from_utf8_lossy(&created.stderr)
        );
        let volume = String::from_utf8(created.stdout).unwrap().trim().to_owned();
        let coordinator = Coordinator::start();
        let mut flags = vec![
            "--store".to_owned(),
            database.to_owned(),
            "--coordinator".to_owned(),
            coordinator.url.clone(),
        ];
        flags.extend_from_slice(signing);
        Store {
            directory,
            volume,
            flags,
            _coordinator: coordinator,
        }
    }

    // mount mounts the volume with consistency on a new directory, and returns once the kernel has it mounted.
    fn mount(&self, consistency: &str) -> Mounted {
        let mount_point = tempfile::tempdir().unwrap();
        let daemon = Command::new(DAEMON)
            .arg("mount")
            .arg(self.directory.path())
            .arg(mount_point.path())
            .args(["--volume", &self.volume])
            .args(["--consistency", consistency])
            .args(&self.flags)
            .stdin(Stdio::null())
            // The daemon writes nothing to its output, so the pipe's one event is closing when the daemon exits.
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let (unmounted, watched) = mpsc::channel();
        let mut mounted = Mounted {
            daemon,
            mount_point,
            _unmounted: unmounted,
        };
        mounted.wait_until_mounted(true);
        // The kernel numbers a FUSE connection by its mount's device minor. A mount that cannot read its own root, as
        // one with the wrong key cannot, has no number to find, and is left unwatched.
        if let Ok(root) = fs::metadata(mounted.mount_point.path()) {
            let connection = libc::minor(root.dev());
            std::thread::spawn(move || {
                if let Err(RecvTimeoutError::Timeout) = watched.recv_timeout(DEADLOCK_TIMEOUT) {
                    abort(connection);
                }
            });
        }
        mounted
    }
}

// abort aborts FUSE connection, through the FUSE control file system.
fn abort(connection: u32) {
    let connections = Path::new("/sys/fs/fuse/connections");
    if !connections.join(connection.to_string()).exists() {
        run(Command::new("mount")
            .args(["-t", "fusectl", "fusectl"])
            .arg(connections));
    }
    eprintln!("aborting FUSE connection {connection}, which looks deadlocked");
    fs::write(connections.join(format!("{connection}/abort")), "1").unwrap();
}

// Mounted is a daemon serving a volume on mount_point, until it is unmounted or DEADLOCK_TIMEOUT passes.
struct Mounted {
    daemon: Child,
    mount_point: TempDir,
    // _unmounted stops the deadlock timeout when the mount goes.
    _unmounted: mpsc::Sender<()>,
}

impl Mounted {
    fn path(&self, name: &str) -> PathBuf {
        self.mount_point.path().join(name)
    }

    fn is_mounted(&self) -> bool {
        is_mounted(self.mount_point.path())
    }

    // wait_until_mounted blocks until is_mounted says mounted, waking at each change to the mount table, and fails if
    // the daemon exits first or MOUNT_TIMEOUT passes.
    fn wait_until_mounted(&mut self, mounted: bool) {
        // Opened before the first check, so a change after it wakes the poll.
        let table = File::open("/proc/self/mountinfo").unwrap();
        let deadline = Instant::now() + MOUNT_TIMEOUT;
        while self.is_mounted() != mounted {
            let output = self.daemon.stdout.as_ref().unwrap();
            let mut events = [
                PollFd::new(table.as_fd(), PollFlags::POLLPRI),
                PollFd::new(output.as_fd(), PollFlags::empty()),
            ];
            let left = deadline.saturating_duration_since(Instant::now());
            let ready = poll(&mut events, PollTimeout::try_from(left).unwrap()).unwrap();
            let change = if mounted { "mounting" } else { "detaching" };
            assert!(
                ready > 0,
                "the mount table never showed the daemon {change}"
            );
            if events[1].revents().is_some_and(|events| !events.is_empty()) {
                let status = self.daemon.wait().unwrap();
                panic!("the daemon exited with {status} before {change}");
            }
        }
    }

    // unmount unmounts the volume, as a person would, and waits for the daemon to exit cleanly.
    fn unmount(mut self) {
        run(Command::new("umount").arg(self.mount_point.path()));
        let status = self.daemon.wait().unwrap();
        assert!(status.success(), "the daemon exited with {status}");
    }

    // kill kills the daemon, as a crash would, and clears its dead mount away.
    fn kill(mut self) {
        self.daemon.kill().unwrap();
        self.daemon.wait().unwrap();
        run(Command::new("umount")
            .arg("-l")
            .arg(self.mount_point.path()));
    }
}

// is_mounted says whether the mount table has a stratafs FUSE volume on mount_point.
fn is_mounted(mount_point: &Path) -> bool {
    let mount_point = mount_point.to_str().unwrap();
    fs::read_to_string("/proc/self/mountinfo")
        .unwrap()
        .lines()
        .any(|line| {
            let (mount, source) = line.split_once(" - ").unwrap();
            let mut source = source.split(' ');
            mount.split(' ').nth(4) == Some(mount_point)
                && source.next().is_some_and(|kind| kind.starts_with("fuse"))
                && source.next() == Some("stratafs")
        })
}

fn run(command: &mut Command) {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn names(directory: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

// one_mount drives a single mount of a fresh volume of database with consistency through every operation the test
// covers, then checks what it wrote survives unmounting, and an fsync survives the daemon being killed.
fn one_mount(database: &str, consistency: &str) {
    let store = Store::new(database);
    let mounted = store.mount(consistency);

    // create, write and read
    fs::write(mounted.path("a.txt"), b"hello").unwrap();
    assert_eq!(fs::read(mounted.path("a.txt")).unwrap(), b"hello");
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(mounted.path("a.txt"))
        .unwrap();
    file.seek(SeekFrom::Start(3)).unwrap();
    file.write_all(b"p me").unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut read = String::new();
    file.read_to_string(&mut read).unwrap();
    assert_eq!(read, "help me");
    drop(file);
    let mut appended = OpenOptions::new()
        .append(true)
        .open(mounted.path("a.txt"))
        .unwrap();
    appended.write_all(b"!").unwrap();
    drop(appended);
    assert_eq!(
        fs::read_to_string(mounted.path("a.txt")).unwrap(),
        "help me!"
    );
    assert_eq!(fs::metadata(mounted.path("a.txt")).unwrap().len(), 8);

    // a larger file, across many pages and writes
    let large: Vec<u8> = (0..3_000_000u32).map(|byte| byte as u8).collect();
    fs::write(mounted.path("large"), &large).unwrap();
    assert_eq!(fs::read(mounted.path("large")).unwrap(), large);
    let truncated = OpenOptions::new()
        .write(true)
        .open(mounted.path("large"))
        .unwrap();
    truncated.set_len(10).unwrap();
    drop(truncated);
    assert_eq!(fs::read(mounted.path("large")).unwrap(), &large[..10]);

    // mkdir, rename and readdir
    fs::create_dir(mounted.path("d")).unwrap();
    fs::rename(mounted.path("a.txt"), mounted.path("d/b.txt")).unwrap();
    assert_eq!(
        fs::metadata(mounted.path("a.txt")).unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(
        fs::read_to_string(mounted.path("d/b.txt")).unwrap(),
        "help me!"
    );
    assert_eq!(names(mounted.mount_point.path()), ["d", "large"]);
    assert_eq!(names(&mounted.path("d")), ["b.txt"]);
    for index in 0..200 {
        File::create(mounted.path(&format!("d/{index:03}"))).unwrap();
    }
    assert_eq!(names(&mounted.path("d")).len(), 201);

    // symbolic and hard links
    symlink("d/b.txt", mounted.path("link")).unwrap();
    assert_eq!(
        fs::read_link(mounted.path("link")).unwrap(),
        Path::new("d/b.txt")
    );
    assert_eq!(
        fs::read_to_string(mounted.path("link")).unwrap(),
        "help me!"
    );
    fs::hard_link(mounted.path("d/b.txt"), mounted.path("hard")).unwrap();
    assert_eq!(fs::metadata(mounted.path("hard")).unwrap().nlink(), 2);

    // extended attributes
    xattr::set(mounted.path("hard"), "user.color", b"blue").unwrap();
    assert_eq!(
        xattr::get(mounted.path("d/b.txt"), "user.color").unwrap(),
        Some(b"blue".to_vec())
    );
    let listed: Vec<_> = xattr::list(mounted.path("hard")).unwrap().collect();
    assert_eq!(listed, ["user.color"]);
    xattr::remove(mounted.path("hard"), "user.color").unwrap();
    assert_eq!(
        xattr::get(mounted.path("hard"), "user.color").unwrap(),
        None
    );

    // unlink and rmdir
    fs::remove_file(mounted.path("d/b.txt")).unwrap();
    assert_eq!(fs::metadata(mounted.path("hard")).unwrap().nlink(), 1);
    for index in 0..200 {
        fs::remove_file(mounted.path(&format!("d/{index:03}"))).unwrap();
    }
    assert!(names(&mounted.path("d")).is_empty());
    fs::remove_dir(mounted.path("d")).unwrap();
    fs::remove_file(mounted.path("link")).unwrap();
    assert_eq!(names(mounted.mount_point.path()), ["hard", "large"]);

    // An unmount keeps everything committed.
    fs::write(mounted.path("unsynced"), b"kept on unmount").unwrap();
    mounted.unmount();
    let mounted = store.mount(consistency);
    assert_eq!(
        fs::read_to_string(mounted.path("unsynced")).unwrap(),
        "kept on unmount"
    );
    assert_eq!(
        fs::read_to_string(mounted.path("hard")).unwrap(),
        "help me!"
    );

    // An fsync keeps a write through the daemon being killed.
    let mut synced = File::create(mounted.path("synced")).unwrap();
    synced.write_all(b"kept through a crash").unwrap();
    synced.sync_all().unwrap();
    drop(synced);
    mounted.kill();
    let mounted = store.mount(consistency);
    assert_eq!(
        fs::read_to_string(mounted.path("synced")).unwrap(),
        "kept through a crash"
    );
    mounted.unmount();
}

// Coordinator is a coordinator the mounts of a volume share, running on a Tokio runtime of its own.
struct Coordinator {
    _runtime: tokio::runtime::Runtime,
    url: String,
}

impl Coordinator {
    fn start() -> Coordinator {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        runtime.spawn(stratafs_websocket_pubsub_native::serve(listener));
        Coordinator {
            _runtime: runtime,
            url,
        }
    }
}

#[test]
fn an_eventual_mdbx_volume() {
    one_mount("mdbx", "eventual");
}

#[test]
fn an_eventual_redb_volume() {
    one_mount("redb", "eventual");
}

#[test]
fn an_eventual_sqlite_volume() {
    one_mount("sqlite", "eventual");
}

#[test]
fn a_strong_mdbx_volume() {
    one_mount("mdbx", "strong");
}

#[test]
fn a_strong_redb_volume() {
    one_mount("redb", "strong");
}

#[test]
fn a_strong_sqlite_volume() {
    one_mount("sqlite", "strong");
}

// An append through a file opened to append lands at the end another mount moved, which this mount's kernel still
// takes for where the file ends.
#[test]
fn an_append_lands_at_the_end_another_mount_moved() {
    let store = Store::new("mdbx");
    let (first, second) = (store.mount("strong"), store.mount("strong"));
    let append = |mounted: &Mounted| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(mounted.path("log"))
            .unwrap()
    };
    let mut log = append(&first);
    log.write_all(b"a").unwrap();
    append(&second).write_all(b"bb").unwrap();
    log.write_all(b"c").unwrap();
    drop(log);
    assert_eq!(fs::read_to_string(first.path("log")).unwrap(), "abbc");
    assert_eq!(fs::read_to_string(second.path("log")).unwrap(), "abbc");
    first.unmount();
    second.unmount();
}

// CONVERGES_WITHIN is how long an eventual mount may take to see another mount's change.
const CONVERGES_WITHIN: Duration = Duration::from_secs(1);

// two_eventual_mounts has two daemons mount one volume of database eventually consistent, and checks that each sees
// the other's changes to what it caches.
fn two_eventual_mounts(database: &str) {
    let store = Store::new(database);
    let (first, second) = (store.mount("eventual"), store.mount("eventual"));
    fs::write(first.path("file"), b"one").unwrap();
    fs::write(first.path("gone"), b"").unwrap();
    assert_eq!(fs::read(second.path("file")).unwrap(), b"one");
    assert!(second.path("gone").exists());
    fs::write(first.path("file"), b"two").unwrap();
    fs::remove_file(first.path("gone")).unwrap();
    std::thread::sleep(CONVERGES_WITHIN);
    let (content, gone) = (
        fs::read(second.path("file")).unwrap(),
        !second.path("gone").exists(),
    );
    first.unmount();
    second.unmount();
    assert_eq!(
        content, b"two",
        "the second mount kept the first's old content"
    );
    assert!(gone, "the second mount kept a name the first removed");
}

#[test]
fn two_eventual_mdbx_mounts_see_each_others_changes() {
    two_eventual_mounts("mdbx");
}

#[test]
fn two_eventual_sqlite_mounts_see_each_others_changes() {
    two_eventual_mounts("sqlite");
}

// A strong mount sees each of an eventual mount's changes to what its kernel caches as soon as the change returns:
// content of another length, a mode, and a name it resolved that the eventual mount unlinks. Every write waits until
// each strong mount dropped its copy.
#[test]
fn a_strong_mount_sees_an_eventual_mounts_changes_as_soon_as_they_return() {
    let store = Store::new("mdbx");
    let (strong, eventual) = (store.mount("strong"), store.mount("eventual"));
    fs::write(eventual.path("file"), b"one").unwrap();
    assert_eq!(fs::read(strong.path("file")).unwrap(), b"one");
    fs::write(eventual.path("file"), b"three").unwrap();
    assert_eq!(fs::read(strong.path("file")).unwrap(), b"three");
    assert_eq!(fs::metadata(strong.path("file")).unwrap().len(), 5);
    assert_ne!(
        fs::metadata(strong.path("file")).unwrap().mode() & 0o777,
        0o600
    );
    fs::set_permissions(eventual.path("file"), fs::Permissions::from_mode(0o600)).unwrap();
    assert_eq!(
        fs::metadata(strong.path("file")).unwrap().mode() & 0o777,
        0o600
    );
    File::create(eventual.path("gone")).unwrap();
    assert!(strong.path("gone").exists());
    fs::remove_file(eventual.path("gone")).unwrap();
    assert!(!strong.path("gone").exists());
    strong.unmount();
    eventual.unmount();
}

// new_key is a new signing key as the command line takes it.
fn new_key() -> String {
    Ed25519Signer::generate().key()
}

// Every mount of a signed volume signs what it writes and verifies what it reads, so a mount with the volume's key
// reads what another wrote, and one with another key reads nothing.
#[test]
fn a_signed_volume_is_read_with_its_key_only() {
    let mut store = Store::signed("mdbx");
    let (strong, eventual) = (store.mount("strong"), store.mount("eventual"));
    fs::create_dir(eventual.path("dir")).unwrap();
    fs::write(eventual.path("dir/file"), vec![7; 100_000]).unwrap();
    assert_eq!(fs::read(strong.path("dir/file")).unwrap(), vec![7; 100_000]);
    let mut file = OpenOptions::new()
        .write(true)
        .open(strong.path("dir/file"))
        .unwrap();
    file.seek(SeekFrom::Start(50_000)).unwrap();
    file.write_all(b"middle").unwrap();
    drop(file);
    fs::rename(strong.path("dir/file"), strong.path("moved")).unwrap();
    symlink("moved", strong.path("link")).unwrap();
    assert_eq!(names(strong.mount_point.path()), ["dir", "link", "moved"]);
    assert_eq!(
        fs::read(strong.path("link")).unwrap()[49_999..50_007],
        *b"\x07middle\x07"
    );
    strong.unmount();
    eventual.unmount();

    let again = store.mount("strong");
    assert_eq!(fs::read(again.path("moved")).unwrap().len(), 100_000);
    again.unmount();

    // Another key signs nothing this volume holds.
    *store.flags.last_mut().unwrap() = new_key();
    let untrusted = store.mount("eventual");
    // It cannot read even the volume's root directory.
    let refused = fs::metadata(untrusted.mount_point.path()).unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(libc::EIO));
    let refused = fs::read(untrusted.path("moved")).unwrap_err();
    assert_eq!(refused.raw_os_error(), Some(libc::EIO));
    untrusted.unmount();
}

// A mount serves only the user who mounted it, unless it allows other users.
#[test]
fn only_a_mount_that_allows_other_users_serves_them() {
    let mut store = Store::new("mdbx");
    let read_as_nobody = |mounted: &Mounted| {
        Command::new("cat")
            .arg(mounted.path("file"))
            .uid(NOBODY)
            .gid(NOBODY)
            .output()
            .unwrap()
    };
    let mounted = store.mount("eventual");
    fs::write(mounted.path("file"), b"shared").unwrap();
    let refused = read_as_nobody(&mounted);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("Permission denied"));
    mounted.unmount();
    store.flags.push("--allow-other".to_owned());
    let mounted = store.mount("eventual");
    let read = read_as_nobody(&mounted);
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stderr)
    );
    assert_eq!(read.stdout, b"shared");
    mounted.unmount();
}

#[test]
fn sigterm_unmounts_the_volume() {
    let store = Store::new("mdbx");
    let mut mounted = store.mount("eventual");
    fs::write(mounted.path("file"), b"kept on SIGTERM").unwrap();
    let daemon = Pid::from_raw(i32::try_from(mounted.daemon.id()).unwrap());
    kill(daemon, Signal::SIGTERM).unwrap();
    let status = mounted.daemon.wait().unwrap();
    assert!(status.success(), "the daemon exited with {status}");
    assert!(!mounted.is_mounted());
    let mounted = store.mount("eventual");
    assert_eq!(
        fs::read_to_string(mounted.path("file")).unwrap(),
        "kept on SIGTERM"
    );
    mounted.unmount();
}

// SIGTERM while a file on the volume is open detaches the mount, which the daemon serves until the file closes.
#[test]
fn sigterm_while_a_file_is_open_unmounts_once_it_closes() {
    let store = Store::new("mdbx");
    let mut mounted = store.mount("eventual");
    let mut open = File::create(mounted.path("file")).unwrap();
    open.write_all(b"kept").unwrap();
    let daemon = Pid::from_raw(i32::try_from(mounted.daemon.id()).unwrap());
    kill(daemon, Signal::SIGTERM).unwrap();
    mounted.wait_until_mounted(false);
    open.write_all(b" while detached").unwrap();
    assert!(mounted.daemon.try_wait().unwrap().is_none());
    drop(open);
    let status = mounted.daemon.wait().unwrap();
    assert!(status.success(), "the daemon exited with {status}");
    let mounted = store.mount("eventual");
    assert_eq!(
        fs::read_to_string(mounted.path("file")).unwrap(),
        "kept while detached"
    );
    mounted.unmount();
}
