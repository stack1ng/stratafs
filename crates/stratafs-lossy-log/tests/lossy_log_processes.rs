use std::env;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use stratafs_lossy_log::{Read, Reader, Writer};

// A child process runs this test binary again, running only child, with its role in these variables.
const DIRECTORY: &str = "STRATAFS_LOSSY_LOG_TEST_DIRECTORY";
const SEGMENT: &str = "STRATAFS_LOSSY_LOG_TEST_SEGMENT";
const WRITER: &str = "STRATAFS_LOSSY_LOG_TEST_WRITER";
const APPENDS: &str = "STRATAFS_LOSSY_LOG_TEST_APPENDS";

const CHILDREN: usize = 4;
// A large segment fills a few times in a test, and a small one thousands of times.
const LARGE: usize = 1 << 15;
const SMALL: usize = 8;
// MARKER is the writer of the entry the test appends once the children are done.
const MARKER: u64 = u64::MAX;
// DEADLINE is how long a test waits for a read it expects.
const DEADLINE: Duration = Duration::from_secs(30);

#[test]
#[ignore = "runs only as a child process of the other tests in this file"]
fn child() {
    let directory = env::var_os(DIRECTORY).unwrap();
    let writer = env::var(WRITER).unwrap().parse().unwrap();
    let appends = env::var(APPENDS).unwrap().parse().unwrap();
    match env::var(SEGMENT).as_deref() {
        Ok("large") => append::<LARGE>(directory.as_ref(), writer, appends),
        Ok("small") => append::<SMALL>(directory.as_ref(), writer, appends),
        segment => panic!("no segment size {segment:?}"),
    }
}

// append appends entries once the test says go, so every child appends at once. It appends them in runs of 1 to
// BATCH entries, which may cross a segment's end.
fn append<const S: usize>(directory: &Path, writer: u64, appends: u64) {
    const BATCH: u64 = 5;
    let log = Writer::<16, S>::open(directory).unwrap();
    println!("ready");
    let mut go = String::new();
    std::io::stdin().read_line(&mut go).unwrap();
    let mut number = 0;
    while number < appends {
        let end = appends.min(number + number % BATCH + 1);
        log.append((number..end).map(|number| entry(writer, number)))
            .unwrap();
        number = end;
    }
}

#[test]
fn processes_appending_at_once_reach_every_reader_in_order() {
    const APPENDED: u64 = 40_000;
    let directory = tempfile::tempdir().unwrap();
    let readers: Vec<_> = (0..2).map(|_| start::<LARGE>(directory.path())).collect();
    run_children("large", directory.path(), APPENDED);
    let marker = Writer::<16, LARGE>::open(directory.path()).unwrap();
    for (_reader, receiver) in &readers {
        Checker::default().check_all(receiver, &marker, APPENDED);
    }
    assert_eq!(segments(directory.path()), 1);
}

#[test]
fn processes_appending_into_small_segments_at_once_miss_no_entry_without_a_gap() {
    const APPENDED: u64 = 5_000;
    let directory = tempfile::tempdir().unwrap();
    let (_reader, receiver) = start::<SMALL>(directory.path());
    // The slow reader holds its first read until the children are done, so it falls far behind. It drops after
    // release, which lets it go.
    let _slow_reader;
    let (sender, slow_receiver) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let mut first = true;
    _slow_reader = Reader::start::<16, SMALL>(directory.path(), move |read| {
        let _ = sender.send(read);
        if first {
            first = false;
            let _ = released.recv();
        }
    })
    .unwrap();
    run_children("small", directory.path(), APPENDED);
    release.send(()).unwrap();
    let marker = Writer::<16, SMALL>::open(directory.path()).unwrap();
    let mut slow = Checker::default();
    slow.check_all(&slow_receiver, &marker, APPENDED);
    assert!(slow.gaps > 0);
    Checker::default().check_all(&receiver, &marker, APPENDED);
    assert_eq!(segments(directory.path()), 1);
}

// run_children runs CHILDREN children that each append appends entries at once, and waits until they are done.
fn run_children(segment: &str, directory: &Path, appends: u64) {
    let mut children: Vec<_> = (0..CHILDREN)
        .map(|writer| Spawned::new(segment, directory, writer, appends))
        .collect();
    // The outputs stay open until the children end.
    let _outputs: Vec<_> = children
        .iter_mut()
        .map(|child| {
            let mut output = BufReader::new(child.0.stdout.take().unwrap()).lines();
            assert!(output.any(|line| line.unwrap() == "ready"));
            output
        })
        .collect();
    for child in &mut children {
        child.0.stdin.as_mut().unwrap().write_all(b"go\n").unwrap();
    }
    for mut child in children {
        assert!(child.0.wait().unwrap().success());
    }
}

// start starts a reader that passes what it reads on.
fn start<const S: usize>(directory: &Path) -> (Reader, Receiver<Read<16>>) {
    let (sender, receiver) = mpsc::channel();
    let reader = Reader::start::<16, S>(directory, move |read| {
        let _ = sender.send(read);
    })
    .unwrap();
    (reader, receiver)
}

// Checker checks that a reader reads each writer's entries in order, missing none without a Gap before the next.
#[derive(Default)]
struct Checker {
    next: [u64; CHILDREN],
    gapped: [bool; CHILDREN],
    gaps: usize,
}

impl Checker {
    // check_all checks what a reader reads until a marker, then that it read every writer's last entry, unless a
    // Gap came after its last read. A Gap may skip the marker, so marker appends another after each Gap.
    fn check_all<const S: usize>(
        &mut self,
        receiver: &Receiver<Read<16>>,
        marker: &Writer<16, S>,
        appends: u64,
    ) {
        marker.append([entry(MARKER, 0)]).unwrap();
        loop {
            let read = receiver.recv_timeout(DEADLINE).unwrap();
            if read == Read::Gap {
                marker.append([entry(MARKER, 0)]).unwrap();
            }
            if !self.check(read) {
                break;
            }
        }
        for writer in 0..CHILDREN {
            assert!(
                self.next[writer] == appends || self.gapped[writer],
                "writer {writer}'s entries end at {}",
                self.next[writer]
            );
        }
    }

    // check checks read, and says whether reading goes on, which it does until the marker.
    fn check(&mut self, read: Read<16>) -> bool {
        let Read::Entry(entry) = read else {
            self.gaps += 1;
            self.gapped = [true; CHILDREN];
            return true;
        };
        let (writer, number) = numbers_of(&entry);
        if writer == MARKER {
            return false;
        }
        let writer = usize::try_from(writer).unwrap();
        let next = self.next[writer];
        assert!(
            number == next || self.gapped[writer] && number > next,
            "writer {writer} appended {number}, but {next} was next"
        );
        (self.next[writer], self.gapped[writer]) = (number + 1, false);
        true
    }
}

fn entry(writer: u64, number: u64) -> [u8; 16] {
    let mut entry = [0; 16];
    entry[..8].copy_from_slice(&writer.to_le_bytes());
    entry[8..].copy_from_slice(&number.to_le_bytes());
    entry
}

fn numbers_of(entry: &[u8; 16]) -> (u64, u64) {
    let (writer, number) = entry.split_at(8);
    (
        u64::from_le_bytes(writer.try_into().unwrap()),
        u64::from_le_bytes(number.try_into().unwrap()),
    )
}

fn segments(directory: &Path) -> usize {
    std::fs::read_dir(directory).unwrap().count()
}

struct Spawned(Child);

impl Spawned {
    fn new(segment: &str, directory: &Path, writer: usize, appends: u64) -> Self {
        Self(
            Command::new(env::current_exe().unwrap())
                .args(["child", "--exact", "--ignored", "--nocapture", "--quiet"])
                .env(DIRECTORY, directory)
                .env(SEGMENT, segment)
                .env(WRITER, writer.to_string())
                .env(APPENDS, appends.to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        )
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
