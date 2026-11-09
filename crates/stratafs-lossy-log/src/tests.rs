use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

use crate::{CHECK, Read, Reader, Writer, numbers, path_of};

const S: usize = 4;
const RECORD: usize = 8 + CHECK;
// DEADLINE is how long a test waits for a read it expects.
const DEADLINE: Duration = Duration::from_secs(10);

fn entry(number: u64) -> [u8; 8] {
    number.to_le_bytes()
}

// start starts a reader that passes what it reads on.
fn start(directory: &Path) -> (Reader, Receiver<Read<8>>) {
    let (sender, receiver) = mpsc::channel();
    let reader = Reader::start::<8, S>(directory, move |read| {
        let _ = sender.send(read);
    })
    .unwrap();
    (reader, receiver)
}

// start_held starts a reader that passes what it reads on, and holds its first read until the test lets it go.
fn start_held(directory: &Path) -> (Reader, Receiver<Read<8>>, mpsc::Sender<()>) {
    let (sender, receiver) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let mut first = true;
    let reader = Reader::start::<8, S>(directory, move |read| {
        let _ = sender.send(read);
        if first {
            first = false;
            let _ = released.recv();
        }
    })
    .unwrap();
    (reader, receiver, release)
}

#[track_caller]
fn expect(receiver: &Receiver<Read<8>>, expected: &[Read<8>]) {
    for (position, expected) in expected.iter().enumerate() {
        let Ok(read) = receiver.recv_timeout(DEADLINE) else {
            panic!("no read {position}");
        };
        assert_eq!(read, *expected, "read {position}");
    }
}

fn entries(numbers: impl IntoIterator<Item = u64>) -> Vec<Read<8>> {
    numbers
        .into_iter()
        .map(|number| Read::Entry(entry(number)))
        .collect()
}

fn append_all(writer: &Writer<8, S>, numbers: impl IntoIterator<Item = u64>) {
    for number in numbers {
        writer.append([entry(number)]).unwrap();
    }
}

#[test]
fn a_reader_reads_every_entry_appended_after_it_started_in_order() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    append_all(&writer, 0..3);
    let (_reader, receiver) = start(directory.path());
    // Each append waits for its read, so the reader never falls behind.
    for number in 3..30 {
        append_all(&writer, [number]);
        expect(&receiver, &entries([number]));
    }
    assert_eq!(numbers(directory.path()).unwrap().len(), 1);
}

#[test]
fn a_reader_keeps_reading_a_segment_deleted_under_it() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver, release) = start_held(directory.path());
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
    append_all(&writer, 1..S as u64 + 2);
    assert_eq!(numbers(directory.path()).unwrap(), [1]);
    release.send(()).unwrap();
    expect(&receiver, &entries(1..S as u64 + 2));
}

#[test]
fn a_reader_that_falls_more_than_a_segment_behind_reads_a_gap() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver, release) = start_held(directory.path());
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
    append_all(&writer, 1..3 * S as u64 + 1);
    release.send(()).unwrap();
    // The reader reads the rest of the segment it holds, then finds the next one gone.
    expect(&receiver, &entries(1..S as u64));
    expect(&receiver, &[Read::Gap]);
    append_all(&writer, 100..101);
    expect(&receiver, &entries(100..101));
}

#[test]
fn a_torn_entry_reads_as_a_gap() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver) = start(directory.path());
    append_all(&writer, 0..1);
    let mut segment = OpenOptions::new()
        .append(true)
        .open(path_of(directory.path(), 0))
        .unwrap();
    // A whole entry whose check does not match, then a hole filled with zeros.
    let mut torn = [0; RECORD];
    torn[..8].copy_from_slice(&entry(1));
    segment.write_all(&torn).unwrap();
    segment.write_all(&[0; RECORD]).unwrap();
    append_all(&writer, 2..3);
    expect(&receiver, &[Read::Entry(entry(0)), Read::Gap, Read::Gap]);
    expect(&receiver, &entries(2..3));
}

#[test]
fn entries_after_a_partial_entry_read_as_gaps_until_the_next_segment() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver) = start(directory.path());
    append_all(&writer, 0..1);
    OpenOptions::new()
        .append(true)
        .open(path_of(directory.path(), 0))
        .unwrap()
        .write_all(&[1; RECORD / 2])
        .unwrap();
    // The last entry lands past the segment's end, so it goes to the next segment too.
    append_all(&writer, 1..4);
    expect(
        &receiver,
        &[Read::Entry(entry(0)), Read::Gap, Read::Gap, Read::Gap],
    );
    expect(&receiver, &entries(3..4));
}

#[test]
fn one_append_writes_all_its_entries() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver) = start(directory.path());
    writer.append((0..3).map(entry)).unwrap();
    expect(&receiver, &entries(0..3));
    assert_eq!(
        fs::read(path_of(directory.path(), 0)).unwrap().len(),
        3 * RECORD
    );
}

#[test]
fn the_entries_of_an_append_past_the_segment_end_go_to_the_next_segment() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver) = start(directory.path());
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
    writer.append((1..S as u64 + 2).map(entry)).unwrap();
    expect(&receiver, &entries(1..S as u64 + 2));
    assert_eq!(numbers(directory.path()).unwrap(), [1]);
    let segment = fs::read(path_of(directory.path(), 1)).unwrap();
    assert_eq!(segment.len(), 2 * RECORD);
    assert_eq!(segment[..8], entry(S as u64));
}

#[test]
fn an_append_longer_than_a_segment_ends_in_the_last_segment_it_fills() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    writer.append((0..2 * S as u64 + 2).map(entry)).unwrap();
    assert_eq!(numbers(directory.path()).unwrap(), [2]);
    let segment = fs::read(path_of(directory.path(), 2)).unwrap();
    assert_eq!(segment.len(), 2 * RECORD);
    assert_eq!(segment[RECORD..RECORD + 8], entry(2 * S as u64 + 1));
}

#[test]
fn a_writer_on_a_deleted_segment_appends_to_the_newest() {
    let directory = tempfile::tempdir().unwrap();
    let behind = Writer::<8, S>::open(directory.path()).unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    append_all(&writer, 0..2 * S as u64 + 1);
    append_all(&behind, 100..101);
    assert_eq!(numbers(directory.path()).unwrap(), [2]);
    let segment = fs::read(path_of(directory.path(), 2)).unwrap();
    assert_eq!(segment.len(), 2 * RECORD);
    assert_eq!(segment[RECORD..RECORD + 8], entry(100));
}

#[test]
fn appends_of_two_writers_at_a_segment_end_reach_the_next_segment_in_order() {
    let directory = tempfile::tempdir().unwrap();
    let behind = Writer::<8, S>::open(directory.path()).unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver) = start(directory.path());
    writer.append((0..S as u64 - 1).map(entry)).unwrap();
    // behind's append crosses the segment's end, so its last entry starts the next segment.
    behind.append([entry(10), entry(11)]).unwrap();
    // writer's file is the deleted segment, so its whole append lands past the end and goes to the newest.
    writer.append([entry(20), entry(21)]).unwrap();
    assert_eq!(numbers(directory.path()).unwrap(), [1]);
    let segment = fs::read(path_of(directory.path(), 1)).unwrap();
    assert_eq!(segment.len(), 3 * RECORD);
    assert_eq!(segment[..8], entry(11));
    assert_eq!(segment[RECORD..RECORD + 8], entry(20));
    // The next segment holds no entry twice, and the reader reads them all in order.
    expect(
        &receiver,
        &entries((0..S as u64 - 1).chain([10, 11, 20, 21])),
    );
}

#[test]
fn opening_deletes_a_segment_older_than_the_newest() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(path_of(directory.path(), 3), [0; RECORD]).unwrap();
    // An empty segment older than the newest, as a writer that read the directory before a deletion leaves.
    fs::write(path_of(directory.path(), 2), []).unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    assert_eq!(numbers(directory.path()).unwrap(), [3]);
    let (_reader, receiver) = start(directory.path());
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
}

#[test]
fn threads_appending_at_once_reach_a_reader_in_order() {
    const THREADS: u64 = 4;
    const APPENDS: u64 = 2000;
    let directory = tempfile::tempdir().unwrap();
    let (_reader, receiver) = start(directory.path());
    let shared = Writer::<8, S>::open(directory.path()).unwrap();
    thread::scope(|scope| {
        for thread in 0..THREADS {
            let shared = &shared;
            let directory = directory.path();
            scope.spawn(move || {
                // Half the threads share a writer, and half have their own, as processes do.
                let own = (thread % 2 == 0).then(|| Writer::<8, S>::open(directory).unwrap());
                let writer = own.as_ref().unwrap_or(shared);
                append_all(writer, (0..APPENDS).map(|number| thread << 32 | number));
            });
        }
    });
    // The marker ends the reads. A Gap may skip it, so another follows each Gap.
    shared.append([entry(u64::MAX)]).unwrap();
    let mut next = [0; THREADS as usize];
    let mut gapped = [false; THREADS as usize];
    loop {
        match receiver.recv_timeout(DEADLINE).unwrap() {
            Read::Gap => {
                gapped = [true; THREADS as usize];
                shared.append([entry(u64::MAX)]).unwrap();
            }
            Read::Entry(entry) => {
                let entry = u64::from_le_bytes(entry);
                if entry == u64::MAX {
                    break;
                }
                let (thread, number) = ((entry >> 32) as usize, entry & u64::from(u32::MAX));
                assert!(
                    number == next[thread] || gapped[thread] && number > next[thread],
                    "thread {thread} appended {number} after {}",
                    next[thread]
                );
                (next[thread], gapped[thread]) = (number + 1, false);
            }
        }
    }
    for thread in 0..THREADS as usize {
        assert!(next[thread] == APPENDS || gapped[thread]);
    }
    assert_eq!(numbers(directory.path()).unwrap().len(), 1);
}

#[test]
fn dropping_a_reader_stops_its_thread() {
    let directory = tempfile::tempdir().unwrap();
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (reader, receiver) = start(directory.path());
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
    drop(reader);
    append_all(&writer, 1..2);
    // The thread dropped on_read, and the sender in it, when it ended.
    assert!(receiver.recv().is_err());
}

#[test]
fn the_end_of_the_watch_reads_as_a_last_gap() {
    let parent = tempfile::tempdir().unwrap();
    let directory = parent.path().join("log");
    let writer = Writer::<8, S>::open(&directory).unwrap();
    let (mut reader, receiver, release) = start_held(&directory);
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
    end_watch(&mut reader, &directory);
    release.send(()).unwrap();
    expect(&receiver, &[Read::Gap]);
    append_all(&writer, 1..2);
    assert!(receiver.recv().is_err());
}

// end_watch ends a reader's watch. On Linux it removes the watch as the kernel would, since deleting the directory
// ends it only once no file in it is open, and the reader holds its segment.
#[cfg(target_os = "linux")]
fn end_watch(reader: &mut Reader, _directory: &Path) {
    reader.stop.end();
}

#[cfg(target_os = "macos")]
fn end_watch(_reader: &mut Reader, directory: &Path) {
    fs::remove_dir_all(directory).unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn an_overflow_of_the_watch_misses_no_new_segment() {
    let directory = tempfile::tempdir().unwrap();
    let others = fill_queue_with_others(directory.path());
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (_reader, receiver, release) = start_held(directory.path());
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
    // The overflow drops the next segment's creation.
    overflow_queue(&others);
    append_all(&writer, 1..S as u64 + 1);
    release.send(()).unwrap();
    expect(&receiver, &entries(1..S as u64 + 1));
}

// fill_queue_with_others creates one more file in directory than inotify's queue holds events.
#[cfg(target_os = "linux")]
fn fill_queue_with_others(directory: &Path) -> Vec<PathBuf> {
    let queued: usize = fs::read_to_string("/proc/sys/fs/inotify/max_queued_events")
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let others: Vec<_> = (0..=queued)
        .map(|other| directory.join(format!("other {other}")))
        .collect();
    for other in &others {
        fs::write(other, []).unwrap();
    }
    others
}

// overflow_queue writes to each of others, so a held reader's queue overflows and drops the events after it.
#[cfg(target_os = "linux")]
fn overflow_queue(others: &[PathBuf]) {
    for other in others {
        OpenOptions::new()
            .append(true)
            .open(other)
            .unwrap()
            .write_all(&[1])
            .unwrap();
    }
}

#[cfg(target_os = "linux")]
#[test]
fn dropping_a_reader_after_an_overflow_stops_its_thread() {
    let directory = tempfile::tempdir().unwrap();
    let others = fill_queue_with_others(directory.path());
    let writer = Writer::<8, S>::open(directory.path()).unwrap();
    let (mut reader, receiver, release) = start_held(directory.path());
    append_all(&writer, 0..1);
    expect(&receiver, &entries(0..1));
    overflow_queue(&others);
    // Stop's removal finds inotify's queue full, so the kernel drops its IN_IGNORED.
    reader.stop.stop();
    release.send(()).unwrap();
    let (dropped, done) = mpsc::channel();
    thread::spawn(move || {
        drop(reader);
        let _ = dropped.send(());
    });
    assert!(done.recv_timeout(DEADLINE).is_ok(), "the drop hangs");
}
