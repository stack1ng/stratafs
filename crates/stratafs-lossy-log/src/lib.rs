// A lossy log is an append only log of fixed size entries in a directory, which processes append to and read at
// once. It keeps one segment file of S entries, so a reader that falls more than a segment behind reads a Gap where
// the entries it missed were.

use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};
use std::thread::{self, JoinHandle};

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux::{Stop, Watch};
#[cfg(target_os = "macos")]
use macos::{Stop, Watch};

// Each entry is followed by its check, the xxh32 of the entry, so a write torn by a crash, or a hole a crash filled
// with zeros, reads as a Gap.
const CHECK: usize = size_of::<u32>();
// RUN is how many records a reader reads at once.
const RUN: usize = 1024;

// Writer appends entries of N bytes to the log in a directory that other processes read.
pub struct Writer<const N: usize, const S: usize> {
    directory: PathBuf,
    appending: Mutex<Appending>,
}

// Appending is the segment a writer appends to, with the entries it appends, each followed by its check.
struct Appending {
    segment: Segment,
    records: Vec<u8>,
}

// Read is what a reader read next: an entry, or a Gap where it may have missed some.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Read<const N: usize> {
    Entry([u8; N]),
    Gap,
}

// Reader calls on_read from its own thread for each entry appended after it started, until it is dropped.
pub struct Reader {
    stop: Stop,
    thread: Option<JoinHandle<()>>,
}

// Segment is an open segment file, by its number.
struct Segment {
    number: u64,
    file: File,
}

// Reading is where a reader's thread reads: the next record of its segment, and the segment after it, once it
// exists. It reads runs of records into run, and hands those from handed to filled first.
struct Reading<const N: usize, const S: usize, F> {
    directory: PathBuf,
    segment: Segment,
    index: u64,
    next: Option<Segment>,
    // next_path is the path the next segment is looked for at, which happens only once the names in the directory
    // changed after the last look, as names_changed says.
    next_path: PathBuf,
    names_changed: bool,
    run: Box<[u8]>,
    handed: usize,
    filled: usize,
    watch: Watch,
    on_read: F,
}

// Wake is what woke a reader's thread: a change of the log, which may be a change of the names in its directory,
// the end of its watch, as when the directory is deleted, or the reader's drop.
enum Wake {
    Changed { names: bool },
    Ended,
    Stop,
}

// Next is what a reader found after its segment.
enum Next {
    Opened(Segment),
    Waiting,
    // The reader is more than a segment behind.
    Behind,
}

impl<const N: usize, const S: usize> Writer<N, S> {
    const RECORD: usize = N + CHECK;

    pub fn open(directory: &Path) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        Ok(Self {
            directory: directory.to_owned(),
            appending: Mutex::new(Appending {
                segment: settle(directory, &append_options(), None)?,
                records: Vec::new(),
            }),
        })
    }

    // append adds entries as one write, which never interleaves with another process's.
    pub fn append(&self, entries: impl IntoIterator<Item = [u8; N]>) -> io::Result<()> {
        let mut appending = self
            .appending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let Appending { segment, records } = &mut *appending;
        records.clear();
        for entry in entries {
            records.extend_from_slice(&entry);
            records.extend_from_slice(&check(&entry).to_le_bytes());
        }
        let segment_end = (S * Self::RECORD) as u64;
        let mut unwritten = &records[..];
        while !unwritten.is_empty() {
            let written = match segment.file.write(unwritten) {
                Ok(written) => written,
                // An interrupted write wrote nothing.
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
            if written < unwritten.len() {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "the entries were written in part",
                ));
            }
            // The file's offset is where this write ended, as the lock keeps this writer's other appends out.
            let end = segment.file.stream_position()?;
            if end < segment_end {
                return Ok(());
            }
            // The append filled the segment, or found it full, so it starts the next one. Entries not wholly
            // inside the segment belong to the next one, so they go there too.
            *segment = settle(&self.directory, &append_options(), Some(segment.number))?;
            let start = end - unwritten.len() as u64;
            let inside = segment_end.saturating_sub(start) / Self::RECORD as u64;
            unwritten = &unwritten[inside as usize * Self::RECORD..];
        }
        Ok(())
    }
}

impl Reader {
    pub fn start<const N: usize, const S: usize>(
        directory: &Path,
        on_read: impl FnMut(Read<N>) + Send + 'static,
    ) -> io::Result<Self> {
        fs::create_dir_all(directory)?;
        let segment = settle(directory, &read_options(), None)?;
        let (mut watch, stop) = Watch::new(directory)?;
        // The watch is in place before the reader takes the segment's end, so it misses no append after it.
        watch.follow(&segment.file)?;
        let index = end_index::<N, S>(&segment)?;
        let mut reading = Reading::<N, S, _> {
            directory: directory.to_owned(),
            segment,
            index,
            next: None,
            next_path: path_of(directory, 0),
            names_changed: true,
            run: vec![0; (N + CHECK) * RUN.min(S)].into_boxed_slice(),
            handed: 0,
            filled: 0,
            watch,
            on_read,
        };
        let thread = thread::Builder::new()
            .name("lossy log reader".to_owned())
            .spawn(move || reading.run())?;
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.stop.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl<const N: usize, const S: usize, F: FnMut(Read<N>)> Reading<N, S, F> {
    const RECORD: usize = N + CHECK;

    // run reads what the log has once its watch is in place, then again on each change, until the reader is
    // dropped or the watch ends. A watch that ends, or fails, may miss changes, so the reader reads a last Gap.
    fn run(&mut self) {
        self.read_available();
        loop {
            match self.watch.wait() {
                Ok(Wake::Changed { names }) => {
                    self.names_changed |= names;
                    self.read_available();
                }
                Ok(Wake::Ended) | Err(_) => {
                    (self.on_read)(Read::Gap);
                    return;
                }
                Ok(Wake::Stop) => return,
            }
        }
    }

    // read_available calls on_read for each entry the log has now. A read that fails moves the reader to the log's
    // end, then reads a Gap, so every entry appended after a Gap is read.
    fn read_available(&mut self) {
        let mut result = self.find_next().and_then(|_| self.read_next());
        loop {
            match result {
                Ok(Some(read)) => (self.on_read)(read),
                Ok(None) => return,
                Err(_) => {
                    let moved = self.move_to_end();
                    (self.on_read)(Read::Gap);
                    if moved.is_err() {
                        return;
                    }
                }
            }
            result = self.read_next();
        }
    }

    // read_next reads the next entry, or None when the log has no more yet.
    fn read_next(&mut self) -> io::Result<Option<Read<N>>> {
        loop {
            if self.handed < self.filled {
                let record = &self.run[self.handed..self.handed + Self::RECORD];
                self.handed += Self::RECORD;
                return Ok(Some(entry_of(record)));
            }
            if self.index < S as u64 {
                // A run ends at the segment's end, and at the last whole record the file has.
                let length = (S - self.index as usize).min(RUN) * Self::RECORD;
                let offset = self.index * Self::RECORD as u64;
                match self.segment.file.read_at(&mut self.run[..length], offset) {
                    Ok(read) if read < Self::RECORD => return Ok(None),
                    Ok(read) => {
                        let records = read / Self::RECORD;
                        self.index += records as u64;
                        (self.handed, self.filled) = (0, records * Self::RECORD);
                    }
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(error),
                }
                continue;
            }
            if self.find_next()? {
                continue;
            }
            let Some(next) = self.next.take() else {
                return Ok(None);
            };
            self.move_to(next, false)?;
        }
    }

    // find_next opens the segment after the reader's, once it exists, which it looks for only when the names in the
    // directory changed. When the reader is behind, it moves to the log's end and reads a Gap, and find_next says it
    // moved.
    fn find_next(&mut self) -> io::Result<bool> {
        if self.next.is_some() || !self.names_changed {
            return Ok(false);
        }
        // A change after this look wakes the reader again.
        self.names_changed = false;
        match self.open_next()? {
            Next::Opened(next) => self.next = Some(next),
            Next::Waiting => {}
            Next::Behind => {
                self.move_to_end()?;
                (self.on_read)(Read::Gap);
                return Ok(true);
            }
        }
        Ok(false)
    }

    // open_next opens the segment after the reader's, once it exists. The reader's segment is deleted only once the
    // next one exists, so when it is gone before the next fails to open, the next one is gone too and the reader is
    // behind.
    fn open_next(&mut self) -> io::Result<Next> {
        let deleted = self.segment.file.metadata()?.nlink() == 0;
        let number = self.segment.number + 1;
        self.next_path
            .set_file_name(OsStr::from_bytes(&name(number)));
        match read_options().open(&self.next_path) {
            Ok(file) => {
                let next = Segment { number, file };
                if recreated(newest(&self.directory)?, &next)? {
                    Ok(Next::Behind)
                } else {
                    Ok(Next::Opened(next))
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound && deleted => Ok(Next::Behind),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Next::Waiting),
            Err(error) => Err(error),
        }
    }

    // move_to_end moves the reader to the end of the log's newest segment.
    fn move_to_end(&mut self) -> io::Result<()> {
        let segment = settle(&self.directory, &read_options(), None)?;
        self.move_to(segment, true)
    }

    // move_to moves the reader to the start of segment, or its end. The segment after it may exist already, while
    // the change of names that told so was taken before, so the reader looks for it again. Dropping the old segment
    // closes it, which ends its watch.
    fn move_to(&mut self, segment: Segment, at_end: bool) -> io::Result<()> {
        // The watch is in place before the reader takes the segment's end, so it misses no append after it.
        self.watch.follow(&segment.file)?;
        self.index = if at_end {
            end_index::<N, S>(&segment)?
        } else {
            0
        };
        self.segment = segment;
        self.next = None;
        self.names_changed = true;
        (self.handed, self.filled) = (0, 0);
        Ok(())
    }
}

// settle opens the segment appends go to: the newest, or a new one after the segment numbered after, if that is the
// newest. It creates a segment exclusively, or uses it if it exists, and deletes every older segment.
fn settle(directory: &Path, options: &OpenOptions, after: Option<u64>) -> io::Result<Segment> {
    let mut after = after;
    loop {
        let newest = newest(directory)?;
        let number = match (newest, after) {
            (Some(newest), Some(after)) if newest > after => newest,
            (_, Some(after)) => after + 1,
            (Some(newest), None) => newest,
            (None, None) => 0,
        };
        let path = path_of(directory, number);
        if newest.is_none_or(|newest| number > newest) {
            match append_options().create_new(true).open(&path) {
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        let file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        let segment = Segment { number, file };
        let numbers = numbers(directory)?;
        if recreated(numbers.last().copied(), &segment)? {
            after = Some(number);
            continue;
        }
        for older in numbers.into_iter().take_while(|&older| older < number) {
            match fs::remove_file(path_of(directory, older)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        return Ok(segment);
    }
}

// recreated says whether segment was created again after its deletion, by a writer that read the directory before
// it. Such a segment is never the newest and stays empty, as nobody appends to it, and the next settle deletes it.
// A segment of the log is the newest, or holds entries, as the next one starts only once it is full.
fn recreated(newest: Option<u64>, segment: &Segment) -> io::Result<bool> {
    Ok(newest != Some(segment.number) && segment.file.metadata()?.len() == 0)
}

// end_index is the index of the entry after the last whole one in segment, at most S.
fn end_index<const N: usize, const S: usize>(segment: &Segment) -> io::Result<u64> {
    Ok((segment.file.metadata()?.len() / (N + CHECK) as u64).min(S as u64))
}

fn newest(directory: &Path) -> io::Result<Option<u64>> {
    Ok(numbers(directory)?.last().copied())
}

// numbers are the numbers of the segments in directory, in order.
fn numbers(directory: &Path) -> io::Result<Vec<u64>> {
    let mut numbers = Vec::new();
    for entry in fs::read_dir(directory)? {
        if let Some(number) = entry?
            .file_name()
            .to_str()
            .and_then(|name| name.parse().ok())
        {
            numbers.push(number);
        }
    }
    numbers.sort_unstable();
    Ok(numbers)
}

fn path_of(directory: &Path, number: u64) -> PathBuf {
    directory.join(OsStr::from_bytes(&name(number)))
}

// name is the file name of the segment numbered number: its 20 digits.
fn name(number: u64) -> [u8; 20] {
    let mut name = [0; 20];
    write!(&mut name[..], "{number:020}").expect("a u64 has at most 20 digits");
    name
}

fn append_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.append(true);
    options
}

fn read_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true);
    options
}

// check is entry's check, which is never zero, so zeros never read as an entry.
fn check(entry: &[u8]) -> u32 {
    xxhash_rust::xxh32::xxh32(entry, 0).max(1)
}

fn entry_of<const N: usize>(record: &[u8]) -> Read<N> {
    let (entry, stored) = record.split_at(N);
    if stored == check(entry).to_le_bytes() {
        Read::Entry(entry.try_into().expect("a record holds an entry"))
    } else {
        Read::Gap
    }
}

#[cfg(test)]
mod tests;
