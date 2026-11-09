// The watch on Linux: an inotify watch on the log's directory, which tells appends to its segments too, read by the
// reader's thread with blocking reads.

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use inotify::{EventMask, Inotify, WatchDescriptor, WatchMask, Watches};
use nix::fcntl::{FcntlArg, OFlag, fcntl};

use crate::Wake;

// Watch waits for changes to the log's directory and its segments. Its end tells IN_IGNORED, once Stop removes it, or
// once the directory is deleted or unmounted, which the kernel does only once no file in it is open, so never while
// the reader holds its segment.
pub(crate) struct Watch {
    inotify: Inotify,
    stopping: Arc<AtomicBool>,
    // events holds the events of a read, which takes at least one whole event: 16 bytes and a name of at most 256.
    events: [u8; 4096],
}

// Stop removes the directory's watch, to stop a reader's thread, and says so in stopping.
pub(crate) struct Stop {
    watches: Watches,
    directory: WatchDescriptor,
    stopping: Arc<AtomicBool>,
}

impl Watch {
    pub(crate) fn new(directory: &Path) -> io::Result<(Self, Stop)> {
        let inotify = Inotify::init()?;
        // The reader's thread blocks on reads.
        let flags = OFlag::from_bits_retain(fcntl(&inotify, FcntlArg::F_GETFL)?);
        fcntl(&inotify, FcntlArg::F_SETFL(flags - OFlag::O_NONBLOCK))?;
        let mut watches = inotify.watches();
        let directory = watches.add(
            directory,
            WatchMask::MODIFY
                | WatchMask::CREATE
                | WatchMask::DELETE
                | WatchMask::MOVED_FROM
                | WatchMask::MOVED_TO
                | WatchMask::ONLYDIR,
        )?;
        let stopping = Arc::new(AtomicBool::new(false));
        let watch = Self {
            inotify,
            stopping: stopping.clone(),
            events: [0; 4096],
        };
        let stop = Stop {
            watches,
            directory,
            stopping,
        };
        Ok((watch, stop))
    }

    // follow watches segment for appends, which the directory's watch already does.
    pub(crate) fn follow(&mut self, _segment: &File) -> io::Result<()> {
        Ok(())
    }

    // wait blocks until the log changes, the watch ends or the reader stops. An overflow of inotify's queue may have
    // dropped a change of the directory's names.
    pub(crate) fn wait(&mut self) -> io::Result<Wake> {
        let events = loop {
            match self.inotify.read_events(&mut self.events) {
                Ok(events) => break events,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        };
        let mut names = false;
        let mut overflow = false;
        for event in events {
            if event.mask.contains(EventMask::IGNORED) {
                return Ok(if self.stopping.load(Ordering::Acquire) {
                    Wake::Stop
                } else {
                    Wake::Ended
                });
            }
            overflow |= event.mask.contains(EventMask::Q_OVERFLOW);
            names |= event.mask.intersects(
                EventMask::CREATE
                    | EventMask::DELETE
                    | EventMask::MOVED_FROM
                    | EventMask::MOVED_TO
                    | EventMask::Q_OVERFLOW,
            );
        }
        // An overflow may have dropped the IN_IGNORED of Stop's removal, which set stopping before it.
        if overflow && self.stopping.load(Ordering::Acquire) {
            return Ok(Wake::Stop);
        }
        Ok(Wake::Changed { names })
    }
}

impl Stop {
    pub(crate) fn stop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        self.end();
    }

    // end removes the directory's watch, as the kernel does when the watch ends.
    pub(crate) fn end(&mut self) {
        let _ = self.watches.remove(self.directory.clone());
    }
}
