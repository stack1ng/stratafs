// The watch on macOS: a kqueue the reader's thread waits on, with the log's directory, the reader's segment and a
// user event that stops it.

use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use nix::errno::Errno;
use nix::sys::event::{EvFlags, EventFilter, FilterFlag, KEvent, Kqueue};

use crate::Wake;

// Watch waits for changes to the log's directory and to the reader's segment, which kqueue tells only a watch on the
// file. Closing a segment's file ends its watch.
pub(crate) struct Watch {
    queue: Arc<Kqueue>,
    directory: File,
}

// Stop wakes a reader's thread with the user event, to stop it.
pub(crate) struct Stop {
    queue: Arc<Kqueue>,
}

// STOP is the identifier of the user event.
const STOP: usize = 0;

impl Watch {
    pub(crate) fn new(directory: &Path) -> io::Result<(Self, Stop)> {
        let queue = Arc::new(Kqueue::new()?);
        let directory = File::open(directory)?;
        register(
            &queue,
            &[
                vnode(
                    &directory,
                    FilterFlag::NOTE_WRITE | FilterFlag::NOTE_DELETE | FilterFlag::NOTE_REVOKE,
                ),
                KEvent::new(
                    STOP,
                    EventFilter::EVFILT_USER,
                    EvFlags::EV_ADD | EvFlags::EV_CLEAR,
                    FilterFlag::empty(),
                    0,
                    0,
                ),
            ],
        )?;
        let stop = Stop {
            queue: queue.clone(),
        };
        Ok((Self { queue, directory }, stop))
    }

    // follow watches segment for appends.
    pub(crate) fn follow(&mut self, segment: &File) -> io::Result<()> {
        register(
            &self.queue,
            &[vnode(
                segment,
                FilterFlag::NOTE_WRITE | FilterFlag::NOTE_EXTEND,
            )],
        )
    }

    // wait blocks until the log changes, the directory is deleted or revoked, or the reader stops.
    pub(crate) fn wait(&mut self) -> io::Result<Wake> {
        let mut events = [KEvent::new(
            0,
            EventFilter::EVFILT_USER,
            EvFlags::empty(),
            FilterFlag::empty(),
            0,
            0,
        ); 4];
        let count = loop {
            match self.queue.kevent(&[], &mut events, None) {
                Ok(count) => break count,
                Err(Errno::EINTR) => {}
                Err(error) => return Err(error.into()),
            }
        };
        let mut names = false;
        for event in &events[..count] {
            if event.filter() == Ok(EventFilter::EVFILT_USER) {
                return Ok(Wake::Stop);
            }
            let directory = event.ident() == self.directory.as_raw_fd() as usize;
            if directory
                && event
                    .fflags()
                    .intersects(FilterFlag::NOTE_DELETE | FilterFlag::NOTE_REVOKE)
            {
                return Ok(Wake::Ended);
            }
            names |= directory;
        }
        Ok(Wake::Changed { names })
    }
}

impl Stop {
    pub(crate) fn stop(&mut self) {
        // The queue stays open while Stop holds it, so the trigger fails only without memory.
        let _ = register(
            &self.queue,
            &[KEvent::new(
                STOP,
                EventFilter::EVFILT_USER,
                EvFlags::empty(),
                FilterFlag::NOTE_TRIGGER,
                0,
                0,
            )],
        );
    }
}

// vnode is the registration of a watch on file for the changes in changes, which it tells once each wait.
fn vnode(file: &File, changes: FilterFlag) -> KEvent {
    KEvent::new(
        file.as_raw_fd() as usize,
        EventFilter::EVFILT_VNODE,
        EvFlags::EV_ADD | EvFlags::EV_CLEAR,
        changes,
        0,
        0,
    )
}

// register applies changes to queue without waiting, as it asks for no events.
fn register(queue: &Kqueue, changes: &[KEvent]) -> io::Result<()> {
    queue.kevent(changes, &mut [], None)?;
    Ok(())
}
