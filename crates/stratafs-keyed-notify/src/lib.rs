use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use event_listener::{Event, listener};
use stratafs_lossy_log::{Read, Reader, Writer};
use xxhash_rust::xxh3::xxh3_64;

// Waiters are kept by the hash of their key, so a key's hash alone wakes them. Keys sharing a hash wake each other's
// waiters, which check their condition again.
#[derive(Default)]
pub struct KeyedNotify {
    waiters: Arc<Waiters>,
    // changes is set when other processes share this notify.
    changes: Option<Changes>,
}

#[derive(Default)]
struct Waiters(Mutex<HashMap<u64, Arc<Event>>>);

// Changes is a shared notify's change log, which it appends the keys it notifies to, by owner, and the reader that
// wakes its waiters on the entries of other owners.
struct Changes {
    log: ChangeLog,
    owner: u64,
    _reading: Reader,
}

// CHANGES is how many entries a segment of the change log holds: room for a burst of commits, as a reader only
// wakes waiters.
const CHANGES: usize = 1 << 16;

// ChangeLog holds an entry for each key notified: the key's hash, then the owner of the notify, as a little endian
// u128.
type ChangeLog = Writer<16, CHANGES>;

// key_hash is the hash KeyedNotify keeps a key's waiters by.
fn key_hash(key: &[u8]) -> u64 {
    xxh3_64(key)
}

// KeyedNotify is a minimum allocation implementation of a keyed notify system.
impl KeyedNotify {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(keys: usize) -> Self {
        Self {
            waiters: Arc::new(Waiters(Mutex::new(HashMap::with_capacity(keys)))),
            changes: None,
        }
    }

    // shared is a notify that the shared notifies of other processes on directory wake, and that wakes theirs. A Gap
    // in the change log wakes every waiter.
    pub fn shared(directory: &Path) -> io::Result<Self> {
        let log = ChangeLog::open(directory)?;
        let owner = rand::random();
        let waiters = Arc::new(Waiters::default());
        let woken = Arc::clone(&waiters);
        let reading = Reader::start::<16, CHANGES>(directory, move |read| match read {
            Read::Entry(entry) => {
                let entry = u128::from_le_bytes(entry);
                if (entry >> 64) as u64 != owner {
                    woken.wake([entry as u64]);
                }
            }
            Read::Gap => woken.wake_all(),
        })?;
        Ok(Self {
            waiters,
            changes: Some(Changes {
                log,
                owner,
                _reading: reading,
            }),
        })
    }

    pub async fn wait_until<E, F>(
        &self,
        key: &Bytes,
        mut condition: impl FnMut() -> F,
    ) -> Result<(), E>
    where
        F: Future<Output = Result<bool, E>>,
    {
        loop {
            let registration = self.register(key);
            listener!(registration.event => listener);
            if condition().await? {
                return Ok(());
            }
            if registration.current() {
                listener.await;
            }
        }
    }

    // notify_waiters wakes the waiters of keys, and a shared notify appends keys to its change log in one write. A
    // failed append is only logged, and other processes' waiters miss the change.
    pub fn notify_waiters(&self, keys: &[Bytes]) {
        if keys.is_empty() {
            return;
        }
        self.waiters.wake(keys.iter().map(|key| key_hash(key)));
        let Some(Changes { log, owner, .. }) = &self.changes else {
            return;
        };
        let owner = u128::from(*owner) << 64;
        let entries = keys
            .iter()
            .map(|key| (owner | u128::from(key_hash(key))).to_le_bytes());
        if let Err(error) = log.append(entries) {
            tracing::warn!(
                %error,
                "a commit's changes were not appended to the change log, so other processes' waiters may miss it"
            );
        }
    }

    fn register(&self, key: &Bytes) -> Registration<'_> {
        let hash = key_hash(key);
        let event = self.waiters.lock().entry(hash).or_default().clone();
        Registration {
            notify: self,
            hash,
            event,
        }
    }
}

impl Waiters {
    // wake wakes the waiters of the keys with these hashes.
    fn wake(&self, hashes: impl IntoIterator<Item = u64>) {
        let mut waiters = self.lock();
        if waiters.is_empty() {
            return;
        }
        for hash in hashes {
            if let Some(event) = waiters.remove(&hash) {
                event.notify(usize::MAX);
            }
        }
    }

    fn wake_all(&self) {
        for (_, event) in self.lock().drain() {
            event.notify(usize::MAX);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<u64, Arc<Event>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

struct Registration<'a> {
    notify: &'a KeyedNotify,
    hash: u64,
    event: Arc<Event>,
}

impl Registration<'_> {
    fn current(&self) -> bool {
        self.notify
            .waiters
            .lock()
            .get(&self.hash)
            .is_some_and(|current| Arc::ptr_eq(current, &self.event))
    }
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut waiters = self.notify.waiters.lock();
        if let Some(current) = waiters.get(&self.hash)
            && Arc::ptr_eq(current, &self.event)
            && Arc::strong_count(&self.event) == 2
        {
            waiters.remove(&self.hash);
        }
    }
}

#[cfg(test)]
mod tests;
