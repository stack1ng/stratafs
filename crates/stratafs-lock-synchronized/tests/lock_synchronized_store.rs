use std::convert::Infallible;
use std::io;
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_lite::future::block_on;
use object_store::memory::InMemory;
use redb::backends::InMemoryBackend;
use redb::{Database, StorageBackend};
use stratafs_lock_synchronized::{LockSynchronizedFailure, LockSynchronizedStore};
use stratafs_object_store::{ObjectStoreContentStore, ObjectStoreError};
use stratafs_redb::RedbStore;
use stratafs_store::content::{ContentHash, ContentStore, GetRejection};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction};
use stratafs_store::spawn::Inline;
use stratafs_store::store::Store;

// Durable is which store made itself durable, or that a write transaction ran, in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Durable {
    Content,
    Metadata,
    Wrote,
}

type Log = Arc<Mutex<Vec<Durable>>>;

// LoggedContent keeps content in memory, and logs each ensure_durable, failing it when told to. With a
// resume, ensure_durable waits for it after logging.
#[derive(Clone)]
struct LoggedContent {
    store: ObjectStoreContentStore,
    log: Log,
    fails: bool,
    resume: Option<Arc<Mutex<Receiver<()>>>>,
}

impl ContentStore for LoggedContent {
    type Failure = ObjectStoreError;
    type Content = Bytes;

    fn put(
        &self,
        content: impl Buf + Send,
    ) -> impl Future<Output = Result<ContentHash, Self::Failure>> + Send {
        self.store.put(content)
    }

    fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<Bytes, StoreError<GetRejection, Self::Failure>>> + Send {
        self.store.get(hash, offset, size)
    }

    async fn ensure_durable(&self, _against: DurableAgainst) -> Result<(), Self::Failure> {
        self.log.lock().unwrap().push(Durable::Content);
        if let Some(resume) = &self.resume {
            resume.lock().unwrap().recv().unwrap();
        }
        if self.fails {
            return Err(ObjectStoreError::Flush(io::Error::other(
                "the disk is gone",
            )));
        }
        Ok(())
    }
}

// LoggedFlushes is an in memory backend that logs each flush of the metadata to the disk.
#[derive(Debug)]
struct LoggedFlushes {
    backend: InMemoryBackend,
    log: Log,
}

impl StorageBackend for LoggedFlushes {
    fn len(&self) -> io::Result<u64> {
        self.backend.len()
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.backend.read(offset, out)
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.backend.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        self.log.lock().unwrap().push(Durable::Metadata);
        self.backend.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.backend.write(offset, data)
    }
}

type TestStore = LockSynchronizedStore<RedbStore<Inline>, LoggedContent>;

// store is a store with a write committed but not yet durable, with the log of what each ensure_durable makes
// durable.
fn store(content_fails: bool, resume: Option<Receiver<()>>) -> (TestStore, Log) {
    let log = Log::default();
    let database = Database::builder()
        .create_with_backend(LoggedFlushes {
            backend: InMemoryBackend::new(),
            log: Arc::clone(&log),
        })
        .unwrap();
    let content = LoggedContent {
        store: ObjectStoreContentStore::new(InMemory::new()),
        log: Arc::clone(&log),
        fails: content_fails,
        resume: resume.map(|resume| Arc::new(Mutex::new(resume))),
    };
    let store = LockSynchronizedStore::new(RedbStore::new(database, Inline).unwrap(), content);
    block_on(store.metadata().transact(|mut transaction| {
        Box::pin(async move {
            let set = transaction.set(Bytes::from_static(b"key"), Bytes::from_static(b"value"));
            set.map(|()| ((), transaction))
        })
    }))
    .unwrap();
    log.lock().unwrap().clear();
    (store, log)
}

#[test]
fn ensure_durable_makes_the_content_durable_before_the_metadata() {
    let (store, log) = store(false, None);
    block_on(store.ensure_durable(DurableAgainst::PowerFailure)).unwrap();
    let durable = log.lock().unwrap().clone();
    assert!(
        matches!(&durable[..], [Durable::Content, rest @ ..] if !rest.is_empty() && rest.iter().all(|store| *store == Durable::Metadata)),
        "{durable:?}"
    );
}

#[test]
fn ensure_durable_leaves_the_metadata_alone_when_the_content_fails() {
    let (store, log) = store(true, None);
    assert!(matches!(
        block_on(store.ensure_durable(DurableAgainst::PowerFailure)),
        Err(LockSynchronizedFailure::Content(ObjectStoreError::Flush(_)))
    ));
    assert_eq!(*log.lock().unwrap(), [Durable::Content]);
}

#[test]
fn no_write_commits_between_the_content_and_the_metadata_becoming_durable() {
    let (resume, resumed) = channel();
    let (store, log) = store(false, Some(resumed));
    thread::scope(|scope| {
        let durable = scope.spawn(|| block_on(store.ensure_durable(DurableAgainst::PowerFailure)));
        while !log.lock().unwrap().contains(&Durable::Content) {
            thread::yield_now();
        }
        // ensure_durable is between its two steps. A write started now must wait for both.
        let write = scope.spawn(|| {
            block_on(store.metadata().transact(|transaction| {
                let log = Arc::clone(&log);
                Box::pin(async move {
                    log.lock().unwrap().push(Durable::Wrote);
                    Ok::<_, Infallible>(((), transaction))
                })
            }))
        });
        thread::sleep(Duration::from_millis(50));
        resume.send(()).unwrap();
        durable.join().unwrap().unwrap();
        write.join().unwrap().unwrap();
    });
    // Dropping the store makes the content durable again, which waits for a resume too.
    resume.send(()).unwrap();
    let durable = log.lock().unwrap().clone();
    assert!(
        matches!(&durable[..], [Durable::Content, rest @ .., Durable::Wrote] if !rest.is_empty() && rest.iter().all(|store| *store == Durable::Metadata)),
        "{durable:?}"
    );
}

#[test]
fn dropping_the_store_makes_the_content_durable_before_the_metadata_store_closes() {
    let (store, log) = store(false, None);
    drop(store);
    let durable = log.lock().unwrap().clone();
    assert!(
        matches!(&durable[..], [Durable::Content, rest @ ..] if !rest.is_empty() && rest.iter().all(|store| *store == Durable::Metadata)),
        "{durable:?}"
    );
}

#[test]
fn dropping_the_store_leaves_the_metadata_store_open_when_the_content_fails() {
    let (store, log) = store(true, None);
    drop(store);
    assert_eq!(*log.lock().unwrap(), [Durable::Content]);
}
