mod vfs;

use std::borrow::Cow;
use std::ffi::{CStr, CString, c_int};
use std::ops::{Bound, Deref, DerefMut, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use futures_util::lock::{Mutex, MutexGuard, OwnedMutexGuard};
use rusqlite::hooks::Wal;
use rusqlite::wal::CheckpointMode;
use rusqlite::{Connection, ErrorCode, OpenFlags, ffi, params};

use stratafs_keyed_notify::KeyedNotify;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
    TransactionHandle, assert_valid_range,
};
use stratafs_store::spawn::SpawnBlocking;

// A local store checkpoints the write ahead log once a commit leaves it this many pages long, 256 MiB. A longer log
// copies a node the tree rewrote often once, and syncs less often, for a longer wait at each checkpoint.
const CHECKPOINT_PAGES: u32 = 16 * 1024;
// SQLite refuses a row over a billion bytes, counting its header, which is at most 19 bytes for a key and a value.
const LARGEST_ACCEPTED_ENTRY: usize = 1_000_000_000 - 19;

// A rowid table keeps a value of up to its page size, less 35 bytes, in its leaf. A tree node is at most 16 KiB, so a
// page that size holds nearly every node whole, and reading one reads one page.
const CREATE: &str = "
    PRAGMA page_size = 16384;
    PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS kv (key BLOB NOT NULL UNIQUE, value BLOB NOT NULL);
";
// The writer commits without a sync, and keeps the keys clear_range returns in memory. It reads the database mapped
// into memory, as far as the bundled SQLite maps, with a cache. Another connection's commit drops both, so the
// writer reads slower while another process writes.
const WRITER: &str = "
    PRAGMA synchronous = NORMAL;
    PRAGMA temp_store = MEMORY;
    PRAGMA mmap_size = 2147418112;
    PRAGMA cache_size = -65536;
";
const BEGIN_WRITE: &str = "BEGIN IMMEDIATE";
const BEGIN_READ: &str = "BEGIN";
const COMMIT: &str = "COMMIT";
const ROLLBACK: &str = "ROLLBACK";
const DATA_VERSION: &str = "PRAGMA data_version";
const GET: &str = "SELECT value FROM kv WHERE key = ?1";
const GET_BETWEEN: &str =
    "SELECT key, value FROM kv WHERE key >= ?1 AND key < ?2 ORDER BY key LIMIT ?3";
const GET_FROM: &str = "SELECT key, value FROM kv WHERE key >= ?1 ORDER BY key LIMIT ?2";
// set changes no row when the key already holds the value, so its change count says whether the value changed.
const SET: &str = "INSERT INTO kv (key, value) VALUES (?1, ?2)
    ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE value IS NOT excluded.value";
const CLEAR: &str = "DELETE FROM kv WHERE key = ?1";
const CLEAR_BETWEEN: &str = "DELETE FROM kv WHERE key >= ?1 AND key < ?2 RETURNING key";
const CLEAR_FROM: &str = "DELETE FROM kv WHERE key >= ?1 RETURNING key";

pub struct SqliteStore<S> {
    location: Location,
    checkpoint_pages: u32,
    // running is how many transactions are running, so a read takes the writer's connection only while it runs
    // alone. A write or ensure_durable that starts meanwhile waits for that read, so one awaited inside the read's
    // work never begins.
    running: AtomicUsize,
    writer: Arc<Mutex<Writer>>,
    // idle_readers are the connections reads use while the writer is busy, kept open between them. A commit drops
    // every page a reader cached, and a reader mapping the database would map it again, so they read with neither.
    idle_readers: std::sync::Mutex<Vec<std::sync::Mutex<Connection>>>,
    notify: KeyedNotify,
    spawner: S,
}

// Location is where the store's connections open its database: a path, through a SQLite VFS.
struct Location {
    path: PathBuf,
    vfs: CString,
}

impl Location {
    fn connect(&self) -> rusqlite::Result<Connection> {
        Connection::open_with_flags_and_vfs(&self.path, OpenFlags::default(), &*self.vfs)
    }
}

// Writer is the one write transaction at a time, and what it keeps between commits.
struct Writer {
    connection: std::sync::Mutex<Connection>,
    written: Vec<Bytes>,
    // current_durable_against is what every commit so far is durable against, or None while one is only in memory.
    current_durable_against: Option<DurableAgainst>,
    // synced_data_version is the database's data version at the last sync. A commit on another connection changes it.
    synced_data_version: i64,
    // log_pages is how long the write ahead log was after the last commit.
    log_pages: Arc<AtomicU32>,
    // checkpointed_pages is how long the log was at the last checkpoint. A reader using the log keeps it from
    // starting over, so the store checkpoints again once the log grew by checkpoint_pages since.
    checkpointed_pages: u32,
}

impl<S> SqliteStore<S> {
    // new keeps the store in the database at path, opened through the SQLite VFS named vfs, and checkpoints the
    // write ahead log once a commit leaves it checkpoint_pages long.
    pub fn new(
        path: impl AsRef<Path>,
        vfs: &CStr,
        checkpoint_pages: u32,
        spawner: S,
    ) -> rusqlite::Result<Self> {
        let location = Location {
            path: path.as_ref().to_owned(),
            vfs: vfs.to_owned(),
        };
        let mut connection = location.connect()?;
        // Another process opening the database meanwhile fails the switch to a write ahead log at once, whatever the
        // busy timeout, so the store waits for it.
        while_busy(|| connection.execute_batch(CREATE))?;
        connection.execute_batch(WRITER)?;
        // Another process's write fails a begin at once, so transact can wait for it on the spawner.
        connection.busy_timeout(Duration::ZERO)?;
        // The hook replaces SQLite's own checkpoint after a commit, which would wait on the disk on the calling thread.
        let log_pages = Arc::new(AtomicU32::new(0));
        let hook_log_pages = Arc::clone(&log_pages);
        connection.wal_hook(Some(move |_: &mut Wal, pages: c_int| {
            hook_log_pages.store(pages.unsigned_abs(), Ordering::Relaxed);
            Ok(())
        }))?;
        Ok(Self {
            location,
            checkpoint_pages,
            running: AtomicUsize::new(0),
            writer: Arc::new(Mutex::new(Writer {
                connection: std::sync::Mutex::new(connection),
                written: Vec::new(),
                // The database may hold commits a killed process left only in memory.
                current_durable_against: None,
                synced_data_version: 0,
                log_pages,
                checkpointed_pages: 0,
            })),
            idle_readers: std::sync::Mutex::new(Vec::new()),
            notify: KeyedNotify::new(),
            spawner,
        })
    }

    // local keeps the store in a database file at path on this machine's disk, syncing it with the call each
    // failure needs.
    pub fn local(path: impl AsRef<Path>, spawner: S) -> rusqlite::Result<Self> {
        Self::new(path, vfs::local_disk()?, CHECKPOINT_PAGES, spawner)
    }

    // reading is the connection a read uses: the writer's, whose cache commits keep, while the read runs alone, or
    // else an idle reader, or a new one when every reader is in use. A read hands it back with done_reading, and a
    // read that fails or is cancelled closes a reader.
    fn reading(&self, running: &Running<'_>) -> rusqlite::Result<Reading<'_>> {
        if running.alone()
            && let Some(writer) = self.writer.try_lock()
        {
            return Ok(Reading::Writer(writer));
        }
        self.reader().map(Reading::Reader)
    }

    fn reader(&self) -> rusqlite::Result<std::sync::Mutex<Connection>> {
        let idle = lock(&self.idle_readers).pop();
        match idle {
            Some(connection) => Ok(connection),
            None => self.location.connect().map(std::sync::Mutex::new),
        }
    }

    fn done_reading(&self, reading: Reading<'_>) {
        match reading {
            Reading::Writer(_) => {}
            Reading::Reader(connection) => lock(&self.idle_readers).push(connection),
        }
    }

    // checkpoint copies the write ahead log into the database on a reader's connection, so writes go on meanwhile.
    // A log that kept growing through the last checkpoint gets the commits made meanwhile copied too, inside a write
    // transaction, which holds off every process's writes, so the next commit starts the log over. It waits on the
    // disk, so it goes to the spawner.
    async fn checkpoint(&self, kept_growing: bool) -> rusqlite::Result<()>
    where
        S: SpawnBlocking,
    {
        let mut connection = self.reader()?;
        let (mut connection, checkpointed) = self
            .spawner
            .spawn_blocking(move || {
                let checkpointed = checkpoint(get_mut(&mut connection));
                (connection, checkpointed)
            })
            .await;
        match checkpointed {
            // Another connection checkpointing copies the log itself.
            Err(failure) if is_busy(&failure) => {}
            checkpointed => checkpointed?,
        }
        if kept_growing {
            let writer = Arc::clone(&self.writer).lock_owned().await;
            let mut transaction = begin_write(writer, &self.spawner).await?;
            let (caught_up, checkpointed) = self
                .spawner
                .spawn_blocking(move || {
                    let checkpointed = while_busy(|| checkpoint(get_mut(&mut connection)));
                    (connection, checkpointed)
                })
                .await;
            connection = caught_up;
            checkpointed?;
            transaction.roll_back()?;
        }
        lock(&self.idle_readers).push(connection);
        Ok(())
    }
}

impl<S: SpawnBlocking + 'static> KeyValueStore for SqliteStore<S> {
    type Rejection = std::convert::Infallible;
    // set reports entries over SQLite's limit as SetRejection::TooLarge, so Failure excludes SQLITE_TOOBIG.
    type Failure = rusqlite::Error;
    type ReadTransaction<'txn> = SqliteTransaction<'txn, ()>;
    type Transaction<'txn> = SqliteTransaction<'txn, &'txn mut Vec<Bytes>>;

    const LARGEST_ACCEPTED_ENTRY: usize = LARGEST_ACCEPTED_ENTRY;

    async fn transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<
            'txn,
            Result<(T, TransactionHandle<'txn, 'store, Self::Transaction<'txn>>), E>,
        > + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        let running = Running::start(&self.running);
        let writer = Arc::clone(&self.writer).lock_owned().await;
        let mut transaction = begin_write(writer, &self.spawner)
            .await
            .map_err(TransactError::Failed)?;
        transaction.written.clear();
        let Writer {
            connection,
            written,
            ..
        } = &mut *transaction;
        let output = work(TransactionHandle::new(SqliteTransaction {
            connection,
            written,
        }))
        .await
        .map(|(output, _)| output);
        let output = match output {
            Ok(output) => output,
            Err(error) => {
                transaction.roll_back().map_err(TransactError::Failed)?;
                return Err(TransactError::Aborted(error));
            }
        };
        transaction.commit().map_err(TransactError::Failed)?;
        transaction.current_durable_against = None;
        self.notify.notify_waiters(&transaction.written);
        let log_pages = transaction.log_pages.load(Ordering::Relaxed);
        // A log that started over is shorter than at the last checkpoint.
        if log_pages < transaction.checkpointed_pages {
            transaction.checkpointed_pages = 0;
        }
        let checkpoint_due = log_pages - transaction.checkpointed_pages >= self.checkpoint_pages;
        if checkpoint_due {
            transaction.checkpointed_pages = log_pages;
        }
        drop((transaction, running));
        if checkpoint_due {
            // Only a log that never started over since the last checkpoint is this long.
            let kept_growing = log_pages - self.checkpoint_pages >= self.checkpoint_pages;
            self.checkpoint(kept_growing)
                .await
                .map_err(TransactError::Failed)?;
        }
        Ok(output)
    }

    async fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        let result: Result<Result<T, E>, rusqlite::Error> = async {
            let running = Running::start(&self.running);
            let reading = self.reading(&running)?;
            let connection = reading.connection();
            execute(&lock(connection), BEGIN_READ)?;
            let transaction = Begun(connection);
            let output = work(TransactionHandle::new(SqliteTransaction {
                connection,
                written: (),
            }))
            .await;
            transaction.end()?;
            self.done_reading(reading);
            Ok(output)
        }
        .await;
        match result {
            Ok(Ok(output)) => Ok(output),
            Ok(Err(error)) => Err(TransactError::Aborted(error)),
            Err(failure) => Err(TransactError::Failed(failure)),
        }
    }

    async fn changed(&self, key: Bytes, seen: Option<Bytes>) -> Result<(), Self::Failure> {
        let (key, seen) = (&key, &seen);
        self.notify
            .wait_until(key, || async move {
                let running = Running::start(&self.running);
                let reading = self.reading(&running)?;
                let current = get(&lock(reading.connection()), key)?;
                self.done_reading(reading);
                Ok(current != *seen)
            })
            .await
    }

    async fn ensure_durable(&self, against: DurableAgainst) -> Result<(), Self::Failure> {
        let mut writer = Arc::clone(&self.writer).lock_owned().await;
        let data_version = get_mut(&mut writer.connection)
            .prepare_cached(DATA_VERSION)?
            .query_row([], |row| row.get(0))?;
        if data_version == writer.synced_data_version
            && matches!(writer.current_durable_against, Some(durable) if durable >= against)
        {
            return Ok(());
        }
        // A commit reaches the write ahead log first, and a checkpoint syncs the database before the log starts over,
        // so syncing the log makes every commit before it durable, whichever connection made it. A sync against a
        // power failure flushes the disk's whole cache, with what a checkpoint wrote in it. It waits on the disk, so
        // it goes to the spawner.
        let (mut writer, synced) = self
            .spawner
            .spawn_blocking(move || {
                let synced = vfs::sync_log(get_mut(&mut writer.connection), against);
                (writer, synced)
            })
            .await;
        synced?;
        writer.current_durable_against = Some(against);
        writer.synced_data_version = data_version;
        Ok(())
    }
}

// begin_write begins the writer's transaction. While another process writes it waits on the spawner, trying again
// until that process commits.
async fn begin_write<S: SpawnBlocking>(
    mut writer: OwnedMutexGuard<Writer>,
    spawner: &S,
) -> rusqlite::Result<WriteTransaction> {
    match execute(get_mut(&mut writer.connection), BEGIN_WRITE) {
        Err(failure) if is_busy(&failure) => {
            spawner
                .spawn_blocking(move || {
                    let connection = get_mut(&mut writer.connection);
                    while_busy(|| execute(connection, BEGIN_WRITE))
                        .map(|()| WriteTransaction(writer))
                })
                .await
        }
        begun => begun.map(|()| WriteTransaction(writer)),
    }
}

// while_busy tries attempt until no other connection holds what it needs, pausing a little longer each time.
fn while_busy<T>(mut attempt: impl FnMut() -> rusqlite::Result<T>) -> rusqlite::Result<T> {
    let mut pause = Duration::from_micros(50);
    loop {
        match attempt() {
            Err(failure) if is_busy(&failure) => {
                std::thread::sleep(pause);
                pause = (pause * 2).min(Duration::from_millis(5));
            }
            done => return done,
        }
    }
}

fn is_busy(failure: &rusqlite::Error) -> bool {
    failure.sqlite_error_code() == Some(ErrorCode::DatabaseBusy)
}

// checkpoint copies the write ahead log into the database, as far as no reader still needs it, and is busy while
// another connection checkpoints. A connection opens the log with its first read, so it reads first.
fn checkpoint(connection: &Connection) -> rusqlite::Result<()> {
    connection.pragma_query_value(None, "schema_version", |_| Ok(()))?;
    connection
        .wal_checkpoint_v2(None::<&str>, CheckpointMode::PASSIVE)
        .map(|_| ())
}

enum Reading<'store> {
    Writer(MutexGuard<'store, Writer>),
    Reader(std::sync::Mutex<Connection>),
}

impl Reading<'_> {
    fn connection(&self) -> &std::sync::Mutex<Connection> {
        match self {
            Reading::Writer(writer) => &writer.connection,
            Reading::Reader(connection) => connection,
        }
    }
}

// Running is a transaction running, counted until dropped.
struct Running<'store> {
    running: &'store AtomicUsize,
    alone: bool,
}

impl<'store> Running<'store> {
    fn start(running: &'store AtomicUsize) -> Self {
        Self {
            running,
            alone: running.fetch_add(1, Ordering::Relaxed) == 0,
        }
    }

    fn alone(&self) -> bool {
        self.alone
    }
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.running.fetch_sub(1, Ordering::Relaxed);
    }
}

// WriteTransaction is the writer with its transaction begun, rolled back when dropped before it ends, even where a
// cancelled transact leaves it, on the spawner.
struct WriteTransaction(OwnedMutexGuard<Writer>);

impl WriteTransaction {
    // commit commits the transaction, and fails once SQLite rolled it back after a failure.
    fn commit(&mut self) -> rusqlite::Result<()> {
        execute(begun(get_mut(&mut self.0.connection))?, COMMIT)
    }

    // roll_back rolls the transaction back, unless SQLite already did after a failure.
    fn roll_back(&mut self) -> rusqlite::Result<()> {
        let connection = get_mut(&mut self.0.connection);
        if connection.is_autocommit() {
            return Ok(());
        }
        execute(connection, ROLLBACK)
    }
}

impl Deref for WriteTransaction {
    type Target = Writer;

    fn deref(&self) -> &Writer {
        &self.0
    }
}

impl DerefMut for WriteTransaction {
    fn deref_mut(&mut self) -> &mut Writer {
        &mut self.0
    }
}

impl Drop for WriteTransaction {
    fn drop(&mut self) {
        let _ = self.roll_back();
    }
}

// Begun is a read begun on a connection, rolled back when dropped before it ends.
struct Begun<'txn>(&'txn std::sync::Mutex<Connection>);

impl Begun<'_> {
    // end ends the read, unless SQLite already did after a failure.
    fn end(self) -> rusqlite::Result<()> {
        let connection = lock(self.0);
        if connection.is_autocommit() {
            return Ok(());
        }
        execute(&connection, COMMIT)
    }
}

impl Drop for Begun<'_> {
    fn drop(&mut self) {
        let connection = lock(self.0);
        if !connection.is_autocommit() {
            let _ = execute(&connection, ROLLBACK);
        }
    }
}

// SqliteTransaction is a transaction on a connection, which can write when it keeps the keys it writes in written.
pub struct SqliteTransaction<'txn, W> {
    connection: &'txn std::sync::Mutex<Connection>,
    written: W,
}

impl<'txn, W: Send + Sync> ReadKeyValueTransaction for SqliteTransaction<'txn, W> {
    type Failure = rusqlite::Error;

    async fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Failure> {
        get(begun(&lock(self.connection))?, &key)
    }

    async fn get_range<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
        limit: usize,
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + use<'txn, W, R>, Self::Failure> {
        assert_valid_range(&range);
        get_range(begun(&lock(self.connection))?, &range, limit).map(Vec::into_iter)
    }
}

impl KeyValueTransaction for SqliteTransaction<'_, &mut Vec<Bytes>> {
    type Rejection = std::convert::Infallible;

    fn set(
        &mut self,
        key: Bytes,
        mut value: impl Buf,
    ) -> Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>> {
        if key.len() + value.remaining() > LARGEST_ACCEPTED_ENTRY {
            return Err(StoreError::Rejected(SetRejection::TooLarge));
        }
        let joined;
        let contiguous = if value.chunk().len() == value.remaining() {
            value.chunk()
        } else {
            joined = value.copy_to_bytes(value.remaining());
            &joined[..]
        };
        let changed = begun(&lock(self.connection))?
            .prepare_cached(SET)?
            .execute(params![&key[..], contiguous])?
            > 0;
        value.advance(value.remaining());
        if changed {
            self.written.push(key);
        }
        Ok(())
    }

    fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure> {
        if begun(&lock(self.connection))?
            .prepare_cached(CLEAR)?
            .execute([&key[..]])?
            > 0
        {
            self.written.push(key);
        }
        Ok(())
    }

    fn clear_range(&mut self, range: impl RangeBounds<Bytes>) -> Result<(), Self::Failure> {
        assert_valid_range(&range);
        let (start, end) = half_open(&range);
        let connection = lock(self.connection);
        let connection = begun(&connection)?;
        let written = &mut *self.written;
        let mut cleared = |row: &rusqlite::Row<'_>| {
            written.push(Bytes::from(row.get::<_, Vec<u8>>(0)?));
            Ok(())
        };
        match end {
            Some(end) => connection
                .prepare_cached(CLEAR_BETWEEN)?
                .query_map(params![&*start, &*end], &mut cleared)?
                .collect(),
            None => connection
                .prepare_cached(CLEAR_FROM)?
                .query_map([&*start], &mut cleared)?
                .collect(),
        }
    }
}

// begun is a connection while its transaction is open. Once SQLite rolled the transaction back after a failure, a
// statement fails as SQLite fails one it aborted, so nothing commits on its own.
fn begun(connection: &Connection) -> rusqlite::Result<&Connection> {
    if connection.is_autocommit() {
        return Err(rusqlite::Error::SqliteFailure(
            ffi::Error::new(ffi::SQLITE_ABORT_ROLLBACK),
            None,
        ));
    }
    Ok(connection)
}

fn get(connection: &Connection, key: &[u8]) -> rusqlite::Result<Option<Bytes>> {
    let mut statement = connection.prepare_cached(GET)?;
    let mut rows = statement.query([key])?;
    rows.next()?
        .map(|row| row.get::<_, Vec<u8>>(0).map(Bytes::from))
        .transpose()
}

fn get_range(
    connection: &Connection,
    range: &impl RangeBounds<Bytes>,
    limit: usize,
) -> rusqlite::Result<Vec<(Bytes, Bytes)>> {
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let (start, end) = half_open(range);
    let entry = |row: &rusqlite::Row<'_>| {
        Ok((
            Bytes::from(row.get::<_, Vec<u8>>(0)?),
            Bytes::from(row.get::<_, Vec<u8>>(1)?),
        ))
    };
    match end {
        Some(end) => connection
            .prepare_cached(GET_BETWEEN)?
            .query_map(params![&*start, &*end, limit], entry)?
            .collect(),
        None => connection
            .prepare_cached(GET_FROM)?
            .query_map(params![&*start, limit], entry)?
            .collect(),
    }
}

// half_open is a range of keys as its first key and the key after its last, or None when it has no last key.
// The key right after a key is that key followed by a zero byte, and the empty key is the first of all.
fn half_open(range: &impl RangeBounds<Bytes>) -> (Cow<'_, [u8]>, Option<Cow<'_, [u8]>>) {
    let start = match range.start_bound() {
        Bound::Included(key) => Cow::Borrowed(&key[..]),
        Bound::Excluded(key) => Cow::Owned(after(key)),
        Bound::Unbounded => Cow::Borrowed(&[][..]),
    };
    let end = match range.end_bound() {
        Bound::Included(key) => Some(Cow::Owned(after(key))),
        Bound::Excluded(key) => Some(Cow::Borrowed(&key[..])),
        Bound::Unbounded => None,
    };
    (start, end)
}

fn after(key: &[u8]) -> Vec<u8> {
    let mut after = Vec::with_capacity(key.len() + 1);
    after.extend_from_slice(key);
    after.push(0);
    after
}

fn execute(connection: &Connection, statement: &str) -> rusqlite::Result<()> {
    connection.prepare_cached(statement)?.execute([])?;
    Ok(())
}

fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn get_mut<T>(mutex: &mut std::sync::Mutex<T>) -> &mut T {
    mutex.get_mut().unwrap_or_else(PoisonError::into_inner)
}
