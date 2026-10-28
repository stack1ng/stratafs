#![allow(unsafe_code)]

use std::ffi::{CStr, c_int};
use std::io;
use std::mem::ManuallyDrop;
use std::ops::Bound;
use std::ptr::{self, NonNull};
use std::sync::{Mutex, MutexGuard, Once, PoisonError};

use bytes::Bytes;
use mdbx_sys as ffi;
use stratafs_store::content::ContentHash;
use stratafs_store::durability::DurableAgainst;

use crate::{MdbxConfiguration, MdbxFailure, MdbxOpenError};

// LARGEST_KEY is the largest key libmdbx stores in a file of 4 KiB pages.
pub(crate) const LARGEST_KEY: usize = 2022;

// CHUNK_SIZE is how much of a piece of content each of its entries keeps. libmdbx keeps a long value in pages next
// to each other, so a smaller chunk needs a shorter run of free pages.
const CHUNK_SIZE: usize = 256 * 1024;

// The store keeps its metadata and its content in two named tables, whose names libmdbx keeps in its main table.
const TABLES: u64 = 2;
const METADATA_TABLE: &CStr = c"metadata";
const CONTENT_TABLE: &CStr = c"content";

static LOG_FATAL_ONLY: Once = Once::new();

// Environment is a libmdbx environment: the store's file mapped into memory, and the lock file beside it that
// every process with the store open shares.
pub(crate) struct Environment {
    environment: NonNull<ffi::MDBX_env>,
    metadata: ffi::MDBX_dbi,
    // content keeps each piece of content's length under its hash, and each chunk of it under its hash followed by
    // the chunk's index.
    content: ffi::MDBX_dbi,
    largest_key: usize,
    largest_value: usize,
    // idle is the read only transactions no snapshot is using. Each is reset, which keeps its slot in the reader
    // table, so a snapshot that renews one takes no lock that a new transaction takes to claim a slot.
    idle: Mutex<Vec<TransactionPointer>>,
}

// SAFETY: an environment opened with MDBX_NOSTICKYTHREADS may be used from any thread, and from several at once.
unsafe impl Send for Environment {}
unsafe impl Sync for Environment {}

impl Environment {
    // open opens the store's file at path as configuration tells libmdbx to, after allowing the store's two
    // tables, which configuration's options may change. It adds MDBX_NOSTICKYTHREADS to the flags, since the store
    // moves its transactions between threads.
    pub(crate) fn open(
        path: &CStr,
        configuration: &MdbxConfiguration<'_>,
    ) -> Result<Self, MdbxOpenError> {
        // libmdbx logs to stderr, down to notices such as each new file it makes. Every failure it logs also
        // comes back as a code, so it logs only what ends the process.
        // SAFETY: the logger is left as it is, so no buffer is needed.
        LOG_FATAL_ONLY.call_once(|| unsafe {
            ffi::mdbx_setup_debug_nofmt(
                ffi::MDBX_LOG_FATAL,
                ffi::MDBX_DBG_DONTCHANGE,
                None,
                ptr::null_mut(),
                0,
            );
        });
        let mut environment = ptr::null_mut();
        // SAFETY: environment receives the new environment.
        check(unsafe { ffi::mdbx_env_create(&mut environment) })?;
        // Dropping it from here on closes it.
        let mut opened = Self {
            environment: NonNull::new(environment).expect("libmdbx made an environment"),
            metadata: 0,
            content: 0,
            largest_key: 0,
            largest_value: 0,
            idle: Mutex::new(Vec::new()),
        };
        let raw = opened.environment.as_ptr();
        let geometry = configuration.geometry;
        // SAFETY: the environment is made and not yet open, which is when these settings apply.
        unsafe {
            check(ffi::mdbx_env_set_option(raw, ffi::MDBX_opt_max_db, TABLES))?;
            for &(option, value) in configuration.options {
                check(ffi::mdbx_env_set_option(raw, option, value))?;
            }
            check(ffi::mdbx_env_set_geometry(
                raw,
                geometry.size_lower,
                geometry.size_now,
                geometry.size_upper,
                geometry.growth_step,
                geometry.shrink_threshold,
                geometry.page_size,
            ))?;
        }
        // SAFETY: path is a nul terminated string.
        let code = unsafe {
            ffi::mdbx_env_open(
                raw,
                path.as_ptr(),
                configuration.flags | ffi::MDBX_NOSTICKYTHREADS,
                configuration.mode,
            )
        };
        match code {
            ffi::MDBX_SUCCESS => {}
            // Another opening of the file holds its lock: one in this process, or, when this opening uses
            // MDBX_EXCLUSIVE, one in any process.
            libc::EAGAIN | ffi::MDBX_BUSY => return Err(MdbxOpenError::AlreadyOpen),
            code => return Err(MdbxFailure(code).into()),
        }
        // SAFETY: the environment is open.
        let (largest_key, largest_value) = unsafe {
            (
                ffi::mdbx_env_get_maxkeysize_ex(raw, ffi::MDBX_DB_DEFAULTS),
                ffi::mdbx_env_get_maxvalsize_ex(raw, ffi::MDBX_DB_DEFAULTS),
            )
        };
        opened.largest_key =
            usize::try_from(largest_key).expect("an open environment has a largest key");
        opened.largest_value =
            usize::try_from(largest_value).expect("an open environment has a largest value");
        // libmdbx keeps the page size a file was made with.
        if opened.largest_key < LARGEST_KEY {
            return Err(MdbxOpenError::PagesTooSmall);
        }
        // SAFETY: the environment is open, which is when the options libmdbx keeps in the lock file apply.
        unsafe {
            for &(option, value) in configuration.shared_options {
                check(ffi::mdbx_env_set_option(raw, option, value))?;
            }
        }
        (opened.metadata, opened.content) = opened.open_tables()?;
        Ok(opened)
    }

    // open_tables opens the store's two tables in a write transaction, making each one the file lacks.
    fn open_tables(&self) -> Result<(ffi::MDBX_dbi, ffi::MDBX_dbi), MdbxFailure> {
        let mut transaction = ptr::null_mut();
        let (mut metadata, mut content) = (0, 0);
        // SAFETY: the environment is open, and transaction receives the new transaction, which ends on this
        // thread, where it began.
        unsafe {
            check(ffi::mdbx_txn_begin_ex(
                self.environment.as_ptr(),
                ptr::null_mut(),
                ffi::MDBX_TXN_READWRITE,
                &mut transaction,
                ptr::null_mut(),
            ))?;
            let opened = check(ffi::mdbx_dbi_open(
                transaction,
                METADATA_TABLE.as_ptr(),
                ffi::MDBX_CREATE,
                &mut metadata,
            ))
            .and_then(|()| {
                check(ffi::mdbx_dbi_open(
                    transaction,
                    CONTENT_TABLE.as_ptr(),
                    ffi::MDBX_CREATE,
                    &mut content,
                ))
            });
            match opened {
                Ok(()) => check(ffi::mdbx_txn_commit_ex(transaction, ptr::null_mut()))?,
                Err(failure) => {
                    ffi::mdbx_txn_abort_ex(transaction, ptr::null_mut());
                    return Err(failure);
                }
            }
        }
        Ok((metadata, content))
    }

    // fits says whether libmdbx stores a key and value of these lengths.
    pub(crate) fn fits(&self, key: usize, value: usize) -> bool {
        key <= self.largest_key && value <= self.largest_value
    }

    pub(crate) fn snapshot(&self) -> Result<Snapshot<'_>, MdbxFailure> {
        let idle = self
            .idle
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop();
        let transaction = match idle {
            Some(transaction) => {
                // SAFETY: the transaction is reset, and nothing else holds it.
                if let Err(failure) = check(unsafe { ffi::mdbx_txn_renew(transaction.0.as_ptr()) })
                {
                    // SAFETY: the transaction is reset, and nothing uses it after this.
                    unsafe { ffi::mdbx_txn_abort_ex(transaction.0.as_ptr(), ptr::null_mut()) };
                    return Err(failure);
                }
                transaction
            }
            None => {
                let mut transaction = ptr::null_mut();
                // SAFETY: the environment is open, and transaction receives the new transaction.
                check(unsafe {
                    ffi::mdbx_txn_begin_ex(
                        self.environment.as_ptr(),
                        ptr::null_mut(),
                        ffi::MDBX_TXN_RDONLY,
                        &mut transaction,
                        ptr::null_mut(),
                    )
                })?;
                TransactionPointer(NonNull::new(transaction).expect("libmdbx began a transaction"))
            }
        };
        Ok(Snapshot {
            environment: self,
            transaction: Mutex::new(transaction),
        })
    }

    // try_write begins the write transaction, or returns None while another one holds libmdbx's writer lock.
    pub(crate) fn try_write(&self) -> Result<Option<WriteTransaction<'_>>, MdbxFailure> {
        let mut transaction = ptr::null_mut();
        // SAFETY: the environment is open, and transaction receives the new transaction.
        let code = unsafe {
            ffi::mdbx_txn_begin_ex(
                self.environment.as_ptr(),
                ptr::null_mut(),
                ffi::MDBX_TXN_READWRITE | ffi::MDBX_TXN_TRY,
                &mut transaction,
                ptr::null_mut(),
            )
        };
        match code {
            ffi::MDBX_SUCCESS => Ok(Some(WriteTransaction {
                environment: self,
                transaction: NonNull::new(transaction).expect("libmdbx began a transaction"),
            })),
            ffi::MDBX_BUSY => Ok(None),
            code => Err(MdbxFailure(code)),
        }
    }

    // wait_for_writer returns once no write transaction holds libmdbx's writer lock. It waits on the lock.
    pub(crate) fn wait_for_writer(&self) -> Result<(), MdbxFailure> {
        let mut transaction = ptr::null_mut();
        // SAFETY: the environment is open, and transaction receives the new transaction, which ends on this
        // thread, where it began.
        unsafe {
            check(ffi::mdbx_txn_begin_ex(
                self.environment.as_ptr(),
                ptr::null_mut(),
                ffi::MDBX_TXN_READWRITE,
                &mut transaction,
                ptr::null_mut(),
            ))?;
            check(ffi::mdbx_txn_abort_ex(transaction, ptr::null_mut()))
        }
    }

    // sync makes every commit so far durable against a failure. It waits on the disk, and on libmdbx's
    // writer lock.
    pub(crate) fn sync(&self, against: DurableAgainst) -> Result<(), MdbxFailure> {
        // Without MDBX_WRITEMAP, libmdbx flushes the pages the commits wrote, then writes the meta page that names
        // them and flushes it. On Apple's systems each flush is F_BARRIERFSYNC (see vendor/mdbx-sys/PATCHES), which keeps the pages
        // ahead of the meta page on the disk without waiting for the disk's own cache.
        // SAFETY: the environment is open.
        check(unsafe { ffi::mdbx_env_sync_ex(self.environment.as_ptr(), true, false) })?;
        match against {
            DurableAgainst::SoftwareFailure => Ok(()),
            // flush_file also takes to the disk a new file's first pages, which libmdbx counts durable before
            // any flush.
            DurableAgainst::PowerFailure => self.flush_file(),
        }
    }

    // flush_file waits for the disk to hold everything written to the store's file, the disk's own cache
    // included: F_FULLFSYNC on Apple's systems, fdatasync elsewhere.
    fn flush_file(&self) -> Result<(), MdbxFailure> {
        let mut file = 0;
        // SAFETY: the environment is open, and file receives its file descriptor, which it keeps open.
        check(unsafe { ffi::mdbx_env_get_fd(self.environment.as_ptr(), &mut file) })?;
        // SAFETY: file is the environment's open file descriptor.
        #[cfg(target_vendor = "apple")]
        let flushed = unsafe { libc::fcntl(file, libc::F_FULLFSYNC) };
        // SAFETY: file is the environment's open file descriptor.
        #[cfg(not(target_vendor = "apple"))]
        let flushed = unsafe { libc::fdatasync(file) };
        if flushed == -1 {
            return Err(last_os_failure());
        }
        Ok(())
    }

    // newest_commit is the transaction id of the newest commit of any process.
    pub(crate) fn newest_commit(&self) -> Result<u64, MdbxFailure> {
        let snapshot = self.snapshot()?;
        // SAFETY: the transaction is open and this thread holds it.
        Ok(unsafe { ffi::mdbx_txn_id(snapshot.lock().0.as_ptr()) })
    }
}

#[cfg(test)]
impl Environment {
    // is_steady says whether the newest commit is durable: its meta page is signed as libmdbx signs one it has
    // synced, which a process that opens the store after an OS crash takes as the store's state.
    pub(crate) fn is_steady(&self) -> Result<bool, MdbxFailure> {
        let info = self.info()?;
        Ok((0..3).any(|meta| {
            info.mi_meta_txnid[meta] == info.mi_recent_txnid && info.mi_meta_sign[meta] > 1
        }))
    }

    // syncs is how many flushes libmdbx has made of the store's file, counted in the lock file every process
    // shares.
    pub(crate) fn syncs(&self) -> Result<u64, MdbxFailure> {
        Ok(self.info()?.mi_pgop_stat.fsync)
    }

    fn info(&self) -> Result<ffi::MDBX_envinfo, MdbxFailure> {
        // SAFETY: MDBX_envinfo is plain numbers, which mdbx_env_info_ex fills in.
        let mut info: ffi::MDBX_envinfo = unsafe { std::mem::zeroed() };
        // SAFETY: the environment is open, and info is as large as libmdbx is told.
        check(unsafe {
            ffi::mdbx_env_info_ex(
                self.environment.as_ptr(),
                ptr::null(),
                &mut info,
                size_of::<ffi::MDBX_envinfo>(),
            )
        })?;
        Ok(info)
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        for transaction in self
            .idle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
        {
            // SAFETY: the transaction is reset, and nothing uses it after this.
            unsafe { ffi::mdbx_txn_abort_ex(transaction.0.as_ptr(), ptr::null_mut()) };
        }
        // Closing leaves the commits as durable as the environment's flags and ensure_durable made them.
        // SAFETY: every other transaction borrows the environment, so none is left.
        unsafe { ffi::mdbx_env_close_ex(self.environment.as_ptr(), true) };
    }
}

// Snapshot is a read only transaction: the store as the last commit before it began left it.
pub(crate) struct Snapshot<'env> {
    environment: &'env Environment,
    transaction: Mutex<TransactionPointer>,
}

// TransactionPointer is a libmdbx transaction, which one thread uses at a time.
struct TransactionPointer(NonNull<ffi::MDBX_txn>);

// SAFETY: with MDBX_NOSTICKYTHREADS a read only transaction may move between threads.
unsafe impl Send for TransactionPointer {}

impl<'env> Snapshot<'env> {
    pub(crate) fn environment(&self) -> &'env Environment {
        self.environment
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Bytes>, MdbxFailure> {
        let transaction = self.lock();
        // SAFETY: the transaction is open and this thread holds it, and the value stays mapped while it is open.
        Ok(
            unsafe { find(transaction.0, self.environment.metadata, key) }?
                .map(|value| unsafe { copy_of(&value) }),
        )
    }

    // entries walks the snapshot's entries from start to end in key order.
    pub(crate) fn entries<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
    ) -> Result<Entries<'a>, MdbxFailure> {
        let transaction = self.lock();
        let cursor = open_cursor(transaction.0, self.environment.metadata)?;
        Ok(Entries {
            walk: Walk::new(cursor, start, end),
            _transaction: transaction,
        })
    }

    // content_length is the length of the content kept under hash, or None when the store keeps no such content.
    pub(crate) fn content_length(&self, hash: &ContentHash) -> Result<Option<u64>, MdbxFailure> {
        let transaction = self.lock();
        // SAFETY: the transaction is open and this thread holds it, and the value stays mapped while it is open.
        let Some(length) = unsafe { find(transaction.0, self.environment.content, &hash.0) }?
        else {
            return Ok(None);
        };
        // SAFETY: as above.
        let length = unsafe { slice_of(&length) };
        Ok(Some(u64::from_be_bytes(
            length.try_into().expect("a content length is eight bytes"),
        )))
    }

    // read_content fills destination with the bytes from offset of the content kept under hash, which holds them,
    // through each chunk they are in.
    pub(crate) fn read_content(
        &self,
        hash: &ContentHash,
        offset: u64,
        destination: &mut [u8],
    ) -> Result<(), MdbxFailure> {
        let transaction = self.lock();
        let chunk_size = CHUNK_SIZE as u64;
        let mut chunk = offset / chunk_size;
        // start is where the read starts in the chunk, which is the start of every chunk after the first.
        let mut start = (offset % chunk_size) as usize;
        let mut read = 0;
        while read < destination.len() {
            let index = u32::try_from(chunk).expect("content has fewer chunks than u32 counts");
            // SAFETY: the transaction is open and this thread holds it, and the value stays mapped while it is
            // open.
            let found = unsafe {
                find(
                    transaction.0,
                    self.environment.content,
                    &chunk_key(hash, index),
                )
            }?
            .expect("content keeps every chunk its length covers");
            // SAFETY: as above.
            let found = unsafe { slice_of(&found) };
            let length = (found.len() - start).min(destination.len() - read);
            destination[read..read + length].copy_from_slice(&found[start..start + length]);
            read += length;
            chunk += 1;
            start = 0;
        }
        Ok(())
    }

    // is_latest says whether no commit has landed since the snapshot began.
    pub(crate) fn is_latest(&self) -> Result<bool, MdbxFailure> {
        let transaction = self.lock();
        // SAFETY: MDBX_txn_info is plain numbers, which mdbx_txn_info fills in.
        let mut info: ffi::MDBX_txn_info = unsafe { std::mem::zeroed() };
        // SAFETY: the transaction is open and this thread holds it.
        check(unsafe { ffi::mdbx_txn_info(transaction.0.as_ptr(), &mut info, false) })?;
        Ok(info.txn_reader_lag == 0)
    }

    fn lock(&self) -> MutexGuard<'_, TransactionPointer> {
        self.transaction
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        let transaction = self
            .transaction
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .0;
        // SAFETY: the transaction is open, and only the environment's idle transactions keep it after this.
        match unsafe { ffi::mdbx_txn_reset(transaction.as_ptr()) } {
            ffi::MDBX_SUCCESS => self
                .environment
                .idle
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(TransactionPointer(transaction)),
            // SAFETY: the transaction is open, and nothing uses it after this.
            _ => unsafe {
                ffi::mdbx_txn_abort_ex(transaction.as_ptr(), ptr::null_mut());
            },
        }
    }
}

// Entries are a snapshot's entries, copied out one at a time. They hold the snapshot while they walk it.
pub(crate) struct Entries<'snapshot> {
    walk: Walk<'snapshot>,
    _transaction: MutexGuard<'snapshot, TransactionPointer>,
}

impl Iterator for Entries<'_> {
    type Item = Result<(Bytes, Bytes), MdbxFailure>;

    fn next(&mut self) -> Option<Self::Item> {
        self.walk
            .next()
            // SAFETY: the walk's cursor sits on the entry, in a transaction this iterator holds open.
            .map(|entry| entry.map(|(key, value)| unsafe { (copy_of(&key), copy_of(&value)) }))
    }
}

// WriteTransaction is the store's write transaction. It ends on the thread that began it, which holds
// libmdbx's writer lock, so it stays on that thread: it is neither Send nor Sync.
pub(crate) struct WriteTransaction<'env> {
    environment: &'env Environment,
    transaction: NonNull<ffi::MDBX_txn>,
}

impl WriteTransaction<'_> {
    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), MdbxFailure> {
        // SAFETY: the transaction is open, and libmdbx copies the key and value.
        check(unsafe {
            ffi::mdbx_put(
                self.transaction.as_ptr(),
                self.environment.metadata,
                &value_of(key),
                &mut value_of(value),
                ffi::MDBX_UPSERT,
            )
        })
    }

    // clear removes every entry from start to end, and adds each key it removes to cleared.
    pub(crate) fn clear(
        &mut self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        cleared: &mut Vec<Bytes>,
    ) -> Result<(), MdbxFailure> {
        let mut walk = Walk::new(
            open_cursor(self.transaction, self.environment.metadata)?,
            start,
            end,
        );
        while let Some(entry) = walk.next() {
            let (key, _) = entry?;
            // SAFETY: the walk's cursor sits on the entry, in this open transaction.
            cleared.push(unsafe { copy_of(&key) });
            walk.remove_current()?;
        }
        Ok(())
    }

    // put_content keeps content under hash, in chunks, unless the store keeps it already.
    pub(crate) fn put_content(
        &mut self,
        hash: &ContentHash,
        content: &[u8],
    ) -> Result<(), MdbxFailure> {
        let length = (content.len() as u64).to_be_bytes();
        // SAFETY: the transaction is open, and libmdbx copies the key and value.
        match unsafe {
            ffi::mdbx_put(
                self.transaction.as_ptr(),
                self.environment.content,
                &value_of(&hash.0),
                &mut value_of(&length),
                ffi::MDBX_NOOVERWRITE,
            )
        } {
            ffi::MDBX_SUCCESS => {}
            ffi::MDBX_KEYEXIST => return Ok(()),
            code => return Err(MdbxFailure(code)),
        }
        for (index, chunk) in (0..).zip(content.chunks(CHUNK_SIZE)) {
            // SAFETY: as above.
            check(unsafe {
                ffi::mdbx_put(
                    self.transaction.as_ptr(),
                    self.environment.content,
                    &value_of(&chunk_key(hash, index)),
                    &mut value_of(chunk),
                    ffi::MDBX_UPSERT,
                )
            })?;
        }
        Ok(())
    }

    pub(crate) fn commit(self) -> Result<(), MdbxFailure> {
        let transaction = ManuallyDrop::new(self);
        // SAFETY: the transaction is open, and this thread began it. libmdbx ends it whether or not it commits.
        match unsafe { ffi::mdbx_txn_commit_ex(transaction.transaction.as_ptr(), ptr::null_mut()) }
        {
            ffi::MDBX_SUCCESS => Ok(()),
            // libmdbx returns MDBX_RESULT_TRUE when it aborted the transaction after an earlier failure.
            code => Err(MdbxFailure(code)),
        }
    }
}

impl Drop for WriteTransaction<'_> {
    fn drop(&mut self) {
        // SAFETY: the transaction is open, and this thread began it.
        unsafe { ffi::mdbx_txn_abort_ex(self.transaction.as_ptr(), ptr::null_mut()) };
    }
}

// Walk moves a cursor over the entries from a start bound to an end bound in key order.
struct Walk<'bounds> {
    cursor: NonNull<ffi::MDBX_cursor>,
    start: Option<Bound<&'bounds [u8]>>,
    end: Bound<&'bounds [u8]>,
}

impl<'bounds> Walk<'bounds> {
    fn new(
        cursor: NonNull<ffi::MDBX_cursor>,
        start: Bound<&'bounds [u8]>,
        end: Bound<&'bounds [u8]>,
    ) -> Self {
        Self {
            cursor,
            start: Some(start),
            end,
        }
    }

    // next moves to the next entry in the bounds, and returns where its key and value are in the map.
    fn next(&mut self) -> Option<Result<(ffi::MDBX_val, ffi::MDBX_val), MdbxFailure>> {
        let (mut key, mut value) = (empty_value(), empty_value());
        let found = match self.start.take() {
            Some(Bound::Included(start)) => {
                self.get(&mut key, &mut value, start, ffi::MDBX_SET_RANGE)
            }
            Some(Bound::Excluded(start)) => {
                match self.get(&mut key, &mut value, start, ffi::MDBX_SET_RANGE) {
                    // SAFETY: the cursor sits on the entry whose key this is.
                    Ok(true) if unsafe { slice_of(&key) } == start => {
                        self.get(&mut key, &mut value, &[], ffi::MDBX_NEXT)
                    }
                    found => found,
                }
            }
            Some(Bound::Unbounded) => self.get(&mut key, &mut value, &[], ffi::MDBX_FIRST),
            None => self.get(&mut key, &mut value, &[], ffi::MDBX_NEXT),
        };
        match found {
            Ok(true) => {}
            Ok(false) => return None,
            Err(failure) => return Some(Err(failure)),
        }
        // SAFETY: the cursor sits on the entry whose key this is.
        let at = unsafe { slice_of(&key) };
        let past_end = match self.end {
            Bound::Included(end) => at > end,
            Bound::Excluded(end) => at >= end,
            Bound::Unbounded => false,
        };
        (!past_end).then_some(Ok((key, value)))
    }

    // remove_current removes the entry the cursor sits on. The next step then reaches the entry after it.
    fn remove_current(&mut self) -> Result<(), MdbxFailure> {
        // SAFETY: the cursor sits on an entry of an open write transaction.
        check(unsafe { ffi::mdbx_cursor_del(self.cursor.as_ptr(), ffi::MDBX_CURRENT) })
    }

    // get moves the cursor by op, seeking target where op seeks, and says whether it found an entry.
    fn get(
        &mut self,
        key: &mut ffi::MDBX_val,
        value: &mut ffi::MDBX_val,
        target: &[u8],
        op: ffi::MDBX_cursor_op,
    ) -> Result<bool, MdbxFailure> {
        *key = value_of(target);
        // SAFETY: the cursor is open in an open transaction, and libmdbx only reads the target.
        match unsafe { ffi::mdbx_cursor_get(self.cursor.as_ptr(), key, value, op) } {
            ffi::MDBX_SUCCESS | ffi::MDBX_RESULT_TRUE => Ok(true),
            ffi::MDBX_NOTFOUND => Ok(false),
            code => Err(MdbxFailure(code)),
        }
    }
}

impl Drop for Walk<'_> {
    fn drop(&mut self) {
        // SAFETY: the cursor is open, and nothing uses it after this.
        unsafe { ffi::mdbx_cursor_close(self.cursor.as_ptr()) };
    }
}

// chunk_key is the key a chunk of content is kept under: the content's hash, then the chunk's index.
fn chunk_key(hash: &ContentHash, index: u32) -> [u8; 20] {
    let mut key = [0; 20];
    key[..16].copy_from_slice(&hash.0);
    key[16..].copy_from_slice(&index.to_be_bytes());
    key
}

// find is where table's value for key is in the map, or None when table has no such key.
// SAFETY: transaction must be open and held by this thread.
unsafe fn find(
    transaction: NonNull<ffi::MDBX_txn>,
    table: ffi::MDBX_dbi,
    key: &[u8],
) -> Result<Option<ffi::MDBX_val>, MdbxFailure> {
    let mut value = empty_value();
    // SAFETY: the caller holds the open transaction, and libmdbx only reads the key.
    match unsafe { ffi::mdbx_get(transaction.as_ptr(), table, &value_of(key), &mut value) } {
        ffi::MDBX_SUCCESS => Ok(Some(value)),
        ffi::MDBX_NOTFOUND => Ok(None),
        code => Err(MdbxFailure(code)),
    }
}

fn open_cursor(
    transaction: NonNull<ffi::MDBX_txn>,
    table: ffi::MDBX_dbi,
) -> Result<NonNull<ffi::MDBX_cursor>, MdbxFailure> {
    let mut cursor = ptr::null_mut();
    // SAFETY: the transaction is open and held by this thread, and cursor receives the new cursor.
    check(unsafe { ffi::mdbx_cursor_open(transaction.as_ptr(), table, &mut cursor) })?;
    Ok(NonNull::new(cursor).expect("libmdbx opened a cursor"))
}

fn check(code: c_int) -> Result<(), MdbxFailure> {
    match code {
        // MDBX_RESULT_TRUE means there was nothing to do.
        ffi::MDBX_SUCCESS | ffi::MDBX_RESULT_TRUE => Ok(()),
        code => Err(MdbxFailure(code)),
    }
}

fn last_os_failure() -> MdbxFailure {
    MdbxFailure(
        io::Error::last_os_error()
            .raw_os_error()
            .expect("a failed system call sets errno"),
    )
}

// describe is libmdbx's message for a failure.
pub(crate) fn describe(code: c_int) -> String {
    // SAFETY: mdbx_strerror returns a nul terminated string, which stays valid for any code.
    unsafe { CStr::from_ptr(ffi::mdbx_strerror(code)) }
        .to_string_lossy()
        .into_owned()
}

fn empty_value() -> ffi::MDBX_val {
    ffi::MDBX_val {
        iov_base: ptr::null_mut(),
        iov_len: 0,
    }
}

// value_of points libmdbx at bytes, which it only reads.
fn value_of(bytes: &[u8]) -> ffi::MDBX_val {
    ffi::MDBX_val {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    }
}

// SAFETY: value must point at memory that stays valid and unchanged for 'a.
unsafe fn slice_of<'a>(value: &ffi::MDBX_val) -> &'a [u8] {
    match value.iov_len {
        0 => &[],
        length => unsafe { std::slice::from_raw_parts(value.iov_base.cast(), length) },
    }
}

// SAFETY: value must point at valid memory.
unsafe fn copy_of(value: &ffi::MDBX_val) -> Bytes {
    Bytes::copy_from_slice(unsafe { slice_of(value) })
}
