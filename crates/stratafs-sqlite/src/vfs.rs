// The store reaches SQLite's C interface to sync its write ahead log, and on Apple's systems to register a VFS,
// both of which need unsafe code.
#![allow(unsafe_code)]

use std::ffi::CStr;
use std::ptr;

use rusqlite::{Connection, ffi};
use stratafs_store::durability::DurableAgainst;

// sync_log makes the database's write ahead log durable against a failure. SQLite asks for the same sync, as
// SQLITE_SYNC_NORMAL against a software failure and SQLITE_SYNC_FULL against a power failure.
pub(crate) fn sync_log(connection: &Connection, against: DurableAgainst) -> rusqlite::Result<()> {
    let flags = match against {
        DurableAgainst::SoftwareFailure => ffi::SQLITE_SYNC_NORMAL,
        DurableAgainst::PowerFailure => ffi::SQLITE_SYNC_FULL,
    };
    let mut log: *mut ffi::sqlite3_file = ptr::null_mut();
    // SAFETY: the connection is open, and SQLITE_FCNTL_JOURNAL_POINTER writes a file pointer to its argument.
    let found = unsafe {
        ffi::sqlite3_file_control(
            connection.handle(),
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_JOURNAL_POINTER,
            (&raw mut log).cast(),
        )
    };
    if found != ffi::SQLITE_OK {
        return Err(rusqlite::Error::SqliteFailure(ffi::Error::new(found), None));
    }
    // SAFETY: the log belongs to the connection, which this thread holds, so nothing else uses it meanwhile.
    // A log without methods is unopened, and the store's read of the data version before each sync opens it.
    let synced = unsafe {
        match (*log).pMethods.as_ref().and_then(|methods| methods.xSync) {
            Some(sync) => sync(log, flags),
            None => ffi::SQLITE_OK,
        }
    };
    match synced {
        ffi::SQLITE_OK => Ok(()),
        failed => Err(rusqlite::Error::SqliteFailure(
            ffi::Error::new(failed),
            None,
        )),
    }
}

// local_disk is the VFS a store on this machine's disk opens its database through.
#[cfg(not(target_vendor = "apple"))]
pub(crate) fn local_disk() -> rusqlite::Result<&'static CStr> {
    Ok(c"unix")
}

// On Apple's systems SQLite syncs a file with fsync, which leaves it in the disk's cache unordered, and with
// F_FULLFSYNC when asked to flush the cache. The local disk VFS is SQLite's unix VFS with its sync replaced:
// SQLITE_SYNC_NORMAL becomes F_BARRIERFSYNC, which orders the file before every later write, and SQLITE_SYNC_FULL
// stays F_FULLFSYNC.
#[cfg(target_vendor = "apple")]
pub(crate) fn local_disk() -> rusqlite::Result<&'static CStr> {
    use std::ffi::c_int;
    use std::sync::OnceLock;

    static REGISTERED: OnceLock<c_int> = OnceLock::new();
    let registered = *REGISTERED.get_or_init(|| {
        // SAFETY: sqlite3_vfs_find returns a VFS SQLite keeps for the life of the process, and
        // sqlite3_vfs_register keeps the leaked VFS for the life of the process.
        unsafe {
            let unix = ffi::sqlite3_vfs_find(c"unix".as_ptr());
            if unix.is_null() {
                return ffi::SQLITE_ERROR;
            }
            let barrier = Box::leak(Box::new(apple::BarrierVfs::wrapping(unix)));
            ffi::sqlite3_vfs_register(&raw mut barrier.base, 0)
        }
    });
    match registered {
        ffi::SQLITE_OK => Ok(apple::NAME),
        failed => Err(rusqlite::Error::SqliteFailure(
            ffi::Error::new(failed),
            None,
        )),
    }
}

#[cfg(target_vendor = "apple")]
mod apple {
    use std::ffi::{CStr, OsStr, c_char, c_int, c_void};
    use std::os::fd::{AsFd, BorrowedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::Path;
    use std::{io, ptr};

    use nix::fcntl::{FcntlArg, fcntl};
    use rusqlite::ffi;
    use stratafs_store::durability::DurableAgainst;

    pub(super) const NAME: &CStr = c"stratafs-barrier";

    // BarrierVfs is SQLite's unix VFS with its files' sync replaced. SQLite hands a VFS method this VFS, so it keeps
    // the unix VFS's own fields, and unix beside them for opening files.
    #[repr(C)]
    pub(super) struct BarrierVfs {
        pub(super) base: ffi::sqlite3_vfs,
        unix: *mut ffi::sqlite3_vfs,
    }

    impl BarrierVfs {
        // SAFETY: unix is SQLite's unix VFS.
        pub(super) unsafe fn wrapping(unix: *mut ffi::sqlite3_vfs) -> Self {
            // SAFETY: unix points at a VFS, per the caller.
            let unix_vfs = unsafe { *unix };
            Self {
                base: ffi::sqlite3_vfs {
                    szOsFile: c_int::try_from(size_of::<BarrierFile>())
                        .expect("a file header fits in a c_int")
                        + unix_vfs.szOsFile,
                    pNext: ptr::null_mut(),
                    zName: NAME.as_ptr(),
                    xOpen: Some(open),
                    ..unix_vfs
                },
                unix,
            }
        }
    }

    // BarrierFile is a file the unix VFS opened, which follows it in memory, and the descriptor that file syncs.
    #[repr(C)]
    struct BarrierFile {
        base: ffi::sqlite3_file,
        descriptor: c_int,
        // unsynced_directory is the name of a log or journal SQLite may have created, whose directory the next sync
        // syncs too so the file's entry survives, or null once it has.
        unsynced_directory: *const c_char,
    }

    // UnixFile is how SQLite's unix VFS begins the file object it opens.
    #[repr(C)]
    struct UnixFile {
        methods: *const ffi::sqlite3_io_methods,
        vfs: *mut ffi::sqlite3_vfs,
        inode: *mut c_void,
        descriptor: c_int,
    }

    static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
        iVersion: 3,
        xClose: Some(close),
        xRead: Some(read),
        xWrite: Some(write),
        xTruncate: Some(truncate),
        xSync: Some(sync),
        xFileSize: Some(file_size),
        xLock: Some(lock),
        xUnlock: Some(unlock),
        xCheckReservedLock: Some(check_reserved_lock),
        xFileControl: Some(file_control),
        xSectorSize: Some(sector_size),
        xDeviceCharacteristics: Some(device_characteristics),
        xShmMap: Some(shm_map),
        xShmLock: Some(shm_lock),
        xShmBarrier: Some(shm_barrier),
        xShmUnmap: Some(shm_unmap),
        xFetch: Some(fetch),
        xUnfetch: Some(unfetch),
    };

    // unix is the file the unix VFS opened behind a barrier file.
    unsafe fn unix(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
        // SAFETY: SQLite allocates szOsFile bytes for the file, and the unix file follows the barrier file in them.
        unsafe { file.cast::<u8>().add(size_of::<BarrierFile>()).cast() }
    }

    // methods are the unix file's methods.
    unsafe fn methods(unix: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
        // SAFETY: an open unix file points at its methods, which the unix VFS keeps for the life of the process.
        unsafe { &*(*unix).pMethods }
    }

    unsafe extern "C" fn open(
        vfs: *mut ffi::sqlite3_vfs,
        name: ffi::sqlite3_filename,
        file: *mut ffi::sqlite3_file,
        flags: c_int,
        out_flags: *mut c_int,
    ) -> c_int {
        // SAFETY: SQLite hands open the barrier VFS it registered and szOsFile bytes for file, which the unix VFS
        // opens its file into after the barrier file.
        unsafe {
            let unix_vfs = (*vfs.cast::<BarrierVfs>()).unix;
            let unix = unix(file);
            let opened = ((*unix_vfs).xOpen.expect("the unix VFS opens files"))(
                unix_vfs, name, unix, flags, out_flags,
            );
            if opened != ffi::SQLITE_OK {
                (*file).pMethods = ptr::null();
                return opened;
            }
            let descriptor = (*unix.cast::<UnixFile>()).descriptor;
            if !name.is_null() && !is_named(descriptor, CStr::from_ptr(name)) {
                methods(unix).xClose.expect("the unix VFS closes files")(unix);
                (*file).pMethods = ptr::null();
                return ffi::SQLITE_CANTOPEN;
            }
            // The unix VFS syncs the directory of a log or journal it was allowed to create, the same way.
            let created_log = flags & ffi::SQLITE_OPEN_CREATE != 0
                && matches!(
                    flags & 0x0FFF00,
                    ffi::SQLITE_OPEN_WAL
                        | ffi::SQLITE_OPEN_MAIN_JOURNAL
                        | ffi::SQLITE_OPEN_SUPER_JOURNAL
                );
            file.cast::<BarrierFile>().write(BarrierFile {
                base: ffi::sqlite3_file {
                    pMethods: &raw const METHODS,
                },
                descriptor,
                unsynced_directory: if created_log { name } else { ptr::null() },
            });
            ffi::SQLITE_OK
        }
    }

    // is_named is whether descriptor is the file at name. Opening a file checks it, so a sync only ever reaches the
    // file SQLite names, however a later unix VFS lays out its files.
    fn is_named(descriptor: c_int, name: &CStr) -> bool {
        // SAFETY: the unix VFS keeps descriptor open while its file is.
        let opened = nix::sys::stat::fstat(unsafe { BorrowedFd::borrow_raw(descriptor) });
        match (opened, nix::sys::stat::stat(name)) {
            (Ok(opened), Ok(named)) => {
                (opened.st_dev, opened.st_ino) == (named.st_dev, named.st_ino)
            }
            _ => false,
        }
    }

    unsafe extern "C" fn sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
        // SAFETY: SQLite syncs a file it opened through this VFS, from the one thread using it.
        let file = unsafe { &mut *file.cast::<BarrierFile>() };
        let against = match flags & 0x0F {
            ffi::SQLITE_SYNC_FULL => DurableAgainst::PowerFailure,
            _ => DurableAgainst::SoftwareFailure,
        };
        // SAFETY: the unix VFS keeps the descriptor open while its file is.
        let descriptor = unsafe { BorrowedFd::borrow_raw(file.descriptor) };
        if sync_against(descriptor, against).is_err() {
            return ffi::SQLITE_IOERR_FSYNC;
        }
        if !file.unsynced_directory.is_null() {
            // SAFETY: SQLite keeps a file's name unchanged until it closes the file.
            let name = unsafe { CStr::from_ptr(file.unsynced_directory) };
            if sync_directory(name, against).is_err() {
                return ffi::SQLITE_IOERR_DIR_FSYNC;
            }
            file.unsynced_directory = ptr::null();
        }
        ffi::SQLITE_OK
    }

    fn sync_directory(name: &CStr, against: DurableAgainst) -> io::Result<()> {
        let name = Path::new(OsStr::from_bytes(name.to_bytes()));
        let directory = std::fs::File::open(name.parent().unwrap_or(Path::new(".")))?;
        sync_against(&directory, against)?;
        Ok(())
    }

    // sync_against hands file to the disk, either with a barrier, so nothing written after it reaches the disk
    // before it, or flushing the disk's own cache too.
    fn sync_against(file: impl AsFd, against: DurableAgainst) -> nix::Result<()> {
        let command = match against {
            DurableAgainst::SoftwareFailure => FcntlArg::F_BARRIERFSYNC,
            DurableAgainst::PowerFailure => FcntlArg::F_FULLFSYNC,
        };
        fcntl(file, command).map(|_| ())
    }

    unsafe extern "C" fn close(file: *mut ffi::sqlite3_file) -> c_int {
        // SAFETY: SQLite closes a file it opened through this VFS once, and the unix file closes itself.
        unsafe {
            let unix = unix(file);
            methods(unix).xClose.expect("the unix VFS closes files")(unix)
        }
    }

    unsafe extern "C" fn read(
        file: *mut ffi::sqlite3_file,
        buffer: *mut c_void,
        amount: c_int,
        offset: ffi::sqlite3_int64,
    ) -> c_int {
        // SAFETY: each method below hands the unix file the call SQLite made on the barrier file.
        unsafe {
            let unix = unix(file);
            methods(unix).xRead.expect("the unix VFS reads files")(unix, buffer, amount, offset)
        }
    }

    unsafe extern "C" fn write(
        file: *mut ffi::sqlite3_file,
        buffer: *const c_void,
        amount: c_int,
        offset: ffi::sqlite3_int64,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix).xWrite.expect("the unix VFS writes files")(unix, buffer, amount, offset)
        }
    }

    unsafe extern "C" fn truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix)
                .xTruncate
                .expect("the unix VFS truncates files")(unix, size)
        }
    }

    unsafe extern "C" fn file_size(
        file: *mut ffi::sqlite3_file,
        size: *mut ffi::sqlite3_int64,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix).xFileSize.expect("the unix VFS sizes files")(unix, size)
        }
    }

    unsafe extern "C" fn lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix).xLock.expect("the unix VFS locks files")(unix, level)
        }
    }

    unsafe extern "C" fn unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix).xUnlock.expect("the unix VFS unlocks files")(unix, level)
        }
    }

    unsafe extern "C" fn check_reserved_lock(
        file: *mut ffi::sqlite3_file,
        reserved: *mut c_int,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix)
                .xCheckReservedLock
                .expect("the unix VFS checks locks")(unix, reserved)
        }
    }

    unsafe extern "C" fn file_control(
        file: *mut ffi::sqlite3_file,
        operation: c_int,
        argument: *mut c_void,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix)
                .xFileControl
                .expect("the unix VFS controls files")(unix, operation, argument)
        }
    }

    unsafe extern "C" fn sector_size(file: *mut ffi::sqlite3_file) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix)
                .xSectorSize
                .expect("the unix VFS sizes sectors")(unix)
        }
    }

    unsafe extern "C" fn device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
        unsafe {
            let unix = unix(file);
            methods(unix)
                .xDeviceCharacteristics
                .expect("the unix VFS describes devices")(unix)
        }
    }

    // The unix VFS shares memory and maps files only where its locking allows, so a file without those methods
    // reports the failure SQLite would see from a VFS that could not.
    unsafe extern "C" fn shm_map(
        file: *mut ffi::sqlite3_file,
        region: c_int,
        size: c_int,
        extend: c_int,
        mapped: *mut *mut c_void,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            match methods(unix).xShmMap {
                Some(shm_map) => shm_map(unix, region, size, extend, mapped),
                None => ffi::SQLITE_IOERR_SHMMAP,
            }
        }
    }

    unsafe extern "C" fn shm_lock(
        file: *mut ffi::sqlite3_file,
        offset: c_int,
        count: c_int,
        flags: c_int,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            match methods(unix).xShmLock {
                Some(shm_lock) => shm_lock(unix, offset, count, flags),
                None => ffi::SQLITE_IOERR_SHMLOCK,
            }
        }
    }

    unsafe extern "C" fn shm_barrier(file: *mut ffi::sqlite3_file) {
        unsafe {
            let unix = unix(file);
            if let Some(shm_barrier) = methods(unix).xShmBarrier {
                shm_barrier(unix);
            }
        }
    }

    unsafe extern "C" fn shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
        unsafe {
            let unix = unix(file);
            match methods(unix).xShmUnmap {
                Some(shm_unmap) => shm_unmap(unix, delete),
                None => ffi::SQLITE_OK,
            }
        }
    }

    unsafe extern "C" fn fetch(
        file: *mut ffi::sqlite3_file,
        offset: ffi::sqlite3_int64,
        amount: c_int,
        fetched: *mut *mut c_void,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            match methods(unix).xFetch {
                Some(fetch) => fetch(unix, offset, amount, fetched),
                None => {
                    *fetched = ptr::null_mut();
                    ffi::SQLITE_OK
                }
            }
        }
    }

    unsafe extern "C" fn unfetch(
        file: *mut ffi::sqlite3_file,
        offset: ffi::sqlite3_int64,
        fetched: *mut c_void,
    ) -> c_int {
        unsafe {
            let unix = unix(file);
            match methods(unix).xUnfetch {
                Some(unfetch) => unfetch(unix, offset, fetched),
                None => ffi::SQLITE_OK,
            }
        }
    }
}
