// The crashable VFS implements SQLite's C VFS interface, which needs unsafe code.
#![allow(unsafe_code)]

use std::collections::HashMap;
use std::ffi::{CStr, OsStr, c_char, c_int, c_void};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::{ptr, slice};

use rusqlite::ffi;
use stratafs_kv_conformance::durability::CrashableStorage;
use stratafs_sqlite::SqliteMetadata;
use stratafs_store::durability::DurableAgainst;
use tempfile::TempDir;

const NAME: &CStr = c"stratafs-crashable";

// CrashableMemory is the files of one store, each as written, as last synced, and the changes since. A crash leaves
// each file as last synced, with any of the changes since, since a disk may take writes between two syncs in any
// order.
#[derive(Clone, Default)]
pub struct CrashableMemory(Arc<Mutex<CrashableMemoryState>>);

#[derive(Default)]
pub struct CrashableMemoryState {
    files: HashMap<Vec<u8>, MemoryFile>,
    storage_operations: usize,
    crash_after: Option<usize>,
    // syncs are what each sync was against, in order.
    pub syncs: Vec<DurableAgainst>,
    // failing_reads makes every read fail, as a failing disk would.
    pub failing_reads: bool,
}

#[derive(Clone, Default)]
struct MemoryFile {
    current: Vec<u8>,
    synced: Vec<u8>,
    since_sync: Vec<Change>,
}

#[derive(Clone)]
enum Change {
    Write { offset: usize, data: Vec<u8> },
    Truncate { size: usize },
}

impl Change {
    fn apply(&self, contents: &mut Vec<u8>) {
        match self {
            Change::Write { offset, data } => {
                if contents.len() < offset + data.len() {
                    contents.resize(offset + data.len(), 0);
                }
                contents[*offset..offset + data.len()].copy_from_slice(data);
            }
            Change::Truncate { size } => contents.truncate(*size),
        }
    }
}

impl CrashableMemory {
    pub fn state(&self) -> std::sync::MutexGuard<'_, CrashableMemoryState> {
        self.0.lock().unwrap()
    }

    // length is how long the file of a name is as written, or 0 while there is none.
    pub fn length(&self, name: &str) -> usize {
        self.state()
            .files
            .get(name.as_bytes())
            .map_or(0, |file| file.current.len())
    }

    fn perform(&self, operation: impl FnOnce(&mut CrashableMemoryState)) -> bool {
        let mut state = self.state();
        state.storage_operations += 1;
        if state
            .crash_after
            .is_some_and(|crash_after| state.storage_operations > crash_after)
        {
            return false;
        }
        operation(&mut state);
        true
    }

    // crashed is what a crash leaves of the files: each as last synced, with the changes since that kept says
    // reached the disk.
    fn crashed(&self, kept: impl Fn(&[u8], usize) -> bool) -> CrashableMemory {
        let files = self
            .state()
            .files
            .iter()
            .map(|(name, file)| {
                let mut contents = file.synced.clone();
                for (index, change) in file.since_sync.iter().enumerate() {
                    if kept(name, index) {
                        change.apply(&mut contents);
                    }
                }
                let file = MemoryFile {
                    current: contents.clone(),
                    synced: contents,
                    since_sync: Vec::new(),
                };
                (name.clone(), file)
            })
            .collect();
        CrashableMemory(Arc::new(Mutex::new(CrashableMemoryState {
            files,
            ..CrashableMemoryState::default()
        })))
    }
}

impl CrashableStorage for CrashableMemory {
    fn storage_operations(&self) -> usize {
        self.state().storage_operations
    }

    fn crash_after(&self, storage_operations: usize) {
        self.state().crash_after = Some(storage_operations);
    }
}

// CrashableStores is a temporary directory that gives each store a fresh directory of its own, where the unix VFS
// keeps the files SQLite locks and shares memory through, while the files' contents stay in a CrashableMemory.
pub struct CrashableStores {
    directory: TempDir,
    opened: AtomicUsize,
    checkpoint_pages: u32,
}

impl CrashableStores {
    pub fn new(checkpoint_pages: u32) -> Self {
        Self {
            directory: tempfile::tempdir().unwrap(),
            opened: AtomicUsize::new(0),
            checkpoint_pages,
        }
    }

    pub fn open<S>(&self, spawner: S) -> (SqliteMetadata<S>, CrashableMemory) {
        let memory = CrashableMemory::default();
        (self.open_memory(memory.clone(), spawner), memory)
    }

    // open_two opens two stores on one database, which lock it from each other as two processes would.
    pub fn open_two<S>(
        &self,
        first: S,
        second: S,
    ) -> (SqliteMetadata<S>, SqliteMetadata<S>, CrashableMemory) {
        let memory = CrashableMemory::default();
        let directory = self.directory_for(&memory);
        let open = |spawner| self.open_in(&directory, spawner);
        (open(first), open(second), memory)
    }

    // reopen_killed opens what a killed process leaves of memory: every change, with only what it synced on the disk.
    pub fn reopen_killed<S>(
        &self,
        memory: &CrashableMemory,
        spawner: S,
    ) -> (SqliteMetadata<S>, CrashableMemory) {
        let left = CrashableMemory(Arc::new(Mutex::new(CrashableMemoryState {
            files: memory.state().files.clone(),
            ..CrashableMemoryState::default()
        })));
        (self.open_memory(left.clone(), spawner), left)
    }

    // reopen_synced opens what a crash that loses every change since the last sync leaves of memory.
    pub fn reopen_synced<S>(&self, memory: CrashableMemory, spawner: S) -> SqliteMetadata<S> {
        self.open_memory(memory.crashed(|_, _| false), spawner)
    }

    // reopen_torn opens what a crash leaves of memory when the disk took some of the changes since the last sync,
    // chosen by a hash of the file, the change and how many storage operations there were.
    pub fn reopen_torn<S>(&self, memory: CrashableMemory, spawner: S) -> SqliteMetadata<S> {
        let operations = memory.storage_operations();
        self.open_memory(
            memory.crashed(|name, index| {
                let mut hasher = DefaultHasher::new();
                (name, index, operations).hash(&mut hasher);
                hasher.finish().is_multiple_of(2)
            }),
            spawner,
        )
    }

    fn open_memory<S>(&self, memory: CrashableMemory, spawner: S) -> SqliteMetadata<S> {
        let directory = self.directory_for(&memory);
        self.open_in(&directory, spawner)
    }

    // directory_for is a fresh directory whose files keep their contents in memory.
    fn directory_for(&self, memory: &CrashableMemory) -> PathBuf {
        let directory = self
            .directory
            .path()
            .join(self.opened.fetch_add(1, Ordering::Relaxed).to_string());
        std::fs::create_dir(&directory).unwrap();
        // SQLite opens a file by its full path, which follows symbolic links.
        let directory = std::fs::canonicalize(directory).unwrap();
        let mut memories = memories().lock().unwrap();
        memories.retain(|_, memory| memory.strong_count() > 0);
        memories.insert(directory.clone(), Arc::downgrade(&memory.0));
        directory
    }

    fn open_in<S>(&self, directory: &Path, spawner: S) -> SqliteMetadata<S> {
        SqliteMetadata::new(
            directory.join("metadata.sqlite"),
            register(),
            self.checkpoint_pages,
            spawner,
        )
        .unwrap()
    }
}

// memories are the memory of each store's directory, while the store or its CrashableMemory is alive.
fn memories() -> &'static Mutex<HashMap<PathBuf, Weak<Mutex<CrashableMemoryState>>>> {
    static MEMORIES: OnceLock<Mutex<HashMap<PathBuf, Weak<Mutex<CrashableMemoryState>>>>> =
        OnceLock::new();
    MEMORIES.get_or_init(Mutex::default)
}

// memory_of is the memory a file belongs to, and the file's name in it.
fn memory_of(name: *const c_char) -> Option<(CrashableMemory, Vec<u8>)> {
    if name.is_null() {
        return None;
    }
    // SAFETY: SQLite names a file with a string it keeps while the call lasts.
    let path = Path::new(OsStr::from_bytes(
        unsafe { CStr::from_ptr(name) }.to_bytes(),
    ));
    let memory = memories().lock().unwrap().get(path.parent()?)?.upgrade()?;
    Some((
        CrashableMemory(memory),
        path.file_name()?.as_bytes().to_vec(),
    ))
}

#[repr(C)]
struct CrashableVfs {
    base: ffi::sqlite3_vfs,
    unix: *mut ffi::sqlite3_vfs,
}

// CrashableFile is a file whose contents are in a CrashableMemory, and the unix VFS's file of the same name, which
// follows it in memory and keeps its locks and shared memory.
#[repr(C)]
struct CrashableFile {
    base: ffi::sqlite3_file,
    memory: CrashableMemory,
    name: Vec<u8>,
}

fn register() -> &'static CStr {
    static REGISTERED: OnceLock<()> = OnceLock::new();
    REGISTERED.get_or_init(|| unsafe {
        let unix = ffi::sqlite3_vfs_find(c"unix".as_ptr());
        let unix_vfs = *unix;
        let vfs = Box::leak(Box::new(CrashableVfs {
            base: ffi::sqlite3_vfs {
                szOsFile: size_of::<CrashableFile>() as c_int + unix_vfs.szOsFile,
                pNext: ptr::null_mut(),
                zName: NAME.as_ptr(),
                xOpen: Some(open),
                xDelete: Some(delete),
                xAccess: Some(access),
                ..unix_vfs
            },
            unix,
        }));
        assert_eq!(
            ffi::sqlite3_vfs_register(&raw mut vfs.base, 0),
            ffi::SQLITE_OK
        );
    });
    NAME
}

static METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
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
    xFetch: None,
    xUnfetch: None,
};

unsafe fn unix_vfs(vfs: *mut ffi::sqlite3_vfs) -> *mut ffi::sqlite3_vfs {
    unsafe { (*vfs.cast::<CrashableVfs>()).unix }
}

unsafe fn unix(file: *mut ffi::sqlite3_file) -> *mut ffi::sqlite3_file {
    unsafe { file.cast::<u8>().add(size_of::<CrashableFile>()).cast() }
}

unsafe fn methods(unix: *mut ffi::sqlite3_file) -> &'static ffi::sqlite3_io_methods {
    unsafe { &*(*unix).pMethods }
}

unsafe fn crashable<'a>(file: *mut ffi::sqlite3_file) -> &'a CrashableFile {
    unsafe { &*file.cast::<CrashableFile>() }
}

unsafe extern "C" fn open(
    vfs: *mut ffi::sqlite3_vfs,
    name: ffi::sqlite3_filename,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    unsafe {
        let unix_vfs = unix_vfs(vfs);
        let unix = unix(file);
        (*file).pMethods = ptr::null();
        let Some((memory, name_in_memory)) = memory_of(name) else {
            return ffi::SQLITE_CANTOPEN;
        };
        {
            let mut state = memory.state();
            if !state.files.contains_key(&name_in_memory) {
                if flags & ffi::SQLITE_OPEN_CREATE == 0 {
                    return ffi::SQLITE_CANTOPEN;
                }
                state
                    .files
                    .insert(name_in_memory.clone(), MemoryFile::default());
            }
        }
        let opened = ((*unix_vfs).xOpen.unwrap())(unix_vfs, name, unix, flags, out_flags);
        if opened != ffi::SQLITE_OK {
            return opened;
        }
        file.cast::<CrashableFile>().write(CrashableFile {
            base: ffi::sqlite3_file {
                pMethods: &raw const METHODS,
            },
            memory,
            name: name_in_memory,
        });
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn delete(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    sync_directory: c_int,
) -> c_int {
    unsafe {
        let unix_vfs = unix_vfs(vfs);
        if let Some((memory, name_in_memory)) = memory_of(name)
            && !memory.perform(|state| {
                state.files.remove(&name_in_memory);
            })
        {
            return ffi::SQLITE_IOERR_DELETE;
        }
        match ((*unix_vfs).xDelete.unwrap())(unix_vfs, name, sync_directory) {
            ffi::SQLITE_IOERR_DELETE_NOENT => ffi::SQLITE_OK,
            deleted => deleted,
        }
    }
}

unsafe extern "C" fn access(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    flags: c_int,
    exists: *mut c_int,
) -> c_int {
    unsafe {
        let unix_vfs = unix_vfs(vfs);
        match (flags, memory_of(name)) {
            // The unix VFS counts an empty file as absent, the same way.
            (ffi::SQLITE_ACCESS_EXISTS, Some((memory, name_in_memory))) => {
                *exists = c_int::from(
                    memory
                        .state()
                        .files
                        .get(&name_in_memory)
                        .is_some_and(|file| !file.current.is_empty()),
                );
                ffi::SQLITE_OK
            }
            _ => ((*unix_vfs).xAccess.unwrap())(unix_vfs, name, flags, exists),
        }
    }
}

unsafe extern "C" fn close(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let unix = unix(file);
        let closed = methods(unix).xClose.unwrap()(unix);
        ptr::drop_in_place(file.cast::<CrashableFile>());
        closed
    }
}

unsafe extern "C" fn read(
    file: *mut ffi::sqlite3_file,
    buffer: *mut c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let file = crashable(file);
        let out = slice::from_raw_parts_mut(buffer.cast::<u8>(), amount as usize);
        let state = file.memory.state();
        if state.failing_reads {
            return ffi::SQLITE_IOERR_READ;
        }
        let current = &state.files[&file.name].current;
        let start = (offset as usize).min(current.len());
        let read = (current.len() - start).min(out.len());
        out[..read].copy_from_slice(&current[start..start + read]);
        if read < out.len() {
            out[read..].fill(0);
            return ffi::SQLITE_IOERR_SHORT_READ;
        }
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn write(
    file: *mut ffi::sqlite3_file,
    buffer: *const c_void,
    amount: c_int,
    offset: ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let file = crashable(file);
        let data = slice::from_raw_parts(buffer.cast::<u8>(), amount as usize);
        let offset = offset as usize;
        let written = file.memory.perform(|state| {
            let memory_file = state.files.get_mut(&file.name).unwrap();
            let change = Change::Write {
                offset,
                data: data.to_vec(),
            };
            change.apply(&mut memory_file.current);
            memory_file.since_sync.push(change);
        });
        if written {
            ffi::SQLITE_OK
        } else {
            ffi::SQLITE_IOERR_WRITE
        }
    }
}

unsafe extern "C" fn truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    unsafe {
        let file = crashable(file);
        let truncated = file.memory.perform(|state| {
            let memory_file = state.files.get_mut(&file.name).unwrap();
            let change = Change::Truncate {
                size: size as usize,
            };
            change.apply(&mut memory_file.current);
            memory_file.since_sync.push(change);
        });
        if truncated {
            ffi::SQLITE_OK
        } else {
            ffi::SQLITE_IOERR_TRUNCATE
        }
    }
}

unsafe extern "C" fn sync(file: *mut ffi::sqlite3_file, flags: c_int) -> c_int {
    unsafe {
        let file = crashable(file);
        let synced = file.memory.perform(|state| {
            let memory_file = state.files.get_mut(&file.name).unwrap();
            memory_file.synced = memory_file.current.clone();
            memory_file.since_sync.clear();
            state.syncs.push(match flags & 0x0F {
                ffi::SQLITE_SYNC_FULL => DurableAgainst::PowerFailure,
                _ => DurableAgainst::SoftwareFailure,
            });
        });
        if synced {
            ffi::SQLITE_OK
        } else {
            ffi::SQLITE_IOERR_FSYNC
        }
    }
}

unsafe extern "C" fn file_size(
    file: *mut ffi::sqlite3_file,
    size: *mut ffi::sqlite3_int64,
) -> c_int {
    unsafe {
        let file = crashable(file);
        *size = file.memory.state().files[&file.name].current.len() as ffi::sqlite3_int64;
        ffi::SQLITE_OK
    }
}

unsafe extern "C" fn lock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let unix = unix(file);
        methods(unix).xLock.unwrap()(unix, level)
    }
}

unsafe extern "C" fn unlock(file: *mut ffi::sqlite3_file, level: c_int) -> c_int {
    unsafe {
        let unix = unix(file);
        methods(unix).xUnlock.unwrap()(unix, level)
    }
}

unsafe extern "C" fn check_reserved_lock(
    file: *mut ffi::sqlite3_file,
    reserved: *mut c_int,
) -> c_int {
    unsafe {
        let unix = unix(file);
        methods(unix).xCheckReservedLock.unwrap()(unix, reserved)
    }
}

// A file control acts on the unix VFS's file, which holds no contents, except the size hint, which would grow it.
unsafe extern "C" fn file_control(
    file: *mut ffi::sqlite3_file,
    operation: c_int,
    argument: *mut c_void,
) -> c_int {
    unsafe {
        let unix = unix(file);
        match operation {
            ffi::SQLITE_FCNTL_SIZE_HINT => ffi::SQLITE_OK,
            _ => methods(unix).xFileControl.unwrap()(unix, operation, argument),
        }
    }
}

unsafe extern "C" fn sector_size(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let unix = unix(file);
        methods(unix).xSectorSize.unwrap()(unix)
    }
}

unsafe extern "C" fn device_characteristics(file: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let unix = unix(file);
        methods(unix).xDeviceCharacteristics.unwrap()(unix)
    }
}

unsafe extern "C" fn shm_map(
    file: *mut ffi::sqlite3_file,
    region: c_int,
    size: c_int,
    extend: c_int,
    mapped: *mut *mut c_void,
) -> c_int {
    unsafe {
        let unix = unix(file);
        methods(unix).xShmMap.unwrap()(unix, region, size, extend, mapped)
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
        methods(unix).xShmLock.unwrap()(unix, offset, count, flags)
    }
}

unsafe extern "C" fn shm_barrier(file: *mut ffi::sqlite3_file) {
    unsafe {
        let unix = unix(file);
        methods(unix).xShmBarrier.unwrap()(unix)
    }
}

unsafe extern "C" fn shm_unmap(file: *mut ffi::sqlite3_file, delete: c_int) -> c_int {
    unsafe {
        let unix = unix(file);
        methods(unix).xShmUnmap.unwrap()(unix, delete)
    }
}
