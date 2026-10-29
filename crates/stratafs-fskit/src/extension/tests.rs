use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex, Once};
use std::thread::{self, ThreadId};
use std::time::Duration;

use block2::RcBlock;
use futures_util::future::FutureObj;
use futures_util::task::SpawnError;
use objc2::encode::RefEncode;
use objc2::{AllocAnyThread, msg_send};
use objc2_foundation::{NSError, NSString, NSURL};
use objc2_fs_kit::{
    FSActivateResult, FSContext, FSCreateItemResult, FSDeactivateOptions, FSGenericURLResource,
    FSGetAttributesResult, FSItemAttribute, FSItemGetAttributesRequest, FSItemID,
    FSItemSetAttributesRequest, FSItemType, FSPathURLResource, FSRemoveItemResult, FSResource,
    FSSetAttributesResult, FSSyncFlags, FSTaskOptions, FSUnaryFileSystemOperations, FSVolume,
    FSVolumeHandler,
};
use object_store::memory::InMemory;
use redb::backends::InMemoryBackend;
use stratafs_fs_engine::attributes::{
    Attributes, ChangeVersion, FileType, Hints, Mode, Ownership, Restrictions, SettableAttributes,
};
use stratafs_fs_engine::observation::KernelCache;
use stratafs_fs_engine::{Eventual, FsConfig, FsEngine, Inode, MountId, Timestamp};
use stratafs_lock_synchronized::LockSynchronizedStore;
use stratafs_object_store::ObjectStoreContentStore;
use stratafs_redb::RedbStore;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::spawn::SpawnBlocking;

use super::changes::attribute_change;
use super::item::{StrataItem, file_name, flags_of, item_attributes, restrictions_and_hints};
use super::operations::Reply;
use super::*;

struct TestConfig;

impl FsConfig for TestConfig {
    const LARGEST_INLINE_ENTRY: usize = 1024;
}

type TestStore = LockSynchronizedStore<RedbStore<SimulatedThreadPool>, ObjectStoreContentStore>;

type TestEngine = FsEngine<TestStore, fn() -> u64, fn() -> Timestamp, TestConfig, Eventual>;

// SimulatedThreadPool yields before it does the work, as a thread pool does when the work is not done by the
// first poll, so making commits durable makes its operation wait.
struct SimulatedThreadPool;

impl SpawnBlocking for SimulatedThreadPool {
    async fn spawn_blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        futures_lite::future::yield_now().await;
        work()
    }
}

async fn new_engine(
    kernel: impl KernelCache,
    spawner: impl Spawn + Send + Sync + 'static,
) -> Result<TestEngine, io::Error> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .map_err(io::Error::other)?;
    let allocate_inode: fn() -> u64 = || 7;
    let now: fn() -> Timestamp = || Timestamp(1_500_000_000);
    FsEngine::new(
        LockSynchronizedStore::new(
            RedbStore::new(database, SimulatedThreadPool).map_err(io::Error::other)?,
            ObjectStoreContentStore::new(InMemory::new()),
        ),
        allocate_inode,
        now,
        MountId(1),
        Some(DurableAgainst::PowerFailure),
        Eventual,
        kernel,
        spawner,
    )
    .await
    .map_err(io::Error::other)
}

// ThreadSpawner finishes each operation that has to wait on a thread of its own, which is enough for tests.
struct ThreadSpawner;

impl Spawn for ThreadSpawner {
    fn spawn_obj(&self, operation: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        std::thread::spawn(move || block_on(operation));
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
enum TestOpenError {
    #[error("the store is down")]
    Down,
    #[error("the store is open in another mount")]
    Busy,
    #[error("the store is not the test store: {0}")]
    WrongStore(PathBuf),
    #[error(transparent)]
    Engine(io::Error),
}

impl From<&TestOpenError> for PosixError {
    fn from(error: &TestOpenError) -> Self {
        match error {
            TestOpenError::Busy => PosixError::EBUSY,
            TestOpenError::Down | TestOpenError::WrongStore(_) | TestOpenError::Engine(_) => {
                PosixError::EIO
            }
        }
    }
}

// store is the store directory the tests mount. Opening only checks it is the directory that was mounted.
fn store() -> PathBuf {
    std::env::temp_dir()
}

// install sets up what run would, once per test process. A volume id of ff fails to open, and bb is
// open in another mount.
fn install() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        assert!(
            OPEN_VOLUME
                .set(opener(
                    ThreadSpawner,
                    async |mounted: &Path,
                           volume: Bytes,
                           kernel: FsKitCache,
                           spawner: Arc<dyn Spawn + Send + Sync>| {
                        if mounted != store() {
                            return Err(TestOpenError::WrongStore(mounted.to_owned()));
                        }
                        match &volume[..] {
                            [0xff] => Err(TestOpenError::Down),
                            [0xbb] => Err(TestOpenError::Busy),
                            _ => new_engine(kernel, spawner)
                                .await
                                .map_err(TestOpenError::Engine),
                        }
                    }
                ))
                .is_ok()
        );
    });
}

fn new_file_system() -> Retained<StrataFileSystem> {
    install();
    // SAFETY: init is NSObject's initializer, which FSUnaryFileSystem inherits.
    unsafe { msg_send![StrataFileSystem::alloc(), init] }
}

fn error_of(error: *mut NSError) -> (isize, String) {
    // SAFETY: FSKit's replies pass a live error whenever they pass one.
    let error = unsafe { error.as_ref() }.expect("the reply passes an error");
    (error.code(), error.localizedDescription().to_string())
}

// Loaded is what loadResource replied: the volume's address, or the error's code and description.
type Loaded = Result<usize, (isize, String)>;

fn store_resource() -> Retained<FSResource> {
    let url = NSURL::from_directory_path(store()).expect("the temporary directory is a path");
    // SAFETY: the URL is a file URL.
    let resource =
        unsafe { FSPathURLResource::initWithURL_writable(FSPathURLResource::alloc(), &url, true) };
    Retained::into_super(resource)
}

// load calls loadResource, and returns the volume it replied with, or its error's code and description.
fn load(
    file_system: &StrataFileSystem,
    resource: &FSResource,
) -> Result<Retained<FSVolume>, (isize, String)> {
    // The volume crosses the reply as an address, since a reply block must be Send and a volume is not.
    let replied: Arc<Mutex<Option<Loaded>>> = Arc::default();
    let reply = RcBlock::<fn(*mut FSVolume, *mut NSError), dyn Send + Sync>::new({
        let replied = replied.clone();
        move |volume: *mut FSVolume, error: *mut NSError| {
            // SAFETY: FSKit's reply passes either a live volume or a live error.
            let result = unsafe { Retained::retain(volume) }
                .map(|volume| Retained::into_raw(volume) as usize)
                .ok_or_else(|| error_of(error));
            *replied.lock().unwrap() = Some(result);
        }
    });
    // SAFETY: the resource, options and reply are all valid for the call.
    unsafe {
        // FSTaskOptions has no public initializer, so the test makes one with NSObject's.
        let options: Retained<FSTaskOptions> = msg_send![FSTaskOptions::alloc(), init];
        file_system.loadResource_options_replyHandler(resource, &options, &reply);
    }
    let result = replied
        .lock()
        .unwrap()
        .take()
        .expect("loadResource replies before it returns");
    // SAFETY: the reply retained the volume and handed that retain over as its address.
    result.map(|volume| unsafe { Retained::from_raw(volume as *mut FSVolume) }.unwrap())
}

// Replied is whether an FSKit operation replied with a result, or the error's code and description.
type Replied = Result<(), (isize, String)>;

// reply is a reply block for an FSKit operation, and a receiver that gets what the operation replied.
fn reply<R: RefEncode + 'static>() -> (Reply<R>, Receiver<Replied>) {
    let (replied, received) = channel();
    let reply = Reply::<R>::new(move |result: *mut R, error: *mut NSError| {
        let result = if result.is_null() {
            Err(error_of(error))
        } else {
            Ok(())
        };
        replied.send(result).unwrap();
    });
    (reply, received)
}

// replying_thread is a reply block for an FSKit operation, and a receiver that gets the thread it was called on.
fn replying_thread<R: RefEncode + 'static>() -> (Reply<R>, Receiver<ThreadId>) {
    let (replied, received) = channel();
    let reply = Reply::<R>::new(move |_result: *mut R, _error: *mut NSError| {
        replied.send(thread::current().id()).unwrap();
    });
    (reply, received)
}

fn strata_volume(volume: &FSVolume) -> &StrataVolume {
    volume
        .downcast_ref::<StrataVolume>()
        .expect("loadResource replies with our volume")
}

// context is an FSContext for a call. FSContext has no public initializer, so the test makes one with
// NSObject's.
fn context() -> Retained<FSContext> {
    // SAFETY: init is NSObject's initializer, which FSContext inherits.
    unsafe { msg_send![FSContext::alloc(), init] }
}

// activate activates a loaded volume as FSKit would with arguments as its options, and returns what it
// replied.
fn activate(volume: &FSVolume, arguments: &[&str]) -> Replied {
    let (reply, replied) = reply::<FSActivateResult>();
    let arguments: Vec<String> = arguments
        .iter()
        .map(|argument| argument.to_string())
        .collect();
    strata_volume(volume).activate_with(&arguments, &reply);
    replied.recv_timeout(Duration::from_secs(10)).unwrap()
}

// mounted is the volume volume loaded from the test store and activated.
fn mounted(volume: &str) -> Retained<FSVolume> {
    let volume_option = format!("volume={volume}");
    let loaded = load(&new_file_system(), &store_resource()).expect("the store loads");
    assert_eq!(activate(&loaded, &["-o", &volume_option]), Ok(()));
    loaded
}

fn name_of(volume: &FSVolume) -> Option<String> {
    // SAFETY: a volume always has a name.
    unsafe { volume.name().string() }.map(|name| name.to_string())
}

// get_attributes calls getAttributes on an inode, and returns a receiver that gets what it replied.
fn get_attributes(volume: &FSVolume, inode: Inode) -> Receiver<Replied> {
    let (reply, replied) = reply::<FSGetAttributesResult>();
    // SAFETY: the request, item, context and reply are all valid for the call.
    unsafe {
        strata_volume(volume).getAttributes_ofItem_context_replyHandler(
            &FSItemGetAttributesRequest::new(),
            &StrataItem::new(inode, FileType::File),
            &context(),
            &reply,
        );
    }
    replied
}

// set_attributes calls setAttributes with request on a directory, and returns a receiver that gets what it
// replied.
fn set_attributes(
    volume: &FSVolume,
    directory: Inode,
    request: &FSItemSetAttributesRequest,
) -> Receiver<Replied> {
    let (reply, replied) = reply::<FSSetAttributesResult>();
    // SAFETY: the request, item, context and reply are all valid for the call.
    unsafe {
        strata_volume(volume).setAttributes_onItem_context_replyHandler(
            request,
            &StrataItem::new(directory, FileType::Directory),
            &context(),
            &reply,
        );
    }
    replied
}

// create_file calls createItem for a file named name in directory with attributes, and returns a receiver
// that gets what it replied.
fn create_file(
    volume: &FSVolume,
    directory: Inode,
    name: &[u8],
    attributes: &FSItemSetAttributesRequest,
) -> Receiver<Replied> {
    let (reply, replied) = reply::<FSCreateItemResult>();
    // SAFETY: the name, item, attributes, context and reply are all valid for the call.
    unsafe {
        strata_volume(volume).createItemNamed_type_inDirectory_attributes_context_replyHandler(
            &file_name(name),
            FSItemType::File,
            &StrataItem::new(directory, FileType::Directory),
            attributes,
            &context(),
            &reply,
        );
    }
    replied
}

#[test]
fn load_resource_takes_a_store_directory_and_refuses_any_other_resource() {
    let file_system = new_file_system();
    let volume = load(&file_system, &store_resource()).expect("the store loads");
    assert_eq!(
        name_of(&volume).as_deref(),
        store().file_name().and_then(|name| name.to_str())
    );
    // SAFETY: the URL is valid.
    let url_resource = unsafe {
        let url =
            NSURL::URLWithString(&NSString::from_str("https://ab01")).expect("the URL is valid");
        Retained::into_super(FSGenericURLResource::initWithURL(
            FSGenericURLResource::alloc(),
            &url,
        ))
    };
    assert!(matches!(
        load(&file_system, &url_resource),
        Err((code, description))
            if code == libc::EINVAL as isize && description.contains("not the path of a store directory")
    ));
}

#[test]
fn activate_opens_the_volume_its_options_name_or_replies_why_not() {
    let volume = mounted("ab01");
    assert_eq!(name_of(&volume).as_deref(), Some("ab01"));
    for (arguments, errno, message) in [
        (&[][..], libc::EINVAL, "no volume was given"),
        (&["-o", "volume=zz"], libc::EINVAL, "not hex"),
        (
            &["-o", "volume=ab01,rdonly"],
            libc::EINVAL,
            "rdonly is not a mount option",
        ),
        (&["-x"], libc::EINVAL, "could not be read"),
        (&["-o", "volume=ff"], libc::EIO, "the store is down"),
        (&["-o", "volume=bb"], libc::EBUSY, "open in another mount"),
    ] {
        let loaded = load(&new_file_system(), &store_resource()).expect("the store loads");
        assert!(
            matches!(
                activate(&loaded, arguments),
                Err((code, description)) if code == errno as isize && description.contains(message)
            ),
            "{arguments:?}"
        );
    }
}

#[test]
fn operations_before_activate_reply_that_the_volume_is_not_active() {
    let loaded = load(&new_file_system(), &store_resource()).expect("the store loads");
    assert!(matches!(
        get_attributes(&loaded, Inode::ROOT).recv_timeout(Duration::from_secs(10)).unwrap(),
        Err((code, description)) if code == libc::ENXIO as isize && description.contains("not active")
    ));
}

#[test]
fn get_attributes_replies_until_the_volume_is_deactivated() {
    let volume = mounted("ab02");
    let replied = |inode| {
        get_attributes(&volume, inode)
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
    };
    assert_eq!(replied(Inode::ROOT), Ok(()));
    assert!(matches!(replied(Inode(5000)), Err((code, _)) if code == libc::ESTALE as isize));

    let deactivated = RcBlock::<fn(*mut NSError), dyn Send + Sync>::new(|_error: *mut NSError| {});
    // SAFETY: the options and reply are valid for the call.
    unsafe {
        strata_volume(&volume)
            .deactivateVolumeWithOptions_replyHandler(FSDeactivateOptions(0), &deactivated)
    };
    assert!(matches!(replied(Inode::ROOT), Err((code, _)) if code == libc::ENXIO as isize));
}

#[test]
fn item_attributes_give_fskit_the_root_as_a_directory_under_its_own_parent() {
    let root = Attributes {
        settable: SettableAttributes {
            mode: Mode::from_bits_truncate(0o755),
            ownership: Ownership {
                user_id: 501,
                group_id: 20,
            },
            restrictions: Restrictions::OWNER_IMMUTABLE | Restrictions::ADMINISTRATOR_APPEND_ONLY,
            hints: Hints::HIDDEN,
            size: 10,
            last_accessed_at: Timestamp(1_500_000_000),
            content_changed_at: Timestamp(-1),
            created_at: Timestamp(0),
        },
        kind: FileType::Directory,
        hard_link_count: 2,
        attribute_changed_at: Timestamp(0),
        parent: Inode::ROOT,
        change_version: ChangeVersion {
            mount: MountId(1),
            number: 0,
        },
    };
    let attributes = item_attributes(Inode::ROOT, &root);
    // SAFETY: the getters read plain values.
    unsafe {
        assert_eq!(attributes.r#type(), FSItemType::Directory);
        assert_eq!(attributes.fileID(), FSItemID::RootDirectory);
        assert_eq!(attributes.parentID(), FSItemID::ParentOfRoot);
        assert_eq!(attributes.mode(), u32::from(libc::S_IFDIR | 0o755));
        assert_eq!(attributes.linkCount(), 2);
        assert_eq!(attributes.uid(), 501);
        assert_eq!(attributes.size(), 10);
        assert_eq!(attributes.allocSize(), 4096);
        assert_eq!(
            attributes.flags(),
            libc::UF_IMMUTABLE | libc::SF_APPEND | libc::UF_HIDDEN
        );
    }
}

#[test]
fn every_restriction_and_hint_round_trips_through_st_flags() {
    for restrictions in Restrictions::all()
        .iter_names()
        .map(|(_, restriction)| restriction)
    {
        for hints in [Hints::empty(), Hints::HIDDEN] {
            assert_eq!(
                restrictions_and_hints(flags_of(restrictions, hints)),
                Some((restrictions, hints))
            );
        }
    }
    assert_eq!(
        restrictions_and_hints(
            libc::UF_IMMUTABLE | libc::UF_APPEND | libc::SF_IMMUTABLE | libc::SF_APPEND
        ),
        Some((Restrictions::all(), Hints::empty()))
    );
    assert_eq!(
        restrictions_and_hints(0),
        Some((Restrictions::empty(), Hints::empty()))
    );
}

#[test]
fn st_flags_with_any_bit_the_volume_does_not_store_are_refused() {
    // libc names only some bits, so the rest are by value: datavault, two undefined owner bits, restricted,
    // nounlink, firmlink and dataless.
    for refused in [
        libc::UF_NODUMP,
        libc::UF_OPAQUE,
        libc::UF_COMPRESSED,
        libc::UF_TRACKED,
        libc::SF_ARCHIVED,
        0x80,
        0x100,
        0x4000,
        0x80000,
        0x100000,
        0x800000,
        0x40000000,
    ] {
        assert_eq!(restrictions_and_hints(refused), None, "{refused:#x}");
        assert_eq!(
            restrictions_and_hints(refused | libc::UF_HIDDEN),
            None,
            "{refused:#x}"
        );
    }
}

fn append_only_settable() -> SettableAttributes {
    SettableAttributes {
        mode: Mode::OWNER_READ,
        ownership: Ownership {
            user_id: 501,
            group_id: 20,
        },
        restrictions: Restrictions::OWNER_APPEND_ONLY,
        hints: Hints::empty(),
        size: 3,
        last_accessed_at: Timestamp(1),
        content_changed_at: Timestamp(2),
        created_at: Timestamp(3),
    }
}

#[test]
fn an_attribute_change_refuses_flags_the_volume_does_not_store_and_applies_the_rest() {
    // SAFETY: the request is a plain value object, and its setters take plain values.
    unsafe {
        let request = FSItemSetAttributesRequest::new();
        request.setMode(0o644);
        request.setFlags(libc::UF_NODUMP);
        let error = attribute_change(&request).err().expect("nodump is refused");
        assert_eq!(error.code(), libc::ENOTSUP as isize);
        assert!(!request.wasAttributeConsumed(FSItemAttribute::Mode));
        assert!(!request.wasAttributeConsumed(FSItemAttribute::Flags));

        let request = FSItemSetAttributesRequest::new();
        request.setFlags(libc::SF_IMMUTABLE | libc::UF_HIDDEN);
        let change = attribute_change(&request).expect("immutable and hidden are stored");
        assert!(request.wasAttributeConsumed(FSItemAttribute::Flags));
        let mut settable = append_only_settable();
        change(&mut settable);
        assert_eq!(
            settable,
            SettableAttributes {
                restrictions: Restrictions::ADMINISTRATOR_IMMUTABLE,
                hints: Hints::HIDDEN,
                ..append_only_settable()
            }
        );
    }
}

#[test]
fn an_attribute_change_without_flags_keeps_the_restrictions_and_hints() {
    // SAFETY: the request is a plain value object, and its setters take plain values.
    unsafe {
        let request = FSItemSetAttributesRequest::new();
        request.setMode(0o644);
        let change = attribute_change(&request).expect("a mode change is applied");
        assert!(!request.wasAttributeConsumed(FSItemAttribute::Flags));
        let hidden = SettableAttributes {
            hints: Hints::HIDDEN,
            ..append_only_settable()
        };
        let mut settable = hidden.clone();
        change(&mut settable);
        assert_eq!(
            settable,
            SettableAttributes {
                mode: Mode::from_bits_truncate(0o644),
                ..hidden
            }
        );
    }
}

#[test]
fn set_attributes_and_create_refuse_flags_the_volume_does_not_store() {
    let volume = mounted("ab03");
    let replied =
        |replies: Receiver<Replied>| replies.recv_timeout(Duration::from_secs(10)).unwrap();
    let not_supported =
        |replied: Replied| matches!(replied, Err((code, _)) if code == libc::ENOTSUP as isize);
    // SAFETY: the requests are plain value objects, and their setters take plain values.
    let (refused, stored, plain) = unsafe {
        let refused = FSItemSetAttributesRequest::new();
        refused.setFlags(libc::UF_NODUMP);
        let stored = FSItemSetAttributesRequest::new();
        stored.setFlags(libc::UF_HIDDEN);
        (refused, stored, FSItemSetAttributesRequest::new())
    };
    assert!(not_supported(replied(set_attributes(
        &volume,
        Inode::ROOT,
        &refused
    ))));
    assert!(not_supported(replied(create_file(
        &volume,
        Inode::ROOT,
        b"made",
        &refused
    ))));
    // the refused create made nothing, so the name is still free
    assert_eq!(
        replied(create_file(&volume, Inode::ROOT, b"made", &plain)),
        Ok(())
    );
    assert_eq!(
        replied(set_attributes(&volume, Inode::ROOT, &stored)),
        Ok(())
    );
}

#[test]
fn an_operation_that_commits_in_memory_replies_on_fskits_thread() {
    let volume = mounted("ab04");
    // setAttributes commits in memory, so it replies before the call returns, on the thread that called it.
    let (reply, replied) = replying_thread::<FSSetAttributesResult>();
    // SAFETY: the request, item, context and reply are all valid for the call.
    unsafe {
        strata_volume(&volume).setAttributes_onItem_context_replyHandler(
            &FSItemSetAttributesRequest::new(),
            &StrataItem::new(Inode::ROOT, FileType::Directory),
            &context(),
            &reply,
        );
    }
    assert_eq!(replied.try_recv(), Ok(thread::current().id()));
}

#[test]
fn synchronize_replies_at_once() {
    let volume = mounted("ab05");
    for flags in [FSSyncFlags::NoWait, FSSyncFlags::Wait] {
        let (replied, received) = channel();
        let reply =
            RcBlock::<fn(*mut NSError), dyn Send + Sync>::new(move |error: *mut NSError| {
                replied.send(error.is_null()).unwrap();
            });
        // SAFETY: the reply is valid for the call.
        unsafe { strata_volume(&volume).synchronizeWithFlags_replyHandler(flags, &reply) };
        assert_eq!(received.try_recv(), Ok(true), "{}", flags.0);
    }
}

// FSKit's tryReclaim refuses an item it did not hand out itself, so the deletion on reclaim is left to a mounted
// volume.
#[test]
fn a_removed_file_still_answers() {
    let volume = mounted("ab06");
    // SAFETY: the request is a plain value object.
    let plain = unsafe { FSItemSetAttributesRequest::new() };
    let created = create_file(&volume, Inode::ROOT, b"f", &plain);
    assert_eq!(
        created.recv_timeout(Duration::from_secs(10)).unwrap(),
        Ok(())
    );
    // The test engine numbers every new inode 7.
    let file = Inode(7);
    let (reply, removed) = reply::<FSRemoveItemResult>();
    // SAFETY: the items, name, context and reply are all valid for the call.
    unsafe {
        strata_volume(&volume).removeItem_named_fromDirectory_context_replyHandler(
            &StrataItem::new(file, FileType::File),
            &file_name(b"f"),
            &StrataItem::new(Inode::ROOT, FileType::Directory),
            &context(),
            &reply,
        );
    }
    assert_eq!(
        removed.recv_timeout(Duration::from_secs(10)).unwrap(),
        Ok(())
    );
    let attributes = get_attributes(&volume, file);
    assert_eq!(
        attributes.recv_timeout(Duration::from_secs(10)).unwrap(),
        Ok(())
    );
}
