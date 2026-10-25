use std::io;
use std::sync::mpsc::{Receiver, channel};
use std::sync::{Arc, Mutex, Once};
use std::time::Duration;

use block2::RcBlock;
use futures_util::future::FutureObj;
use futures_util::task::SpawnError;
use objc2::{AllocAnyThread, msg_send};
use objc2_foundation::{NSError, NSString, NSURL};
use objc2_fs_kit::{
    FSContext, FSDeactivateOptions, FSGenericURLResource, FSGetAttributesResult,
    FSItemGetAttributesRequest, FSItemID, FSItemType, FSTaskOptions, FSUnaryFileSystemOperations,
    FSVolume, FSVolumeHandler,
};
use opendal::Operator;
use redb::backends::InMemoryBackend;
use stratafs_fs_engine::attributes::{Attributes, FileType, Mode, Ownership, SettableAttributes};
use stratafs_fs_engine::{FsConfig, FsEngine, Inode, Timestamp};
use stratafs_opendal::OpendalContentStore;
use stratafs_redb::{Inline, RedbStore};

use super::item::{StrataItem, item_attributes};
use super::*;

struct TestConfig;

impl FsConfig for TestConfig {
    const LARGEST_INLINE_ENTRY: usize = 1024;
}

type TestEngine =
    FsEngine<RedbStore<Inline>, OpendalContentStore, fn() -> u64, fn() -> Timestamp, TestConfig>;

async fn new_engine() -> Result<TestEngine, io::Error> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .map_err(io::Error::other)?;
    let allocate_inode: fn() -> u64 = || 7;
    let now: fn() -> Timestamp = || Timestamp(1_500_000_000);
    FsEngine::new(
        RedbStore::new(database, Inline).map_err(io::Error::other)?,
        OpendalContentStore::new(
            Operator::new(opendal::services::Memory::default()).map_err(io::Error::other)?,
        ),
        allocate_inode,
        now,
    )
    .await
    .map_err(io::Error::other)
}

// ThreadSpawner runs each operation on a thread of its own, which is enough for tests.
struct ThreadSpawner;

impl Spawn for ThreadSpawner {
    fn spawn_obj(&self, operation: FutureObj<'static, ()>) -> Result<(), SpawnError> {
        std::thread::spawn(move || block_on(operation));
        Ok(())
    }
}

// install sets up what run would, once per test process. A volume id of ff fails to open.
fn install() {
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        assert!(
            OPEN_VOLUME
                .set(opener(ThreadSpawner, async |volume: Bytes| {
                    match &volume[..] {
                        [0xff] => Err(io::Error::other("the store is down")),
                        _ => new_engine().await,
                    }
                }))
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

// load calls loadResource, and returns the volume it replied with, or its error's code and description.
fn load(file_system: &StrataFileSystem, url: &str) -> Result<Retained<FSVolume>, (isize, String)> {
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
        let url = NSURL::URLWithString(&NSString::from_str(url)).expect("the test URL is valid");
        let resource = FSGenericURLResource::initWithURL(FSGenericURLResource::alloc(), &url);
        // FSTaskOptions has no public initializer, so the test makes one with NSObject's.
        let options: Retained<FSTaskOptions> = msg_send![FSTaskOptions::alloc(), init];
        file_system.loadResource_options_replyHandler(&resource, &options, &reply);
    }
    let result = replied
        .lock()
        .unwrap()
        .take()
        .expect("loadResource replies before it returns");
    // SAFETY: the reply retained the volume and handed that retain over as its address.
    result.map(|volume| unsafe { Retained::from_raw(volume as *mut FSVolume) }.unwrap())
}

// get_attributes calls getAttributes on an inode, and returns a receiver that gets whether a result
// came back, or the error's code and description.
fn get_attributes(volume: &FSVolume, inode: Inode) -> Receiver<Result<(), (isize, String)>> {
    let (replied, reply_received) = channel();
    let reply = RcBlock::<fn(*mut FSGetAttributesResult, *mut NSError), dyn Send + Sync>::new(
        move |result: *mut FSGetAttributesResult, error: *mut NSError| {
            let result = if result.is_null() {
                Err(error_of(error))
            } else {
                Ok(())
            };
            replied.send(result).unwrap();
        },
    );
    let volume = volume
        .downcast_ref::<StrataVolume>()
        .expect("loadResource replies with our volume");
    // SAFETY: the request, item, context and reply are all valid for the call. FSContext has no public
    // initializer, so the test makes one with NSObject's.
    unsafe {
        let context: Retained<FSContext> = msg_send![FSContext::alloc(), init];
        volume.getAttributes_ofItem_context_replyHandler(
            &FSItemGetAttributesRequest::new(),
            &StrataItem::new(inode),
            &context,
            &reply,
        );
    }
    reply_received
}

#[test]
fn load_resource_opens_the_volume_its_url_names_or_replies_why_not() {
    let file_system = new_file_system();
    let volume = load(&file_system, "stratavolume://ab01").expect("the volume loads");
    // SAFETY: a volume always has a name.
    let name = unsafe { volume.name().string() }.map(|name| name.to_string());
    assert_eq!(name.as_deref(), Some("ab01"));
    assert!(matches!(
        load(&file_system, "https://ab01"),
        Err((code, description))
            if code == libc::EINVAL as isize && description.contains("does not name a volume")
    ));
    assert!(matches!(
        load(&file_system, "stratavolume://ff"),
        Err((code, description))
            if code == libc::EIO as isize && description.contains("the store is down")
    ));
}

#[test]
fn get_attributes_replies_until_the_volume_is_deactivated() {
    let file_system = new_file_system();
    let volume = load(&file_system, "stratavolume://ab02").expect("the volume loads");
    let replied = |inode| {
        get_attributes(&volume, inode)
            .recv_timeout(Duration::from_secs(10))
            .unwrap()
    };
    assert_eq!(replied(Inode::ROOT), Ok(()));
    assert!(matches!(replied(Inode(5000)), Err((code, _)) if code == libc::ESTALE as isize));

    let deactivated = RcBlock::<fn(*mut NSError), dyn Send + Sync>::new(|_error: *mut NSError| {});
    let strata_volume = volume
        .downcast_ref::<StrataVolume>()
        .expect("loadResource replies with our volume");
    // SAFETY: the options and reply are valid for the call.
    unsafe {
        strata_volume.deactivateVolumeWithOptions_replyHandler(FSDeactivateOptions(0), &deactivated)
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
            size: 10,
            last_accessed_at: Timestamp(1_500_000_000),
            content_changed_at: Timestamp(-1),
            created_at: Timestamp(0),
        },
        kind: FileType::Directory,
        hard_link_count: 2,
        attribute_changed_at: Timestamp(0),
        parent: Inode::ROOT,
        change_version: 0,
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
    }
}
