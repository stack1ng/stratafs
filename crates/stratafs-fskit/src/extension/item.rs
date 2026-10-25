use libc::timespec;
use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::{AllocAnyThread, Encode, Encoding, Ivars, define_class, msg_send};
use objc2_foundation::NSObject;
use objc2_fs_kit::{FSItem, FSItemAttributes, FSItemID, FSItemType};
use stratafs_fs_engine::attributes::{Attributes, FileType};
use stratafs_fs_engine::{Inode, Timestamp};

define_class!(
    // StrataItem is an item FSKit holds on to, naming the inode it stands for.
    #[unsafe(super(FSItem, NSObject))]
    #[name = "StrataItem"]
    pub(crate) struct StrataItem {
        inode: Inode,
    }

    unsafe impl NSObjectProtocol for StrataItem {}
);

impl StrataItem {
    pub(crate) fn new(inode: Inode) -> Retained<Self> {
        let this = Self::alloc().set_ivars(Ivars::<Self> { inode });
        // SAFETY: init is NSObject's initializer, which FSItem inherits.
        unsafe { msg_send![super(this), init] }
    }

    pub(crate) fn inode_of(item: &FSItem) -> Inode {
        *item
            .downcast_ref::<Self>()
            .expect("FSKit only hands back items this volume made")
            .inode()
    }
}

// item_id is FSKit's id for an inode. FSKit reserves 0, 1 and 2, with 2 being the root directory, so ids
// are inodes moved up by 2. The engine keeps inodes at most u64::MAX - 2 so that always fits.
pub(crate) fn item_id(inode: Inode) -> FSItemID {
    FSItemID(inode.0 + 2)
}

// item_attributes are an inode's attributes as FSKit takes them.
pub(crate) fn item_attributes(inode: Inode, attributes: &Attributes) -> Retained<FSItemAttributes> {
    let (item_type, type_bits) = match attributes.kind {
        FileType::File => (FSItemType::File, libc::S_IFREG),
        FileType::Directory => (FSItemType::Directory, libc::S_IFDIR),
        FileType::SymbolicLink => (FSItemType::Symlink, libc::S_IFLNK),
    };
    let parent = match inode {
        Inode::ROOT => FSItemID::ParentOfRoot,
        _ => item_id(attributes.parent),
    };
    let settable = &attributes.settable;
    // SAFETY: FSItemAttributes is a plain value object, and every setter takes a plain value.
    unsafe {
        let item = FSItemAttributes::new();
        item.setType(item_type);
        item.setMode(u32::from(settable.mode.bits() | type_bits));
        item.setLinkCount(attributes.hard_link_count);
        item.setUid(settable.ownership.user_id);
        item.setGid(settable.ownership.group_id);
        item.setSize(settable.size);
        // Blocks are reported from the size, rounded up, which never undercounts what a file holds.
        item.setAllocSize(settable.size.next_multiple_of(4096));
        item.setFileID(item_id(inode));
        item.setParentID(parent);
        item.setFlags(0);
        // The bindings have no setters for the times, so they are sent by hand.
        let _: () = msg_send![&*item, setAccessTime: time(settable.last_accessed_at)];
        let _: () = msg_send![&*item, setModifyTime: time(settable.content_changed_at)];
        let _: () = msg_send![&*item, setChangeTime: time(attributes.attribute_changed_at)];
        let _: () = msg_send![&*item, setBirthTime: time(settable.created_at)];
        item
    }
}

#[repr(transparent)]
#[derive(Clone, Copy)]
struct Time(timespec);

// SAFETY: timespec is two C longs on macOS, which is how Objective-C encodes it.
unsafe impl Encode for Time {
    const ENCODING: Encoding = Encoding::Struct("timespec", &[Encoding::C_LONG, Encoding::C_LONG]);
}

// time is a Timestamp, nanoseconds since the Unix epoch, as a timespec.
fn time(timestamp: Timestamp) -> Time {
    const NANOSECONDS: i64 = 1_000_000_000;
    Time(timespec {
        tv_sec: timestamp.0.div_euclid(NANOSECONDS),
        tv_nsec: timestamp.0.rem_euclid(NANOSECONDS),
    })
}
