use bytes::Bytes;
use libc::timespec;
use objc2::rc::Retained;
use objc2::runtime::NSObjectProtocol;
use objc2::{AllocAnyThread, Encode, Encoding, Ivars, define_class, msg_send};
use objc2_foundation::{NSData, NSObject};
use objc2_fs_kit::{FSFileName, FSItem, FSItemAttributes, FSItemID, FSItemType};
use stratafs_fs_engine::attributes::{Attributes, FileType, Hints, Restrictions};
use stratafs_fs_engine::{Inode, Timestamp};

define_class!(
    // StrataItem is an item FSKit holds on to, naming the inode it stands for and that inode's type, which
    // never changes.
    #[unsafe(super(FSItem, NSObject))]
    #[name = "StrataItem"]
    pub(crate) struct StrataItem {
        pub(crate) inode: Inode,
        pub(crate) kind: FileType,
    }

    unsafe impl NSObjectProtocol for StrataItem {}
);

// SAFETY: a StrataItem's only state is its inode and type, which never change after it is made, and FSKit
// itself hands items between threads.
unsafe impl Send for StrataItem {}
unsafe impl Sync for StrataItem {}

impl StrataItem {
    pub(crate) fn new(inode: Inode, kind: FileType) -> Retained<Self> {
        let this = Self::alloc().set_ivars(Ivars::<Self> { inode, kind });
        // SAFETY: init is NSObject's initializer, which FSItem inherits.
        unsafe { msg_send![super(this), init] }
    }

    // of is FSKit's item as the StrataItem this volume made it as.
    pub(crate) fn of(item: &FSItem) -> &Self {
        item.downcast_ref::<Self>()
            .expect("FSKit only hands back items this volume made")
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
        item.setFlags(flags_of(settable.restrictions, settable.hints));
        // The bindings have no setters for the times, so they are sent by hand.
        let _: () = msg_send![&*item, setAccessTime: time(settable.last_accessed_at)];
        let _: () = msg_send![&*item, setModifyTime: time(settable.content_changed_at)];
        let _: () = msg_send![&*item, setChangeTime: time(attributes.attribute_changed_at)];
        let _: () = msg_send![&*item, setBirthTime: time(settable.created_at)];
        item
    }
}

// RESTRICTION_FLAGS and HINT_FLAGS are the st_flags bits the volume stores, each with the restriction or
// hint it stands for. The volume refuses every other bit.
const RESTRICTION_FLAGS: [(u32, Restrictions); 4] = [
    (libc::UF_IMMUTABLE, Restrictions::OWNER_IMMUTABLE),
    (libc::SF_IMMUTABLE, Restrictions::ADMINISTRATOR_IMMUTABLE),
    (libc::UF_APPEND, Restrictions::OWNER_APPEND_ONLY),
    (libc::SF_APPEND, Restrictions::ADMINISTRATOR_APPEND_ONLY),
];
const HINT_FLAGS: [(u32, Hints); 1] = [(libc::UF_HIDDEN, Hints::HIDDEN)];

// flags_of are restrictions and hints as st_flags bits.
pub(crate) fn flags_of(restrictions: Restrictions, hints: Hints) -> u32 {
    let restriction_flags = RESTRICTION_FLAGS
        .into_iter()
        .filter(|&(_, restriction)| restrictions.contains(restriction))
        .map(|(flag, _)| flag);
    let hint_flags = HINT_FLAGS
        .into_iter()
        .filter(|&(_, hint)| hints.contains(hint))
        .map(|(flag, _)| flag);
    restriction_flags
        .chain(hint_flags)
        .fold(0, |flags, flag| flags | flag)
}

// restrictions_and_hints are what st_flags bits stand for, or None if any bit is one the volume refuses.
pub(crate) fn restrictions_and_hints(flags: u32) -> Option<(Restrictions, Hints)> {
    let restrictions = RESTRICTION_FLAGS
        .into_iter()
        .filter(|&(flag, _)| flags & flag != 0)
        .fold(Restrictions::empty(), |restrictions, (_, restriction)| {
            restrictions | restriction
        });
    let hints = HINT_FLAGS
        .into_iter()
        .filter(|&(flag, _)| flags & flag != 0)
        .fold(Hints::empty(), |hints, (_, hint)| hints | hint);
    // a refused bit has no restriction or hint to come back as
    (flags_of(restrictions, hints) == flags).then_some((restrictions, hints))
}

#[repr(transparent)]
#[derive(Clone, Copy)]
pub(crate) struct Time(timespec);

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

// timestamp is a timespec as a Timestamp, nanoseconds since the Unix epoch.
pub(crate) fn timestamp(time: Time) -> Timestamp {
    Timestamp(
        time.0
            .tv_sec
            .saturating_mul(1_000_000_000)
            .saturating_add(time.0.tv_nsec),
    )
}

// name_bytes is an FSKit file name as the engine takes it.
pub(crate) fn name_bytes(name: &FSFileName) -> Bytes {
    // SAFETY: data reads the name's bytes.
    Bytes::from(unsafe { name.data() }.to_vec())
}

// file_name is an engine name as FSKit takes it.
pub(crate) fn file_name(name: &[u8]) -> Retained<FSFileName> {
    // SAFETY: nameWithData copies the bytes.
    unsafe { FSFileName::nameWithData(&NSData::with_bytes(name)) }
}
