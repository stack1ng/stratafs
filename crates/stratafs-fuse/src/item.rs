use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use fuser::{FileAttr, INodeNo};
use stratafs_fs_engine::attributes::{Attributes, FileType};
use stratafs_fs_engine::{Inode, Timestamp};

// BLOCK_SIZE is the block size stat reports, and the unit a file's allocated size is rounded up to.
pub(crate) const BLOCK_SIZE: u32 = 4096;

// node_id is the kernel's node id for an inode. FUSE's root is node 1 and the engine's is inode 0, so node ids are
// inodes moved up by 1. The engine keeps inodes at most u64::MAX - 2, so that always fits.
pub(crate) fn node_id(inode: Inode) -> INodeNo {
    INodeNo(inode.0 + 1)
}

// inode is the inode a kernel node id names. The kernel never names node 0, which would become an inode the engine
// never makes, and so a stale one.
pub(crate) fn inode(node: INodeNo) -> Inode {
    Inode(node.0.wrapping_sub(1))
}

// file_attr is an inode's attributes as the kernel takes them.
pub(crate) fn file_attr(inode: Inode, attributes: &Attributes) -> FileAttr {
    let settable = &attributes.settable;
    FileAttr {
        ino: node_id(inode),
        size: settable.size,
        // Blocks are reported from the size, rounded up, which never undercounts what a file holds.
        blocks: settable.size.next_multiple_of(u64::from(BLOCK_SIZE)) / 512,
        atime: system_time(settable.last_accessed_at),
        mtime: system_time(settable.content_changed_at),
        ctime: system_time(attributes.attribute_changed_at),
        crtime: system_time(settable.created_at),
        kind: file_type(attributes.kind),
        perm: settable.mode.bits(),
        nlink: attributes.hard_link_count,
        uid: settable.ownership.user_id,
        gid: settable.ownership.group_id,
        rdev: 0,
        blksize: BLOCK_SIZE,
        flags: 0,
    }
}

pub(crate) fn file_type(kind: FileType) -> fuser::FileType {
    match kind {
        FileType::File => fuser::FileType::RegularFile,
        FileType::Directory => fuser::FileType::Directory,
        FileType::SymbolicLink => fuser::FileType::Symlink,
    }
}

// system_time is a Timestamp, nanoseconds since the Unix epoch, as a SystemTime.
fn system_time(timestamp: Timestamp) -> SystemTime {
    let since_epoch = Duration::from_nanos(timestamp.0.unsigned_abs());
    match timestamp.0 < 0 {
        true => UNIX_EPOCH - since_epoch,
        false => UNIX_EPOCH + since_epoch,
    }
}

// timestamp is a SystemTime as a Timestamp, nanoseconds since the Unix epoch, saturating outside what it holds.
pub(crate) fn timestamp(time: SystemTime) -> Timestamp {
    let nanoseconds = |since: Duration| i64::try_from(since.as_nanos()).unwrap_or(i64::MAX);
    Timestamp(match time.duration_since(UNIX_EPOCH) {
        Ok(after) => nanoseconds(after),
        Err(before) => -nanoseconds(before.duration()),
    })
}

// name_bytes is a name the kernel hands over as the engine takes it.
pub(crate) fn name_bytes(name: &OsStr) -> Bytes {
    Bytes::copy_from_slice(name.as_bytes())
}

// file_name is an engine name as the kernel takes it.
pub(crate) fn file_name(name: &[u8]) -> &OsStr {
    OsStr::from_bytes(name)
}

#[cfg(test)]
mod tests;
