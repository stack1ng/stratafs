// Whether the kernel's purge of an inode, a FUSE invalidation, waits for each operation in flight on it. A purge drops
// the inode's attributes and pages, waiting for pages a request keeps locked, and takes the lock of the directory it
// drops names in (see volume.rs). A system call holding a directory's lock can wait for an item's lock.

use stratafs_fs_engine::PurgeWaits;

// READ waits: the purge drops the file's pages, which a read keeps locked until the reply. A read may be a read ahead,
// which the kernel sends in the background: it keeps only max_background of those in flight, and a purge may need one.
pub(crate) const READ: PurgeWaits = PurgeWaits::OfAnyInode;

// WRITE waits: without the writeback cache, a write keeps its pages locked until the reply too.
pub(crate) const WRITE: PurgeWaits = PurgeWaits::OfOwnInode;

// WRITEBACK is a write from the page cache, of the pages a shared writable mapping dirtied, which waits as WRITE does.
// A purge also writes out such pages, as a write of this class, and waits for it. The kernel sends it in the
// background, as a read ahead.
pub(crate) const WRITEBACK: PurgeWaits = PurgeWaits::OfAnyInode;

// NAME_CHANGES are create, mkdir, mknod, symlink, link, unlink, rmdir and rename, which wait: each holds the
// directory's lock until the reply.
pub(crate) const NAME_CHANGES: PurgeWaits = PurgeWaits::OfOwnInode;

// ITEM_CHANGES are setattr, setxattr and removexattr, which wait: each holds the item's lock until the reply, which a
// system call holding the lock of a directory the item is in, such as an unlink of it, can wait for.
pub(crate) const ITEM_CHANGES: PurgeWaits = PurgeWaits::OfOwnInode;

// NAME_READS are lookup and readdirplus, which hold the directory's lock too, but a purge takes it only for names
// handed over before it began, and the purge of an item they hand over takes no lock, so it may go ahead of a reply.
// Unknown lets such a reply go: one older than the purge goes uncached, which the kernel asks again about.
pub(crate) const NAME_READS: PurgeWaits = PurgeWaits::Unknown;

// GETATTR waits for what the kernel keeps: it drops the reply to a getattr asked before the purge, as if the purge had
// waited for it. The read that asked still copies up to the size it had. A getattr of a directory asked to check
// permissions can hold the directory's lock, and then its purge waits for it.
pub(crate) const GETATTR: PurgeWaits = PurgeWaits::OfOwnInode;

// No purge waits for READLINK: a readlink holds no lock and no pages.
pub(crate) const READLINK: PurgeWaits = PurgeWaits::No;

// NO_REQUEST is an engine operation no kernel request waits on, such as deleting an item with no name.
pub(crate) const NO_REQUEST: PurgeWaits = PurgeWaits::No;
