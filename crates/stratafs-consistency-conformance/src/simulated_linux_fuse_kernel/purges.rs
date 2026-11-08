// stratafs-fuse's PurgeWaits, copied from its purges.rs, which a change there must be copied to: whether the
// kernel's purge of an inode waits for each operation in flight on it.

use stratafs_fs_engine::PurgeWaits;

// READ and WRITE wait: the purge drops the file's pages, which each keeps locked until the reply. A read may also be a
// read ahead, which the kernel sends in the background.
pub(crate) const READ: PurgeWaits = PurgeWaits::OfAnyInode;
pub(crate) const WRITE: PurgeWaits = PurgeWaits::OfOwnInode;

// NAME_CHANGES are create, link, unlink and rename, which wait: each holds the directory's lock until the reply.
pub(crate) const NAME_CHANGES: PurgeWaits = PurgeWaits::OfOwnInode;

// ITEM_CHANGES are setattr, which waits: it holds the item's lock until the reply, which a request holding the lock of
// a directory the item is in can wait for.
pub(crate) const ITEM_CHANGES: PurgeWaits = PurgeWaits::OfOwnInode;

// NAME_READS are lookup and readdirplus, which hold the directory's lock too, but a purge takes it only for names
// handed over before it began, and the purge of an item they hand over takes no lock. Unknown lets such a reply go:
// one older than the purge goes uncached, which the kernel asks again about.
pub(crate) const NAME_READS: PurgeWaits = PurgeWaits::Unknown;

// GETATTR waits for what the kernel keeps: it drops the reply to a getattr asked before the purge.
pub(crate) const GETATTR: PurgeWaits = PurgeWaits::OfOwnInode;
