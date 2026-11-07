use std::ffi::OsStr;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use fuser::{
    BsdFileFlags, Errno, FileHandle, Filesystem, INodeNo, InitFlags, IoctlFlags, KernelConfig,
    LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyOpen, ReplyStatfs, ReplyWrite,
    ReplyXattr, Request, TimeOrNow, WriteFlags,
};
use stratafs_fs_engine::annotations::SetAnnotationMode;
use stratafs_fs_engine::directories::{NewItem, RenameMode};
use stratafs_fs_engine::{Cached, FsConfig, LARGEST_NAME_LENGTH, Timestamp};
use stratafs_store::store::Store;

use super::changes::{Requested, made};
use super::item::{BLOCK_SIZE, inode, name_bytes};
use super::operations::{Made, Removal, Volume, opened};

// REPORTED_BYTES is the size, free space and file count a volume reports. The engine has no capacity of its own, so
// these only need to be large enough that nothing refuses to write.
const REPORTED_BYTES: u64 = 1 << 50;

// SYMBOLIC_LINK_MODE is every symbolic link's mode: the kernel never checks a link's own permissions.
const SYMBOLIC_LINK_MODE: u32 = 0o777;

// StrataFileSystem is the file system FUSE serves. It turns each request into the volume's own terms and forwards it
// to the volume's operations.
pub(crate) struct StrataFileSystem<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    volume: Arc<Volume<St, A, N, S, C>>,
}

impl<St, A, N, S, C> StrataFileSystem<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    pub(crate) fn new(volume: Arc<Volume<St, A, N, S, C>>) -> Self {
        StrataFileSystem { volume }
    }

    fn volume(&self) -> Arc<Volume<St, A, N, S, C>> {
        Arc::clone(&self.volume)
    }
}

impl<St, A, N, S, C> Filesystem for StrataFileSystem<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    // init asks the kernel to list directories with attributes when that saves it lookups, to drop cached pages only
    // when the volume says so, to look names up in one directory at once, and to share a mapping of a file opened to
    // append: lookups and listings are read transactions, and Items locks each inode a reply hands over. A kernel
    // without one goes without it.
    fn init(&mut self, _request: &Request, config: &mut KernelConfig) -> io::Result<()> {
        // No consistency asks for the writeback cache, though eventual allows it: the kernel would keep a failed
        // write's data, and hide other mounts' changes to a file's size and times.
        let wanted = InitFlags::FUSE_DO_READDIRPLUS
            | InitFlags::FUSE_READDIRPLUS_AUTO
            | InitFlags::FUSE_PARALLEL_DIROPS
            | InitFlags::FUSE_EXPLICIT_INVAL_DATA
            | InitFlags::FUSE_DIRECT_IO_ALLOW_MMAP;
        config
            .add_capabilities(wanted & config.capabilities())
            .expect("the kernel has every capability it says it has");
        Ok(())
    }

    // destroy is the session's end, once the volume is unmounted. Dropping the volume after its last operation then
    // makes every commit durable.
    fn destroy(&mut self) {
        self.volume.delete_unlinked();
    }

    fn lookup(&self, _request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        self.volume().lookup(inode(parent), name_bytes(name), reply);
    }

    fn forget(&self, _request: &Request, node: INodeNo, lookups: u64) {
        self.volume().forget(inode(node), lookups);
    }

    fn getattr(
        &self,
        _request: &Request,
        node: INodeNo,
        _file: Option<FileHandle>,
        reply: ReplyAttr,
    ) {
        self.volume().getattr(inode(node), reply);
    }

    // setattr applies what the request asks of the attributes the engine keeps. The kernel sends a change time, and
    // macOS's times and flags, which the engine sets itself or Linux does not have.
    fn setattr(
        &self,
        _request: &Request,
        node: INodeNo,
        mode: Option<u32>,
        user_id: Option<u32>,
        group_id: Option<u32>,
        size: Option<u64>,
        accessed: Option<TimeOrNow>,
        modified: Option<TimeOrNow>,
        _changed: Option<SystemTime>,
        _file: Option<FileHandle>,
        _created: Option<SystemTime>,
        _change_time: Option<SystemTime>,
        _backed_up: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let requested = Requested {
            mode,
            user_id,
            group_id,
            size,
            accessed,
            modified,
        };
        self.volume().setattr(inode(node), requested, reply);
    }

    fn readlink(&self, _request: &Request, node: INodeNo, reply: ReplyData) {
        self.volume().readlink(inode(node), reply);
    }

    // mknod makes regular files only, the one kind of node the engine keeps that mknod makes.
    fn mknod(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _device: u32,
        reply: ReplyEntry,
    ) {
        if mode & libc::S_IFMT != libc::S_IFREG {
            return reply.error(Errno::EOPNOTSUPP);
        }
        self.volume().make(
            inode(parent),
            name_bytes(name),
            NewItem::File,
            made(mode, request.uid(), request.gid()),
            Made::Entry(reply),
        );
    }

    fn mkdir(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        self.volume().make(
            inode(parent),
            name_bytes(name),
            NewItem::Directory,
            made(mode, request.uid(), request.gid()),
            Made::Entry(reply),
        );
    }

    fn unlink(&self, _request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.volume()
            .remove(inode(parent), name_bytes(name), Removal::Unlink, reply);
    }

    fn rmdir(&self, _request: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.volume()
            .remove(inode(parent), name_bytes(name), Removal::Rmdir, reply);
    }

    fn symlink(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let target = Bytes::copy_from_slice(target.as_os_str().as_bytes());
        self.volume().make(
            inode(parent),
            name_bytes(name),
            NewItem::SymbolicLink(target),
            made(SYMBOLIC_LINK_MODE, request.uid(), request.gid()),
            Made::Entry(reply),
        );
    }

    fn rename(
        &self,
        _request: &Request,
        parent: INodeNo,
        name: &OsStr,
        new_parent: INodeNo,
        new_name: &OsStr,
        flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let mode = if flags.is_empty() {
            RenameMode::Replace
        } else if flags == RenameFlags::RENAME_NOREPLACE {
            RenameMode::NoReplace
        } else if flags == RenameFlags::RENAME_EXCHANGE {
            RenameMode::Exchange
        } else {
            return reply.error(Errno::EINVAL);
        };
        self.volume().rename(
            (inode(parent), name_bytes(name)),
            (inode(new_parent), name_bytes(new_name)),
            mode,
            reply,
        );
    }

    fn link(
        &self,
        _request: &Request,
        node: INodeNo,
        new_parent: INodeNo,
        new_name: &OsStr,
        reply: ReplyEntry,
    ) {
        self.volume()
            .link(inode(node), inode(new_parent), name_bytes(new_name), reply);
    }

    fn open(&self, _request: &Request, _node: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        reply.opened(FileHandle(0), opened(flags.0));
    }

    fn read(
        &self,
        _request: &Request,
        node: INodeNo,
        _file: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        self.volume().read(inode(node), offset, size, reply);
    }

    // write appends when the file was opened to append. A page a shared mapping writes back comes marked as from the
    // page cache, and goes where it belongs.
    fn write(
        &self,
        _request: &Request,
        node: INodeNo,
        _file: FileHandle,
        offset: u64,
        data: &[u8],
        write_flags: WriteFlags,
        flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let appends =
            flags.0 & libc::O_APPEND != 0 && !write_flags.contains(WriteFlags::FUSE_WRITE_CACHE);
        self.volume().write(
            inode(node),
            offset,
            Bytes::copy_from_slice(data),
            appends,
            reply,
        );
    }

    // flush is a close, which asks nothing of the volume: every write it was handed has already committed.
    fn flush(
        &self,
        _request: &Request,
        _node: INodeNo,
        _file: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    // fsync and fdatasync both make every committed operation durable, which covers the file's data and metadata alike.
    fn fsync(
        &self,
        _request: &Request,
        _node: INodeNo,
        _file: FileHandle,
        _data_only: bool,
        reply: ReplyEmpty,
    ) {
        self.volume().make_durable(reply);
    }

    fn readdir(
        &self,
        _request: &Request,
        node: INodeNo,
        _file: FileHandle,
        offset: u64,
        reply: ReplyDirectory,
    ) {
        self.volume().readdir(inode(node), offset, reply);
    }

    fn readdirplus(
        &self,
        _request: &Request,
        node: INodeNo,
        _file: FileHandle,
        offset: u64,
        reply: ReplyDirectoryPlus,
    ) {
        self.volume().readdirplus(inode(node), offset, reply);
    }

    fn fsyncdir(
        &self,
        _request: &Request,
        _node: INodeNo,
        _file: FileHandle,
        _data_only: bool,
        reply: ReplyEmpty,
    ) {
        self.volume().make_durable(reply);
    }

    fn statfs(&self, _request: &Request, _node: INodeNo, reply: ReplyStatfs) {
        let blocks = REPORTED_BYTES / u64::from(BLOCK_SIZE);
        let name_length =
            u32::try_from(LARGEST_NAME_LENGTH).expect("the longest name fits in a u32");
        reply.statfs(
            blocks,
            blocks,
            blocks,
            REPORTED_BYTES,
            REPORTED_BYTES,
            BLOCK_SIZE,
            name_length,
            BLOCK_SIZE,
        );
    }

    // setxattr creates, replaces, or either, as the flags ask. position is macOS's alone.
    fn setxattr(
        &self,
        _request: &Request,
        node: INodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        let mode = match (
            flags & libc::XATTR_CREATE != 0,
            flags & libc::XATTR_REPLACE != 0,
        ) {
            (false, false) => SetAnnotationMode::CreateOrReplace,
            (true, false) => SetAnnotationMode::Create,
            (false, true) => SetAnnotationMode::Replace,
            (true, true) => return reply.error(Errno::EINVAL),
        };
        self.volume().setxattr(
            inode(node),
            name_bytes(name),
            Bytes::copy_from_slice(value),
            mode,
            reply,
        );
    }

    fn getxattr(
        &self,
        _request: &Request,
        node: INodeNo,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        self.volume()
            .getxattr(inode(node), name_bytes(name), size, reply);
    }

    fn listxattr(&self, _request: &Request, node: INodeNo, size: u32, reply: ReplyXattr) {
        self.volume().listxattr(inode(node), size, reply);
    }

    fn removexattr(&self, _request: &Request, node: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        self.volume()
            .removexattr(inode(node), name_bytes(name), reply);
    }

    // create makes a file and opens it. The kernel has already applied the caller's umask to mode.
    fn create(
        &self,
        request: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        self.volume().make(
            inode(parent),
            name_bytes(name),
            NewItem::File,
            made(mode, request.uid(), request.gid()),
            Made::Opened { reply, flags },
        );
    }

    // ioctl knows no commands, as no item is a device.
    fn ioctl(
        &self,
        _request: &Request,
        _node: INodeNo,
        _file: FileHandle,
        _flags: IoctlFlags,
        _command: u32,
        _input: &[u8],
        _output_size: u32,
        reply: ReplyIoctl,
    ) {
        reply.error(Errno::ENOTTY);
    }
}
