use bitflags::bitflags;
use bytes::Bytes;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::error::FsError;
use crate::{FsTransaction, Inode, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Mode: u16 {
        const SET_USER_ID = 0o4000;
        const SET_GROUP_ID = 0o2000;
        const STICKY = 0o1000;
        const OWNER_READ = 0o400;
        const OWNER_WRITE = 0o200;
        const OWNER_EXECUTE = 0o100;
        const GROUP_READ = 0o040;
        const GROUP_WRITE = 0o020;
        const GROUP_EXECUTE = 0o010;
        const OTHERS_READ = 0o004;
        const OTHERS_WRITE = 0o002;
        const OTHERS_EXECUTE = 0o001;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attributes {
    pub kind: FileType,
    pub mode: Mode,
    pub link_count: u32,
    pub owner: Ownership,

    pub size: u64,
    pub allocated_size: u64,

    pub last_accessed_at: Timestamp,
    pub content_changed_at: Timestamp,
    pub attribute_changed_at: Timestamp,
    pub created_at: Timestamp,

    pub parent: Inode,
    pub change_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ownership {
    pub user_id: u32,
    pub group_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetXattrMode {
    CreateOrReplace,
    Create,
    Replace,
}

impl<T: ReadKeyValueTransaction, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn getattr(&self, inode: Inode) -> Result<Attributes, FsError<T::Failure>> {
        todo!()
    }

    pub async fn getxattr(
        &self,
        inode: Inode,
        name: &[u8],
    ) -> Result<Option<Bytes>, FsError<T::Failure>> {
        todo!()
    }

    pub async fn listxattr(&self, inode: Inode) -> Result<Vec<Bytes>, FsError<T::Failure>> {
        todo!()
    }
}

impl<T: KeyValueTransaction, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn setattr(
        &mut self,
        inode: Inode,
        update: impl FnOnce(&mut Attributes) + Send,
    ) -> Result<Attributes, FsError<T::Failure>> {
        todo!()
    }

    pub async fn setxattr(
        &mut self,
        inode: Inode,
        name: &[u8],
        value: Bytes,
        mode: SetXattrMode,
    ) -> Result<(), FsError<T::Failure>> {
        todo!()
    }

    pub async fn removexattr(
        &mut self,
        inode: Inode,
        name: &[u8],
    ) -> Result<(), FsError<T::Failure>> {
        todo!()
    }
}
