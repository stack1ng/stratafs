use bytes::Bytes;
use stratafs_store::content::ContentStore;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::attributes::{Attributes, FileType, SettableAttributes};
use crate::error::FsError;
use crate::{FsTransaction, Inode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub name: Bytes,
    pub inode: Inode,
    pub kind: FileType,
    pub position: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NewItem {
    File,
    Directory,
    Symlink(Bytes),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RenameMode {
    Replace,
    NoReplace,
    Exchange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryChange {
    pub inode: Inode,
    pub item: Attributes,
    pub parent: Attributes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameChange {
    pub inode: Inode,
    pub item: Attributes,
    pub from: Attributes,
    pub to: Attributes,
    pub replaced: Option<(Inode, Attributes)>,
}

impl<T: ReadKeyValueTransaction, C: ContentStore, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn lookup(
        &self,
        dir: Inode,
        name: &[u8],
    ) -> Result<Option<(Inode, Attributes)>, FsError<T::Failure, C::Failure>> {
        todo!()
    }

    pub async fn readdir(
        &self,
        dir: Inode,
        after: u64,
        limit: usize,
    ) -> Result<Vec<DirectoryEntry>, FsError<T::Failure, C::Failure>> {
        todo!()
    }

    pub async fn readdir_plus(
        &self,
        dir: Inode,
        after: u64,
        limit: usize,
    ) -> Result<Vec<(DirectoryEntry, Attributes)>, FsError<T::Failure, C::Failure>> {
        todo!()
    }
}

impl<T: KeyValueTransaction, C: ContentStore, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn create(
        &mut self,
        dir: Inode,
        name: &[u8],
        item: NewItem,
        update: impl FnOnce(&mut SettableAttributes) + Send,
    ) -> Result<EntryChange, FsError<T::Failure, C::Failure>> {
        todo!()
    }

    pub async fn link(
        &mut self,
        inode: Inode,
        dir: Inode,
        name: &[u8],
    ) -> Result<EntryChange, FsError<T::Failure, C::Failure>> {
        todo!()
    }

    pub async fn unlink(
        &mut self,
        dir: Inode,
        name: &[u8],
        expected: Option<Inode>,
    ) -> Result<EntryChange, FsError<T::Failure, C::Failure>> {
        todo!()
    }

    pub async fn rmdir(
        &mut self,
        dir: Inode,
        name: &[u8],
        expected: Option<Inode>,
    ) -> Result<EntryChange, FsError<T::Failure, C::Failure>> {
        todo!()
    }

    pub async fn rename(
        &mut self,
        dir: Inode,
        name: &[u8],
        new_dir: Inode,
        new_name: &[u8],
        mode: RenameMode,
        expected: Option<Inode>,
    ) -> Result<RenameChange, FsError<T::Failure, C::Failure>> {
        todo!()
    }
}
