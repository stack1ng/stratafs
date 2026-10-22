use bytes::{Buf, Bytes};
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::attributes::Attributes;
use crate::error::FsError;
use crate::{FsTransaction, Inode};

impl<T: ReadKeyValueTransaction, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn read(
        &self,
        inode: Inode,
        offset: u64,
        size: u64,
    ) -> Result<impl Buf + Send, FsError<T::Failure>> {
        Ok::<Bytes, _>(todo!())
    }

    pub async fn readlink(&self, inode: Inode) -> Result<Bytes, FsError<T::Failure>> {
        todo!()
    }
}

impl<T: KeyValueTransaction, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn write(
        &mut self,
        inode: Inode,
        offset: u64,
        data: impl Buf + Send,
    ) -> Result<Attributes, FsError<T::Failure>> {
        todo!()
    }
}
