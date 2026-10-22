// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use bytes::{Buf, Bytes};
use futures_util::future::join;
use stratafs_store::content::ContentStore;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::attributes::{Attributes, FileType};
use crate::content::error::ReadSymbolicLinkError;
use crate::error::{CorruptValue, IoFailure};
use crate::key::FsKey;
use crate::spillable::{SpillableContent, SpillableValue};
use crate::{FsTransaction, Inode};

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: Sync>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub async fn read(
        &self,
        inode: Inode,
        offset: u64,
        size: u64,
    ) -> Result<impl Buf + Send, IoFailure<T::Failure, C::Failure>> {
        Ok::<Bytes, _>(todo!())
    }

    pub fn read_symbolic_link(
        &self,
        inode: Inode,
    ) -> impl Future<
        Output = Result<
            SpillableContent<C::Content>,
            ReadSymbolicLinkError<T::Failure, C::Failure>,
        >,
    > + Send {
        async move {
            let (attributes, target) = join(self.getattr(inode), async {
                let Some(stored) = self
                    .metadata
                    .get(FsKey::new_symbolic_link_target(inode).into())
                    .await
                    .map_err(IoFailure::Metadata)?
                else {
                    return Ok(None);
                };
                let value = SpillableValue::from_stored(stored).map_err(IoFailure::Corrupt)?;
                let target = self
                    .read_spillable(value)
                    .await
                    .map_err(IoFailure::ContentRead)?;
                Ok::<_, IoFailure<T::Failure, C::Failure>>(Some(target))
            })
            .await;
            let attributes = attributes?.ok_or(ReadSymbolicLinkError::StaleFileHandle)?;
            match attributes.kind {
                FileType::SymbolicLink => {}
                FileType::File | FileType::Directory => {
                    return Err(ReadSymbolicLinkError::InvalidInput);
                }
            }
            Ok(target?.ok_or(IoFailure::Corrupt(CorruptValue::MissingSymbolicLinkTarget))?)
        }
    }
}

impl<T: KeyValueTransaction, C: ContentStore, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    pub async fn write(
        &mut self,
        inode: Inode,
        offset: u64,
        data: impl Buf + Send,
    ) -> Result<Attributes, IoFailure<T::Failure, C::Failure>> {
        todo!()
    }
}

pub mod error;
