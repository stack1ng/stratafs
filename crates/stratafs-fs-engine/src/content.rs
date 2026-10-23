// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use std::ops::Bound;

use bytes::{Buf, Bytes};
use futures_util::FutureExt;
use futures_util::future::join;
use stratafs_store::content::{ContentHash, ContentStore};
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::attributes::{Attributes, FileType};
use crate::content::error::{ReadError, ReadSymbolicLinkError};
use crate::error::{CorruptValue, IoFailure};
use crate::key::{FsKey, content_extent_end};
use crate::spillable::{SpillableContent, SpillableValue};
use crate::{FsTransaction, Inode};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExtentValue {
    start: u64,
    offset: u64,
    content: SpillableValue,
}

impl ExtentValue {
    fn from_stored(stored: Bytes) -> Result<Self, CorruptValue> {
        let (start, rest) = stored
            .split_first_chunk()
            .ok_or(CorruptValue::WrongLength)?;
        let (offset, content) = rest.split_first_chunk().ok_or(CorruptValue::WrongLength)?;
        Ok(ExtentValue {
            start: u64::from_le_bytes(*start),
            offset: u64::from_le_bytes(*offset),
            content: SpillableValue::from_stored(stored.slice_ref(content))?,
        })
    }

    fn to_stored(&self) -> impl Buf + Send {
        Bytes::from([self.start.to_le_bytes(), self.offset.to_le_bytes()].concat())
            .chain(self.content.to_stored())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentLoadPlan {
    offset: u64,
    size: u64,
    pieces: Vec<ContentPiece>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ContentPiece {
    Inline {
        start: u64,
        content: Bytes,
    },
    Stored {
        start: u64,
        hash: ContentHash,
        offset: u64,
        length: u64,
    },
}

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: Sync>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    // overlapping_extents returns all extents that overlap at all with the range: [start, end)
    fn overlapping_extents(
        &self,
        inode: Inode,
        start: u64,
        end: u64,
    ) -> impl Future<Output = Result<Vec<(u64, ExtentValue)>, IoFailure<T::Failure, C::Failure>>> + Send
    {
        async move {
            let directly_in_bounds_future = self.metadata.get_range(
                (
                    Bound::Excluded(Bytes::from(FsKey::new_extent(inode, start))),
                    Bound::Included(Bytes::from(FsKey::new_extent(inode, end))),
                ),
                usize::try_from(end.saturating_sub(start)).unwrap_or(usize::MAX),
            );
            let indirectly_in_bounds_future = self
                .metadata
                .get_range(
                    (
                        Bound::Excluded(Bytes::from(FsKey::new_extent(inode, end))),
                        Bound::Unbounded,
                    ),
                    1,
                )
                .map(|found| found.map(|extents| extents.into_iter().next()));
            let (directly_in_bounds, indirectly_in_bounds) =
                join(directly_in_bounds_future, indirectly_in_bounds_future).await;
            let directly_in_bounds = directly_in_bounds.map_err(IoFailure::Metadata)?;
            let indirectly_in_bounds = indirectly_in_bounds.map_err(IoFailure::Metadata)?;
            let mut extents = Vec::with_capacity(
                directly_in_bounds.len() + usize::from(indirectly_in_bounds.is_some()),
            );
            for (key, value) in directly_in_bounds.into_iter().chain(indirectly_in_bounds) {
                let extent_end = content_extent_end(&key).map_err(IoFailure::Corrupt)?;
                let extent = ExtentValue::from_stored(value).map_err(IoFailure::Corrupt)?;
                if extent.start >= extent_end {
                    return Err(IoFailure::Corrupt(CorruptValue::InvalidExtent));
                }
                if extent.start < end {
                    extents.push((extent_end, extent));
                }
            }
            Ok(extents)
        }
    }

    pub fn read(
        &self,
        inode: Inode,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<ContentLoadPlan, ReadError<T::Failure, C::Failure>>> + Send
    {
        async move {
            let (attributes, extents) = join(
                self.getattr(inode),
                self.overlapping_extents(inode, offset, offset.saturating_add(size)),
            )
            .await;
            let attributes = attributes?.ok_or(ReadError::StaleFileHandle)?;
            match attributes.kind {
                FileType::File => {}
                FileType::Directory => return Err(ReadError::IsADirectory),
                FileType::SymbolicLink => return Err(ReadError::InvalidInput),
            }
            let size = size.min(attributes.settable.size.saturating_sub(offset));
            let read_end = offset + size;
            let pieces = extents?
                .into_iter()
                .map(
                    |(end, extent)| -> Result<Option<ContentPiece>, IoFailure<T::Failure, C::Failure>> {
                        let view_start = extent.start.max(offset);
                        let view_end = end.min(read_end);
                        if view_start >= view_end {
                            return Ok(None);
                        }
                        let view_offset = extent
                            .offset
                            .checked_add(view_start - extent.start)
                            .ok_or(IoFailure::Corrupt(CorruptValue::InvalidExtent))?;
                        let view_size = view_end - view_start;
                        let view_end_in_content = view_offset
                            .checked_add(view_size)
                            .ok_or(IoFailure::Corrupt(CorruptValue::InvalidExtent))?;
                        Ok(Some(match extent.content {
                            SpillableValue::Inline(content) => {
                                let visible = usize::try_from(view_offset)
                                    .ok()
                                    .zip(usize::try_from(view_end_in_content).ok())
                                    .and_then(|(start, end)| content.get(start..end))
                                    .ok_or(IoFailure::Corrupt(CorruptValue::InvalidExtent))?;
                                ContentPiece::Inline {
                                    start: view_start,
                                    content: content.slice_ref(visible),
                                }
                            }
                            SpillableValue::Overflow { hash, length } => {
                                if view_end_in_content > length {
                                    return Err(IoFailure::Corrupt(CorruptValue::InvalidExtent));
                                }
                                ContentPiece::Stored {
                                    start: view_start,
                                    hash,
                                    offset: view_offset,
                                    length: view_size,
                                }
                            }
                        }))
                    },
                )
                .filter_map(Result::transpose)
                .collect::<Result<Vec<ContentPiece>, IoFailure<T::Failure, C::Failure>>>()?;
            Ok(ContentLoadPlan {
                offset,
                size,
                pieces,
            })
        }
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

#[cfg(test)]
mod tests;
