// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use std::ops::{Bound, Range};

use bytes::{Buf, Bytes};
use futures_util::FutureExt;
use futures_util::future::join;
use stratafs_store::content::{ContentHash, ContentStore};
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::attributes::error::SetattrError;
use crate::attributes::{Attributes, FileType};
use crate::content::error::{ReadError, ReadSymbolicLinkError, WriteError};
use crate::error::{CorruptValue, IoFailure, refused_or_failed};
use crate::key::{FsKey, content_extent_end};
use crate::spillable::{SpillableContent, SpillableValue};
use crate::{FsConfig, FsTransaction, Inode, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExtentValue {
    start: u64,
    content_offset: u64,
    content: SpillableValue,
}

impl ExtentValue {
    const STORED_PREFIX_LENGTH: usize = size_of::<u64>() + size_of::<u64>();

    fn from_stored(stored: Bytes) -> Result<Self, CorruptValue> {
        let (start, rest) = stored
            .split_first_chunk()
            .ok_or(CorruptValue::WrongLength)?;
        let (offset, content) = rest.split_first_chunk().ok_or(CorruptValue::WrongLength)?;
        Ok(ExtentValue {
            start: u64::from_le_bytes(*start),
            content_offset: u64::from_le_bytes(*offset),
            content: SpillableValue::from_stored(stored.slice_ref(content))?,
        })
    }

    fn to_stored(&self) -> impl Buf + Send {
        Bytes::from([self.start.to_le_bytes(), self.content_offset.to_le_bytes()].concat())
            .chain(self.content.to_stored())
    }

    fn slice(&self, range: Range<u64>) -> Result<ExtentValue, CorruptValue> {
        let content_offset = self
            .content_offset
            .checked_add(range.start - self.start)
            .ok_or(CorruptValue::InvalidExtent)?;
        match &self.content {
            SpillableValue::Inline(content) => {
                let visible = content_offset
                    .checked_add(range.end - range.start)
                    .and_then(|content_end| {
                        Some((
                            usize::try_from(content_offset).ok()?,
                            usize::try_from(content_end).ok()?,
                        ))
                    })
                    .and_then(|(start, end)| content.get(start..end))
                    .ok_or(CorruptValue::InvalidExtent)?;
                Ok(ExtentValue {
                    start: range.start,
                    content_offset: 0,
                    content: SpillableValue::Inline(content.slice_ref(visible)),
                })
            }
            SpillableValue::Overflow { .. } => Ok(ExtentValue {
                start: range.start,
                content_offset,
                content: self.content.clone(),
            }),
        }
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

impl<T, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S>
where
    T: ReadKeyValueTransaction,
    C: ContentStore,
    A: Sync,
    N: Sync,
    S: Sync,
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
                        Bound::Included(Bytes::from(FsKey::new_extent(inode, u64::MAX))),
                    ),
                    1,
                )
                .map(|found| found.map(|mut extents| extents.next()));
            let (directly_in_bounds, indirectly_in_bounds) =
                join(directly_in_bounds_future, indirectly_in_bounds_future).await;
            let directly_in_bounds = directly_in_bounds.map_err(IoFailure::Metadata)?;
            let indirectly_in_bounds = indirectly_in_bounds.map_err(IoFailure::Metadata)?;
            let mut extents = Vec::with_capacity(
                directly_in_bounds.size_hint().0 + usize::from(indirectly_in_bounds.is_some()),
            );
            for (key, value) in directly_in_bounds.chain(indirectly_in_bounds) {
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
                            .content_offset
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

impl<T, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S>
where
    T: KeyValueTransaction,
    C: ContentStore,
    A: Sync,
    N: Fn() -> Timestamp + Sync,
    S: FsConfig,
{
    // truncate_content removes a file's content from size up to old_size, keeping the part of the
    // extent that crosses size. No extent ends past a file's size, so nothing lies after old_size.
    pub(crate) fn truncate_content(
        &mut self,
        inode: Inode,
        size: u64,
        old_size: u64,
    ) -> impl Future<Output = Result<(), SetattrError<T::Failure, T::Rejection, C::Failure>>> + Send
    {
        async move {
            let removed = (
                Bound::Excluded(Bytes::from(FsKey::new_extent(inode, size))),
                Bound::Included(Bytes::from(FsKey::new_extent(inode, old_size))),
            );
            let crossing = self
                .metadata
                .get_range(removed.clone(), 1)
                .await
                .map_err(IoFailure::Metadata)?
                .next();
            self.metadata
                .clear_range(removed)
                .map_err(IoFailure::Metadata)?;
            if let Some((_, crossing)) = crossing {
                let crossing = ExtentValue::from_stored(crossing).map_err(IoFailure::Corrupt)?;
                if crossing.start < size {
                    self.metadata
                        .set(
                            Bytes::from(FsKey::new_extent(inode, size)),
                            crossing
                                .slice(crossing.start..size)
                                .map_err(IoFailure::Corrupt)?
                                .to_stored(),
                        )
                        .map_err(refused_or_failed)?;
                }
            }
            Ok(())
        }
    }

    pub fn write(
        &mut self,
        inode: Inode,
        offset: u64,
        data: impl Buf + Send,
    ) -> impl Future<Output = Result<Attributes, WriteError<T::Failure, T::Rejection, C::Failure>>> + Send
    {
        async move {
            let write_start = offset;
            let write_length = data.remaining();
            let write_end = offset
                .checked_add(write_length as u64)
                .ok_or(WriteError::FileTooLarge)?;
            let (attributes, overlap) = join(
                self.getattr(inode),
                self.overlapping_extents(inode, offset, write_end),
            )
            .await;
            let mut attributes = attributes?.ok_or(WriteError::StaleFileHandle)?;
            match attributes.kind {
                FileType::File => {}
                FileType::Directory => return Err(WriteError::IsADirectory),
                FileType::SymbolicLink => return Err(WriteError::InvalidInput),
            }
            if write_length == 0 {
                return Ok(attributes);
            }
            // clear out any overlaps before we write
            for (end, extent) in overlap? {
                let kept_before = (extent.start < write_start).then_some(extent.start..write_start);
                let kept_after = (write_end < end).then_some(write_end..end);

                // handle preserving any part of the extent that extends out to the left of what we are writing
                if let Some(kept_before) = kept_before {
                    self.metadata
                        .set(
                            Bytes::from(FsKey::new_extent(inode, kept_before.end)),
                            extent
                                .slice(kept_before)
                                .map_err(IoFailure::Corrupt)?
                                .to_stored(),
                        )
                        .map_err(refused_or_failed)?
                }

                // handle preserving any part of the extent that extends out to the right of what we are writing
                match kept_after {
                    Some(kept_after) => self
                        .metadata
                        .set(
                            Bytes::from(FsKey::new_extent(inode, end)),
                            extent
                                .slice(kept_after)
                                .map_err(IoFailure::Corrupt)?
                                .to_stored(),
                        )
                        .map_err(refused_or_failed)?,
                    None => self
                        .metadata
                        .clear(Bytes::from(FsKey::new_extent(inode, end)))
                        .map_err(IoFailure::Metadata)?,
                }
            }

            // write the range
            let key = Bytes::from(FsKey::new_extent(inode, write_end));
            let content = self
                .make_spillable(&key, ExtentValue::STORED_PREFIX_LENGTH, data)
                .await
                .map_err(IoFailure::ContentWrite)?;
            self.metadata
                .set(
                    key,
                    ExtentValue {
                        start: write_start,
                        content_offset: 0,
                        content,
                    }
                    .to_stored(),
                )
                .map_err(refused_or_failed)?;

            let now = self.now();
            attributes.settable.size = attributes.settable.size.max(write_end);
            attributes.settable.content_changed_at = now;
            attributes.record_change(now);
            self.write_attributes(inode, &attributes)?;
            Ok(attributes)
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
