// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use std::ops::{Bound, Range};

use bytes::{Buf, Bytes};
use futures_util::future::{try_join, try_join3};
use futures_util::{FutureExt, TryFutureExt};
use stratafs_store::content::{ContentId, ContentReference, ContentStore};
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::attributes::error::SetattrError;
use crate::attributes::{Attributes, FileType};
use crate::content::error::{ReadError, ReadSymbolicLinkError, WriteError};
use crate::error::{ContentReadError, CorruptValue, IoFailure};
use crate::key::{FsKey, content_extent_end};
use crate::spillable::{ContentHash, SpillableValue};
use crate::verification::{Record, RecordChange, RecordKind};
use crate::{CheckedContent, FsConfig, FsTransaction, Inode, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExtentValue {
    start: u64,
    content_offset: u64,
    content: SpillableValue,
}

impl ExtentValue {
    pub(crate) const STORED_PREFIX_LENGTH: usize = size_of::<u64>() + size_of::<u64>();

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

    fn to_stored(&self) -> impl Buf + Send + use<> {
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
            // the piece is one more reference to the extent's blob
            SpillableValue::Overflow {
                reference,
                length,
                hash,
            } => Ok(ExtentValue {
                start: range.start,
                content_offset,
                content: SpillableValue::Overflow {
                    reference: ContentReference::Existing(reference.id()),
                    length: *length,
                    hash: *hash,
                },
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
        id: ContentId,
        blob_length: u64,
        hash: ContentHash,
        offset: u64,
        length: u64,
    },
}

impl ContentLoadPlan {
    // load returns one future per piece of the read, in file order, each resolving to that piece's bytes.
    // Holes resolve to zeros straight away. The caller decides how many of them to run at once.
    pub fn load<C: ContentStore, S: FsConfig>(
        self,
        content: CheckedContent<'_, C, S>,
    ) -> impl Iterator<Item = impl Future<Output = Result<Bytes, ContentReadError<C::Failure>>> + Send>
    {
        let read_end = self.offset + self.size;
        let mut covered_until = self.offset;
        self.pieces
            .into_iter()
            .map(Some)
            .chain([None])
            .flat_map(move |piece| {
                let range = piece
                    .as_ref()
                    .map_or(read_end..read_end, ContentPiece::range);
                let hole = (covered_until < range.start).then(|| ContentPiece::Inline {
                    start: covered_until,
                    content: Bytes::from(vec![0; (range.start - covered_until) as usize]),
                });
                covered_until = range.end;
                hole.into_iter().chain(piece)
            })
            .map(move |piece| async move {
                match piece {
                    ContentPiece::Inline { content, .. } => Ok(content),
                    ContentPiece::Stored {
                        id,
                        blob_length,
                        hash,
                        offset,
                        length,
                        ..
                    } => {
                        let blob = content.get(id, blob_length, hash).await?;
                        // read checked that the piece lies within the blob's length.
                        Ok(blob.slice(offset as usize..(offset + length) as usize))
                    }
                }
            })
    }
}

impl ContentPiece {
    fn range(&self) -> Range<u64> {
        match self {
            ContentPiece::Inline { start, content } => *start..*start + content.len() as u64,
            ContentPiece::Stored { start, length, .. } => *start..*start + length,
        }
    }
}

impl<T, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S>
where
    T: ReadKeyValueTransaction,
    C: ContentStore,
    A: Sync,
    N: Sync,
    S: FsConfig,
{
    // stored_extents reads the extents of inode that end after start and at or before end, and the first that ends
    // after end. It reads ahead the nodes of the inode's hash tree for them.
    fn stored_extents(
        &self,
        inode: Inode,
        start: u64,
        end: u64,
    ) -> impl Future<Output = Result<Vec<Record>, IoFailure<T::Failure, C::Failure>>> + Send {
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
            // A write changes the extent that ends at start too, so its node is read ahead with the others.
            let (directly_in_bounds, indirectly_in_bounds, ()) = try_join3(
                directly_in_bounds_future.map_err(IoFailure::Metadata),
                indirectly_in_bounds_future.map_err(IoFailure::Metadata),
                self.prefetch(
                    inode,
                    RecordKind::Content.at(start),
                    RecordKind::Content.at(end),
                ),
            )
            .await?;
            Ok(directly_in_bounds
                .chain(indirectly_in_bounds)
                .map(Record::from)
                .collect())
        }
    }

    // overlapping_extents returns all extents that overlap at all with the range: [start, end), from stored, which
    // is what stored_extents read for the range of a file of size.
    fn overlapping_extents(
        &self,
        inode: Inode,
        start: u64,
        end: u64,
        size: u64,
        mut stored: Vec<Record>,
    ) -> impl Future<Output = Result<Vec<(u64, ExtentValue)>, IoFailure<T::Failure, C::Failure>>> + Send
    {
        async move {
            let ends = stored
                .iter()
                .map(|extent| content_extent_end(&extent.key))
                .collect::<Result<Vec<u64>, _>>()
                .map_err(IoFailure::Corrupt)?;
            let after = ends.last().is_some_and(|last| *last > end);
            let within = &ends[..ends.len() - usize::from(after)];
            let last = match (within.last(), ends.last()) {
                // Extents never overlap, so none crosses the end of one that ends where the range does.
                (Some(last), _) if *last == end => {
                    stored.truncate(within.len());
                    end
                }
                // The first extent that ends after the range is the only one that can cross its end.
                (_, Some(last)) if after => *last,
                // No extent ends after the range, and none ends past the file's size.
                _ => end.max(size),
            };
            if let Some(first) = start.checked_add(1).filter(|first| *first <= last) {
                let (first, last) = (RecordKind::Content.at(first), RecordKind::Content.at(last));
                self.vouched(inode, first, last, &stored).await?;
            }
            let mut extents = Vec::with_capacity(stored.len());
            for Record { key, value } in stored {
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
            let end = offset.saturating_add(size);
            let (attributes, stored) =
                try_join(self.getattr(inode), self.stored_extents(inode, offset, end)).await?;
            let attributes = attributes.ok_or(ReadError::StaleFileHandle)?;
            match attributes.kind {
                FileType::File => {}
                FileType::Directory => return Err(ReadError::IsADirectory),
                FileType::SymbolicLink => return Err(ReadError::InvalidInput),
            }
            let extents = self
                .overlapping_extents(inode, offset, end, attributes.settable.size, stored)
                .await;
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
                            SpillableValue::Overflow {
                                reference,
                                length,
                                hash,
                            } => {
                                if view_end_in_content > length {
                                    return Err(IoFailure::Corrupt(CorruptValue::InvalidExtent));
                                }
                                ContentPiece::Stored {
                                    start: view_start,
                                    id: reference.id(),
                                    blob_length: length,
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
    ) -> impl Future<Output = Result<Bytes, ReadSymbolicLinkError<T::Failure, C::Failure>>> + Send
    {
        async move {
            let (attributes, target) = try_join(self.getattr(inode), async {
                let key = Bytes::from(FsKey::new_symbolic_link_target(inode));
                let stored = self.metadata.get(key.clone()).map_err(IoFailure::Metadata);
                let (stored, ()) = try_join(stored, self.prefetch_record(inode, &key)).await?;
                let Some(stored) = stored else {
                    return Ok(None);
                };
                self.vouched_record(inode, &key, Some(&stored)).await?;
                let value = SpillableValue::from_stored(stored).map_err(IoFailure::Corrupt)?;
                let target = self
                    .read_spillable(value)
                    .await
                    .map_err(IoFailure::ContentRead)?;
                Ok::<_, IoFailure<T::Failure, C::Failure>>(Some(target))
            })
            .await?;
            let attributes = attributes.ok_or(ReadSymbolicLinkError::StaleFileHandle)?;
            match attributes.kind {
                FileType::SymbolicLink => {}
                FileType::File | FileType::Directory => {
                    return Err(ReadSymbolicLinkError::InvalidInput);
                }
            }
            Ok(target.ok_or(IoFailure::Corrupt(CorruptValue::MissingSymbolicLinkTarget))?)
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
            let range = (
                Bound::Excluded(Bytes::from(FsKey::new_extent(inode, size))),
                Bound::Included(Bytes::from(FsKey::new_extent(inode, old_size))),
            );
            let removed = self.metadata.get_range(range, usize::MAX);
            let (removed, ()) = try_join(
                removed.map_err(IoFailure::Metadata),
                self.prefetch(
                    inode,
                    RecordKind::Content.at(size),
                    RecordKind::Content.at(old_size),
                ),
            )
            .await?;
            let removed: Vec<Record> = removed.map(Record::from).collect();
            if let Some(first) = size.checked_add(1).filter(|first| *first <= old_size) {
                let (first, last) = (
                    RecordKind::Content.at(first),
                    RecordKind::Content.at(old_size),
                );
                self.vouched(inode, first, last, &removed).await?;
            }
            let mut changes = Vec::with_capacity(removed.len() + 1);
            for extent in &removed {
                changes.push(RecordChange {
                    key: extent.key.clone(),
                    value: None,
                });
            }
            if let Some(crossing) = removed.into_iter().next() {
                let crossing =
                    ExtentValue::from_stored(crossing.value).map_err(IoFailure::Corrupt)?;
                if crossing.start < size {
                    let kept = crossing
                        .slice(crossing.start..size)
                        .map_err(IoFailure::Corrupt)?;
                    changes.push(RecordChange {
                        key: Bytes::from(FsKey::new_extent(inode, size)),
                        value: Some(kept.to_stored()),
                    });
                }
            }
            self.change_records(inode, changes).await?;
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
            let (attributes, stored) = try_join(
                self.getattr(inode),
                self.stored_extents(inode, offset, write_end),
            )
            .await?;
            let mut attributes = attributes.ok_or(WriteError::StaleFileHandle)?;
            match attributes.kind {
                FileType::File => {}
                FileType::Directory => return Err(WriteError::IsADirectory),
                FileType::SymbolicLink => return Err(WriteError::InvalidInput),
            }
            let restrictions = attributes.settable.restrictions;
            if restrictions.immutable()
                || restrictions.append_only() && write_start != attributes.settable.size
            {
                return Err(WriteError::OperationNotPermitted);
            }
            if write_length == 0 {
                return Ok(attributes);
            }
            let overlap = self
                .overlapping_extents(inode, offset, write_end, attributes.settable.size, stored)
                .await;
            // clear out any overlaps before we write
            let mut changes = Vec::new();
            for (end, extent) in overlap? {
                let kept_before = (extent.start < write_start).then_some(extent.start..write_start);
                let kept_after = (write_end < end).then_some(write_end..end);

                // handle preserving any part of the extent that extends out to the left of what we are writing
                if let Some(kept_before) = kept_before {
                    let key = Bytes::from(FsKey::new_extent(inode, kept_before.end));
                    let kept = extent.slice(kept_before).map_err(IoFailure::Corrupt)?;
                    changes.push(RecordChange {
                        key,
                        value: Some(kept.to_stored()),
                    });
                }

                // handle preserving any part of the extent that extends out to the right of what we are writing
                let value = match kept_after {
                    Some(kept_after) => {
                        let kept = extent.slice(kept_after).map_err(IoFailure::Corrupt)?;
                        Some(kept.to_stored())
                    }
                    None => None,
                };
                changes.push(RecordChange {
                    key: Bytes::from(FsKey::new_extent(inode, end)),
                    value,
                });
            }

            // write the range
            let key = Bytes::from(FsKey::new_extent(inode, write_end));
            let content = self
                .make_spillable(&key, ExtentValue::STORED_PREFIX_LENGTH, data)
                .await
                .map_err(IoFailure::ContentWrite)?;
            let written = ExtentValue {
                start: write_start,
                content_offset: 0,
                content,
            };
            changes.push(RecordChange {
                key,
                value: Some(written.to_stored()),
            });
            self.change_records(inode, changes).await?;

            let now = self.now();
            attributes.settable.size = attributes.settable.size.max(write_end);
            attributes.settable.content_changed_at = now;
            self.record_change(inode, &mut attributes, now);
            self.write_attributes(inode, &attributes)?;
            Ok(attributes)
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
