// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use std::ops::RangeBounds;

use bytes::{Buf, Bytes};
use futures_util::future::join;
use stratafs_store::content::ContentStore;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::annotations::error::{
    GetAnnotationError, ListAnnotationsError, RemoveAnnotationError, SetAnnotationError,
};
use crate::attributes::Attributes;
use crate::error::IoFailure;
use crate::key::{FsKey, annotation_name, annotations_in, annotations_of};
use crate::spillable::SpillableValue;
use crate::verification::{Record, RecordChange, RecordKind};
use crate::{FsConfig, FsTransaction, Inode, LARGEST_NAME_LENGTH, Timestamp};

// An annotation is a named value on an inode. A portable annotation means the same on every platform. A platform
// annotation belongs to one platform's adapter, which alone gives it meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationKey<'a> {
    Portable(&'a [u8]),
    Platform { platform: &'a [u8], name: &'a [u8] },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnotationScope<'a> {
    Portable,
    Platform(&'a [u8]),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetAnnotationMode {
    CreateOrReplace,
    Create,
    Replace,
}

impl<'a> AnnotationKey<'a> {
    fn name(self) -> &'a [u8] {
        match self {
            AnnotationKey::Portable(name) | AnnotationKey::Platform { name, .. } => name,
        }
    }
}

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: FsConfig>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub fn get_annotation(
        &self,
        inode: Inode,
        key: AnnotationKey<'_>,
    ) -> impl Future<Output = Result<Option<Bytes>, GetAnnotationError<T::Failure, C::Failure>>> + Send
    {
        async move {
            if key.name().is_empty() {
                return Err(GetAnnotationError::InvalidInput);
            }
            if key.name().len() > LARGEST_NAME_LENGTH {
                return Err(GetAnnotationError::FileNameTooLong);
            }
            let (attributes, content) = join(self.getattr(inode), async {
                let key = Bytes::from(FsKey::new_annotation(inode, key));
                let stored = self
                    .metadata
                    .get(key.clone())
                    .await
                    .map_err(IoFailure::Metadata)?;
                self.vouched_record(inode, &key, stored.as_ref()).await?;
                let Some(stored) = stored else {
                    return Ok(None);
                };
                let value = SpillableValue::from_stored(stored).map_err(IoFailure::Corrupt)?;
                let content = self
                    .read_spillable(value)
                    .await
                    .map_err(IoFailure::ContentRead)?;
                Ok::<_, IoFailure<T::Failure, C::Failure>>(Some(content))
            })
            .await;
            if attributes?.is_none() {
                return Err(GetAnnotationError::StaleFileHandle);
            }
            Ok(content?)
        }
    }

    pub fn list_annotations(
        &self,
        inode: Inode,
        scope: AnnotationScope<'_>,
    ) -> impl Future<Output = Result<Vec<Bytes>, ListAnnotationsError<T::Failure, C::Failure>>> + Send
    {
        async move {
            // Annotations are in no order in the inode's hash tree, so a signed volume reads all of them, which is
            // what the tree vouches for, and keeps those of the scope.
            let read = match self.verifier() {
                Some(_) => annotations_of(inode),
                None => annotations_in(inode, scope),
            };
            let (attributes, stored) = join(
                self.getattr(inode),
                self.metadata.get_range(read, usize::MAX),
            )
            .await;
            if attributes?.is_none() {
                return Err(ListAnnotationsError::StaleFileHandle);
            }
            let stored: Vec<Record> = stored
                .map_err(IoFailure::Metadata)?
                .map(Record::from)
                .collect();
            let (first, last) = (
                RecordKind::Annotation.at(u64::MIN),
                RecordKind::Annotation.at(u64::MAX),
            );
            self.vouched(inode, first, last, &stored).await?;
            let in_scope = annotations_in(inode, scope);
            let names = stored
                .iter()
                .filter(|annotation| in_scope.contains(&annotation.key))
                .map(|annotation| annotation_name(&annotation.key))
                .collect::<Result<Vec<_>, _>>()
                .map_err(IoFailure::Corrupt)?;
            Ok(names)
        }
    }
}

impl<T: KeyValueTransaction, C: ContentStore, A: Sync, N: Fn() -> Timestamp + Sync, S: FsConfig>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    // set_annotation returns the item's attributes after the change.
    pub fn set_annotation(
        &mut self,
        inode: Inode,
        key: AnnotationKey<'_>,
        value: impl Buf + Send,
        mode: SetAnnotationMode,
    ) -> impl Future<
        Output = Result<Attributes, SetAnnotationError<T::Failure, T::Rejection, C::Failure>>,
    > + Send {
        async move {
            if key.name().is_empty() {
                return Err(SetAnnotationError::InvalidInput);
            }
            if key.name().len() > LARGEST_NAME_LENGTH {
                return Err(SetAnnotationError::FileNameTooLong);
            }
            let key = Bytes::from(FsKey::new_annotation(inode, key));
            let (attributes, existing) =
                join(self.getattr(inode), self.metadata.get(key.clone())).await;
            let mut attributes = attributes?.ok_or(SetAnnotationError::StaleFileHandle)?;
            if !attributes.settable.restrictions.is_empty() {
                return Err(SetAnnotationError::OperationNotPermitted);
            }
            let existing = existing.map_err(IoFailure::Metadata)?;
            self.vouched_record(inode, &key, existing.as_ref()).await?;
            let exists = existing.is_some();
            match (mode, exists) {
                (SetAnnotationMode::Create, true) => return Err(SetAnnotationError::AlreadyExists),
                (SetAnnotationMode::Replace, false) => {
                    return Err(SetAnnotationError::AttributeNotFound);
                }
                (SetAnnotationMode::CreateOrReplace, _)
                | (SetAnnotationMode::Create, false)
                | (SetAnnotationMode::Replace, true) => {}
            }
            let value = self
                .make_spillable(&key, 0, value)
                .await
                .map_err(IoFailure::ContentWrite)?;
            let set = RecordChange {
                key,
                value: Some(value.to_stored()),
            };
            self.change_records(inode, vec![set]).await?;
            self.record_change(inode, &mut attributes, self.now());
            self.write_attributes(inode, &attributes)?;
            Ok(attributes)
        }
    }

    // remove_annotation returns the item's attributes after the change.
    pub fn remove_annotation(
        &mut self,
        inode: Inode,
        key: AnnotationKey<'_>,
    ) -> impl Future<
        Output = Result<Attributes, RemoveAnnotationError<T::Failure, T::Rejection, C::Failure>>,
    > + Send {
        async move {
            if key.name().is_empty() {
                return Err(RemoveAnnotationError::InvalidInput);
            }
            if key.name().len() > LARGEST_NAME_LENGTH {
                return Err(RemoveAnnotationError::FileNameTooLong);
            }
            let key = Bytes::from(FsKey::new_annotation(inode, key));
            let (attributes, existing) =
                join(self.getattr(inode), self.metadata.get(key.clone())).await;
            let mut attributes = attributes?.ok_or(RemoveAnnotationError::StaleFileHandle)?;
            if !attributes.settable.restrictions.is_empty() {
                return Err(RemoveAnnotationError::OperationNotPermitted);
            }
            let existing = existing.map_err(IoFailure::Metadata)?;
            self.vouched_record(inode, &key, existing.as_ref()).await?;
            if existing.is_none() {
                return Err(RemoveAnnotationError::AttributeNotFound);
            }
            let removed = RecordChange::<Bytes> { key, value: None };
            self.change_records(inode, vec![removed]).await?;
            self.record_change(inode, &mut attributes, self.now());
            self.write_attributes(inode, &attributes)?;
            Ok(attributes)
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
