// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use bytes::{Buf, Bytes};
use futures_util::future::join;
use stratafs_store::content::ContentStore;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::annotations::error::{
    GetAnnotationError, ListAnnotationsError, RemoveAnnotationError, SetAnnotationError,
};
use crate::attributes::Attributes;
use crate::error::{IoFailure, refused_or_failed};
use crate::key::{FsKey, annotation_name, annotations_in};
use crate::spillable::{SpillableContent, SpillableValue};
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

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: Sync>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub fn get_annotation(
        &self,
        inode: Inode,
        key: AnnotationKey<'_>,
    ) -> impl Future<
        Output = Result<
            Option<SpillableContent<C::Content>>,
            GetAnnotationError<T::Failure, C::Failure>,
        >,
    > + Send {
        async move {
            if key.name().is_empty() {
                return Err(GetAnnotationError::InvalidInput);
            }
            if key.name().len() > LARGEST_NAME_LENGTH {
                return Err(GetAnnotationError::FileNameTooLong);
            }
            let (attributes, content) = join(self.getattr(inode), async {
                let Some(stored) = self
                    .metadata
                    .get(FsKey::new_annotation(inode, key).into())
                    .await
                    .map_err(IoFailure::Metadata)?
                else {
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
            let (attributes, stored) = join(
                self.getattr(inode),
                self.metadata
                    .get_range(annotations_in(inode, scope), usize::MAX),
            )
            .await;
            if attributes?.is_none() {
                return Err(ListAnnotationsError::StaleFileHandle);
            }
            let names = stored
                .map_err(IoFailure::Metadata)?
                .map(|(key, _)| annotation_name(&key))
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
            let exists = existing.map_err(IoFailure::Metadata)?.is_some();
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
            self.metadata
                .set(key, value.to_stored())
                .map_err(refused_or_failed)?;
            attributes.record_change(self.now());
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
            if existing.map_err(IoFailure::Metadata)?.is_none() {
                return Err(RemoveAnnotationError::AttributeNotFound);
            }
            self.metadata.clear(key).map_err(IoFailure::Metadata)?;
            attributes.record_change(self.now());
            self.write_attributes(inode, &attributes)?;
            Ok(attributes)
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
