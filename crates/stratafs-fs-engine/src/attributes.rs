// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use bitflags::bitflags;
use bytes::Bytes;
use stratafs_serialized::serialized_value;
use stratafs_store::content::ContentStore;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};
use zerocopy::little_endian::{I64, U16, U32, U64};
use zerocopy::{Immutable, KnownLayout, TryFromBytes};

use crate::attributes::error::SetattrError;
use crate::error::{CorruptValue, IoFailure, refused_or_failed};
use crate::key::FsKey;
use crate::{FsConfig, FsTransaction, Inode, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromBytes, KnownLayout, Immutable)]
#[repr(u8)]
pub enum FileType {
    File,
    Directory,
    SymbolicLink,
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
pub struct SettableAttributes {
    pub mode: Mode,
    pub ownership: Ownership,

    pub size: u64,

    pub last_accessed_at: Timestamp,
    pub content_changed_at: Timestamp,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attributes {
    pub settable: SettableAttributes,

    pub kind: FileType,
    pub hard_link_count: u32,
    pub attribute_changed_at: Timestamp,

    pub parent: Inode,
    pub change_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ownership {
    pub user_id: u32,
    pub group_id: u32,
}

impl Attributes {
    pub(crate) fn record_change(&mut self, now: Timestamp) {
        self.attribute_changed_at = now;
        self.change_version += 1;
    }
}

serialized_value! {
    pub(crate) struct StoredAttributes {
        kind: u8,
        mode: U16,
        hard_link_count: U32,
        user_id: U32,
        group_id: U32,
        size: U64,
        last_accessed_at: I64,
        content_changed_at: I64,
        attribute_changed_at: I64,
        created_at: I64,
        parent: U64,
        change_version: U64,
    }
}

impl From<&Attributes> for StoredAttributes {
    fn from(attributes: &Attributes) -> Self {
        let settable = &attributes.settable;
        StoredAttributes {
            kind: attributes.kind as u8,
            mode: U16::new(settable.mode.bits()),
            hard_link_count: U32::new(attributes.hard_link_count),
            user_id: U32::new(settable.ownership.user_id),
            group_id: U32::new(settable.ownership.group_id),
            size: U64::new(settable.size),
            last_accessed_at: I64::new(settable.last_accessed_at.0),
            content_changed_at: I64::new(settable.content_changed_at.0),
            attribute_changed_at: I64::new(attributes.attribute_changed_at.0),
            created_at: I64::new(settable.created_at.0),
            parent: U64::new(attributes.parent.0),
            change_version: U64::new(attributes.change_version),
        }
    }
}

impl TryFrom<&StoredAttributes> for Attributes {
    type Error = CorruptValue;

    fn try_from(stored: &StoredAttributes) -> Result<Self, CorruptValue> {
        Ok(Attributes {
            settable: SettableAttributes {
                mode: Mode::from_bits(stored.mode.get()).ok_or(CorruptValue::UnknownModeBits)?,
                ownership: Ownership {
                    user_id: stored.user_id.get(),
                    group_id: stored.group_id.get(),
                },
                size: stored.size.get(),
                last_accessed_at: Timestamp(stored.last_accessed_at.get()),
                content_changed_at: Timestamp(stored.content_changed_at.get()),
                created_at: Timestamp(stored.created_at.get()),
            },
            kind: FileType::try_read_from_bytes(&[stored.kind])
                .map_err(|_| CorruptValue::UnknownFileType)?,
            hard_link_count: stored.hard_link_count.get(),
            attribute_changed_at: Timestamp(stored.attribute_changed_at.get()),
            parent: Inode(stored.parent.get()),
            change_version: stored.change_version.get(),
        })
    }
}

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: Sync>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub fn getattr(
        &self,
        inode: Inode,
    ) -> impl Future<Output = Result<Option<Attributes>, IoFailure<T::Failure, C::Failure>>> + Send
    {
        async move {
            let Some(stored) = self
                .metadata
                .get(FsKey::new_attributes(inode).into())
                .await
                .map_err(IoFailure::Metadata)?
            else {
                return Ok(None);
            };
            let stored = StoredAttributes::view(&stored)
                .map_err(|corrupt| IoFailure::Corrupt(corrupt.into()))?;
            Attributes::try_from(stored)
                .map(Some)
                .map_err(IoFailure::Corrupt)
        }
    }
}

impl<T: KeyValueTransaction, C: ContentStore, A: Sync, N: Fn() -> Timestamp + Sync, S: FsConfig>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub(crate) fn write_attributes(
        &mut self,
        inode: Inode,
        attributes: &Attributes,
    ) -> Result<(), StoreError<T::Rejection, T::Failure>> {
        let stored = Bytes::from(&StoredAttributes::from(attributes));
        self.metadata
            .set(FsKey::new_attributes(inode).into(), stored)
            .map_err(refused_or_failed)
    }

    pub(crate) fn create_root_if_missing(
        &mut self,
    ) -> impl Future<Output = Result<(), StoreError<T::Rejection, T::Failure>>> + Send {
        async move {
            if self
                .metadata
                .get(FsKey::new_attributes(Inode::ROOT).into())
                .await?
                .is_some()
            {
                return Ok(());
            }
            let now = self.now();
            let root = Attributes {
                settable: SettableAttributes {
                    mode: Mode::OWNER_READ
                        | Mode::OWNER_WRITE
                        | Mode::OWNER_EXECUTE
                        | Mode::GROUP_READ
                        | Mode::GROUP_EXECUTE
                        | Mode::OTHERS_READ
                        | Mode::OTHERS_EXECUTE,
                    ownership: Ownership {
                        user_id: 0,
                        group_id: 0,
                    },
                    size: 0,
                    last_accessed_at: now,
                    content_changed_at: now,
                    created_at: now,
                },
                kind: FileType::Directory,
                hard_link_count: 2,
                attribute_changed_at: now,
                parent: Inode::ROOT,
                change_version: 0,
            };
            self.write_attributes(Inode::ROOT, &root)
        }
    }

    pub fn setattr(
        &mut self,
        inode: Inode,
        update: impl FnOnce(&mut SettableAttributes) + Send,
    ) -> impl Future<Output = Result<Attributes, SetattrError<T::Failure, T::Rejection, C::Failure>>>
    + Send {
        async move {
            let mut attributes = self
                .getattr(inode)
                .await?
                .ok_or(SetattrError::StaleFileHandle)?;
            let (old_size, old_content_changed_at) = (
                attributes.settable.size,
                attributes.settable.content_changed_at,
            );
            update(&mut attributes.settable);
            let now = self.now();
            if attributes.settable.size < old_size {
                self.truncate_content(inode, attributes.settable.size, old_size)
                    .await?;
            }
            // a resize changes the content, unless the update set the content change time itself
            if attributes.settable.size != old_size
                && attributes.settable.content_changed_at == old_content_changed_at
            {
                attributes.settable.content_changed_at = now;
            }
            attributes.record_change(now);
            self.write_attributes(inode, &attributes)?;
            Ok(attributes)
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
