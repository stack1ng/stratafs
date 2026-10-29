// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use bitflags::bitflags;
use bytes::Bytes;
use stratafs_serialized::{LittleEndian, ViewError, serialized_shape, serialized_value};
use stratafs_store::content::ContentStore;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};
use zerocopy::{Immutable, KnownLayout, TryFromBytes};

use crate::attributes::error::SetattrError;
use crate::error::{IoFailure, refused_or_failed};
use crate::key::FsKey;
use crate::{FsConfig, FsTransaction, Inode, MountId, Timestamp};

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

bitflags! {
    // Restrictions forbid changing an item. The engine enforces them in terms of how a file can be changed,
    // but it is still up to the caller to properly grant permission to actually modify them.
    // An immutable item cannot change at all (outside of its restrictions/hints fields). An append only item behaves the same, except that a file may
    // grow at its end and a directory may gain entries.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Restrictions: u8 {
        const OWNER_IMMUTABLE = 1;
        const ADMINISTRATOR_IMMUTABLE = 1 << 1;
        const OWNER_APPEND_ONLY = 1 << 2;
        const ADMINISTRATOR_APPEND_ONLY = 1 << 3;
    }
}

impl Restrictions {
    pub fn immutable(self) -> bool {
        self.intersects(Restrictions::OWNER_IMMUTABLE | Restrictions::ADMINISTRATOR_IMMUTABLE)
    }

    pub fn append_only(self) -> bool {
        self.intersects(Restrictions::OWNER_APPEND_ONLY | Restrictions::ADMINISTRATOR_APPEND_ONLY)
    }
}

bitflags! {
    // Hints tell the platforms how to present an item. The engine only stores them.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Hints: u8 {
        const HIDDEN = 1;
    }
}

// A file type is stored as its variant index, and an index no variant has is not a file type.
impl LittleEndian for FileType {
    type LittleEndian = u8;

    fn to_little_endian(&self) -> u8 {
        *self as u8
    }

    fn from_little_endian(little_endian: &u8) -> Result<Self, ViewError> {
        FileType::try_read_from_bytes(&[*little_endian]).map_err(|_| ViewError::Invalid)
    }
}

// Flags are stored as their bits, and bits no flag names are not flags.
macro_rules! little_endian_flags {
    ($($flags:ident: $bits:ty),* $(,)?) => {
        $(
            impl LittleEndian for $flags {
                type LittleEndian = <$bits as LittleEndian>::LittleEndian;

                fn to_little_endian(&self) -> Self::LittleEndian {
                    self.bits().to_little_endian()
                }

                fn from_little_endian(little_endian: &Self::LittleEndian) -> Result<Self, ViewError> {
                    $flags::from_bits(<$bits>::from_little_endian(little_endian)?).ok_or(ViewError::Invalid)
                }
            }
        )*
    };
}

little_endian_flags! {
    Mode: u16,
    Restrictions: u8,
    Hints: u8,
}

serialized_shape! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct SettableAttributes as LittleEndianSettableAttributes {
        pub mode: Mode,
        pub ownership: Ownership,
        pub restrictions: Restrictions,
        pub hints: Hints,

        pub size: u64,

        pub last_accessed_at: Timestamp,
        pub content_changed_at: Timestamp,
        pub created_at: Timestamp,
    }
}

serialized_value! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Attributes as LittleEndianAttributes {
        pub settable: SettableAttributes,

        pub kind: FileType,
        pub hard_link_count: u32,
        pub attribute_changed_at: Timestamp,

        pub parent: Inode,
        pub change_version: ChangeVersion,
    }
}

serialized_shape! {
    // ChangeVersion counts an inode's changes, and names the mount that made the latest. Every change moves the
    // number by one, whichever mount makes it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct ChangeVersion as LittleEndianChangeVersion {
        pub mount: MountId,
        pub number: u64,
    }
}

impl ChangeVersion {
    // after is the version a change by mount moves this one to.
    pub(crate) fn after(self, mount: MountId) -> ChangeVersion {
        ChangeVersion {
            mount,
            number: self.number + 1,
        }
    }
}

serialized_shape! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Ownership as LittleEndianOwnership {
        pub user_id: u32,
        pub group_id: u32,
    }
}

impl Attributes {
    pub(crate) fn record_change(&mut self, now: Timestamp, mount: MountId) {
        self.attribute_changed_at = now;
        self.change_version = self.change_version.after(mount);
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
        read_attributes(&*self.metadata, inode)
    }
}

// read_attributes is inode's stored attributes as metadata reads them, or None when the inode does not exist.
pub(crate) fn read_attributes<T: ReadKeyValueTransaction, C>(
    metadata: &T,
    inode: Inode,
) -> impl Future<Output = Result<Option<Attributes>, IoFailure<T::Failure, C>>> + Send {
    async move {
        let Some(stored) = metadata
            .get(FsKey::new_attributes(inode).into())
            .await
            .map_err(IoFailure::Metadata)?
        else {
            return Ok(None);
        };
        Attributes::view(&stored)
            .map(Some)
            .map_err(|corrupt| IoFailure::Corrupt(corrupt.into()))
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
        self.metadata
            .set(FsKey::new_attributes(inode).into(), Bytes::from(attributes))
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
                    restrictions: Restrictions::empty(),
                    hints: Hints::empty(),
                    size: 0,
                    last_accessed_at: now,
                    content_changed_at: now,
                    created_at: now,
                },
                kind: FileType::Directory,
                hard_link_count: 2,
                attribute_changed_at: now,
                parent: Inode::ROOT,
                change_version: ChangeVersion {
                    mount: self.mount,
                    number: 0,
                },
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
            let before = attributes.settable.clone();
            update(&mut attributes.settable);
            // a restricted item may only change its restrictions and hints, which is how a restriction is removed
            let with_old_restrictions_and_hints = SettableAttributes {
                restrictions: before.restrictions,
                hints: before.hints,
                ..attributes.settable.clone()
            };
            if !before.restrictions.is_empty() && with_old_restrictions_and_hints != before {
                return Err(SetattrError::OperationNotPermitted);
            }
            let now = self.now();
            if attributes.settable.size < before.size {
                self.truncate_content(inode, attributes.settable.size, before.size)
                    .await?;
            }
            // a resize changes the content, unless the update set the content change time itself
            if attributes.settable.size != before.size
                && attributes.settable.content_changed_at == before.content_changed_at
            {
                attributes.settable.content_changed_at = now;
            }
            attributes.record_change(now, self.mount);
            self.write_attributes(inode, &attributes)?;
            Ok(attributes)
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
