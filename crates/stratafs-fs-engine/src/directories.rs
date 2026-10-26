// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use bytes::{Buf, Bytes};
use futures_util::future::{join, join3, join4};
use stratafs_store::content::ContentStore;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::attributes::{
    Attributes, FileType, Hints, Mode, Ownership, Restrictions, SettableAttributes,
};
use crate::directories::error::{
    CreateError, HardLinkError, HardUnlinkError, LookupError, ReaddirError, RenameError, RmdirError,
};
use crate::error::{CorruptValue, IoFailure, refused_or_failed};
use crate::key::{
    FsKey, directory_entries_after, directory_entries_between, directory_entry_position,
    every_key_of, hard_link_directory, hard_links_of,
};
use crate::{FsConfig, FsTransaction, Inode, LARGEST_NAME_LENGTH, Timestamp};

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
    SymbolicLink(Bytes),
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

const COLLISION_BITS: u32 = 16;
const HASH_BITS: u32 = u64::BITS - 1 - COLLISION_BITS;
const FIRST_POSITION: u64 = 3;
const LARGEST_INODE: u64 = u64::MAX - 2;

fn positions_of(name: &[u8]) -> (u64, u64) {
    let first = (xxh3_64(name) >> (u64::BITS - HASH_BITS)) << COLLISION_BITS;
    (first, first | ((1 << COLLISION_BITS) - 1))
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct StoredEntryHeader {
    inode: U64,
    kind: u8,
}

fn decode_entry(key: &[u8], value: Bytes) -> Result<DirectoryEntry, CorruptValue> {
    let position = directory_entry_position(key)?;
    let (header, name) =
        StoredEntryHeader::ref_from_prefix(&value).map_err(|_| CorruptValue::WrongLength)?;
    Ok(DirectoryEntry {
        name: value.slice_ref(name),
        inode: Inode(header.inode.get()),
        kind: FileType::try_read_from_bytes(&[header.kind])
            .map_err(|_| CorruptValue::UnknownFileType)?,
        position,
    })
}

enum NameSearch {
    Found(DirectoryEntry),
    Free(u64),
    Full,
}

enum RenameTarget {
    Free(u64),
    Full,
    Replaced(DirectoryEntry),
    Exchanged(DirectoryEntry),
}

fn check_name<E>(name: &[u8], invalid: E, too_long: E) -> Result<(), E> {
    if name.len() > LARGEST_NAME_LENGTH {
        return Err(too_long);
    }
    if name.is_empty() || name == b"." || name == b".." || name.contains(&b'/') || name.contains(&0)
    {
        return Err(invalid);
    }
    Ok(())
}

fn require_directory<E>(
    attributes: Option<Attributes>,
    stale: E,
    not_directory: E,
) -> Result<Attributes, E> {
    let attributes = attributes.ok_or(stale)?;
    match attributes.kind {
        FileType::Directory => Ok(attributes),
        FileType::File | FileType::SymbolicLink => Err(not_directory),
    }
}

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: Sync>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    fn find_name(
        &self,
        dir: Inode,
        name: &[u8],
    ) -> impl Future<Output = Result<NameSearch, IoFailure<T::Failure, C::Failure>>> + Send {
        async move {
            let (first, last) = positions_of(name);
            let entries = self
                .metadata
                .get_range(directory_entries_between(dir, first, last), usize::MAX)
                .await
                .map_err(IoFailure::Metadata)?;
            let mut free = first.max(FIRST_POSITION);
            for (key, value) in entries {
                let entry = decode_entry(&key, value).map_err(IoFailure::Corrupt)?;
                if entry.name == name {
                    return Ok(NameSearch::Found(entry));
                }
                if entry.position == free {
                    free += 1;
                }
            }
            if free > last {
                return Ok(NameSearch::Full);
            }
            Ok(NameSearch::Free(free))
        }
    }

    fn contains(
        &self,
        ancestor: Inode,
        mut current: Inode,
    ) -> impl Future<Output = Result<bool, IoFailure<T::Failure, C::Failure>>> + Send {
        async move {
            loop {
                if current == ancestor {
                    return Ok(true);
                }
                if current == Inode::ROOT {
                    return Ok(false);
                }
                current = self
                    .getattr(current)
                    .await?
                    .ok_or(IoFailure::Corrupt(CorruptValue::MissingParent))?
                    .parent;
            }
        }
    }

    fn is_empty_directory(
        &self,
        dir: Inode,
    ) -> impl Future<Output = Result<bool, IoFailure<T::Failure, C::Failure>>> + Send {
        async move {
            let mut entries = self
                .metadata
                .get_range(directory_entries_after(dir, 0), 1)
                .await
                .map_err(IoFailure::Metadata)?;
            Ok(entries.next().is_none())
        }
    }

    pub fn lookup(
        &self,
        dir: Inode,
        name: &[u8],
    ) -> impl Future<
        Output = Result<Option<(Inode, Attributes)>, LookupError<T::Failure, C::Failure>>,
    > + Send {
        async move {
            check_name(
                name,
                LookupError::InvalidInput,
                LookupError::FileNameTooLong,
            )?;
            let (directory, search) = join(self.getattr(dir), self.find_name(dir, name)).await;
            require_directory(
                directory?,
                LookupError::StaleFileHandle,
                LookupError::NotADirectory,
            )?;
            let NameSearch::Found(entry) = search? else {
                return Ok(None);
            };
            let attributes = self
                .getattr(entry.inode)
                .await?
                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            Ok(Some((entry.inode, attributes)))
        }
    }

    pub fn readdir(
        &self,
        dir: Inode,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<DirectoryEntry>, ReaddirError<T::Failure, C::Failure>>> + Send
    {
        async move {
            let (directory, entries) = join(
                self.getattr(dir),
                self.metadata
                    .get_range(directory_entries_after(dir, after), limit),
            )
            .await;
            require_directory(
                directory?,
                ReaddirError::StaleFileHandle,
                ReaddirError::NotADirectory,
            )?;
            let entries = entries
                .map_err(IoFailure::Metadata)?
                .into_iter()
                .map(|(key, value)| decode_entry(&key, value))
                .collect::<Result<Vec<_>, _>>()
                .map_err(IoFailure::Corrupt)?;
            Ok(entries)
        }
    }
}

impl<T, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S>
where
    T: KeyValueTransaction,
    C: ContentStore,
    A: Fn() -> u64 + Sync,
    N: Fn() -> Timestamp + Sync,
    S: FsConfig,
{
    fn new_inode(&self) -> impl Future<Output = Result<Inode, T::Failure>> + Send {
        async move {
            loop {
                let candidate = (self.allocate_inode)();
                if candidate == Inode::ROOT.0 || candidate > LARGEST_INODE {
                    continue;
                }
                let inode = Inode(candidate);
                if self
                    .metadata
                    .get(FsKey::new_attributes(inode).into())
                    .await?
                    .is_none()
                {
                    return Ok(inode);
                }
            }
        }
    }

    fn add_name(
        &mut self,
        dir: Inode,
        position: u64,
        name: &[u8],
        inode: Inode,
        kind: FileType,
    ) -> Result<(), StoreError<T::Rejection, T::Failure>> {
        let header = StoredEntryHeader {
            inode: U64::new(inode.0),
            kind: kind as u8,
        };
        self.metadata
            .set(
                FsKey::new_directory_entry(dir, position).into(),
                header.as_bytes().chain(name),
            )
            .map_err(refused_or_failed)?;
        match kind {
            FileType::Directory => Ok(()),
            FileType::File | FileType::SymbolicLink => self
                .metadata
                .set(
                    FsKey::new_hard_link(inode, dir, position).into(),
                    Bytes::new(),
                )
                .map_err(refused_or_failed),
        }
    }

    fn remove_name(
        &mut self,
        dir: Inode,
        position: u64,
        inode: Inode,
        kind: FileType,
    ) -> Result<(), T::Failure> {
        self.metadata
            .clear(FsKey::new_directory_entry(dir, position).into())?;
        match kind {
            FileType::Directory => Ok(()),
            FileType::File | FileType::SymbolicLink => self
                .metadata
                .clear(FsKey::new_hard_link(inode, dir, position).into()),
        }
    }

    fn delete_inode(
        &mut self,
        inode: Inode,
    ) -> impl Future<Output = Result<(), T::Failure>> + Send {
        async move {
            let keys = self
                .metadata
                .get_range(every_key_of(inode), usize::MAX)
                .await?;
            for (key, _) in keys {
                self.metadata.clear(key)?;
            }
            Ok(())
        }
    }

    fn release_name(
        &mut self,
        dir: Inode,
        position: u64,
        inode: Inode,
        item: &mut Attributes,
        now: Timestamp,
    ) -> impl Future<Output = Result<bool, IoFailure<T::Failure, C::Failure>>> + Send {
        async move {
            self.remove_name(dir, position, inode, item.kind)
                .map_err(IoFailure::Metadata)?;
            item.record_change(now);
            match item.kind {
                FileType::Directory => item.hard_link_count = 0,
                FileType::File | FileType::SymbolicLink => {
                    item.hard_link_count = item
                        .hard_link_count
                        .checked_sub(1)
                        .ok_or(IoFailure::Corrupt(CorruptValue::UncountedHardLink))?
                }
            }
            if item.hard_link_count == 0 {
                self.delete_inode(inode)
                    .await
                    .map_err(IoFailure::Metadata)?;
                return Ok(true);
            }
            if item.parent == dir {
                let mut remaining = self
                    .metadata
                    .get_range(hard_links_of(inode), 1)
                    .await
                    .map_err(IoFailure::Metadata)?;
                if let Some((key, _)) = remaining.next() {
                    item.parent = hard_link_directory(&key).map_err(IoFailure::Corrupt)?;
                }
            }
            Ok(false)
        }
    }

    fn write_entry_change(
        &mut self,
        dir: Inode,
        directory: &mut Attributes,
        now: Timestamp,
    ) -> Result<(), StoreError<T::Rejection, T::Failure>> {
        directory.settable.content_changed_at = now;
        directory.record_change(now);
        self.write_attributes(dir, directory)
    }

    pub fn create(
        &mut self,
        dir: Inode,
        name: &[u8],
        item: NewItem,
        update: impl FnOnce(&mut SettableAttributes) + Send,
    ) -> impl Future<Output = Result<EntryChange, CreateError<T::Failure, T::Rejection, C::Failure>>>
    + Send {
        async move {
            check_name(
                name,
                CreateError::InvalidInput,
                CreateError::FileNameTooLong,
            )?;
            let (kind, size) = match &item {
                NewItem::File => (FileType::File, 0),
                NewItem::Directory => (FileType::Directory, 0),
                NewItem::SymbolicLink(target) if target.is_empty() => {
                    return Err(CreateError::InvalidInput);
                }
                NewItem::SymbolicLink(target) => (FileType::SymbolicLink, target.len() as u64),
            };
            let (directory, search) = join(self.getattr(dir), self.find_name(dir, name)).await;
            let mut directory = require_directory(
                directory?,
                CreateError::StaleFileHandle,
                CreateError::NotADirectory,
            )?;
            let position = match search? {
                NameSearch::Found(_) => return Err(CreateError::AlreadyExists),
                NameSearch::Full => return Err(CreateError::StorageFull),
                NameSearch::Free(position) => position,
            };
            if directory.settable.restrictions.immutable() {
                return Err(CreateError::OperationNotPermitted);
            }
            let hard_link_count = match kind {
                FileType::Directory => {
                    directory.hard_link_count = directory
                        .hard_link_count
                        .checked_add(1)
                        .ok_or(CreateError::TooManyLinks)?;
                    2
                }
                FileType::File | FileType::SymbolicLink => 1,
            };
            let inode = self.new_inode().await.map_err(IoFailure::Metadata)?;
            let now = self.now();
            let mut attributes = Attributes {
                settable: SettableAttributes {
                    mode: Mode::empty(),
                    ownership: Ownership {
                        user_id: 0,
                        group_id: 0,
                    },
                    restrictions: Restrictions::empty(),
                    hints: Hints::empty(),
                    size,
                    last_accessed_at: now,
                    content_changed_at: now,
                    created_at: now,
                },
                kind,
                hard_link_count,
                attribute_changed_at: now,
                parent: dir,
                change_version: 0,
            };
            update(&mut attributes.settable);
            if let NewItem::SymbolicLink(target) = item {
                let key = Bytes::from(FsKey::new_symbolic_link_target(inode));
                let value = self
                    .make_spillable(&key, 0, target)
                    .await
                    .map_err(IoFailure::ContentWrite)?;
                self.metadata
                    .set(key, value.to_stored())
                    .map_err(refused_or_failed)?;
            }
            self.write_attributes(inode, &attributes)?;
            self.add_name(dir, position, name, inode, kind)?;
            self.write_entry_change(dir, &mut directory, now)?;
            Ok(EntryChange {
                inode,
                item: attributes,
                parent: directory,
            })
        }
    }

    pub fn hard_link(
        &mut self,
        inode: Inode,
        dir: Inode,
        name: &[u8],
    ) -> impl Future<
        Output = Result<EntryChange, HardLinkError<T::Failure, T::Rejection, C::Failure>>,
    > + Send {
        async move {
            check_name(
                name,
                HardLinkError::InvalidInput,
                HardLinkError::FileNameTooLong,
            )?;
            let (item, directory, search) = join3(
                self.getattr(inode),
                self.getattr(dir),
                self.find_name(dir, name),
            )
            .await;
            let mut item = item?.ok_or(HardLinkError::StaleFileHandle)?;
            let mut directory = require_directory(
                directory?,
                HardLinkError::StaleFileHandle,
                HardLinkError::NotADirectory,
            )?;
            let position = match search? {
                NameSearch::Found(_) => return Err(HardLinkError::AlreadyExists),
                NameSearch::Full => None,
                NameSearch::Free(position) => Some(position),
            };
            match item.kind {
                FileType::Directory => return Err(HardLinkError::OperationNotPermitted),
                FileType::File | FileType::SymbolicLink => {}
            }
            if !item.settable.restrictions.is_empty() || directory.settable.restrictions.immutable()
            {
                return Err(HardLinkError::OperationNotPermitted);
            }
            let position = position.ok_or(HardLinkError::StorageFull)?;
            item.hard_link_count = item
                .hard_link_count
                .checked_add(1)
                .ok_or(HardLinkError::TooManyLinks)?;
            let now = self.now();
            item.record_change(now);
            self.write_attributes(inode, &item)?;
            self.add_name(dir, position, name, inode, item.kind)?;
            self.write_entry_change(dir, &mut directory, now)?;
            Ok(EntryChange {
                inode,
                item,
                parent: directory,
            })
        }
    }

    pub fn hard_unlink(
        &mut self,
        dir: Inode,
        name: &[u8],
        expected: Option<Inode>,
    ) -> impl Future<
        Output = Result<EntryChange, HardUnlinkError<T::Failure, T::Rejection, C::Failure>>,
    > + Send {
        async move {
            check_name(
                name,
                HardUnlinkError::InvalidInput,
                HardUnlinkError::FileNameTooLong,
            )?;
            let (directory, search) = join(self.getattr(dir), self.find_name(dir, name)).await;
            let mut directory = require_directory(
                directory?,
                HardUnlinkError::StaleFileHandle,
                HardUnlinkError::NotADirectory,
            )?;
            let NameSearch::Found(entry) = search? else {
                return Err(HardUnlinkError::NotFound);
            };
            if expected.is_some_and(|expected| expected != entry.inode) {
                return Err(HardUnlinkError::StaleFileHandle);
            }
            match entry.kind {
                FileType::Directory => return Err(HardUnlinkError::IsADirectory),
                FileType::File | FileType::SymbolicLink => {}
            }
            let mut item = self
                .getattr(entry.inode)
                .await?
                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            if !directory.settable.restrictions.is_empty() || !item.settable.restrictions.is_empty()
            {
                return Err(HardUnlinkError::OperationNotPermitted);
            }
            let now = self.now();
            if !self
                .release_name(dir, entry.position, entry.inode, &mut item, now)
                .await?
            {
                self.write_attributes(entry.inode, &item)?;
            }
            self.write_entry_change(dir, &mut directory, now)?;
            Ok(EntryChange {
                inode: entry.inode,
                item,
                parent: directory,
            })
        }
    }

    pub fn rmdir(
        &mut self,
        dir: Inode,
        name: &[u8],
        expected: Option<Inode>,
    ) -> impl Future<Output = Result<EntryChange, RmdirError<T::Failure, T::Rejection, C::Failure>>> + Send
    {
        async move {
            check_name(name, RmdirError::InvalidInput, RmdirError::FileNameTooLong)?;
            let (directory, search) = join(self.getattr(dir), self.find_name(dir, name)).await;
            let mut directory = require_directory(
                directory?,
                RmdirError::StaleFileHandle,
                RmdirError::NotADirectory,
            )?;
            let NameSearch::Found(entry) = search? else {
                return Err(RmdirError::NotFound);
            };
            if expected.is_some_and(|expected| expected != entry.inode) {
                return Err(RmdirError::StaleFileHandle);
            }
            match entry.kind {
                FileType::Directory => {}
                FileType::File | FileType::SymbolicLink => return Err(RmdirError::NotADirectory),
            }
            let (item, empty) = join(
                self.getattr(entry.inode),
                self.is_empty_directory(entry.inode),
            )
            .await;
            let mut item = item?.ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            if !directory.settable.restrictions.is_empty() || !item.settable.restrictions.is_empty()
            {
                return Err(RmdirError::OperationNotPermitted);
            }
            if !empty? {
                return Err(RmdirError::DirectoryNotEmpty);
            }
            let now = self.now();
            self.release_name(dir, entry.position, entry.inode, &mut item, now)
                .await?;
            directory.hard_link_count = directory
                .hard_link_count
                .checked_sub(1)
                .ok_or(IoFailure::Corrupt(CorruptValue::UncountedHardLink))?;
            self.write_entry_change(dir, &mut directory, now)?;
            Ok(EntryChange {
                inode: entry.inode,
                item,
                parent: directory,
            })
        }
    }

    pub fn rename(
        &mut self,
        dir: Inode,
        name: &[u8],
        new_dir: Inode,
        new_name: &[u8],
        mode: RenameMode,
        expected: Option<Inode>,
    ) -> impl Future<
        Output = Result<RenameChange, RenameError<T::Failure, T::Rejection, C::Failure>>,
    > + Send {
        async move {
            check_name(
                name,
                RenameError::InvalidInput,
                RenameError::FileNameTooLong,
            )?;
            check_name(
                new_name,
                RenameError::InvalidInput,
                RenameError::FileNameTooLong,
            )?;
            let (from, to, source, target) = join4(
                self.getattr(dir),
                self.getattr(new_dir),
                self.find_name(dir, name),
                self.find_name(new_dir, new_name),
            )
            .await;
            let mut from = require_directory(
                from?,
                RenameError::StaleFileHandle,
                RenameError::NotADirectory,
            )?;
            let mut to = require_directory(
                to?,
                RenameError::StaleFileHandle,
                RenameError::NotADirectory,
            )?;
            let NameSearch::Found(source) = source? else {
                return Err(RenameError::NotFound);
            };
            if expected.is_some_and(|expected| expected != source.inode) {
                return Err(RenameError::StaleFileHandle);
            }
            let mut item = self
                .getattr(source.inode)
                .await?
                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            let target = match (mode, target?) {
                (RenameMode::NoReplace, NameSearch::Found(_)) => {
                    return Err(RenameError::AlreadyExists);
                }
                (RenameMode::Exchange, NameSearch::Free(_) | NameSearch::Full) => {
                    return Err(RenameError::NotFound);
                }
                (RenameMode::Replace | RenameMode::NoReplace, NameSearch::Free(position)) => {
                    RenameTarget::Free(position)
                }
                (RenameMode::Replace | RenameMode::NoReplace, NameSearch::Full) => {
                    RenameTarget::Full
                }
                (RenameMode::Replace, NameSearch::Found(entry)) => RenameTarget::Replaced(entry),
                (RenameMode::Exchange, NameSearch::Found(entry)) => RenameTarget::Exchanged(entry),
            };
            if let RenameTarget::Replaced(entry) | RenameTarget::Exchanged(entry) = &target
                && entry.inode == source.inode
            {
                return Ok(RenameChange {
                    inode: source.inode,
                    item,
                    from,
                    to,
                    replaced: None,
                });
            }
            // the source directory loses an entry, and the destination gains one, losing any it replaces
            let replaces_an_entry = match target {
                RenameTarget::Replaced(_) | RenameTarget::Exchanged(_) => true,
                RenameTarget::Free(_) | RenameTarget::Full => false,
            };
            if !from.settable.restrictions.is_empty()
                || !item.settable.restrictions.is_empty()
                || to.settable.restrictions.immutable()
                || to.settable.restrictions.append_only() && replaces_an_entry
            {
                return Err(RenameError::OperationNotPermitted);
            }
            let moves_between_directories = dir != new_dir;
            let mut from_hard_links: i32 = 0;
            let mut to_hard_links: i32 = 0;
            if moves_between_directories && item.kind == FileType::Directory {
                if self.contains(source.inode, new_dir).await? {
                    return Err(RenameError::InvalidInput);
                }
                from_hard_links -= 1;
                to_hard_links += 1;
            }
            if moves_between_directories
                && let RenameTarget::Replaced(entry) | RenameTarget::Exchanged(entry) = &target
                && entry.kind == FileType::Directory
                && self.contains(entry.inode, dir).await?
            {
                return Err(match target {
                    RenameTarget::Exchanged(_) => RenameError::InvalidInput,
                    RenameTarget::Replaced(_) | RenameTarget::Free(_) | RenameTarget::Full => {
                        RenameError::DirectoryNotEmpty
                    }
                });
            }
            let now = self.now();
            let replaced = match target {
                RenameTarget::Full => return Err(RenameError::StorageFull),
                RenameTarget::Free(position) => {
                    self.remove_name(dir, source.position, source.inode, item.kind)
                        .map_err(IoFailure::Metadata)?;
                    self.add_name(new_dir, position, new_name, source.inode, item.kind)?;
                    None
                }
                RenameTarget::Exchanged(target) => {
                    let mut other = self
                        .getattr(target.inode)
                        .await?
                        .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
                    if !other.settable.restrictions.is_empty() {
                        return Err(RenameError::OperationNotPermitted);
                    }
                    if moves_between_directories && other.kind == FileType::Directory {
                        to_hard_links -= 1;
                        from_hard_links += 1;
                    }
                    self.remove_name(dir, source.position, source.inode, item.kind)
                        .map_err(IoFailure::Metadata)?;
                    self.remove_name(new_dir, target.position, target.inode, other.kind)
                        .map_err(IoFailure::Metadata)?;
                    self.add_name(dir, source.position, name, target.inode, other.kind)?;
                    if other.parent == new_dir {
                        other.parent = dir;
                    }
                    other.record_change(now);
                    self.write_attributes(target.inode, &other)?;
                    Some((target.inode, other, target.position))
                }
                RenameTarget::Replaced(target) => {
                    let mut replaced = self
                        .getattr(target.inode)
                        .await?
                        .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
                    match (item.kind, replaced.kind) {
                        (FileType::Directory, FileType::File | FileType::SymbolicLink) => {
                            return Err(RenameError::NotADirectory);
                        }
                        (FileType::File | FileType::SymbolicLink, FileType::Directory) => {
                            return Err(RenameError::IsADirectory);
                        }
                        (FileType::Directory, FileType::Directory) => {
                            if !self.is_empty_directory(target.inode).await? {
                                return Err(RenameError::DirectoryNotEmpty);
                            }
                            to_hard_links -= 1;
                        }
                        (
                            FileType::File | FileType::SymbolicLink,
                            FileType::File | FileType::SymbolicLink,
                        ) => {}
                    }
                    if !replaced.settable.restrictions.is_empty() {
                        return Err(RenameError::OperationNotPermitted);
                    }
                    self.remove_name(dir, source.position, source.inode, item.kind)
                        .map_err(IoFailure::Metadata)?;
                    if !self
                        .release_name(new_dir, target.position, target.inode, &mut replaced, now)
                        .await?
                    {
                        self.write_attributes(target.inode, &replaced)?;
                    }
                    Some((target.inode, replaced, target.position))
                }
            };
            let replaced = match replaced {
                Some((inode, attributes, position)) => {
                    self.add_name(new_dir, position, new_name, source.inode, item.kind)?;
                    Some((inode, attributes))
                }
                None => None,
            };
            if item.parent == dir {
                item.parent = new_dir;
            }
            item.record_change(now);
            self.write_attributes(source.inode, &item)?;
            if moves_between_directories {
                from.hard_link_count = from
                    .hard_link_count
                    .checked_add_signed(from_hard_links)
                    .ok_or(RenameError::TooManyLinks)?;
                to.hard_link_count = to
                    .hard_link_count
                    .checked_add_signed(to_hard_links)
                    .ok_or(RenameError::TooManyLinks)?;
                self.write_entry_change(dir, &mut from, now)?;
                self.write_entry_change(new_dir, &mut to, now)?;
            } else {
                from.hard_link_count = from
                    .hard_link_count
                    .checked_add_signed(from_hard_links + to_hard_links)
                    .ok_or(RenameError::TooManyLinks)?;
                self.write_entry_change(dir, &mut from, now)?;
                to = from.clone();
            }
            Ok(RenameChange {
                inode: source.inode,
                item,
                from,
                to,
                replaced,
            })
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
