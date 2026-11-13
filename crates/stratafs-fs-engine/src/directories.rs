// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
// The long return types come from that form.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use bytes::{Buf, Bytes};
use futures_util::TryFutureExt;
use futures_util::future::{try_join, try_join4};
use stratafs_store::content::ContentStore;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};
use xxhash_rust::xxh3::xxh3_64;
use zerocopy::little_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::attributes::{
    Attributes, ChangeVersion, FileType, Hints, Mode, Ownership, Restrictions, SettableAttributes,
};
use crate::directories::error::{
    CreateError, HardLinkError, HardUnlinkError, LookupError, ReaddirError, RenameError, RmdirError,
};
use crate::error::{ChangeFailure, CorruptValue, IoFailure};
use crate::key::{
    FsKey, directory_entries_after, directory_entries_between, directory_entry_position,
    every_key_of, hard_link_directory, hard_links_of,
};
use crate::verification::{Record, RecordChange, RecordKind};
use crate::{Change, FsConfig, FsTransaction, Inode, LARGEST_NAME_LENGTH, Timestamp};

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

// Lookup is what a lookup read: the directory's attributes, and the inode its name refers to with that inode's
// attributes, or None when no entry has the name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lookup {
    pub directory: Attributes,
    pub found: Option<(Inode, Attributes)>,
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

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: FsConfig>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    // vouched_entries checks that entries are every entry of dir at the positions from first to last.
    fn vouched_entries<'a>(
        &'a self,
        dir: Inode,
        first: u64,
        last: u64,
        entries: &'a [Record],
    ) -> impl Future<Output = Result<(), IoFailure<T::Failure, C::Failure>>> + Send + 'a {
        let (first, last) = (RecordKind::Content.at(first), RecordKind::Content.at(last));
        self.vouched(dir, first, last, entries)
    }

    // prefetch_names reads ahead the nodes of inode's hash tree for the names it has, which an operation that gives
    // it a name or takes one away checks and changes.
    fn prefetch_names(
        &self,
        inode: Inode,
    ) -> impl Future<Output = Result<(), IoFailure<T::Failure, C::Failure>>> + Send {
        self.prefetch(
            inode,
            RecordKind::Name.at(u64::MIN),
            RecordKind::Name.at(u64::MAX),
        )
    }

    // getattr_with_names is inode's attributes, read with the nodes of its hash tree for its names read ahead.
    fn getattr_with_names(
        &self,
        inode: Inode,
    ) -> impl Future<Output = Result<Option<Attributes>, IoFailure<T::Failure, C::Failure>>> + Send
    {
        async move {
            let (attributes, ()) =
                try_join(self.getattr(inode), self.prefetch_names(inode)).await?;
            Ok(attributes)
        }
    }

    fn find_name(
        &self,
        dir: Inode,
        name: &[u8],
    ) -> impl Future<Output = Result<NameSearch, IoFailure<T::Failure, C::Failure>>> + Send {
        async move {
            let (first, last) = positions_of(name);
            let entries = self
                .metadata
                .get_range(directory_entries_between(dir, first, last), usize::MAX);
            let (entries, ()) = try_join(
                entries.map_err(IoFailure::Metadata),
                self.prefetch(
                    dir,
                    RecordKind::Content.at(first),
                    RecordKind::Content.at(last),
                ),
            )
            .await?;
            let entries: Vec<Record> = entries.map(Record::from).collect();
            self.vouched_entries(dir, first, last, &entries).await?;
            let mut free = first.max(FIRST_POSITION);
            for Record { key, value } in entries {
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
            let first: Vec<Record> = self
                .metadata
                .get_range(directory_entries_after(dir, 0), 1)
                .await
                .map_err(IoFailure::Metadata)?
                .map(Record::from)
                .collect();
            // An entry there is shows the directory has one, and only every position without one shows it has none.
            let (from, to) = match first.first() {
                Some(entry) => {
                    let position =
                        directory_entry_position(&entry.key).map_err(IoFailure::Corrupt)?;
                    (position, position)
                }
                None => (1, u64::MAX),
            };
            self.vouched_entries(dir, from, to, &first).await?;
            Ok(first.is_empty())
        }
    }

    pub fn lookup(
        &self,
        dir: Inode,
        name: &[u8],
    ) -> impl Future<Output = Result<Lookup, LookupError<T::Failure, C::Failure>>> + Send {
        async move {
            check_name(
                name,
                LookupError::InvalidInput,
                LookupError::FileNameTooLong,
            )?;
            let (directory, search) =
                try_join(self.getattr(dir), self.find_name(dir, name)).await?;
            let directory = require_directory(
                directory,
                LookupError::StaleFileHandle,
                LookupError::NotADirectory,
            )?;
            let NameSearch::Found(entry) = search else {
                return Ok(Lookup {
                    directory,
                    found: None,
                });
            };
            let attributes = self
                .getattr(entry.inode)
                .await?
                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            Ok(Lookup {
                directory,
                found: Some((entry.inode, attributes)),
            })
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
            let entries = self
                .metadata
                .get_range(directory_entries_after(dir, after), limit);
            let (directory, entries) =
                try_join(self.getattr(dir), entries.map_err(IoFailure::Metadata)).await?;
            require_directory(
                directory,
                ReaddirError::StaleFileHandle,
                ReaddirError::NotADirectory,
            )?;
            let stored: Vec<Record> = entries.map(Record::from).collect();
            let entries = stored
                .iter()
                .map(|entry| decode_entry(&entry.key, entry.value.clone()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(IoFailure::Corrupt)?;
            // A listing cut short at its limit holds every entry up to its last, and any other every entry there is.
            let last = match entries.last() {
                Some(last) if entries.len() == limit => last.position,
                _ => u64::MAX,
            };
            if let Some(first) = after.checked_add(1).filter(|first| *first <= last) {
                self.vouched_entries(dir, first, last, &stored).await?;
            }
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
    // new_inode allocates an inode number no inode has, reusing one an earlier attempt allocated when it can.
    fn new_inode(&mut self) -> impl Future<Output = Result<Inode, T::Failure>> + Send {
        async move {
            loop {
                let inode = self
                    .reuse
                    .pop()
                    .unwrap_or_else(|| Inode((self.allocate_inode)()));
                if inode == Inode::ROOT || inode.0 > LARGEST_INODE {
                    continue;
                }
                if self
                    .metadata
                    .get(FsKey::new_attributes(inode).into())
                    .await?
                    .is_none()
                {
                    self.allocated.push(inode);
                    return Ok(inode);
                }
            }
        }
    }

    fn add_name<'a>(
        &'a mut self,
        dir: Inode,
        position: u64,
        name: &'a [u8],
        inode: Inode,
        kind: FileType,
    ) -> impl Future<Output = Result<(), ChangeFailure<T::Rejection, T::Failure, C::Failure>>> + Send + 'a
    {
        async move {
            let header = StoredEntryHeader {
                inode: U64::new(inode.0),
                kind: kind as u8,
            };
            let entry = RecordChange {
                key: Bytes::from(FsKey::new_directory_entry(dir, position)),
                value: Some(header.as_bytes().chain(name)),
            };
            self.change_records(dir, vec![entry]).await?;
            match kind {
                FileType::Directory => Ok(()),
                FileType::File | FileType::SymbolicLink => {
                    let key = Bytes::from(FsKey::new_hard_link(inode, dir, position));
                    // An inode the transaction made has no other name yet. Any other may have one whose key starts
                    // the same hash, which this name would replace in its hash tree.
                    let made = matches!(
                        self.changes.get(&inode),
                        Some(Change::Written(made)) if made.change_version.number == 0
                    );
                    if !made {
                        self.vouched_record(inode, &key, None).await?;
                    }
                    let name = RecordChange {
                        key,
                        value: Some(Bytes::new()),
                    };
                    self.change_records(inode, vec![name]).await
                }
            }
        }
    }

    fn remove_name(
        &mut self,
        dir: Inode,
        position: u64,
        inode: Inode,
        kind: FileType,
    ) -> impl Future<Output = Result<(), ChangeFailure<T::Rejection, T::Failure, C::Failure>>> + Send
    {
        async move {
            let entry = RecordChange::<Bytes> {
                key: Bytes::from(FsKey::new_directory_entry(dir, position)),
                value: None,
            };
            self.change_records(dir, vec![entry]).await?;
            match kind {
                FileType::Directory => Ok(()),
                FileType::File | FileType::SymbolicLink => {
                    let key = Bytes::from(FsKey::new_hard_link(inode, dir, position));
                    let name = RecordChange::<Bytes> { key, value: None };
                    self.change_records(inode, vec![name]).await
                }
            }
        }
    }

    // delete_inode deletes every key of inode, whose attributes were item. An inode this transaction made leaves no
    // change behind, and one it changed already is deleted with the version it had before the transaction.
    fn delete_inode(
        &mut self,
        inode: Inode,
        mut item: Attributes,
    ) -> impl Future<Output = Result<(), T::Failure>> + Send {
        async move {
            match self.changes.get(&inode) {
                Some(Change::Written(made)) if made.change_version.number == 0 => {
                    self.changes.remove(&inode);
                }
                Some(Change::Written(_)) => {
                    item.change_version.number -= 1;
                    self.changes.insert(inode, Change::Deleted(item));
                }
                Some(Change::Deleted(_)) | None => {
                    self.changes.insert(inode, Change::Deleted(item));
                }
            }
            let keys = self
                .metadata
                .get_range(every_key_of(inode), usize::MAX)
                .await?;
            for (key, _) in keys {
                self.metadata.clear(key)?;
            }
            self.unmade(inode);
            Ok(())
        }
    }

    // release_name removes one of inode's names. An inode left with no name stays until delete_if_unlinked, so
    // whoever still holds it keeps using it.
    fn release_name(
        &mut self,
        dir: Inode,
        position: u64,
        inode: Inode,
        item: &mut Attributes,
        now: Timestamp,
    ) -> impl Future<Output = Result<(), ChangeFailure<T::Rejection, T::Failure, C::Failure>>> + Send
    {
        async move {
            self.remove_name(dir, position, inode, item.kind).await?;
            self.record_change(inode, item, now);
            match item.kind {
                FileType::Directory => item.hard_link_count = 0,
                FileType::File | FileType::SymbolicLink => {
                    item.hard_link_count = item
                        .hard_link_count
                        .checked_sub(1)
                        .ok_or(IoFailure::Corrupt(CorruptValue::UncountedHardLink))?
                }
            }
            if item.hard_link_count > 0 && item.parent == dir {
                // Its names are in no order in its hash tree, so only all of them are vouched for.
                let names: Vec<Record> = self
                    .metadata
                    .get_range(hard_links_of(inode), usize::MAX)
                    .await
                    .map_err(IoFailure::Metadata)?
                    .map(Record::from)
                    .collect();
                let (first, last) = (RecordKind::Name.at(u64::MIN), RecordKind::Name.at(u64::MAX));
                self.vouched(inode, first, last, &names).await?;
                if let Some(name) = names.first() {
                    item.parent = hard_link_directory(&name.key).map_err(IoFailure::Corrupt)?;
                }
            }
            Ok(())
        }
    }

    fn write_entry_change(
        &mut self,
        dir: Inode,
        directory: &mut Attributes,
        now: Timestamp,
    ) -> Result<(), StoreError<T::Rejection, T::Failure>> {
        directory.settable.content_changed_at = now;
        self.record_change(dir, directory, now);
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
            let (directory, search) =
                try_join(self.getattr(dir), self.find_name(dir, name)).await?;
            let mut directory = require_directory(
                directory,
                CreateError::StaleFileHandle,
                CreateError::NotADirectory,
            )?;
            if directory.hard_link_count == 0 {
                return Err(CreateError::NotFound);
            }
            let position = match search {
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
                change_version: ChangeVersion {
                    mount: self.mount,
                    number: 0,
                },
            };
            update(&mut attributes.settable);
            self.made(inode);
            self.write_attributes(inode, &attributes)?;
            if let NewItem::SymbolicLink(target) = item {
                let key = Bytes::from(FsKey::new_symbolic_link_target(inode));
                let value = self
                    .make_spillable(&key, 0, target)
                    .await
                    .map_err(IoFailure::ContentWrite)?;
                let target = RecordChange {
                    key,
                    value: Some(value.to_stored()),
                };
                self.change_records(inode, vec![target]).await?;
            }
            self.add_name(dir, position, name, inode, kind).await?;
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
            let (item, directory, search, ()) = try_join4(
                self.getattr(inode),
                self.getattr(dir),
                self.find_name(dir, name),
                self.prefetch_names(inode),
            )
            .await?;
            let mut item = item.ok_or(HardLinkError::StaleFileHandle)?;
            let mut directory = require_directory(
                directory,
                HardLinkError::StaleFileHandle,
                HardLinkError::NotADirectory,
            )?;
            if item.hard_link_count == 0 || directory.hard_link_count == 0 {
                return Err(HardLinkError::NotFound);
            }
            let position = match search {
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
            self.record_change(inode, &mut item, now);
            self.write_attributes(inode, &item)?;
            self.add_name(dir, position, name, inode, item.kind).await?;
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
            let (directory, search) =
                try_join(self.getattr(dir), self.find_name(dir, name)).await?;
            let mut directory = require_directory(
                directory,
                HardUnlinkError::StaleFileHandle,
                HardUnlinkError::NotADirectory,
            )?;
            let NameSearch::Found(entry) = search else {
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
                .getattr_with_names(entry.inode)
                .await?
                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            if !directory.settable.restrictions.is_empty() || !item.settable.restrictions.is_empty()
            {
                return Err(HardUnlinkError::OperationNotPermitted);
            }
            let now = self.now();
            self.release_name(dir, entry.position, entry.inode, &mut item, now)
                .await?;
            self.write_attributes(entry.inode, &item)?;
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
            let (directory, search) =
                try_join(self.getattr(dir), self.find_name(dir, name)).await?;
            let mut directory = require_directory(
                directory,
                RmdirError::StaleFileHandle,
                RmdirError::NotADirectory,
            )?;
            let NameSearch::Found(entry) = search else {
                return Err(RmdirError::NotFound);
            };
            if expected.is_some_and(|expected| expected != entry.inode) {
                return Err(RmdirError::StaleFileHandle);
            }
            match entry.kind {
                FileType::Directory => {}
                FileType::File | FileType::SymbolicLink => return Err(RmdirError::NotADirectory),
            }
            let (item, empty) = try_join(
                self.getattr(entry.inode),
                self.is_empty_directory(entry.inode),
            )
            .await?;
            let mut item = item.ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            if !directory.settable.restrictions.is_empty() || !item.settable.restrictions.is_empty()
            {
                return Err(RmdirError::OperationNotPermitted);
            }
            if !empty {
                return Err(RmdirError::DirectoryNotEmpty);
            }
            let now = self.now();
            self.release_name(dir, entry.position, entry.inode, &mut item, now)
                .await?;
            self.write_attributes(entry.inode, &item)?;
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
            let (from, to, source, target) = try_join4(
                self.getattr(dir),
                self.getattr(new_dir),
                self.find_name(dir, name),
                self.find_name(new_dir, new_name),
            )
            .await?;
            let mut from = require_directory(
                from,
                RenameError::StaleFileHandle,
                RenameError::NotADirectory,
            )?;
            let mut to =
                require_directory(to, RenameError::StaleFileHandle, RenameError::NotADirectory)?;
            if to.hard_link_count == 0 {
                return Err(RenameError::NotFound);
            }
            let NameSearch::Found(source) = source else {
                return Err(RenameError::NotFound);
            };
            if expected.is_some_and(|expected| expected != source.inode) {
                return Err(RenameError::StaleFileHandle);
            }
            let mut item = self
                .getattr_with_names(source.inode)
                .await?
                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
            let target = match (mode, target) {
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
                        .await?;
                    self.add_name(new_dir, position, new_name, source.inode, item.kind)
                        .await?;
                    None
                }
                RenameTarget::Exchanged(target) => {
                    let mut other = self
                        .getattr_with_names(target.inode)
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
                        .await?;
                    self.remove_name(new_dir, target.position, target.inode, other.kind)
                        .await?;
                    self.add_name(dir, source.position, name, target.inode, other.kind)
                        .await?;
                    if other.parent == new_dir {
                        other.parent = dir;
                    }
                    self.record_change(target.inode, &mut other, now);
                    self.write_attributes(target.inode, &other)?;
                    Some((target.inode, other, target.position))
                }
                RenameTarget::Replaced(target) => {
                    let mut replaced = self
                        .getattr_with_names(target.inode)
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
                        .await?;
                    self.release_name(new_dir, target.position, target.inode, &mut replaced, now)
                        .await?;
                    self.write_attributes(target.inode, &replaced)?;
                    Some((target.inode, replaced, target.position))
                }
            };
            let replaced = match replaced {
                Some((inode, attributes, position)) => {
                    self.add_name(new_dir, position, new_name, source.inode, item.kind)
                        .await?;
                    Some((inode, attributes))
                }
                None => None,
            };
            if item.parent == dir {
                item.parent = new_dir;
            }
            self.record_change(source.inode, &mut item, now);
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

    // delete_if_unlinked deletes inode if no name refers to it. Its holder calls it on letting go of it.
    pub fn delete_if_unlinked(
        &mut self,
        inode: Inode,
    ) -> impl Future<Output = Result<(), IoFailure<T::Failure, C::Failure>>> + Send {
        async move {
            if let Some(item) = self.getattr(inode).await?
                && item.hard_link_count == 0
            {
                self.delete_inode(inode, item)
                    .await
                    .map_err(IoFailure::Metadata)?;
            }
            Ok(())
        }
    }
}

pub mod error;

#[cfg(test)]
mod tests;
