use std::collections::BTreeSet;
use std::convert::Infallible;

use futures_lite::future::block_on;
use stratafs_opendal::OpendalError;

use super::*;
use crate::attributes::SetXattrMode;
use crate::content::error::ReadSymbolicLinkError;
use crate::test_support::{InMemoryEngine, MetadataFailure, aborted, new_test_engine, set_clock};

type Create = CreateError<MetadataFailure, Infallible, OpendalError>;
type HardLink = HardLinkError<MetadataFailure, Infallible, OpendalError>;
type HardUnlink = HardUnlinkError<MetadataFailure, Infallible, OpendalError>;
type Rmdir = RmdirError<MetadataFailure, Infallible, OpendalError>;
type Rename = RenameError<MetadataFailure, Infallible, OpendalError>;

fn create(
    engine: &InMemoryEngine,
    dir: Inode,
    name: &[u8],
    item: NewItem,
) -> Result<EntryChange, Create> {
    block_on(engine.transact(|mut txn| {
        let item = item.clone();
        Box::pin(async move {
            let result = txn.create(dir, name, item, |_| {}).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn file(engine: &InMemoryEngine, dir: Inode, name: &[u8]) -> Inode {
    create(engine, dir, name, NewItem::File).unwrap().inode
}

fn directory(engine: &InMemoryEngine, dir: Inode, name: &[u8]) -> Inode {
    create(engine, dir, name, NewItem::Directory).unwrap().inode
}

fn getattr(engine: &InMemoryEngine, inode: Inode) -> Option<Attributes> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })))
        .map_err(aborted)
        .unwrap()
}

fn lookup(
    engine: &InMemoryEngine,
    dir: Inode,
    name: &[u8],
) -> Result<Option<(Inode, Attributes)>, LookupError<MetadataFailure, OpendalError>> {
    block_on(engine.read_transact(|txn| Box::pin(async move { txn.lookup(dir, name).await })))
        .map_err(aborted)
}

fn named(engine: &InMemoryEngine, dir: Inode, name: &[u8]) -> Option<Inode> {
    lookup(engine, dir, name).unwrap().map(|(inode, _)| inode)
}

fn readdir(
    engine: &InMemoryEngine,
    dir: Inode,
    after: u64,
    limit: usize,
) -> Result<Vec<DirectoryEntry>, ReaddirError<MetadataFailure, OpendalError>> {
    block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.readdir(dir, after, limit).await })),
    )
    .map_err(aborted)
}

fn names(engine: &InMemoryEngine, dir: Inode) -> BTreeSet<Bytes> {
    readdir(engine, dir, 0, usize::MAX)
        .unwrap()
        .into_iter()
        .map(|entry| entry.name)
        .collect()
}

fn hard_link(
    engine: &InMemoryEngine,
    inode: Inode,
    dir: Inode,
    name: &[u8],
) -> Result<EntryChange, HardLink> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.hard_link(inode, dir, name).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn hard_unlink(
    engine: &InMemoryEngine,
    dir: Inode,
    name: &[u8],
    expected: Option<Inode>,
) -> Result<EntryChange, HardUnlink> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.hard_unlink(dir, name, expected).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn rmdir(engine: &InMemoryEngine, dir: Inode, name: &[u8]) -> Result<EntryChange, Rmdir> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.rmdir(dir, name, None).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn rename(
    engine: &InMemoryEngine,
    dir: Inode,
    name: &[u8],
    new_dir: Inode,
    new_name: &[u8],
    mode: RenameMode,
) -> Result<RenameChange, Rename> {
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.rename(dir, name, new_dir, new_name, mode, None).await;
            result.map(|result| (result, txn))
        })
    }))
    .map_err(aborted)
}

fn read_symbolic_link(
    engine: &InMemoryEngine,
    inode: Inode,
) -> Result<Bytes, ReadSymbolicLinkError<MetadataFailure, OpendalError>> {
    block_on(engine.read_transact(|txn| {
        Box::pin(async move {
            let mut target = txn.read_symbolic_link(inode).await?;
            Ok(target.copy_to_bytes(target.remaining()))
        })
    }))
    .map_err(aborted)
}

fn stored_keys_of(engine: &InMemoryEngine, inode: Inode) -> usize {
    block_on(
        engine.read_transact(|txn| {
            Box::pin(async move { count_keys_of(&*txn.metadata, inode).await })
        }),
    )
    .unwrap()
}

// count_keys_of is generic so Send is proven against the trait, not each store's own future (see lib.rs).
fn count_keys_of<T: ReadKeyValueTransaction>(
    metadata: &T,
    inode: Inode,
) -> impl Future<Output = Result<usize, T::Failure>> + Send {
    async move {
        metadata
            .get_range(every_key_of(inode), usize::MAX)
            .await
            .map(|entries| entries.count())
    }
}

const MISSING: Inode = Inode(5000);

#[test]
fn create_names_a_new_inode_and_records_the_change_on_its_directory() {
    set_clock(5);
    let engine = new_test_engine();
    set_clock(9);
    let change = block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn
                .create(Inode::ROOT, b"a", NewItem::File, |settable| {
                    settable.mode = Mode::OWNER_READ | Mode::OWNER_WRITE;
                })
                .await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    assert_eq!(change.item.kind, FileType::File);
    assert_eq!(change.item.hard_link_count, 1);
    assert_eq!(change.item.parent, Inode::ROOT);
    assert_eq!(
        change.item.settable.mode,
        Mode::OWNER_READ | Mode::OWNER_WRITE
    );
    assert_eq!(change.item.settable.created_at, Timestamp(9));
    assert_eq!(change.parent.settable.content_changed_at, Timestamp(9));
    assert_eq!(change.parent.attribute_changed_at, Timestamp(9));
    assert_eq!(change.parent.change_version, 1);
    assert_eq!(change.parent.hard_link_count, 2);
    assert_eq!(
        lookup(&engine, Inode::ROOT, b"a").unwrap(),
        Some((change.inode, change.item))
    );
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), change.parent);
}

#[test]
fn a_new_directory_hard_links_back_to_its_parent() {
    let engine = new_test_engine();
    let change = create(&engine, Inode::ROOT, b"d", NewItem::Directory).unwrap();
    assert_eq!(change.item.kind, FileType::Directory);
    assert_eq!(change.item.hard_link_count, 2);
    assert_eq!(change.parent.hard_link_count, 3);
}

#[test]
fn names_follow_the_posix_rules() {
    let engine = new_test_engine();
    for name in [&b""[..], b".", b"..", b"a/b", b"a\0b"] {
        assert!(
            matches!(
                create(&engine, Inode::ROOT, name, NewItem::File),
                Err(CreateError::InvalidInput)
            ),
            "{name:?}"
        );
    }
    assert!(matches!(
        create(
            &engine,
            Inode::ROOT,
            &[b'a'; LARGEST_NAME_LENGTH + 1],
            NewItem::File
        ),
        Err(CreateError::FileNameTooLong)
    ));
    create(
        &engine,
        Inode::ROOT,
        &[b'a'; LARGEST_NAME_LENGTH],
        NewItem::File,
    )
    .unwrap();
    create(&engine, Inode::ROOT, &[0xff, b'.', 1], NewItem::File).unwrap();
}

#[test]
fn create_fails_on_an_existing_name_a_missing_directory_or_a_file() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    assert!(matches!(
        create(&engine, Inode::ROOT, b"a", NewItem::Directory),
        Err(CreateError::AlreadyExists)
    ));
    assert!(matches!(
        create(&engine, MISSING, b"b", NewItem::File),
        Err(CreateError::StaleFileHandle)
    ));
    assert!(matches!(
        create(&engine, a, b"b", NewItem::File),
        Err(CreateError::NotADirectory)
    ));
}

#[test]
fn a_symbolic_link_stores_its_target_and_reports_its_length_as_its_size() {
    let engine = new_test_engine();
    let change = create(
        &engine,
        Inode::ROOT,
        b"hard_link",
        NewItem::SymbolicLink(Bytes::from_static(b"../target")),
    )
    .unwrap();
    assert_eq!(change.item.kind, FileType::SymbolicLink);
    assert_eq!(change.item.settable.size, 9);
    assert_eq!(
        read_symbolic_link(&engine, change.inode).unwrap(),
        "../target"
    );
    let long_target = Bytes::from(vec![b'x'; 3000]);
    let long = create(
        &engine,
        Inode::ROOT,
        b"long",
        NewItem::SymbolicLink(long_target.clone()),
    )
    .unwrap();
    assert_eq!(
        read_symbolic_link(&engine, long.inode).unwrap(),
        long_target
    );
    assert!(matches!(
        create(
            &engine,
            Inode::ROOT,
            b"empty",
            NewItem::SymbolicLink(Bytes::new())
        ),
        Err(CreateError::InvalidInput)
    ));
}

#[test]
fn read_symbolic_link_needs_an_existing_symbolic_link() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    assert!(matches!(
        read_symbolic_link(&engine, a),
        Err(ReadSymbolicLinkError::InvalidInput)
    ));
    assert!(matches!(
        read_symbolic_link(&engine, MISSING),
        Err(ReadSymbolicLinkError::StaleFileHandle)
    ));
}

#[test]
fn lookup_finds_names_and_reports_missing_ones_as_none() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    assert_eq!(named(&engine, Inode::ROOT, b"a"), Some(a));
    assert_eq!(named(&engine, Inode::ROOT, b"b"), None);
    assert!(matches!(
        lookup(&engine, a, b"b"),
        Err(LookupError::NotADirectory)
    ));
    assert!(matches!(
        lookup(&engine, MISSING, b"b"),
        Err(LookupError::StaleFileHandle)
    ));
    assert!(matches!(
        lookup(&engine, Inode::ROOT, b"a/b"),
        Err(LookupError::InvalidInput)
    ));
}

#[test]
fn readdir_pages_through_every_entry_exactly_once() {
    let engine = new_test_engine();
    let mut expected = BTreeSet::new();
    for index in 0..50 {
        let name = Bytes::from(format!("file {index}"));
        file(&engine, Inode::ROOT, &name);
        expected.insert(name);
    }
    let mut seen = Vec::new();
    let mut after = 0;
    loop {
        let page = readdir(&engine, Inode::ROOT, after, 7).unwrap();
        let Some(last) = page.last() else { break };
        after = last.position;
        seen.extend(page);
    }
    assert!(
        seen.windows(2)
            .all(|pair| pair[0].position < pair[1].position)
    );
    assert!(seen.iter().all(|entry| entry.position >= FIRST_POSITION));
    assert_eq!(seen.len(), 50);
    assert_eq!(
        seen.into_iter()
            .map(|entry| entry.name)
            .collect::<BTreeSet<_>>(),
        expected
    );
}

#[test]
fn readdir_needs_an_existing_directory() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    assert!(matches!(
        readdir(&engine, a, 0, 10),
        Err(ReaddirError::NotADirectory)
    ));
    assert!(matches!(
        readdir(&engine, MISSING, 0, 10),
        Err(ReaddirError::StaleFileHandle)
    ));
}

#[test]
fn hard_link_adds_another_name_for_the_same_inode() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    let d = directory(&engine, Inode::ROOT, b"d");
    let change = hard_link(&engine, a, d, b"b").unwrap();
    assert_eq!(change.item.hard_link_count, 2);
    assert_eq!(change.item.parent, Inode::ROOT);
    assert_eq!(named(&engine, d, b"b"), Some(a));
    assert_eq!(named(&engine, Inode::ROOT, b"a"), Some(a));
}

#[test]
fn hard_link_refuses_directories_existing_names_and_missing_inodes() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    let d = directory(&engine, Inode::ROOT, b"d");
    assert!(matches!(
        hard_link(&engine, d, Inode::ROOT, b"e"),
        Err(HardLinkError::OperationNotPermitted)
    ));
    assert!(matches!(
        hard_link(&engine, a, Inode::ROOT, b"d"),
        Err(HardLinkError::AlreadyExists)
    ));
    assert!(matches!(
        hard_link(&engine, MISSING, Inode::ROOT, b"e"),
        Err(HardLinkError::StaleFileHandle)
    ));
}

#[test]
fn hard_unlinking_the_last_name_deletes_the_inode_and_all_its_keys() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn
                .setxattr(a, b"user.x", &b"1"[..], SetXattrMode::Create)
                .await;
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    let change = hard_unlink(&engine, Inode::ROOT, b"a", None).unwrap();
    assert_eq!(change.item.hard_link_count, 0);
    assert_eq!(named(&engine, Inode::ROOT, b"a"), None);
    assert_eq!(getattr(&engine, a), None);
    assert_eq!(stored_keys_of(&engine, a), 0);
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), change.parent);
}

#[test]
fn hard_unlinking_one_of_several_names_keeps_the_inode_and_repoints_its_parent() {
    let engine = new_test_engine();
    let first = directory(&engine, Inode::ROOT, b"first");
    let second = directory(&engine, Inode::ROOT, b"second");
    let f = file(&engine, first, b"f");
    hard_link(&engine, f, second, b"g").unwrap();
    let change = hard_unlink(&engine, first, b"f", None).unwrap();
    assert_eq!(change.item.hard_link_count, 1);
    assert_eq!(change.item.parent, second);
    assert_eq!(getattr(&engine, f).unwrap(), change.item);
    assert_eq!(named(&engine, second, b"g"), Some(f));
}

#[test]
fn hard_unlink_refuses_directories_missing_names_and_unexpected_inodes() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    directory(&engine, Inode::ROOT, b"d");
    assert!(matches!(
        hard_unlink(&engine, Inode::ROOT, b"d", None),
        Err(HardUnlinkError::IsADirectory)
    ));
    assert!(matches!(
        hard_unlink(&engine, Inode::ROOT, b"b", None),
        Err(HardUnlinkError::NotFound)
    ));
    assert!(matches!(
        hard_unlink(&engine, Inode::ROOT, b"a", Some(Inode(a.0 + 100))),
        Err(HardUnlinkError::StaleFileHandle)
    ));
    hard_unlink(&engine, Inode::ROOT, b"a", Some(a)).unwrap();
}

fn set_hard_link_count(engine: &InMemoryEngine, inode: Inode, hard_link_count: u32) {
    let mut attributes = getattr(engine, inode).unwrap();
    attributes.hard_link_count = hard_link_count;
    block_on(engine.transact(|mut txn| {
        let attributes = attributes.clone();
        Box::pin(async move {
            let result = txn.write_attributes(inode, &attributes);
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
}

#[test]
fn hard_unlinking_a_name_its_inode_does_not_count_is_corrupt() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    set_hard_link_count(&engine, a, 0);
    assert!(matches!(
        hard_unlink(&engine, Inode::ROOT, b"a", None),
        Err(HardUnlinkError::InputOutputError(IoFailure::Corrupt(
            CorruptValue::UncountedHardLink
        )))
    ));
}

#[test]
fn rmdir_under_a_parent_that_does_not_count_the_directory_is_corrupt() {
    let engine = new_test_engine();
    directory(&engine, Inode::ROOT, b"d");
    set_hard_link_count(&engine, Inode::ROOT, 0);
    assert!(matches!(
        rmdir(&engine, Inode::ROOT, b"d"),
        Err(RmdirError::InputOutputError(IoFailure::Corrupt(
            CorruptValue::UncountedHardLink
        )))
    ));
}

#[test]
fn rmdir_removes_an_empty_directory_and_its_hard_link_to_the_parent() {
    let engine = new_test_engine();
    let d = directory(&engine, Inode::ROOT, b"d");
    let change = rmdir(&engine, Inode::ROOT, b"d").unwrap();
    assert_eq!(change.parent.hard_link_count, 2);
    assert_eq!(getattr(&engine, d), None);
    assert_eq!(stored_keys_of(&engine, d), 0);
    assert_eq!(named(&engine, Inode::ROOT, b"d"), None);
}

#[test]
fn rmdir_refuses_non_empty_directories_and_files() {
    let engine = new_test_engine();
    let d = directory(&engine, Inode::ROOT, b"d");
    file(&engine, d, b"f");
    file(&engine, Inode::ROOT, b"a");
    assert!(matches!(
        rmdir(&engine, Inode::ROOT, b"d"),
        Err(RmdirError::DirectoryNotEmpty)
    ));
    assert!(matches!(
        rmdir(&engine, Inode::ROOT, b"a"),
        Err(RmdirError::NotADirectory)
    ));
}

#[test]
fn rename_within_a_directory_moves_the_name() {
    set_clock(5);
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    set_clock(9);
    let change = rename(
        &engine,
        Inode::ROOT,
        b"a",
        Inode::ROOT,
        b"b",
        RenameMode::Replace,
    )
    .unwrap();
    assert_eq!(named(&engine, Inode::ROOT, b"a"), None);
    assert_eq!(named(&engine, Inode::ROOT, b"b"), Some(a));
    assert_eq!(change.item.attribute_changed_at, Timestamp(9));
    assert_eq!(change.from, change.to);
    assert_eq!(change.replaced, None);
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), change.from);
}

#[test]
fn moving_a_directory_moves_its_parent_hard_link() {
    let engine = new_test_engine();
    let a = directory(&engine, Inode::ROOT, b"a");
    let b = directory(&engine, Inode::ROOT, b"b");
    let c = directory(&engine, a, b"c");
    let change = rename(&engine, a, b"c", b, b"c", RenameMode::Replace).unwrap();
    assert_eq!(change.from.hard_link_count, 2);
    assert_eq!(change.to.hard_link_count, 3);
    assert_eq!(change.item.parent, b);
    assert_eq!(getattr(&engine, a).unwrap(), change.from);
    assert_eq!(getattr(&engine, b).unwrap(), change.to);
    assert_eq!(getattr(&engine, c).unwrap(), change.item);
    assert_eq!(names(&engine, a), BTreeSet::new());
    assert_eq!(named(&engine, b, b"c"), Some(c));
}

#[test]
fn moving_a_file_updates_its_parent_and_hard_link() {
    let engine = new_test_engine();
    let a = directory(&engine, Inode::ROOT, b"a");
    let b = directory(&engine, Inode::ROOT, b"b");
    let f = file(&engine, a, b"f");
    let change = rename(&engine, a, b"f", b, b"g", RenameMode::Replace).unwrap();
    assert_eq!(change.item.parent, b);
    hard_link(&engine, f, Inode::ROOT, b"h").unwrap();
    let hard_unlinked = hard_unlink(&engine, b, b"g", None).unwrap();
    assert_eq!(hard_unlinked.item.parent, Inode::ROOT);
}

#[test]
fn rename_without_replacing_refuses_an_existing_name() {
    let engine = new_test_engine();
    file(&engine, Inode::ROOT, b"a");
    file(&engine, Inode::ROOT, b"b");
    assert!(matches!(
        rename(
            &engine,
            Inode::ROOT,
            b"a",
            Inode::ROOT,
            b"b",
            RenameMode::NoReplace
        ),
        Err(RenameError::AlreadyExists)
    ));
}

#[test]
fn replacing_a_file_releases_its_name() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    let b = file(&engine, Inode::ROOT, b"b");
    let kept = file(&engine, Inode::ROOT, b"kept");
    hard_link(&engine, kept, Inode::ROOT, b"kept too").unwrap();
    let change = rename(
        &engine,
        Inode::ROOT,
        b"a",
        Inode::ROOT,
        b"b",
        RenameMode::Replace,
    )
    .unwrap();
    let (replaced, attributes) = change.replaced.unwrap();
    assert_eq!(replaced, b);
    assert_eq!(attributes.hard_link_count, 0);
    assert_eq!(getattr(&engine, b), None);
    assert_eq!(named(&engine, Inode::ROOT, b"b"), Some(a));
    let change = rename(
        &engine,
        Inode::ROOT,
        b"b",
        Inode::ROOT,
        b"kept",
        RenameMode::Replace,
    )
    .unwrap();
    assert_eq!(change.replaced.unwrap().1.hard_link_count, 1);
    assert_eq!(getattr(&engine, kept).unwrap().hard_link_count, 1);
    assert_eq!(named(&engine, Inode::ROOT, b"kept too"), Some(kept));
}

#[test]
fn replacing_follows_the_posix_type_rules() {
    let engine = new_test_engine();
    let d = directory(&engine, Inode::ROOT, b"d");
    directory(&engine, Inode::ROOT, b"empty");
    let full = directory(&engine, Inode::ROOT, b"full");
    file(&engine, full, b"f");
    file(&engine, Inode::ROOT, b"a");
    assert!(matches!(
        rename(
            &engine,
            Inode::ROOT,
            b"d",
            Inode::ROOT,
            b"a",
            RenameMode::Replace
        ),
        Err(RenameError::NotADirectory)
    ));
    assert!(matches!(
        rename(
            &engine,
            Inode::ROOT,
            b"a",
            Inode::ROOT,
            b"d",
            RenameMode::Replace
        ),
        Err(RenameError::IsADirectory)
    ));
    assert!(matches!(
        rename(
            &engine,
            Inode::ROOT,
            b"d",
            Inode::ROOT,
            b"full",
            RenameMode::Replace
        ),
        Err(RenameError::DirectoryNotEmpty)
    ));
    let change = rename(
        &engine,
        Inode::ROOT,
        b"d",
        Inode::ROOT,
        b"empty",
        RenameMode::Replace,
    )
    .unwrap();
    assert_eq!(change.from.hard_link_count, 4);
    assert_eq!(named(&engine, Inode::ROOT, b"empty"), Some(d));
}

#[test]
fn a_directory_cannot_move_into_itself_or_below_itself() {
    let engine = new_test_engine();
    let a = directory(&engine, Inode::ROOT, b"a");
    let b = directory(&engine, a, b"b");
    assert!(matches!(
        rename(&engine, Inode::ROOT, b"a", b, b"a", RenameMode::Replace),
        Err(RenameError::InvalidInput)
    ));
    assert!(matches!(
        rename(&engine, Inode::ROOT, b"a", a, b"a", RenameMode::Replace),
        Err(RenameError::InvalidInput)
    ));
}

#[test]
fn exchange_swaps_two_names_and_their_parents() {
    let engine = new_test_engine();
    let a = directory(&engine, Inode::ROOT, b"a");
    let b = directory(&engine, Inode::ROOT, b"b");
    let x = directory(&engine, a, b"x");
    let y = file(&engine, b, b"y");
    let change = rename(&engine, a, b"x", b, b"y", RenameMode::Exchange).unwrap();
    assert_eq!(named(&engine, a, b"x"), Some(y));
    assert_eq!(named(&engine, b, b"y"), Some(x));
    assert_eq!(change.item.parent, b);
    let (other, other_attributes) = change.replaced.unwrap();
    assert_eq!(other, y);
    assert_eq!(other_attributes.parent, a);
    assert_eq!(change.from.hard_link_count, 2);
    assert_eq!(change.to.hard_link_count, 3);
    assert!(matches!(
        rename(&engine, a, b"x", b, b"missing", RenameMode::Exchange),
        Err(RenameError::NotFound)
    ));
}

#[test]
fn exchange_refuses_moving_a_directory_below_itself() {
    let engine = new_test_engine();
    let a = directory(&engine, Inode::ROOT, b"a");
    directory(&engine, a, b"b");
    assert!(matches!(
        rename(&engine, a, b"b", Inode::ROOT, b"a", RenameMode::Exchange),
        Err(RenameError::InvalidInput)
    ));
}

#[test]
fn renaming_onto_another_name_of_the_same_inode_does_nothing() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    hard_link(&engine, a, Inode::ROOT, b"b").unwrap();
    let before = getattr(&engine, Inode::ROOT).unwrap();
    rename(
        &engine,
        Inode::ROOT,
        b"a",
        Inode::ROOT,
        b"b",
        RenameMode::Replace,
    )
    .unwrap();
    assert_eq!(getattr(&engine, Inode::ROOT).unwrap(), before);
    assert_eq!(named(&engine, Inode::ROOT, b"a"), Some(a));
    assert_eq!(named(&engine, Inode::ROOT, b"b"), Some(a));
}

#[test]
fn rename_of_a_missing_name_is_not_found() {
    let engine = new_test_engine();
    assert!(matches!(
        rename(
            &engine,
            Inode::ROOT,
            b"a",
            Inode::ROOT,
            b"b",
            RenameMode::Replace
        ),
        Err(RenameError::NotFound)
    ));
}

#[test]
fn colliding_names_take_the_next_free_position() {
    let engine = new_test_engine();
    let (first, _) = positions_of(b"x");
    let occupant = file(&engine, Inode::ROOT, b"occupant");
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            let result = txn.add_name(
                Inode::ROOT,
                first.max(FIRST_POSITION),
                b"collides with x",
                occupant,
                FileType::File,
            );
            result.map(|result| (result, txn))
        })
    }))
    .unwrap();
    let x = file(&engine, Inode::ROOT, b"x");
    assert_eq!(named(&engine, Inode::ROOT, b"x"), Some(x));
    let positions: Vec<u64> = readdir(&engine, Inode::ROOT, 0, usize::MAX)
        .unwrap()
        .into_iter()
        .filter(|entry| entry.name == "x" || entry.name == "collides with x")
        .map(|entry| entry.position)
        .collect();
    assert_eq!(
        positions,
        vec![first.max(FIRST_POSITION), first.max(FIRST_POSITION) + 1]
    );
}

#[test]
fn a_full_collision_range_is_storage_full() {
    let engine = new_test_engine();
    let (first, last) = positions_of(b"x");
    let occupant = directory(&engine, Inode::ROOT, b"occupant");
    block_on(engine.transact(|mut txn| {
        Box::pin(async move {
            for position in first.max(FIRST_POSITION)..=last {
                txn.add_name(
                    Inode::ROOT,
                    position,
                    b"collides with x",
                    occupant,
                    FileType::Directory,
                )?;
            }
            Ok::<_, StoreError<Infallible, MetadataFailure>>(((), txn))
        })
    }))
    .unwrap();
    assert!(matches!(
        create(&engine, Inode::ROOT, b"x", NewItem::File),
        Err(CreateError::StorageFull)
    ));
}

#[test]
fn no_replace_refuses_any_existing_name_even_one_of_the_same_inode() {
    let engine = new_test_engine();
    let a = file(&engine, Inode::ROOT, b"a");
    hard_link(&engine, a, Inode::ROOT, b"b").unwrap();
    directory(&engine, Inode::ROOT, b"d");
    for (name, new_name) in [(&b"a"[..], &b"b"[..]), (b"a", b"a"), (b"d", b"d")] {
        assert!(
            matches!(
                rename(
                    &engine,
                    Inode::ROOT,
                    name,
                    Inode::ROOT,
                    new_name,
                    RenameMode::NoReplace
                ),
                Err(RenameError::AlreadyExists)
            ),
            "{name:?} onto {new_name:?}"
        );
    }
}

#[test]
fn existence_errors_come_before_the_subtree_check() {
    let engine = new_test_engine();
    let d = directory(&engine, Inode::ROOT, b"d");
    let b = directory(&engine, d, b"b");
    file(&engine, b, b"x");
    assert!(matches!(
        rename(&engine, Inode::ROOT, b"d", b, b"x", RenameMode::NoReplace),
        Err(RenameError::AlreadyExists)
    ));
    assert!(matches!(
        rename(
            &engine,
            Inode::ROOT,
            b"d",
            b,
            b"missing",
            RenameMode::Exchange
        ),
        Err(RenameError::NotFound)
    ));
}

#[test]
fn replacing_a_directory_that_contains_the_source_is_not_empty() {
    let engine = new_test_engine();
    let p = directory(&engine, Inode::ROOT, b"p");
    let q = directory(&engine, p, b"q");
    file(&engine, p, b"f");
    file(&engine, q, b"g");
    assert!(matches!(
        rename(&engine, p, b"f", Inode::ROOT, b"p", RenameMode::Replace),
        Err(RenameError::DirectoryNotEmpty)
    ));
    assert!(matches!(
        rename(&engine, q, b"g", Inode::ROOT, b"p", RenameMode::Replace),
        Err(RenameError::DirectoryNotEmpty)
    ));
}

#[test]
fn hard_link_reports_an_existing_name_before_a_directory_source() {
    let engine = new_test_engine();
    let d = directory(&engine, Inode::ROOT, b"d");
    file(&engine, Inode::ROOT, b"f");
    assert!(matches!(
        hard_link(&engine, d, Inode::ROOT, b"f"),
        Err(HardLinkError::AlreadyExists)
    ));
}
