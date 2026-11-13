use std::fmt::Debug;
use std::ops::Bound;

use futures_lite::future::block_on;
use futures_util::future::try_join_all;
use stratafs_store::kv::{KeyValueStore, TransactError};
use stratafs_store::store::Store;

use super::*;
use crate::PurgeWaits;
use crate::annotations::{AnnotationKey, AnnotationScope, SetAnnotationMode};
use crate::directories::{NewItem, RenameMode};
use crate::key::every_key_of;
use crate::test_support::{InMemoryEngine, new_test_engine};

// done is the result of an operation whose transaction ran.
fn done<T, R: Debug, F: Debug, E>(result: Result<T, TransactError<R, F, E>>) -> Result<T, E> {
    result.map_err(crate::test_support::aborted)
}

// refused says whether an operation failed because no trusted signer vouches for what the store returned.
fn refused<T, E: Debug>(result: Result<T, E>) -> bool {
    result.is_err_and(|error| format!("{error:?}").contains("Unverified"))
}

fn create(engine: &InMemoryEngine, dir: Inode, name: &'static [u8], item: NewItem) -> Inode {
    let created = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        let item = item.clone();
        Box::pin(async move {
            let result = txn.create(dir, name, item, |_| {}).await;
            result.map(|result| (result, txn))
        })
    }));
    done(created).unwrap().inode
}

fn write(engine: &InMemoryEngine, inode: Inode, offset: u64, content: &[u8]) -> bool {
    let content = Bytes::copy_from_slice(content);
    let written = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        let content = content.clone();
        Box::pin(async move {
            let result = txn.write(inode, offset, content).await;
            result.map(|result| (result, txn))
        })
    }));
    !refused(done(written))
}

fn set_annotation(
    engine: &InMemoryEngine,
    inode: Inode,
    name: &'static [u8],
    value: &'static [u8],
) {
    let set = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let key = AnnotationKey::Portable(name);
            let mode = SetAnnotationMode::CreateOrReplace;
            let result = txn.set_annotation(inode, key, value, mode).await;
            result.map(|result| (result, txn))
        })
    }));
    done(set).unwrap();
}

// The reads below say whether the engine read what was asked for: Ok with what it read, or Err(()) when it refused
// what the store returned.

fn attributes(engine: &InMemoryEngine, inode: Inode) -> Result<Option<Attributes>, ()> {
    let read =
        block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })));
    let read = done(read);
    assert!(read.is_ok() || refused(read.as_ref()), "{read:?}");
    read.map_err(|_| ())
}

fn content(engine: &InMemoryEngine, inode: Inode, size: u64) -> Result<Bytes, ()> {
    let plan = block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.read(inode, 0, size).await })),
    );
    let plan = done(plan);
    assert!(plan.is_ok() || refused(plan.as_ref()), "{:?}", plan.err());
    let pieces = block_on(try_join_all(plan.map_err(|_| ())?.load(engine.content()))).unwrap();
    Ok(Bytes::from(pieces.concat()))
}

fn named(engine: &InMemoryEngine, dir: Inode, name: &'static [u8]) -> Result<Option<Inode>, ()> {
    let found =
        block_on(engine.read_transact(|txn| Box::pin(async move { txn.lookup(dir, name).await })));
    let found = done(found);
    assert!(
        found.is_ok() || refused(found.as_ref()),
        "{:?}",
        found.err()
    );
    found
        .map(|found| found.found.map(|(inode, _)| inode))
        .map_err(|_| ())
}

fn names(engine: &InMemoryEngine, dir: Inode, limit: usize) -> Result<Vec<Bytes>, ()> {
    let listed = block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.readdir(dir, 0, limit).await })),
    );
    let listed = done(listed);
    assert!(
        listed.is_ok() || refused(listed.as_ref()),
        "{:?}",
        listed.err()
    );
    listed
        .map(|entries| entries.into_iter().map(|entry| entry.name).collect())
        .map_err(|_| ())
}

fn annotation(
    engine: &InMemoryEngine,
    inode: Inode,
    name: &'static [u8],
) -> Result<Option<Bytes>, ()> {
    let read = block_on(engine.read_transact(|txn| {
        Box::pin(async move {
            txn.get_annotation(inode, AnnotationKey::Portable(name))
                .await
        })
    }));
    let read = done(read);
    assert!(read.is_ok() || refused(read.as_ref()), "{:?}", read.err());
    read.map_err(|_| ())
}

fn annotations(engine: &InMemoryEngine, inode: Inode) -> Result<Vec<Bytes>, ()> {
    let listed = block_on(engine.read_transact(|txn| {
        Box::pin(async move { txn.list_annotations(inode, AnnotationScope::Portable).await })
    }));
    let listed = done(listed);
    assert!(
        listed.is_ok() || refused(listed.as_ref()),
        "{:?}",
        listed.err()
    );
    listed.map_err(|_| ())
}

fn target(engine: &InMemoryEngine, inode: Inode) -> Result<Bytes, ()> {
    let read = block_on(
        engine.read_transact(|txn| Box::pin(async move { txn.read_symbolic_link(inode).await })),
    );
    let read = done(read);
    assert!(read.is_ok() || refused(read.as_ref()), "{:?}", read.err());
    read.map_err(|_| ())
}

// The functions below change the store behind the engine, as a store that is not trusted may.

// entries_in is every entry of metadata with a key in range. It returns `impl Future + Send` so Send is proven here,
// for every store.
#[allow(clippy::manual_async_fn)]
fn entries_in<T: ReadKeyValueTransaction>(
    metadata: &T,
    range: (Bound<Bytes>, Bound<Bytes>),
) -> impl Future<Output = Result<Vec<(Bytes, Bytes)>, T::Failure>> + Send + '_ {
    async move { Ok(metadata.get_range(range, usize::MAX).await?.collect()) }
}

fn stored(engine: &InMemoryEngine, range: (Bound<Bytes>, Bound<Bytes>)) -> Vec<(Bytes, Bytes)> {
    let read = block_on(engine.mount.store.metadata().read_transact(|metadata| {
        let range = range.clone();
        Box::pin(async move { entries_in(&*metadata, range).await.map_err(|_| ()) })
    }));
    done(read).unwrap()
}

fn stored_at(engine: &InMemoryEngine, key: FsKey<'_>) -> Bytes {
    let key = Bytes::from(key);
    let range = (Bound::Included(key.clone()), Bound::Included(key));
    let (_, value) = stored(engine, range).pop().expect("the key is stored");
    value
}

// replace sets each key to its value, or removes it for None.
fn replace(engine: &InMemoryEngine, entries: Vec<(Bytes, Option<Bytes>)>) {
    let replaced = block_on(engine.mount.store.metadata().transact(|mut metadata| {
        let entries = entries.clone();
        Box::pin(async move {
            for (key, value) in entries {
                match value {
                    Some(value) => metadata.set(key, value).map_err(|_| ())?,
                    None => metadata.clear(key).map_err(|_| ())?,
                }
            }
            Ok::<_, ()>(((), metadata))
        })
    }));
    done(replaced).unwrap();
}

fn set(engine: &InMemoryEngine, key: FsKey<'_>, value: Bytes) {
    replace(engine, vec![(key.into(), Some(value))]);
}

fn remove(engine: &InMemoryEngine, key: FsKey<'_>) {
    replace(engine, vec![(key.into(), None)]);
}

// altered is value with its last byte changed.
fn altered(value: &Bytes) -> Bytes {
    let mut altered = value.to_vec();
    *altered.last_mut().expect("the value is not empty") ^= 1;
    Bytes::from(altered)
}

// restore puts every key of inode back as entries had it.
fn restore(engine: &InMemoryEngine, inode: Inode, entries: &[(Bytes, Bytes)]) {
    let mut replaced: Vec<(Bytes, Option<Bytes>)> = stored(engine, every_key_of(inode))
        .into_iter()
        .map(|(key, _)| (key, None))
        .collect();
    replaced.extend(
        entries
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone()))),
    );
    replace(engine, replaced);
}

#[test]
fn what_the_engine_writes_reads_back() {
    let engine = new_test_engine();
    let file = create(&engine, Inode::ROOT, b"file", NewItem::File);
    assert!(write(&engine, file, 0, b"0123456789"));
    assert!(write(&engine, file, 4, b"ab"));
    set_annotation(&engine, file, b"one", b"1");
    set_annotation(&engine, file, b"two", b"2");
    let link = create(
        &engine,
        Inode::ROOT,
        b"link",
        NewItem::SymbolicLink(Bytes::from_static(b"file")),
    );

    assert!(attributes(&engine, file).unwrap().is_some());
    assert_eq!(content(&engine, file, 10).unwrap(), "0123ab6789");
    assert_eq!(named(&engine, Inode::ROOT, b"file"), Ok(Some(file)));
    assert_eq!(named(&engine, Inode::ROOT, b"missing"), Ok(None));
    assert_eq!(names(&engine, Inode::ROOT, usize::MAX).unwrap().len(), 2);
    assert_eq!(names(&engine, Inode::ROOT, 1).unwrap().len(), 1);
    assert_eq!(
        annotation(&engine, file, b"one"),
        Ok(Some(Bytes::from_static(b"1")))
    );
    assert_eq!(annotation(&engine, file, b"three"), Ok(None));
    assert_eq!(annotations(&engine, file).unwrap().len(), 2);
    assert_eq!(target(&engine, link).unwrap(), "file");
}

#[test]
fn attributes_nobody_signed_are_refused() {
    let engine = new_test_engine();
    let file = create(&engine, Inode::ROOT, b"file", NewItem::File);
    let other = create(&engine, Inode::ROOT, b"other", NewItem::File);
    let signed = stored(&engine, every_key_of(file));

    let attributes_of = |inode| stored_at(&engine, FsKey::new_attributes(inode));
    set(
        &engine,
        FsKey::new_attributes(file),
        altered(&attributes_of(file)),
    );
    assert_eq!(attributes(&engine, file), Err(()));
    restore(&engine, file, &signed);
    assert!(attributes(&engine, file).is_ok());

    let verifier = stored_at(&engine, FsKey::new_signature(file));
    set(&engine, FsKey::new_signature(file), altered(&verifier));
    assert_eq!(attributes(&engine, file), Err(()));
    remove(&engine, FsKey::new_signature(file));
    assert_eq!(attributes(&engine, file), Err(()));
    restore(&engine, file, &signed);

    // Another inode's attributes and signature are signed, but not for this inode.
    set(&engine, FsKey::new_attributes(file), attributes_of(other));
    let verifier = stored_at(&engine, FsKey::new_signature(other));
    set(&engine, FsKey::new_signature(file), verifier);
    assert_eq!(attributes(&engine, file), Err(()));
}

#[test]
fn attributes_from_before_a_change_are_refused_with_the_signature_after_it() {
    let engine = new_test_engine();
    let file = create(&engine, Inode::ROOT, b"file", NewItem::File);
    let before = stored_at(&engine, FsKey::new_attributes(file));
    assert!(write(&engine, file, 0, b"content"));
    set(&engine, FsKey::new_attributes(file), before);
    assert_eq!(attributes(&engine, file), Err(()));
}

#[test]
fn file_content_nobody_wrote_is_refused() {
    let engine = new_test_engine();
    let file = create(&engine, Inode::ROOT, b"file", NewItem::File);
    assert!(write(&engine, file, 0, b"0123456789"));
    let first = stored(&engine, every_key_of(file));
    assert!(write(&engine, file, 0, b"abcdefghij"));
    let second = stored(&engine, every_key_of(file));
    let extent = FsKey::new_extent(file, 10);

    // An extent with other bytes, no extent, and the extent an earlier write left.
    set(
        &engine,
        FsKey::new_extent(file, 10),
        altered(&stored_at(&engine, FsKey::new_extent(file, 10))),
    );
    assert_eq!(content(&engine, file, 10), Err(()));
    remove(&engine, FsKey::new_extent(file, 10));
    assert_eq!(content(&engine, file, 10), Err(()));
    let earlier = first
        .iter()
        .find(|(key, _)| *key == Bytes::from(FsKey::new_extent(file, 10)));
    replace(
        &engine,
        vec![(extent.into(), Some(earlier.unwrap().1.clone()))],
    );
    assert_eq!(content(&engine, file, 10), Err(()));

    // An extent nobody wrote, past the ones there are.
    restore(&engine, file, &second);
    assert_eq!(content(&engine, file, 10).unwrap(), "abcdefghij");
    set(
        &engine,
        FsKey::new_extent(file, 20),
        stored_at(&engine, FsKey::new_extent(file, 10)),
    );
    assert_eq!(content(&engine, file, 30), Err(()));
    // A write reads the extents it overlaps, so it is refused too, and signs nothing.
    assert!(!write(&engine, file, 15, b"more"));
}

// The server may serve an old version of a whole inode, which nothing short of signing the whole volume's state on
// every write would tell from the latest.
#[test]
fn every_key_of_an_inode_as_an_earlier_write_left_it_reads_as_that_write() {
    let engine = new_test_engine();
    let file = create(&engine, Inode::ROOT, b"file", NewItem::File);
    assert!(write(&engine, file, 0, b"0123456789"));
    let first = stored(&engine, every_key_of(file));
    assert!(write(&engine, file, 0, b"abcdefghij"));
    restore(&engine, file, &first);
    assert_eq!(content(&engine, file, 10).unwrap(), "0123456789");
}

#[test]
fn a_directory_whose_entries_nobody_wrote_is_refused() {
    let engine = new_test_engine();
    let dir = create(&engine, Inode::ROOT, b"dir", NewItem::Directory);
    let file = create(&engine, dir, b"file", NewItem::File);
    create(&engine, dir, b"other", NewItem::File);
    let signed = stored(&engine, every_key_of(dir));
    let entry = |name: &[u8]| {
        let found = signed.iter().find(|(_, value)| value.ends_with(name));
        found.expect("the directory has the entry").clone()
    };

    // A name hidden.
    replace(&engine, vec![(entry(b"file").0, None)]);
    assert_eq!(named(&engine, dir, b"file"), Err(()));
    assert_eq!(names(&engine, dir, usize::MAX), Err(()));
    restore(&engine, dir, &signed);
    assert_eq!(named(&engine, dir, b"file"), Ok(Some(file)));

    // A name that points at another inode.
    replace(&engine, vec![(entry(b"file").0, Some(entry(b"other").1))]);
    assert_eq!(named(&engine, dir, b"file"), Err(()));
    restore(&engine, dir, &signed);

    // A name nobody made, and a name that was removed.
    let made = (
        Bytes::from(FsKey::new_directory_entry(dir, 5)),
        entry(b"file").1,
    );
    replace(&engine, vec![(made.0.clone(), Some(made.1))]);
    assert_eq!(names(&engine, dir, usize::MAX), Err(()));
    restore(&engine, dir, &signed);
    let unlinked = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn.hard_unlink(dir, b"file", None).await;
            result.map(|result| (result, txn))
        })
    }));
    done(unlinked).unwrap();
    assert_eq!(named(&engine, dir, b"file"), Ok(None));
    replace(&engine, vec![(entry(b"file").0, Some(entry(b"file").1))]);
    assert_eq!(named(&engine, dir, b"file"), Err(()));
}

#[test]
fn a_listing_cut_short_vouches_for_the_entries_it_returns() {
    let engine = new_test_engine();
    let dir = create(&engine, Inode::ROOT, b"dir", NewItem::Directory);
    for name in [b"a", b"b", b"c", b"d"] {
        create(&engine, dir, name, NewItem::File);
    }
    let first_two = names(&engine, dir, 2).unwrap();
    let entries = stored(&engine, every_key_of(dir));
    let second = entries
        .iter()
        .find(|(_, value)| value.ends_with(&first_two[1]));
    replace(&engine, vec![(second.unwrap().0.clone(), None)]);
    assert_eq!(names(&engine, dir, 2), Err(()));
}

#[test]
fn annotations_nobody_wrote_are_refused() {
    let engine = new_test_engine();
    let file = create(&engine, Inode::ROOT, b"file", NewItem::File);
    set_annotation(&engine, file, b"one", b"1");
    set_annotation(&engine, file, b"two", b"2");
    let signed = stored(&engine, every_key_of(file));
    let key = || FsKey::new_annotation(file, AnnotationKey::Portable(b"one"));

    set(&engine, key(), altered(&stored_at(&engine, key())));
    assert_eq!(annotation(&engine, file, b"one"), Err(()));
    assert_eq!(annotations(&engine, file), Err(()));
    remove(&engine, key());
    assert_eq!(annotation(&engine, file, b"one"), Err(()));
    assert_eq!(annotations(&engine, file), Err(()));
    restore(&engine, file, &signed);
    assert_eq!(annotations(&engine, file).unwrap().len(), 2);

    let made = FsKey::new_annotation(file, AnnotationKey::Portable(b"three"));
    set(&engine, made, stored_at(&engine, key()));
    assert_eq!(annotation(&engine, file, b"three"), Err(()));
    assert_eq!(annotations(&engine, file), Err(()));
}

#[test]
fn a_symbolic_link_target_nobody_wrote_is_refused() {
    let engine = new_test_engine();
    let link = create(
        &engine,
        Inode::ROOT,
        b"link",
        NewItem::SymbolicLink(Bytes::from_static(b"file")),
    );
    let key = || FsKey::new_symbolic_link_target(link);
    set(&engine, key(), altered(&stored_at(&engine, key())));
    assert_eq!(target(&engine, link), Err(()));
}

#[test]
fn a_file_renamed_and_linked_keeps_names_that_are_vouched_for() {
    let engine = new_test_engine();
    let dir = create(&engine, Inode::ROOT, b"dir", NewItem::Directory);
    let file = create(&engine, Inode::ROOT, b"file", NewItem::File);
    let changed = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            txn.hard_link(file, dir, b"linked")
                .await
                .map_err(|error| format!("{error:?}"))?;
            let renamed = txn.rename(
                Inode::ROOT,
                b"file",
                dir,
                b"moved",
                RenameMode::NoReplace,
                None,
            );
            renamed.await.map_err(|error| format!("{error:?}"))?;
            let unlinked = txn.hard_unlink(dir, b"linked", None).await;
            unlinked.map_err(|error| format!("{error:?}"))?;
            Ok::<_, String>(((), txn))
        })
    }));
    done(changed).unwrap();
    assert_eq!(named(&engine, Inode::ROOT, b"file"), Ok(None));
    assert_eq!(named(&engine, dir, b"moved"), Ok(Some(file)));
    assert_eq!(named(&engine, dir, b"linked"), Ok(None));
    assert_eq!(
        attributes(&engine, file).unwrap().unwrap().hard_link_count,
        1
    );
}

#[test]
fn a_position_is_its_kind_then_its_number_in_order() {
    assert_eq!(RecordKind::Content.at(0x0102), [0, 0, 0, 0, 0, 0, 0, 1, 2]);
    assert_eq!(
        RecordKind::Name.at(u64::MAX),
        [2, 255, 255, 255, 255, 255, 255, 255, 255]
    );
    assert!(RecordKind::Content.at(u64::MAX) < RecordKind::Annotation.at(0));
    assert!(RecordKind::Content.at(255) < RecordKind::Content.at(256));
    let hashed = RecordKind::Annotation.of::<sha2::Sha256>(b"key");
    assert_eq!(hashed[0], 1);
    assert_eq!(hashed[1..], sha2::Sha256::digest(b"key")[..8]);
}
