use std::collections::{BTreeMap, BTreeSet};
use std::future::poll_fn;
use std::pin::pin;
use std::task::Poll;

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::future::join;
use stratafs_store::metadata::{Entry, KeyValue, MetadataStore, Seq, Stored, VolumeId};
use stratafs_store::transaction::{DeleteKey, Op, SetKey};
use vec1::Vec1;

use crate::support::{
    blob, collect_entries, collect_keys, create, delete, delete_entry, fork, head, key, key_bytes,
    listed, non_empty, numbered, pairs, set, set_entry, transact, truncate, truncate_entry, value,
    write_at, write_entry,
};

pub async fn entries_from_zero_lists_whole_history<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    write_at(store, &volume, "f", 2, 3, x, 4).await;
    delete(store, &volume, "a").await;
    truncate(store, &volume, "f", 1).await;
    set(store, &volume, "b", "").await;
    assert_eq!(
        collect_entries(store, &volume, 0).await,
        numbered(
            1,
            vec![
                set_entry("a", "1"),
                write_entry("f", 2, 3, x, 4),
                delete_entry("a"),
                truncate_entry("f", 1),
                set_entry("b", ""),
            ]
        )
    );
}

pub async fn entries_from_middle<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    for index in 1..=6 {
        set(store, &volume, &format!("k{index}"), "v").await;
    }
    let all = collect_entries(store, &volume, 0).await;
    assert_eq!(all.len(), 6);
    for after in 0..=6_usize {
        assert_eq!(
            collect_entries(store, &volume, after as u64).await,
            all[after..].to_vec(),
            "entries after {after}"
        );
    }
}

pub async fn entries_at_or_after_head_yield_nothing<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    assert!(collect_entries(store, &volume, 0).await.is_empty());
    assert!(collect_entries(store, &volume, 5).await.is_empty());
    set(store, &volume, "a", "1").await;
    set(store, &volume, "b", "1").await;
    for after in [2, 3, 100, u64::MAX] {
        assert!(
            collect_entries(store, &volume, after).await.is_empty(),
            "entries after {after}"
        );
    }
    let child = fork(store, &volume, 1).await;
    assert!(collect_entries(store, &child, 1).await.is_empty());
    assert!(collect_entries(store, &child, 2).await.is_empty());
}

pub async fn entries_across_fork_boundary<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "p").await;
    set(store, &parent, "b", "p").await;
    set(store, &parent, "c", "p").await;
    let child = fork(store, &parent, 2).await;
    set(store, &child, "d", "c").await;
    delete(store, &child, "a").await;
    let expected = numbered(
        1,
        vec![
            set_entry("a", "p"),
            set_entry("b", "p"),
            set_entry("d", "c"),
            delete_entry("a"),
        ],
    );
    for after in 0..=4_usize {
        assert_eq!(
            collect_entries(store, &child, after as u64).await,
            expected[after..].to_vec(),
            "entries after {after}"
        );
    }
    assert_eq!(
        collect_entries(store, &parent, 0).await,
        numbered(
            1,
            vec![
                set_entry("a", "p"),
                set_entry("b", "p"),
                set_entry("c", "p")
            ]
        )
    );
}

pub async fn entries_across_nested_forks<S: MetadataStore>(store: &S) {
    let grandparent = create(store).await;
    for name in ["g1", "g2", "g3"] {
        set(store, &grandparent, name, "g").await;
    }
    let parent = fork(store, &grandparent, 2).await;
    set(store, &parent, "p3", "p").await;
    set(store, &parent, "p4", "p").await;
    set(store, &grandparent, "g4", "g").await;
    let child = fork(store, &parent, 3).await;
    set(store, &child, "c4", "c").await;
    set(store, &child, "c5", "c").await;
    set(store, &parent, "p5", "p").await;
    let expected = numbered(
        1,
        vec![
            set_entry("g1", "g"),
            set_entry("g2", "g"),
            set_entry("p3", "p"),
            set_entry("c4", "c"),
            set_entry("c5", "c"),
        ],
    );
    for after in 0..=6_usize {
        let start = after.min(expected.len());
        assert_eq!(
            collect_entries(store, &child, after as u64).await,
            expected[start..].to_vec(),
            "entries after {after}"
        );
    }
}

pub async fn list_keys_is_in_ascending_order<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    for name in ["m", "c", "x", "a", "k", "ca", "b"] {
        set(store, &volume, name, name).await;
    }
    let listing = collect_keys(store, &volume, &key("")).await;
    let expected: Vec<Stored<KeyValue>> = [
        ("a", 4),
        ("b", 7),
        ("c", 2),
        ("ca", 6),
        ("k", 5),
        ("m", 1),
        ("x", 3),
    ]
    .into_iter()
    .map(|(name, seq)| Stored {
        seq: Seq(seq),
        entry: KeyValue {
            key: key(name),
            value: value(name),
        },
    })
    .collect();
    assert_eq!(listing, expected);
}

pub async fn list_keys_filters_by_prefix<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    for name in ["a", "a/1", "a/2", "ab", "b/1", "b", "a/1/x"] {
        set(store, &volume, name, "v").await;
    }
    let names = |items: Vec<(String, String)>| -> Vec<String> {
        items.into_iter().map(|(name, _)| name).collect()
    };
    assert_eq!(
        names(listed(store, &volume, "a/").await),
        ["a/1", "a/1/x", "a/2"]
    );
    assert_eq!(
        names(listed(store, &volume, "a").await),
        ["a", "a/1", "a/1/x", "a/2", "ab"]
    );
    assert_eq!(names(listed(store, &volume, "b/").await), ["b/1"]);
    assert_eq!(names(listed(store, &volume, "a/1").await), ["a/1", "a/1/x"]);
    assert_eq!(names(listed(store, &volume, "a/1/").await), ["a/1/x"]);
    assert!(listed(store, &volume, "c").await.is_empty());
    assert!(listed(store, &volume, "a/3").await.is_empty());
    assert!(listed(store, &volume, "ab/").await.is_empty());
    assert_eq!(listed(store, &volume, "").await.len(), 7);
}

pub async fn list_keys_skips_deleted_keys<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    set(store, &volume, "b", "2").await;
    set(store, &volume, "c", "3").await;
    delete(store, &volume, "b").await;
    assert_eq!(
        collect_keys(store, &volume, &key("")).await,
        vec![
            Stored {
                seq: Seq(1),
                entry: KeyValue {
                    key: key("a"),
                    value: value("1")
                }
            },
            Stored {
                seq: Seq(3),
                entry: KeyValue {
                    key: key("c"),
                    value: value("3")
                }
            },
        ]
    );
    delete(store, &volume, "a").await;
    delete(store, &volume, "c").await;
    assert!(listed(store, &volume, "").await.is_empty());
    set(store, &volume, "b", "again").await;
    assert_eq!(listed(store, &volume, "").await, pairs(&[("b", "again")]));
}

pub async fn list_keys_merges_fork_overlay<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    for name in ["a", "b", "c", "d"] {
        set(store, &parent, name, "p").await;
    }
    let child = fork(store, &parent, 4).await;
    set(store, &child, "b", "child").await;
    delete(store, &child, "c").await;
    set(store, &child, "e", "child").await;
    set(store, &child, "0", "child").await;
    set(store, &parent, "a", "parent2").await;
    delete(store, &parent, "d").await;
    set(store, &parent, "f", "parent").await;

    let stored = |name: &str, text: &str, seq: u64| Stored {
        seq: Seq(seq),
        entry: KeyValue {
            key: key(name),
            value: value(text),
        },
    };
    assert_eq!(
        collect_keys(store, &child, &key("")).await,
        vec![
            stored("0", "child", 8),
            stored("a", "p", 1),
            stored("b", "child", 5),
            stored("d", "p", 4),
            stored("e", "child", 7),
        ]
    );
    assert_eq!(
        collect_keys(store, &parent, &key("")).await,
        vec![
            stored("a", "parent2", 5),
            stored("b", "p", 2),
            stored("c", "p", 3),
            stored("f", "parent", 7),
        ]
    );
    let grandchild = fork(store, &child, 6).await;
    delete(store, &grandchild, "d").await;
    set(store, &grandchild, "c", "grandchild").await;
    assert_eq!(
        collect_keys(store, &grandchild, &key("")).await,
        vec![
            stored("a", "p", 1),
            stored("b", "child", 5),
            stored("c", "grandchild", 8),
        ]
    );
    assert_eq!(
        collect_keys(store, &grandchild, &key("c")).await,
        vec![stored("c", "grandchild", 8)]
    );
    assert_eq!(
        collect_keys(store, &child, &key("d")).await,
        vec![stored("d", "p", 4)]
    );
}

#[derive(Default)]
pub(crate) struct KeyModel {
    pub(crate) head: u64,
    pub(crate) keys: BTreeMap<Vec<u8>, (Bytes, u64)>,
}

impl KeyModel {
    pub(crate) async fn set_many<S: MetadataStore>(
        &mut self,
        store: &S,
        volume: &VolumeId,
        pairs: Vec<(Vec<u8>, Bytes)>,
    ) {
        for chunk in pairs.chunks(250) {
            let operations: Vec<SetKey> = chunk
                .iter()
                .map(|(name, text)| SetKey {
                    key: key_bytes(name),
                    value: text.clone(),
                })
                .collect();
            transact(store, volume, non_empty(operations)).await;
            for (name, text) in chunk {
                self.head += 1;
                self.keys.insert(name.clone(), (text.clone(), self.head));
            }
        }
        assert_eq!(head(store, volume).await, Seq(self.head));
    }

    pub(crate) async fn delete_many<S: MetadataStore>(
        &mut self,
        store: &S,
        volume: &VolumeId,
        names: Vec<Vec<u8>>,
    ) {
        for chunk in names.chunks(250) {
            let operations: Vec<DeleteKey> = chunk
                .iter()
                .map(|name| DeleteKey {
                    key: key_bytes(name),
                })
                .collect();
            transact(store, volume, non_empty(operations)).await;
            for name in chunk {
                if self.keys.remove(name).is_some() {
                    self.head += 1;
                }
            }
        }
        assert_eq!(head(store, volume).await, Seq(self.head));
    }

    pub(crate) fn listing(&self, prefix: &[u8]) -> Vec<Stored<KeyValue>> {
        self.keys
            .iter()
            .filter(|(name, _)| name.starts_with(prefix))
            .map(|(name, (text, seq))| Stored {
                seq: Seq(*seq),
                entry: KeyValue {
                    key: key_bytes(name),
                    value: text.clone(),
                },
            })
            .collect()
    }

    pub(crate) async fn check<S: MetadataStore>(
        &self,
        store: &S,
        volume: &VolumeId,
        prefixes: &[&[u8]],
    ) {
        for prefix in prefixes {
            let listing = collect_keys(store, volume, &key_bytes(prefix)).await;
            let expected = self.listing(prefix);
            let first_difference = listing
                .iter()
                .zip(&expected)
                .position(|(listed, wanted)| listed != wanted);
            assert!(
                first_difference.is_none() && listing.len() == expected.len(),
                "listing of prefix {prefix:?} gave {} keys, expected {}; first difference at {first_difference:?}: listed {:?}, expected {:?}",
                listing.len(),
                expected.len(),
                first_difference.and_then(|position| listing.get(position)),
                first_difference.and_then(|position| expected.get(position)),
            );
        }
    }
}

pub(crate) fn numbered_key(group: &str, index: u32) -> Vec<u8> {
    format!("{group}/{index:04}").into_bytes()
}

pub async fn list_keys_pages_through_many_keys<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    let mut model = KeyModel::default();
    let mut pairs: Vec<(Vec<u8>, Bytes)> = (0..1500)
        .map(|index| {
            (
                numbered_key("k", index),
                Bytes::from(format!("value-{index}")),
            )
        })
        .collect();
    pairs.push((b"j".to_vec(), value("before")));
    pairs.push((b"l".to_vec(), value("after")));
    fastrand::Rng::with_seed(7).shuffle(&mut pairs);
    model.set_many(store, &volume, pairs).await;
    model
        .check(store, &volume, &[b"", b"k/", b"k/1", b"k/04", b"j", b"m"])
        .await;
    assert_eq!(collect_keys(store, &volume, &key("k/")).await.len(), 1500);

    let deleted: Vec<Vec<u8>> = (0..1500)
        .filter(|index| index % 3 == 0)
        .map(|index| numbered_key("k", index))
        .collect();
    model.delete_many(store, &volume, deleted).await;
    let overwritten: Vec<(Vec<u8>, Bytes)> = (0..1500)
        .filter(|index| index % 7 == 1)
        .map(|index| (numbered_key("k", index), value("overwritten")))
        .collect();
    model.set_many(store, &volume, overwritten).await;
    model
        .check(store, &volume, &[b"", b"k/", b"k/0", b"k/1", b"k/14"])
        .await;
}

pub async fn list_keys_pages_through_fork_overlay<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    let mut parent_model = KeyModel::default();
    let initial: Vec<(Vec<u8>, Bytes)> = (0..1200)
        .map(|index| (numbered_key("k", index), value("parent")))
        .collect();
    parent_model.set_many(store, &parent, initial).await;

    let child = fork(store, &parent, parent_model.head).await;
    let mut child_model = KeyModel {
        head: parent_model.head,
        keys: parent_model.keys.clone(),
    };
    let child_deleted: Vec<Vec<u8>> = (0..1200)
        .filter(|index| index % 4 == 0)
        .map(|index| numbered_key("k", index))
        .collect();
    child_model.delete_many(store, &child, child_deleted).await;
    let child_overrides: Vec<(Vec<u8>, Bytes)> = (0..1200)
        .filter(|index| index % 5 == 2)
        .map(|index| (numbered_key("k", index), value("child")))
        .collect();
    child_model.set_many(store, &child, child_overrides).await;
    let child_added: Vec<(Vec<u8>, Bytes)> = (0..400)
        .map(|index| {
            (
                format!("k/{:04}x", index * 3).into_bytes(),
                value("child-new"),
            )
        })
        .collect();
    child_model.set_many(store, &child, child_added).await;

    let parent_deleted: Vec<Vec<u8>> = (0..1200)
        .filter(|index| index % 6 == 1)
        .map(|index| numbered_key("k", index))
        .collect();
    parent_model
        .delete_many(store, &parent, parent_deleted)
        .await;
    let parent_added: Vec<(Vec<u8>, Bytes)> = (0..300)
        .map(|index| {
            (
                format!("k/{:04}p", index * 4).into_bytes(),
                value("parent-new"),
            )
        })
        .collect();
    parent_model.set_many(store, &parent, parent_added).await;

    let prefixes: [&[u8]; 5] = [b"", b"k/", b"k/0", b"k/11", b"k/0003"];
    child_model.check(store, &child, &prefixes).await;
    parent_model.check(store, &parent, &prefixes).await;

    let grandchild = fork(store, &child, child_model.head).await;
    let mut grandchild_model = KeyModel {
        head: child_model.head,
        keys: child_model.keys.clone(),
    };
    let grandchild_deleted: Vec<Vec<u8>> = (0..1200)
        .filter(|index| index % 2 == 1)
        .map(|index| numbered_key("k", index))
        .collect();
    grandchild_model
        .delete_many(store, &grandchild, grandchild_deleted)
        .await;
    grandchild_model.check(store, &grandchild, &prefixes).await;
    child_model.check(store, &child, &prefixes).await;
}

pub async fn watch_yields_current_head_immediately<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    for name in ["a", "b", "c"] {
        set(store, &volume, name, "v").await;
    }
    for after in [0, 1, 2] {
        let mut watch = pin!(store.watch(volume.clone(), Seq(after)));
        let first = watch.next().await;
        assert!(
            matches!(first, Some(Ok(Seq(3)))),
            "watch after {after} gave {first:?}"
        );
    }
    let child = fork(store, &volume, 2).await;
    let mut watch = pin!(store.watch(child.clone(), Seq(0)));
    let first = watch.next().await;
    assert!(matches!(first, Some(Ok(Seq(2)))), "{first:?}");
}

pub async fn watch_yields_after_append<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    let mut watch = pin!(store.watch(volume.clone(), Seq(1)));
    let (first, appended) = join(watch.next(), set(store, &volume, "b", "1")).await;
    assert_eq!(appended, Seq(2));
    assert!(matches!(first, Some(Ok(Seq(2)))), "{first:?}");
    let (second, appended) = join(watch.next(), set(store, &volume, "c", "1")).await;
    assert_eq!(appended, Seq(3));
    assert!(matches!(second, Some(Ok(Seq(3)))), "{second:?}");

    let empty = create(store).await;
    let mut watch = pin!(store.watch(empty.clone(), Seq(0)));
    let (first, _) = join(watch.next(), set(store, &empty, "a", "1")).await;
    assert!(matches!(first, Some(Ok(Seq(1)))), "{first:?}");
}

pub async fn watch_values_strictly_increase<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    let mut watch = pin!(store.watch(volume.clone(), Seq(0)));
    let mut last = Seq(0);
    for round in 0..5 {
        let (item, current) = join(watch.next(), async {
            set(store, &volume, &format!("a{round}"), "v").await;
            set(store, &volume, &format!("b{round}"), "v").await
        })
        .await;
        match item {
            Some(Ok(seq)) => {
                assert!(seq > last, "watch went from {last} to {seq}");
                assert!(seq <= current, "watch yielded {seq} beyond head {current}");
                last = seq;
            }
            other => panic!("watch gave {other:?}"),
        }
    }
    let final_head = head(store, &volume).await;
    assert_eq!(final_head, Seq(10));
    while last < final_head {
        match watch.next().await {
            Some(Ok(seq)) => {
                assert!(seq > last, "watch went from {last} to {seq}");
                assert!(
                    seq <= final_head,
                    "watch yielded {seq} beyond head {final_head}"
                );
                last = seq;
            }
            other => panic!("watch gave {other:?}"),
        }
    }
}

pub async fn watch_on_fork_ignores_parent_appends<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    let child = fork(store, &parent, 1).await;
    let mut watch = pin!(store.watch(child.clone(), Seq(1)));
    let (item, ()) = join(watch.next(), async {
        set(store, &parent, "b", "1").await;
        set(store, &parent, "c", "1").await;
        set(store, &child, "d", "1").await;
    })
    .await;
    assert!(matches!(item, Some(Ok(Seq(2)))), "{item:?}");
    set(store, &parent, "e", "1").await;
    let (item, _) = join(watch.next(), set(store, &child, "f", "1")).await;
    assert!(matches!(item, Some(Ok(Seq(3)))), "{item:?}");
}

pub async fn watch_on_parent_ignores_fork_appends<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    let child = fork(store, &parent, 1).await;
    let mut watch = pin!(store.watch(parent.clone(), Seq(1)));
    let (item, ()) = join(watch.next(), async {
        set(store, &child, "b", "1").await;
        set(store, &child, "c", "1").await;
        set(store, &child, "d", "1").await;
        set(store, &parent, "e", "1").await;
    })
    .await;
    assert!(matches!(item, Some(Ok(Seq(2)))), "{item:?}");
}

fn churn_key(index: u32) -> Vec<u8> {
    format!("c/{index:05}").into_bytes()
}

async fn churn<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    deleted: &[u32],
    inserted: &[u32],
    overwritten: &[u32],
) {
    let deletions: Vec<DeleteKey> = deleted
        .iter()
        .map(|index| DeleteKey {
            key: key_bytes(&churn_key(*index)),
        })
        .collect();
    let insertions: Vec<SetKey> = inserted
        .iter()
        .map(|index| SetKey {
            key: key_bytes(&churn_key(*index)),
            value: value("inserted"),
        })
        .chain(overwritten.iter().map(|index| SetKey {
            key: key_bytes(&churn_key(*index)),
            value: value("changed"),
        }))
        .collect();
    transact(store, volume, (non_empty(deletions), non_empty(insertions))).await;
}

async fn assert_listing_tolerates_changes<S: MetadataStore>(
    store: &S,
    listed_volume: &VolumeId,
    writer: &VolumeId,
) {
    let initial: Vec<u32> = (0..2400).step_by(2).collect();
    let first_deleted: Vec<u32> = initial
        .iter()
        .copied()
        .filter(|index| index % 8 == 0 && (*index < 300 || *index > 1600))
        .collect();
    let first_inserted: Vec<u32> = (0..2400).filter(|index| index % 10 == 1).collect();
    let first_overwritten: Vec<u32> = initial
        .iter()
        .copied()
        .filter(|index| index % 6 == 2)
        .collect();
    let second_deleted: Vec<u32> = initial
        .iter()
        .copied()
        .filter(|index| index % 8 == 4)
        .collect();
    let second_inserted: Vec<u32> = (0..2600).filter(|index| index % 10 == 7).collect();
    let deleted: BTreeSet<u32> = first_deleted
        .iter()
        .chain(&second_deleted)
        .copied()
        .collect();
    let overwritten: BTreeSet<u32> = first_overwritten.iter().copied().collect();
    let existed: BTreeSet<Vec<u8>> = initial
        .iter()
        .chain(&first_inserted)
        .chain(&second_inserted)
        .map(|index| churn_key(*index))
        .collect();

    let mut yielded: Vec<Stored<KeyValue>> = Vec::new();
    {
        let prefix = key("c/");
        let mut listing = pin!(store.list_keys(listed_volume.clone(), prefix));
        for _ in 0..400 {
            match listing.next().await {
                Some(Ok(stored)) => yielded.push(stored),
                other => panic!("list_keys ended early with {other:?}"),
            }
        }
        churn(
            store,
            writer,
            &first_deleted,
            &first_inserted,
            &first_overwritten,
        )
        .await;
        for _ in 0..300 {
            match listing.next().await {
                Some(Ok(stored)) => yielded.push(stored),
                other => panic!("list_keys ended early with {other:?}"),
            }
        }
        churn(store, writer, &second_deleted, &second_inserted, &[]).await;
        while let Some(item) = listing.next().await {
            match item {
                Ok(stored) => yielded.push(stored),
                Err(error) => panic!("list_keys failed: {error:?}"),
            }
        }
    }

    for pair in yielded.windows(2) {
        assert!(
            pair[0].entry.key < pair[1].entry.key,
            "list_keys yielded {:?} before {:?}",
            pair[0].entry.key,
            pair[1].entry.key
        );
    }
    let yielded_keys: BTreeSet<Vec<u8>> = yielded
        .iter()
        .map(|stored| stored.entry.key.0.to_vec())
        .collect();
    for stored in &yielded {
        assert!(
            existed.contains(stored.entry.key.0.as_ref()),
            "list_keys yielded {:?}, which never existed",
            stored.entry.key
        );
    }
    for index in initial.iter().filter(|index| !deleted.contains(index)) {
        let name = churn_key(*index);
        assert!(
            yielded_keys.contains(&name),
            "list_keys skipped {}, which was live the whole time",
            String::from_utf8_lossy(&name)
        );
        let stored = yielded
            .iter()
            .find(|stored| stored.entry.key.0.as_ref() == name.as_slice());
        let allowed: &[&str] = if overwritten.contains(index) {
            &["initial", "changed"]
        } else {
            &["initial"]
        };
        assert!(
            stored
                .is_some_and(|stored| allowed.iter().any(|text| stored.entry.value == value(text))),
            "list_keys yielded {stored:?} for {}",
            String::from_utf8_lossy(&name)
        );
    }
}

async fn seed_churn_keys<S: MetadataStore>(store: &S, volume: &VolumeId) {
    let operations: Vec<SetKey> = (0..2400)
        .step_by(2)
        .map(|index| SetKey {
            key: key_bytes(&churn_key(index)),
            value: value("initial"),
        })
        .collect();
    for chunk in operations.chunks(300) {
        transact(store, volume, non_empty(chunk.to_vec())).await;
    }
    set(store, volume, "b", "outside").await;
    set(store, volume, "d", "outside").await;
}

pub async fn list_keys_tolerates_concurrent_changes<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    seed_churn_keys(store, &volume).await;
    assert_listing_tolerates_changes(store, &volume, &volume).await;
}

pub async fn list_keys_on_fork_tolerates_concurrent_changes<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    seed_churn_keys(store, &parent).await;
    let parent_head = head(store, &parent).await;
    let child = fork(store, &parent, parent_head.0).await;
    assert_listing_tolerates_changes(store, &child, &child).await;
    let other = fork(store, &parent, parent_head.0).await;
    let prefix = key("c/");
    let mut listing = pin!(store.list_keys(other.clone(), prefix));
    let mut count = 0;
    for _ in 0..10 {
        assert!(matches!(listing.next().await, Some(Ok(_))));
        count += 1;
    }
    churn(store, &parent, &[100, 2000], &[1, 2001], &[]).await;
    while let Some(item) = listing.next().await {
        assert!(item.is_ok(), "{item:?}");
        count += 1;
    }
    assert_eq!(count, 1200);
}

async fn append_numbered_keys<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    group: &str,
    count: u32,
    history: &mut Vec<Entry>,
) {
    let names: Vec<Vec<u8>> = (0..count).map(|index| numbered_key(group, index)).collect();
    for chunk in names.chunks(250) {
        let operations: Vec<SetKey> = chunk
            .iter()
            .map(|name| SetKey {
                key: key_bytes(name),
                value: value("v"),
            })
            .collect();
        transact(store, volume, non_empty(operations)).await;
    }
    history.extend(names.iter().map(|name| {
        Entry::SetKey(KeyValue {
            key: key_bytes(name),
            value: value("v"),
        })
    }));
}

pub async fn entries_page_through_long_history_across_forks<S: MetadataStore>(store: &S) {
    let grandparent = create(store).await;
    let mut grandparent_history = Vec::new();
    append_numbered_keys(store, &grandparent, "g", 300, &mut grandparent_history).await;
    let parent = fork(store, &grandparent, 300).await;
    let mut parent_history = grandparent_history.clone();
    append_numbered_keys(store, &grandparent, "g-later", 40, &mut grandparent_history).await;
    append_numbered_keys(store, &parent, "p", 250, &mut parent_history).await;
    let child = fork(store, &parent, 550).await;
    let mut child_history = parent_history.clone();
    append_numbered_keys(store, &parent, "p-later", 40, &mut parent_history).await;
    append_numbered_keys(store, &child, "c", 250, &mut child_history).await;

    let expected = numbered(1, child_history);
    for after in [
        0, 1, 255, 256, 257, 299, 300, 301, 511, 512, 513, 549, 550, 551, 767, 768, 769, 799, 800,
        801,
    ] {
        let start = after.min(expected.len());
        let listed = collect_entries(store, &child, after as u64).await;
        assert!(
            listed == expected[start..],
            "entries of the child after {after} gave {} entries starting at {:?}, expected {} starting at {:?}",
            listed.len(),
            listed.first().map(|stored| stored.seq),
            expected.len() - start,
            expected.get(start).map(|stored| stored.seq),
        );
    }
    assert!(collect_entries(store, &parent, 0).await == numbered(1, parent_history));
    assert!(collect_entries(store, &grandparent, 0).await == numbered(1, grandparent_history));
}

pub async fn list_keys_skips_long_runs_of_deleted_keys<S: MetadataStore>(store: &S) {
    let prefixes: [&[u8]; 4] = [b"", b"q/", b"p/", b"p/0"];
    let parent = create(store).await;
    let mut parent_model = KeyModel::default();
    let lineage_run: Vec<Vec<u8>> = (0..300).map(|index| numbered_key("q", index)).collect();
    parent_model
        .set_many(
            store,
            &parent,
            lineage_run
                .iter()
                .map(|name| (name.clone(), value("dead")))
                .collect(),
        )
        .await;
    parent_model
        .delete_many(store, &parent, lineage_run.clone())
        .await;
    parent_model
        .set_many(store, &parent, vec![(b"q/zzz".to_vec(), value("live"))])
        .await;
    let inherited: Vec<Vec<u8>> = (0..300).map(|index| numbered_key("p", index)).collect();
    parent_model
        .set_many(
            store,
            &parent,
            inherited
                .iter()
                .map(|name| (name.clone(), value("inherited")))
                .collect(),
        )
        .await;
    parent_model.check(store, &parent, &prefixes).await;

    let child = fork(store, &parent, parent_model.head).await;
    let mut child_model = KeyModel {
        head: parent_model.head,
        keys: parent_model.keys.clone(),
    };
    child_model.check(store, &child, &prefixes).await;
    child_model.delete_many(store, &child, inherited).await;
    child_model.check(store, &child, &prefixes).await;
    child_model
        .set_many(store, &child, vec![(b"p/zzz".to_vec(), value("live"))])
        .await;
    child_model.check(store, &child, &prefixes).await;

    let grandchild = fork(store, &child, child_model.head).await;
    let grandchild_model = KeyModel {
        head: child_model.head,
        keys: child_model.keys.clone(),
    };
    grandchild_model.check(store, &grandchild, &prefixes).await;
    parent_model.check(store, &parent, &prefixes).await;
}

pub async fn list_keys_on_fork_skips_parent_keys_written_after_fork<S: MetadataStore>(store: &S) {
    let prefixes: [&[u8]; 4] = [b"", b"d/", b"d/0", b"e"];
    let parent = create(store).await;
    let mut parent_model = KeyModel::default();
    parent_model
        .set_many(
            store,
            &parent,
            (0..40)
                .map(|index| (numbered_key("d", index * 25), value("before")))
                .collect(),
        )
        .await;
    let child = fork(store, &parent, parent_model.head).await;
    let mut child_model = KeyModel {
        head: parent_model.head,
        keys: parent_model.keys.clone(),
    };
    parent_model
        .set_many(
            store,
            &parent,
            (0..1000)
                .filter(|index| index % 25 != 0)
                .map(|index| (numbered_key("d", index), value("parent-later")))
                .collect(),
        )
        .await;
    child_model
        .set_many(
            store,
            &child,
            (0..1000)
                .filter(|index| index % 3 == 0 && index % 25 != 0)
                .map(|index| (format!("d/{index:04}c").into_bytes(), value("child")))
                .collect(),
        )
        .await;
    child_model.check(store, &child, &prefixes).await;
    parent_model.check(store, &parent, &prefixes).await;
    let grandchild = fork(store, &child, child_model.head).await;
    let grandchild_model = KeyModel {
        head: child_model.head,
        keys: child_model.keys.clone(),
    };
    child_model
        .set_many(
            store,
            &child,
            (0..1000)
                .filter(|index| index % 3 == 1)
                .map(|index| (format!("d/{index:04}c").into_bytes(), value("child-later")))
                .collect(),
        )
        .await;
    grandchild_model.check(store, &grandchild, &prefixes).await;
    child_model.check(store, &child, &prefixes).await;
}

pub async fn list_keys_across_deep_fork_chain<S: MetadataStore>(store: &S) {
    let prefixes: [&[u8]; 5] = [b"", b"s/", b"s/03", b"s/11/", b"t"];
    let mut volume = create(store).await;
    let mut model = KeyModel::default();
    let mut levels = Vec::new();
    for level in 0..12_u32 {
        model
            .set_many(
                store,
                &volume,
                (0..60)
                    .map(|index| {
                        (
                            format!("s/{level:02}/{index:03}").into_bytes(),
                            Bytes::from(format!("level-{level}")),
                        )
                    })
                    .collect(),
            )
            .await;
        if level > 0 {
            let earlier = level - 1;
            model
                .delete_many(
                    store,
                    &volume,
                    (0..60)
                        .filter(|index| index % 4 == 0)
                        .map(|index| format!("s/{earlier:02}/{index:03}").into_bytes())
                        .collect(),
                )
                .await;
            model
                .set_many(
                    store,
                    &volume,
                    (0..60)
                        .filter(|index| index % 5 == 1)
                        .map(|index| {
                            (
                                format!("s/{earlier:02}/{index:03}").into_bytes(),
                                Bytes::from(format!("override-{level}")),
                            )
                        })
                        .collect(),
                )
                .await;
        }
        model.check(store, &volume, &prefixes).await;
        levels.push((volume.clone(), model.head, model.keys.clone()));
        volume = fork(store, &volume, model.head).await;
    }
    model.check(store, &volume, &prefixes).await;
    for (ancestor, head, keys) in levels {
        KeyModel { head, keys }
            .check(store, &ancestor, &prefixes)
            .await;
    }
}

pub async fn watch_wakes_every_pending_watcher<S: MetadataStore>(store: &S) {
    let first_volume = create(store).await;
    let second_volume = create(store).await;
    let unrelated = create(store).await;
    let mut first_watch = pin!(store.watch(first_volume.clone(), Seq(0)));
    let mut second_watch = pin!(store.watch(second_volume.clone(), Seq(0)));
    let mut second_watch_again = pin!(store.watch(second_volume.clone(), Seq(0)));
    let (first_item, (second_item, (second_again_item, ()))) = join(first_watch.next(), async {
        let second_items = join(
            second_watch.next(),
            join(second_watch_again.next(), async {
                for _ in 0..5 {
                    head(store, &unrelated).await;
                }
                set(store, &second_volume, "b", "1").await;
            }),
        )
        .await;
        set(store, &first_volume, "a", "1").await;
        second_items
    })
    .await;
    assert!(matches!(first_item, Some(Ok(Seq(1)))), "{first_item:?}");
    assert!(matches!(second_item, Some(Ok(Seq(1)))), "{second_item:?}");
    assert!(
        matches!(second_again_item, Some(Ok(Seq(1)))),
        "{second_again_item:?}"
    );
}

pub async fn watch_after_beyond_head_waits_until_head_passes_it<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    let mut watch = pin!(store.watch(volume.clone(), Seq(3)));
    let (item, ()) = join(watch.next(), async {
        for name in ["b", "c", "d"] {
            set(store, &volume, name, "1").await;
        }
    })
    .await;
    assert!(matches!(item, Some(Ok(Seq(4)))), "{item:?}");

    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    let child = fork(store, &parent, 1).await;
    let mut watch = pin!(store.watch(child.clone(), Seq(3)));
    let (item, ()) = join(watch.next(), async {
        for name in ["b", "c", "d", "e"] {
            set(store, &parent, name, "1").await;
        }
        for name in ["f", "g", "h"] {
            set(store, &child, name, "1").await;
        }
    })
    .await;
    assert!(matches!(item, Some(Ok(Seq(4)))), "{item:?}");
}

pub async fn watch_sees_commit_of_abandoned_execute<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    let operations: Vec<Op> = (0..200)
        .map(|index| {
            Op::SetKey(SetKey {
                key: key_bytes(&numbered_key("abandoned", index)),
                value: value("v"),
            })
        })
        .collect();
    let operations = Vec1::try_from_vec(operations)
        .unwrap_or_else(|error| panic!("operation list is empty: {error:?}"));
    let mut watch = pin!(store.watch(volume.clone(), Seq(1)));
    let (item, final_head) = join(watch.next(), async {
        for _ in 0..3 {
            head(store, &volume).await;
        }
        let observed = {
            let mut abandoned = pin!(store.execute(&volume, operations));
            poll_fn(|context| {
                let _ = abandoned.as_mut().poll(context);
                Poll::Ready(())
            })
            .await;
            let mut observed = head(store, &volume).await;
            for _ in 0..20_000 {
                if observed != Seq(1) {
                    break;
                }
                observed = head(store, &volume).await;
            }
            observed
        };
        if observed == Seq(1) {
            set(store, &volume, "released", "1").await;
        }
        head(store, &volume).await
    })
    .await;
    assert!(
        matches!(item, Some(Ok(seq)) if seq > Seq(1) && seq <= final_head),
        "watch gave {item:?} after an abandoned execute, with the head now at {final_head}"
    );
}

fn owned<T: Send + 'static>(value: T) -> T {
    value
}

#[cfg(not(target_arch = "wasm32"))]
fn run_detached<T: Send + 'static>(
    work: impl Future<Output = T> + Send + 'static,
) -> impl Future<Output = T> {
    let thread = std::thread::spawn(move || crate::run(work));
    async move {
        thread
            .join()
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload))
    }
}

#[cfg(target_arch = "wasm32")]
fn run_detached<T: Send + 'static>(
    work: impl Future<Output = T> + Send + 'static,
) -> impl Future<Output = T> {
    work
}

pub async fn streams_are_owned_and_static<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    set(store, &volume, "b", "2").await;
    let expected_entries = collect_entries(store, &volume, 0).await;
    let expected_listing = collect_keys(store, &volume, &key("")).await;
    let entries = owned(store.entries(volume.clone(), Seq(0)));
    let listing = owned(store.list_keys(volume.clone(), key("")));
    let watch = owned(store.watch(volume.clone(), Seq(2)));
    let entries = run_detached(entries.collect::<Vec<_>>());
    let listing = run_detached(listing.collect::<Vec<_>>());
    let entries: Vec<_> = entries
        .await
        .into_iter()
        .map(|item| item.unwrap_or_else(|error| panic!("entries failed: {error:?}")))
        .collect();
    assert_eq!(entries, expected_entries);
    let listing: Vec<_> = listing
        .await
        .into_iter()
        .map(|item| item.unwrap_or_else(|error| panic!("list_keys failed: {error:?}")))
        .collect();
    assert_eq!(listing, expected_listing);
    let watched = run_detached(async move {
        let mut watch = pin!(watch);
        watch.next().await
    });
    let appended = set(store, &volume, "c", "3").await;
    let watched = watched.await;
    assert!(
        matches!(watched, Some(Ok(seq)) if seq == appended),
        "a detached watch gave {watched:?}"
    );
}

pub async fn entries_end_at_the_head_observed_when_reading_starts<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    let mut parent_history = Vec::new();
    append_numbered_keys(store, &parent, "p", 300, &mut parent_history).await;
    let child = fork(store, &parent, 300).await;
    let mut child_history = parent_history.clone();
    append_numbered_keys(store, &child, "c", 400, &mut child_history).await;
    for after in [0_usize, 299, 650] {
        let expected = numbered(1, child_history.clone())[after..].to_vec();
        let mut entries = pin!(store.entries(child.clone(), Seq(after as u64)));
        let mut read = Vec::new();
        let mut round = 0;
        while let Some(item) = entries.next().await {
            read.push(item.unwrap_or_else(|error| panic!("entries failed: {error:?}")));
            if read.len() % 100 == 1 {
                let group = format!("late-{after}-{round}");
                append_numbered_keys(store, &child, &group, 30, &mut child_history).await;
                set(store, &parent, &group, "parent").await;
                round += 1;
            }
        }
        assert!(round > 0, "entries after {after} yielded nothing");
        assert!(
            read == expected,
            "entries after {after} yielded {} entries ending at {:?} while appends were interleaved, expected {} ending at {:?}",
            read.len(),
            read.last().map(|stored| stored.seq),
            expected.len(),
            expected.last().map(|stored| stored.seq),
        );
    }
    assert!(collect_entries(store, &child, 0).await == numbered(1, child_history));
}
