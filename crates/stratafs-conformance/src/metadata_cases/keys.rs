use bytes::Bytes;
use stratafs_store::metadata::{KeyValue, MetadataStore, Seq, Stored};
use stratafs_store::transaction::{Assert, Condition, SetKey};

use crate::support::{
    collect_entries, collect_keys, create, delete, delete_entry, fork, get, get_value, head, key,
    key_bytes, listed, non_empty, numbered, pairs, set, set_entry, stored_seq, transact, value,
};

pub async fn set_then_get_returns_value_and_seq<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    assert_eq!(set(store, &volume, "a", "1").await, Seq(1));
    assert_eq!(set(store, &volume, "b", "2").await, Seq(2));
    assert_eq!(
        get(store, &volume, "a").await,
        Some(Stored {
            seq: Seq(1),
            entry: KeyValue {
                key: key("a"),
                value: value("1")
            }
        })
    );
    assert_eq!(
        get(store, &volume, "b").await,
        Some(Stored {
            seq: Seq(2),
            entry: KeyValue {
                key: key("b"),
                value: value("2")
            }
        })
    );
    assert_eq!(get(store, &volume, "c").await, None);
    assert_eq!(get(store, &volume, "").await, None);
}

pub async fn overwrite_replaces_value_and_seq<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    set(store, &volume, "b", "x").await;
    set(store, &volume, "a", "2").await;
    assert_eq!(
        get(store, &volume, "a").await,
        Some(Stored {
            seq: Seq(3),
            entry: KeyValue {
                key: key("a"),
                value: value("2")
            }
        })
    );
    assert_eq!(stored_seq(store, &volume, "b").await, Some(Seq(2)));
    assert_eq!(
        listed(store, &volume, "").await,
        pairs(&[("a", "2"), ("b", "x")])
    );
}

pub async fn setting_unchanged_value_appends_entry<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    assert_eq!(set(store, &volume, "a", "1").await, Seq(2));
    assert_eq!(stored_seq(store, &volume, "a").await, Some(Seq(2)));
    assert_eq!(
        collect_entries(store, &volume, 0).await,
        numbered(1, vec![set_entry("a", "1"), set_entry("a", "1")])
    );
}

pub async fn delete_then_get_returns_none<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    set(store, &volume, "b", "2").await;
    assert_eq!(delete(store, &volume, "a").await, Seq(3));
    assert_eq!(get(store, &volume, "a").await, None);
    assert_eq!(get_value(store, &volume, "b").await, Some(value("2")));
    assert_eq!(
        collect_entries(store, &volume, 2).await,
        numbered(3, vec![delete_entry("a")])
    );
}

pub async fn deleting_absent_key_is_noop<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    assert_eq!(delete(store, &volume, "never").await, Seq(0));
    set(store, &volume, "a", "1").await;
    delete(store, &volume, "a").await;
    assert_eq!(delete(store, &volume, "a").await, Seq(2));
    assert_eq!(delete(store, &volume, "never").await, Seq(2));
    assert_eq!(
        collect_entries(store, &volume, 0).await,
        numbered(1, vec![set_entry("a", "1"), delete_entry("a")])
    );
}

pub async fn set_after_delete_restores_key<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    delete(store, &volume, "a").await;
    set(store, &volume, "a", "2").await;
    assert_eq!(
        get(store, &volume, "a").await,
        Some(Stored {
            seq: Seq(3),
            entry: KeyValue {
                key: key("a"),
                value: value("2")
            }
        })
    );
    assert_eq!(listed(store, &volume, "").await, pairs(&[("a", "2")]));
}

pub async fn empty_value_is_live<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "").await;
    assert_eq!(get_value(store, &volume, "a").await, Some(Bytes::new()));
    let ((), ()) = transact(
        store,
        &volume,
        (
            Assert(Condition::Equals {
                key: key("a"),
                value: Some(Bytes::new()),
            }),
            Assert(Condition::Not(Box::new(Condition::Equals {
                key: key("a"),
                value: None,
            }))),
        ),
    )
    .await;
    assert_eq!(listed(store, &volume, "").await, pairs(&[("a", "")]));
    assert_eq!(delete(store, &volume, "a").await, Seq(2));
    assert_eq!(get(store, &volume, "a").await, None);
}

pub async fn deletion_in_fork_hides_parent_key_in_fork_only<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    set(store, &parent, "b", "2").await;
    let child = fork(store, &parent, 2).await;
    delete(store, &child, "a").await;
    assert_eq!(get(store, &child, "a").await, None);
    assert_eq!(listed(store, &child, "").await, pairs(&[("b", "2")]));
    assert_eq!(
        get(store, &parent, "a").await.map(|stored| stored.seq),
        Some(Seq(1))
    );
    assert_eq!(
        listed(store, &parent, "").await,
        pairs(&[("a", "1"), ("b", "2")])
    );
    let grandchild = fork(store, &child, 3).await;
    assert_eq!(get(store, &grandchild, "a").await, None);
    let before_delete = fork(store, &child, 2).await;
    assert_eq!(
        get_value(store, &before_delete, "a").await,
        Some(value("1"))
    );
}

pub async fn deleting_inherited_key_appends_entry<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    let child = fork(store, &parent, 1).await;
    assert_eq!(delete(store, &child, "a").await, Seq(2));
    assert_eq!(delete(store, &child, "a").await, Seq(2));
    assert_eq!(
        collect_entries(store, &child, 0).await,
        numbered(1, vec![set_entry("a", "1"), delete_entry("a")])
    );
    set(store, &child, "a", "again").await;
    assert_eq!(stored_seq(store, &child, "a").await, Some(Seq(3)));
}

pub async fn deleting_key_deleted_before_fork_is_noop<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    delete(store, &parent, "a").await;
    let child = fork(store, &parent, 2).await;
    assert_eq!(delete(store, &child, "a").await, Seq(2));
    let grandchild = fork(store, &child, 2).await;
    assert_eq!(delete(store, &grandchild, "a").await, Seq(2));
    assert_eq!(delete(store, &grandchild, "never").await, Seq(2));
}

pub async fn parent_deletion_after_fork_does_not_affect_child<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    let child = fork(store, &parent, 1).await;
    delete(store, &parent, "a").await;
    set(store, &parent, "b", "parent").await;
    assert_eq!(
        get(store, &child, "a").await,
        Some(Stored {
            seq: Seq(1),
            entry: KeyValue {
                key: key("a"),
                value: value("1")
            }
        })
    );
    assert_eq!(listed(store, &child, "").await, pairs(&[("a", "1")]));
    assert_eq!(listed(store, &parent, "").await, pairs(&[("b", "parent")]));
}

pub async fn setting_in_fork_does_not_leak_to_parent<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    let child = fork(store, &parent, 1).await;
    set(store, &child, "a", "child").await;
    set(store, &child, "b", "child").await;
    assert_eq!(
        get(store, &parent, "a").await.map(|stored| stored.seq),
        Some(Seq(1))
    );
    assert_eq!(get_value(store, &parent, "a").await, Some(value("1")));
    assert_eq!(get_value(store, &parent, "b").await, None);
    assert_eq!(head(store, &parent).await, Seq(1));
    assert_eq!(get_value(store, &child, "a").await, Some(value("child")));
}

pub async fn keys_compare_bytewise<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    let names: [&[u8]; 11] = [
        b"b",
        &[0xff],
        b"ab",
        &[b'a', 0xff],
        b"A",
        &[0x00],
        b"a",
        &[b'a', 0x00],
        &[0xff, 0xff],
        &[0xff, 0x00],
        &[b'a', 0xff, 0xff],
    ];
    let operations: Vec<SetKey> = names
        .iter()
        .map(|name| SetKey {
            key: key_bytes(name),
            value: Bytes::copy_from_slice(name),
        })
        .collect();
    transact(store, &volume, non_empty(operations)).await;

    let listed_keys = |items: Vec<Stored<KeyValue>>| -> Vec<Vec<u8>> {
        items
            .into_iter()
            .map(|stored| {
                assert_eq!(stored.entry.value, stored.entry.key.0);
                stored.entry.key.0.to_vec()
            })
            .collect()
    };
    let mut everything: Vec<Vec<u8>> = names.iter().map(|name| name.to_vec()).collect();
    everything.sort();
    assert_eq!(
        listed_keys(collect_keys(store, &volume, &key("")).await),
        everything
    );
    assert_eq!(
        listed_keys(collect_keys(store, &volume, &key("a")).await),
        vec![
            b"a".to_vec(),
            vec![b'a', 0x00],
            b"ab".to_vec(),
            vec![b'a', 0xff],
            vec![b'a', 0xff, 0xff],
        ]
    );
    assert_eq!(
        listed_keys(collect_keys(store, &volume, &key_bytes(&[0xff])).await),
        vec![vec![0xff], vec![0xff, 0x00], vec![0xff, 0xff]]
    );
    assert_eq!(
        listed_keys(collect_keys(store, &volume, &key_bytes(&[b'a', 0xff])).await),
        vec![vec![b'a', 0xff], vec![b'a', 0xff, 0xff]]
    );
    assert_eq!(
        listed_keys(collect_keys(store, &volume, &key_bytes(&[0x00])).await),
        vec![vec![0x00]]
    );
    assert!(
        collect_keys(store, &volume, &key_bytes(&[0xff, 0xff, 0xff]))
            .await
            .is_empty()
    );
}

pub async fn empty_key_behaves_like_any_other_key<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    let stored = |text: &str, seq: u64| Stored {
        seq: Seq(seq),
        entry: KeyValue {
            key: key(""),
            value: value(text),
        },
    };
    assert_eq!(get(store, &volume, "").await, None);
    assert_eq!(delete(store, &volume, "").await, Seq(0));
    assert_eq!(set(store, &volume, "", "empty").await, Seq(1));
    set(store, &volume, "a", "1").await;
    assert_eq!(get(store, &volume, "").await, Some(stored("empty", 1)));
    assert_eq!(
        collect_keys(store, &volume, &key("")).await[0],
        stored("empty", 1)
    );
    assert_eq!(
        listed(store, &volume, "").await,
        pairs(&[("", "empty"), ("a", "1")])
    );
    assert_eq!(listed(store, &volume, "a").await, pairs(&[("a", "1")]));
    assert_eq!(delete(store, &volume, "a").await, Seq(3));
    let ((), (), ()) = transact(
        store,
        &volume,
        (
            Assert(Condition::Equals {
                key: key(""),
                value: Some(value("empty")),
            }),
            Assert(Condition::Not(Box::new(Condition::Empty {
                prefix: key(""),
            }))),
            Assert(Condition::Empty { prefix: key("a") }),
        ),
    )
    .await;
    assert_eq!(set(store, &volume, "", "again").await, Seq(4));
    assert_eq!(get(store, &volume, "").await, Some(stored("again", 4)));
    let child = fork(store, &volume, 4).await;
    let older = fork(store, &volume, 2).await;
    assert_eq!(delete(store, &volume, "").await, Seq(5));
    assert_eq!(get(store, &volume, "").await, None);
    assert_eq!(delete(store, &volume, "").await, Seq(5));
    assert!(listed(store, &volume, "").await.is_empty());
    let ((), ()) = transact(
        store,
        &volume,
        (
            Assert(Condition::Equals {
                key: key(""),
                value: None,
            }),
            Assert(Condition::Empty { prefix: key("") }),
        ),
    )
    .await;
    assert_eq!(
        collect_entries(store, &volume, 0).await,
        numbered(
            1,
            vec![
                set_entry("", "empty"),
                set_entry("a", "1"),
                delete_entry("a"),
                set_entry("", "again"),
                delete_entry(""),
            ]
        )
    );
    assert_eq!(get(store, &child, "").await, Some(stored("again", 4)));
    assert_eq!(get(store, &older, "").await, Some(stored("empty", 1)));
    assert_eq!(delete(store, &older, "").await, Seq(3));
    assert_eq!(get(store, &older, "").await, None);
    assert_eq!(listed(store, &older, "").await, pairs(&[("a", "1")]));
    assert_eq!(get(store, &child, "").await, Some(stored("again", 4)));
}
