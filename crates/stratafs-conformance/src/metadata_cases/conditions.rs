use stratafs_store::error::StoreError;
use stratafs_store::metadata::{MetadataStore, Seq, TransactRejection, VolumeId};
use stratafs_store::transaction::{Assert, Condition, DeleteKey, GetHead, SetKey, Write, WriteAt};
use vec1::{Vec1, vec1};

use crate::metadata_cases::streams::{KeyModel, numbered_key};
use crate::support::{
    blob, create, delete, file, fork, head, key, key_bytes, set, transact, transact_rejection,
    truncate, value, write_at,
};

async fn holds<S: MetadataStore>(store: &S, volume: &VolumeId, condition: Condition) -> bool {
    let description = format!("{condition:?}");
    match store.transact(volume, Assert(condition)).await {
        Ok(()) => true,
        Err(StoreError::Rejected(TransactRejection::AssertionFailed { index: 0 })) => false,
        Err(error) => panic!("checking {description} on {volume:?} failed: {error:?}"),
    }
}

fn equals(name: &str, expected: Option<&str>) -> Condition {
    Condition::Equals {
        key: key(name),
        value: expected.map(value),
    }
}

fn empty(prefix: &str) -> Condition {
    Condition::Empty {
        prefix: key(prefix),
    }
}

fn file_version(name: &str, seq: Option<u64>) -> Condition {
    Condition::FileVersion {
        file: file(name),
        seq: seq.map(Seq),
    }
}

fn not(condition: Condition) -> Condition {
    Condition::Not(Box::new(condition))
}

fn all(conditions: Vec1<Condition>) -> Condition {
    Condition::All(conditions)
}

fn any(conditions: Vec1<Condition>) -> Condition {
    Condition::Any(conditions)
}

fn truth() -> Condition {
    not(falsehood())
}

fn falsehood() -> Condition {
    Condition::Head(Seq(u64::MAX))
}

pub async fn equals_condition<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    set(store, &volume, "b", "2").await;
    delete(store, &volume, "b").await;
    assert!(holds(store, &volume, equals("a", Some("1"))).await);
    assert!(!holds(store, &volume, equals("a", Some("2"))).await);
    assert!(!holds(store, &volume, equals("a", Some(""))).await);
    assert!(!holds(store, &volume, equals("a", Some("10"))).await);
    assert!(!holds(store, &volume, equals("a", None)).await);
    assert!(holds(store, &volume, equals("b", None)).await);
    assert!(!holds(store, &volume, equals("b", Some("2"))).await);
    assert!(holds(store, &volume, equals("c", None)).await);
    assert!(!holds(store, &volume, equals("c", Some(""))).await);
}

pub async fn empty_condition_with_nested_prefixes<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "dir/a/x", "1").await;
    set(store, &volume, "dir/b", "2").await;
    for prefix in [
        "", "d", "di", "dir", "dir/", "dir/a", "dir/a/", "dir/a/x", "dir/b",
    ] {
        assert!(
            !holds(store, &volume, empty(prefix)).await,
            "prefix {prefix:?} should not be empty"
        );
    }
    for prefix in [
        "dir/a/x/", "dir/a/y", "dir/c", "dir/bb", "dir//", "dis", "e", "a", "dir/a/xx",
    ] {
        assert!(
            holds(store, &volume, empty(prefix)).await,
            "prefix {prefix:?} should be empty"
        );
    }
    assert!(
        holds(
            store,
            &volume,
            Condition::Empty {
                prefix: key_bytes(&[0xff])
            }
        )
        .await
    );
    assert!(
        holds(
            store,
            &volume,
            Condition::Empty {
                prefix: key_bytes(b"dir/\xff")
            }
        )
        .await
    );
}

pub async fn empty_condition_ignores_deleted_keys<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "p/a", "1").await;
    set(store, &volume, "q", "1").await;
    delete(store, &volume, "p/a").await;
    assert!(holds(store, &volume, empty("p/")).await);
    assert!(holds(store, &volume, empty("p")).await);
    assert!(!holds(store, &volume, empty("q")).await);
    delete(store, &volume, "q").await;
    assert!(holds(store, &volume, empty("")).await);
    set(store, &volume, "p/b", "1").await;
    assert!(!holds(store, &volume, empty("p/")).await);
}

pub async fn empty_prefix_covers_whole_key_space<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    assert!(holds(store, &volume, empty("")).await);
    transact(
        store,
        &volume,
        SetKey {
            key: key_bytes(&[0xff, 0xff]),
            value: value("1"),
        },
    )
    .await;
    assert!(!holds(store, &volume, empty("")).await);
    transact(
        store,
        &volume,
        DeleteKey {
            key: key_bytes(&[0xff, 0xff]),
        },
    )
    .await;
    assert!(holds(store, &volume, empty("")).await);
    transact(
        store,
        &volume,
        SetKey {
            key: key_bytes(&[0x00]),
            value: value("1"),
        },
    )
    .await;
    assert!(!holds(store, &volume, empty("")).await);
}

pub async fn empty_condition_sees_fork_overlay<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "p/a", "1").await;
    set(store, &parent, "q/a", "1").await;
    let child = fork(store, &parent, 2).await;
    assert!(!holds(store, &child, empty("p/")).await);
    delete(store, &child, "p/a").await;
    assert!(holds(store, &child, empty("p/")).await);
    assert!(!holds(store, &parent, empty("p/")).await);
    assert!(!holds(store, &child, empty("q/")).await);
    assert!(!holds(store, &child, empty("")).await);

    delete(store, &parent, "q/a").await;
    assert!(holds(store, &parent, empty("q/")).await);
    assert!(!holds(store, &child, empty("q/")).await);

    set(store, &child, "r/a", "1").await;
    assert!(!holds(store, &child, empty("r/")).await);
    assert!(holds(store, &parent, empty("r/")).await);

    delete(store, &child, "q/a").await;
    delete(store, &child, "r/a").await;
    assert!(holds(store, &child, empty("")).await);
    assert!(!holds(store, &parent, empty("")).await);

    let grandchild = fork(store, &child, 3).await;
    assert!(holds(store, &grandchild, empty("p/")).await);
    assert!(!holds(store, &grandchild, empty("q/")).await);
}

pub async fn head_condition<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    assert!(holds(store, &volume, Condition::Head(Seq(0))).await);
    assert!(!holds(store, &volume, Condition::Head(Seq(1))).await);
    set(store, &volume, "a", "1").await;
    assert!(holds(store, &volume, Condition::Head(Seq(1))).await);
    assert!(!holds(store, &volume, Condition::Head(Seq(0))).await);
    assert!(!holds(store, &volume, Condition::Head(Seq(2))).await);
    let ((), (), observed) = transact(
        store,
        &volume,
        (
            SetKey {
                key: key("b"),
                value: value("1"),
            },
            Assert(Condition::Head(Seq(2))),
            GetHead,
        ),
    )
    .await;
    assert_eq!(observed, Seq(2));
    let child = fork(store, &volume, 1).await;
    assert!(holds(store, &child, Condition::Head(Seq(1))).await);
}

pub async fn file_version_condition<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    assert!(holds(store, &volume, file_version("f", None)).await);
    assert!(!holds(store, &volume, file_version("f", Some(0))).await);
    write_at(store, &volume, "f", 0, 4, x, 0).await;
    assert!(holds(store, &volume, file_version("f", Some(1))).await);
    assert!(!holds(store, &volume, file_version("f", None)).await);
    assert!(!holds(store, &volume, file_version("f", Some(2))).await);
    assert!(holds(store, &volume, file_version("g", None)).await);
    truncate(store, &volume, "f", 2).await;
    assert!(holds(store, &volume, file_version("f", Some(2))).await);
    assert!(!holds(store, &volume, file_version("f", Some(1))).await);
    transact(
        store,
        &volume,
        Write {
            file: file("f"),
            at: WriteAt::End,
            size: 0,
            content: x,
            content_offset: 0,
        },
    )
    .await;
    set(store, &volume, "a", "1").await;
    assert!(holds(store, &volume, file_version("f", Some(2))).await);
    write_at(store, &volume, "g", 10, 1, x, 0).await;
    assert!(holds(store, &volume, file_version("f", Some(2))).await);
    assert!(holds(store, &volume, file_version("g", Some(4))).await);
}

pub async fn file_version_condition_in_fork<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let parent = create(store).await;
    write_at(store, &parent, "f", 0, 4, x, 0).await;
    set(store, &parent, "a", "1").await;
    let child = fork(store, &parent, 2).await;
    assert!(holds(store, &child, file_version("f", Some(1))).await);
    truncate(store, &child, "f", 1).await;
    assert!(holds(store, &child, file_version("f", Some(3))).await);
    assert!(holds(store, &parent, file_version("f", Some(1))).await);
    write_at(store, &parent, "f", 0, 1, x, 0).await;
    assert!(holds(store, &parent, file_version("f", Some(3))).await);
    let empty_fork = fork(store, &parent, 0).await;
    assert!(holds(store, &empty_fork, file_version("f", None)).await);
    let early_fork = fork(store, &parent, 1).await;
    assert!(holds(store, &early_fork, file_version("f", Some(1))).await);
}

pub async fn combinators<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    assert!(holds(store, &volume, truth()).await);
    assert!(!holds(store, &volume, falsehood()).await);
    assert!(!holds(store, &volume, not(truth())).await);
    assert!(holds(store, &volume, not(falsehood())).await);
    assert!(holds(store, &volume, not(not(truth()))).await);
    assert!(holds(store, &volume, all(vec1![truth()])).await);
    assert!(holds(store, &volume, all(vec1![truth(), truth(), truth()])).await);
    assert!(!holds(store, &volume, all(vec1![truth(), falsehood(), truth()])).await);
    assert!(!holds(store, &volume, all(vec1![falsehood()])).await);
    assert!(holds(store, &volume, any(vec1![truth()])).await);
    assert!(
        holds(
            store,
            &volume,
            any(vec1![falsehood(), falsehood(), truth()])
        )
        .await
    );
    assert!(!holds(store, &volume, any(vec1![falsehood(), falsehood()])).await);
    assert!(!holds(store, &volume, any(vec1![falsehood()])).await);
    set(store, &volume, "a", "1").await;
    let nested = all(vec1![
        any(vec1![equals("a", Some("2")), equals("a", Some("1"))]),
        not(equals("a", None)),
        any(vec1![all(vec1![empty("b"), Condition::Head(Seq(1))])]),
    ]);
    assert!(holds(store, &volume, nested.clone()).await);
    set(store, &volume, "b", "1").await;
    assert!(!holds(store, &volume, nested).await);
}

pub async fn conditions_observe_earlier_operations<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    let ((), (), (), (), (), _, (), ()) = transact(
        store,
        &volume,
        (
            SetKey {
                key: key("a"),
                value: value("1"),
            },
            Assert(equals("a", Some("1"))),
            DeleteKey { key: key("a") },
            Assert(all(vec1![equals("a", None), empty("")])),
            Assert(file_version("f", None)),
            Write {
                file: file("f"),
                at: WriteAt::Offset(0),
                size: 1,
                content: x,
                content_offset: 0,
            },
            Assert(file_version("f", Some(3))),
            Assert(Condition::Head(Seq(3))),
        ),
    )
    .await;
    let rejection = transact_rejection(
        store,
        &volume,
        (
            SetKey {
                key: key("b"),
                value: value("1"),
            },
            Assert(equals("b", None)),
        ),
    )
    .await;
    assert!(
        matches!(rejection, TransactRejection::AssertionFailed { index: 1 }),
        "{rejection:?}"
    );
    assert!(holds(store, &volume, Condition::Head(Seq(3))).await);
}

pub async fn empty_condition_pages_through_deleted_keys<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    let mut parent_model = KeyModel::default();
    let lineage_run: Vec<Vec<u8>> = (0..300).map(|index| numbered_key("q", index)).collect();
    let inherited: Vec<Vec<u8>> = (0..300).map(|index| numbered_key("p", index)).collect();
    parent_model
        .set_many(
            store,
            &parent,
            lineage_run
                .iter()
                .chain(&inherited)
                .map(|name| (name.clone(), value("v")))
                .collect(),
        )
        .await;
    parent_model
        .set_many(store, &parent, vec![(b"q/zzz".to_vec(), value("live"))])
        .await;
    parent_model.delete_many(store, &parent, lineage_run).await;
    assert!(!holds(store, &parent, empty("q/")).await);

    let child = fork(store, &parent, parent_model.head).await;
    assert!(!holds(store, &child, empty("q/")).await);
    assert!(!holds(store, &child, empty("p/")).await);
    let mut child_model = KeyModel {
        head: parent_model.head,
        keys: parent_model.keys.clone(),
    };
    child_model.delete_many(store, &child, inherited).await;
    assert!(holds(store, &child, empty("p/")).await);
    assert!(!holds(store, &parent, empty("p/")).await);
    set(store, &child, "p/zzz", "live").await;
    assert!(!holds(store, &child, empty("p/")).await);
    assert!(!holds(store, &child, empty("q/")).await);
    delete(store, &child, "q/zzz").await;
    assert!(holds(store, &child, empty("q/")).await);
    assert!(!holds(store, &parent, empty("q/")).await);

    let grandchild = fork(store, &child, head(store, &child).await.0).await;
    assert!(!holds(store, &grandchild, empty("p/")).await);
    assert!(holds(store, &grandchild, empty("q/")).await);
    delete(store, &grandchild, "p/zzz").await;
    assert!(holds(store, &grandchild, empty("p/")).await);
    assert!(holds(store, &grandchild, empty("")).await);
    assert!(!holds(store, &child, empty("")).await);
}
