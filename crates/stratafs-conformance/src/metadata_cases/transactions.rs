use stratafs_store::error::StoreError;
use stratafs_store::metadata::{
    FileInfo, KeyValue, MetadataStore, Seq, Stored, TransactRejection, VolumeId, Written,
};
use stratafs_store::transaction::{
    Assert, Condition, DeleteKey, GetFileInfo, GetHead, GetKey, Op, Outcome, SetKey, Truncate,
    VisibleBlobs, Write, WriteAt,
};
use vec1::vec1;

use crate::support::{
    blob, collect_entries, create, data, delete_entry, file, file_info, fork, get, get_value, head,
    holes, key, listed, numbered, pairs, provenance, read_file, set, set_entry, stored_seq,
    transact, transact_rejection, truncate_entry, value, write_at, write_entry,
};

fn assert_head(seq: u64) -> Assert {
    Assert(Condition::Head(Seq(seq)))
}

fn always() -> Assert {
    Assert(Condition::Empty {
        prefix: key("no-key-has-this-prefix"),
    })
}

fn never() -> Assert {
    Assert(Condition::Not(Box::new(Condition::Empty {
        prefix: key("no-key-has-this-prefix"),
    })))
}

pub async fn operations_run_in_order_with_read_your_writes<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    let (
        (absent, (), first, (), second, (), deleted, written, written_info, content, (), truncated),
        final_head,
    ) = transact(
        store,
        &volume,
        (
            (
                GetKey { key: key("a") },
                SetKey {
                    key: key("a"),
                    value: value("1"),
                },
                GetKey { key: key("a") },
                SetKey {
                    key: key("a"),
                    value: value("2"),
                },
                GetKey { key: key("a") },
                DeleteKey { key: key("a") },
                GetKey { key: key("a") },
                Write {
                    file: file("f"),
                    at: WriteAt::Offset(0),
                    size: 4,
                    content: x,
                    content_offset: 0,
                },
                GetFileInfo { file: file("f") },
                VisibleBlobs {
                    file: file("f"),
                    offset: 0,
                    size: 4,
                },
                Truncate {
                    file: file("f"),
                    len: 2,
                },
                GetFileInfo { file: file("f") },
            ),
            GetHead,
        ),
    )
    .await;
    assert_eq!(absent, None);
    assert_eq!(
        first,
        Some(Stored {
            seq: Seq(1),
            entry: KeyValue {
                key: key("a"),
                value: value("1")
            }
        })
    );
    assert_eq!(
        second,
        Some(Stored {
            seq: Seq(2),
            entry: KeyValue {
                key: key("a"),
                value: value("2")
            }
        })
    );
    assert_eq!(deleted, None);
    assert_eq!(written, Written { offset: 0 });
    assert_eq!(
        written_info,
        FileInfo {
            len: 4,
            seq: Some(Seq(4))
        }
    );
    assert_eq!(content.info, written_info);
    assert_eq!(provenance(&content, 0, 4), data(x, 0, 4));
    assert_eq!(
        truncated,
        FileInfo {
            len: 2,
            seq: Some(Seq(5))
        }
    );
    assert_eq!(final_head, Seq(5));
    assert_eq!(
        collect_entries(store, &volume, 0).await,
        numbered(
            1,
            vec![
                set_entry("a", "1"),
                set_entry("a", "2"),
                delete_entry("a"),
                write_entry("f", 0, 4, x, 0),
                truncate_entry("f", 2),
            ]
        )
    );
}

async fn assert_failed_assertion_leaves_no_trace<S: MetadataStore>(store: &S, volume: &VolumeId) {
    let y = blob("y");
    let before_entries = collect_entries(store, volume, 0).await;
    let before_head = head(store, volume).await;
    let before_keys = listed(store, volume, "").await;
    let before_a = get(store, volume, "a").await;
    let before_f = read_file(store, volume, "f").await;
    let before_f_info = file_info(store, volume, "f").await;

    let rejection = transact_rejection(
        store,
        volume,
        (
            SetKey {
                key: key("a"),
                value: value("changed"),
            },
            SetKey {
                key: key("b"),
                value: value("new"),
            },
            DeleteKey { key: key("a") },
            Write {
                file: file("f"),
                at: WriteAt::Offset(2),
                size: 4,
                content: y,
                content_offset: 0,
            },
            Truncate {
                file: file("f"),
                len: 1,
            },
            Truncate {
                file: file("g"),
                len: 3,
            },
            assert_head(0),
        ),
    )
    .await;
    assert!(
        matches!(rejection, TransactRejection::AssertionFailed { index: 6 }),
        "{rejection:?}"
    );
    assert_eq!(head(store, volume).await, before_head);
    assert_eq!(collect_entries(store, volume, 0).await, before_entries);
    assert_eq!(listed(store, volume, "").await, before_keys);
    assert_eq!(get(store, volume, "a").await, before_a);
    assert_eq!(get_value(store, volume, "b").await, None);
    assert_eq!(read_file(store, volume, "f").await, before_f);
    assert_eq!(file_info(store, volume, "f").await, before_f_info);
    assert_eq!(
        file_info(store, volume, "g").await,
        FileInfo { len: 0, seq: None }
    );

    set(store, volume, "c", "after").await;
    assert_eq!(
        stored_seq(store, volume, "c").await,
        Some(Seq(before_head.0 + 1))
    );
}

pub async fn failed_assertion_leaves_no_trace<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    write_at(store, &volume, "f", 0, 4, blob("x"), 0).await;
    assert_failed_assertion_leaves_no_trace(store, &volume).await;
}

pub async fn failed_assertion_in_fork_leaves_no_trace<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    write_at(store, &parent, "f", 0, 4, blob("x"), 0).await;
    set(store, &parent, "z", "1").await;
    let child = fork(store, &parent, 3).await;
    set(store, &child, "own", "1").await;
    assert_failed_assertion_leaves_no_trace(store, &child).await;
    assert_eq!(
        listed(store, &parent, "").await,
        pairs(&[("a", "1"), ("z", "1")])
    );
}

pub async fn assertion_index_counts_tuple_operations<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    let rejection = transact_rejection(
        store,
        &volume,
        (
            GetHead,
            SetKey {
                key: key("a"),
                value: value("1"),
            },
            (GetKey { key: key("a") }, always()),
            never(),
        ),
    )
    .await;
    assert!(
        matches!(rejection, TransactRejection::AssertionFailed { index: 4 }),
        "{rejection:?}"
    );

    let rejection = transact_rejection(store, &volume, never()).await;
    assert!(
        matches!(rejection, TransactRejection::AssertionFailed { index: 0 }),
        "{rejection:?}"
    );
    assert_eq!(head(store, &volume).await, Seq(0));
}

pub async fn assertion_index_counts_vec_operations<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    let rejection = transact_rejection(
        store,
        &volume,
        (
            GetHead,
            vec1![
                SetKey {
                    key: key("a"),
                    value: value("1"),
                },
                SetKey {
                    key: key("b"),
                    value: value("2"),
                },
                SetKey {
                    key: key("c"),
                    value: value("3"),
                },
            ],
            vec1![always(), assert_head(3), always(), assert_head(0)],
        ),
    )
    .await;
    assert!(
        matches!(rejection, TransactRejection::AssertionFailed { index: 7 }),
        "{rejection:?}"
    );

    let operations = vec1![
        Op::GetHead(GetHead),
        Op::Assert(always()),
        Op::SetKey(SetKey {
            key: key("a"),
            value: value("1"),
        }),
        Op::Assert(assert_head(0)),
    ];
    let result = store.execute(&volume, operations).await;
    assert!(
        matches!(
            result,
            Err(StoreError::Rejected(TransactRejection::AssertionFailed {
                index: 3
            }))
        ),
        "{result:?}"
    );
    assert_eq!(head(store, &volume).await, Seq(0));
    assert!(listed(store, &volume, "").await.is_empty());
}

pub async fn evaluation_stops_at_first_failing_assertion<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    let rejection = transact_rejection(
        store,
        &volume,
        (
            always(),
            never(),
            assert_head(7),
            SetKey {
                key: key("b"),
                value: value("1"),
            },
            never(),
        ),
    )
    .await;
    assert!(
        matches!(rejection, TransactRejection::AssertionFailed { index: 1 }),
        "{rejection:?}"
    );
    assert_eq!(head(store, &volume).await, Seq(1));
}

pub async fn passing_assertions_append_nothing<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    let ((), (), (), observed) = transact(
        store,
        &volume,
        (
            always(),
            assert_head(1),
            Assert(Condition::Equals {
                key: key("a"),
                value: Some(value("1")),
            }),
            GetHead,
        ),
    )
    .await;
    assert_eq!(observed, Seq(1));
    assert_eq!(head(store, &volume).await, Seq(1));
    assert_eq!(collect_entries(store, &volume, 0).await.len(), 1);
}

pub async fn get_head_reflects_earlier_writes<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    set(store, &volume, "existing", "1").await;
    let heads = transact(
        store,
        &volume,
        (
            GetHead,
            SetKey {
                key: key("a"),
                value: value("1"),
            },
            GetHead,
            DeleteKey { key: key("absent") },
            GetHead,
            Write {
                file: file("f"),
                at: WriteAt::End,
                size: 0,
                content: x,
                content_offset: 0,
            },
            GetHead,
            Truncate {
                file: file("f"),
                len: 3,
            },
            GetHead,
            DeleteKey {
                key: key("existing"),
            },
            GetHead,
        ),
    )
    .await;
    assert_eq!(
        (heads.0, heads.2, heads.4, heads.6, heads.8, heads.10),
        (Seq(1), Seq(2), Seq(2), Seq(2), Seq(3), Seq(4))
    );
    assert_eq!(head(store, &volume).await, Seq(4));
}

pub async fn read_only_transaction_leaves_head_unchanged<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    write_at(store, &volume, "f", 0, 4, blob("x"), 0).await;
    let before = collect_entries(store, &volume, 0).await;
    let (value_read, info, content, observed, ()) = transact(
        store,
        &volume,
        (
            GetKey { key: key("a") },
            GetFileInfo { file: file("f") },
            VisibleBlobs {
                file: file("f"),
                offset: 0,
                size: 10,
            },
            GetHead,
            always(),
        ),
    )
    .await;
    assert_eq!(value_read.map(|stored| stored.seq), Some(Seq(1)));
    assert_eq!(info.seq, Some(Seq(2)));
    assert_eq!(content.info, info);
    assert_eq!(observed, Seq(2));
    assert_eq!(head(store, &volume).await, Seq(2));
    assert_eq!(collect_entries(store, &volume, 0).await, before);
}

pub async fn execute_returns_one_outcome_per_operation<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    let operations = vec1![
        Op::SetKey(SetKey {
            key: key("a"),
            value: value("1"),
        }),
        Op::DeleteKey(DeleteKey { key: key("b") }),
        Op::GetKey(GetKey { key: key("a") }),
        Op::Assert(always()),
        Op::Write(Write {
            file: file("f"),
            at: WriteAt::Offset(1),
            size: 2,
            content: x,
            content_offset: 5,
        }),
        Op::Truncate(Truncate {
            file: file("g"),
            len: 0,
        }),
        Op::VisibleBlobs(VisibleBlobs {
            file: file("f"),
            offset: 0,
            size: 3,
        }),
        Op::GetFileInfo(GetFileInfo { file: file("f") }),
        Op::GetHead(GetHead),
    ];
    let outcomes = match store.execute(&volume, operations).await {
        Ok(outcomes) => outcomes.into_vec(),
        Err(error) => panic!("execute failed: {error:?}"),
    };
    let info = FileInfo {
        len: 3,
        seq: Some(Seq(2)),
    };
    assert_eq!(outcomes.len(), 9);
    assert_eq!(outcomes[0], Outcome::Done);
    assert_eq!(outcomes[1], Outcome::Done);
    assert_eq!(
        outcomes[2],
        Outcome::Value(Some(Stored {
            seq: Seq(1),
            entry: KeyValue {
                key: key("a"),
                value: value("1")
            }
        }))
    );
    assert_eq!(outcomes[3], Outcome::Done);
    assert_eq!(outcomes[4], Outcome::Written(Written { offset: 1 }));
    assert_eq!(outcomes[5], Outcome::Done);
    match &outcomes[6] {
        Outcome::FileContent(content) => {
            assert_eq!(content.info, info);
            assert_eq!(
                provenance(content, 0, 3),
                [holes(1), data(x, 5, 2)].concat()
            );
        }
        other => panic!("VisibleBlobs gave {other:?}"),
    }
    assert_eq!(outcomes[7], Outcome::FileInfo(info));
    assert_eq!(outcomes[8], Outcome::Head(Seq(3)));
}

pub async fn write_operations_append_one_entry_each<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    set(store, &volume, "present", "1").await;
    let (.., observed) = transact(
        store,
        &volume,
        (
            SetKey {
                key: key("a"),
                value: value("1"),
            },
            DeleteKey {
                key: key("present"),
            },
            DeleteKey { key: key("absent") },
            Write {
                file: file("f"),
                at: WriteAt::Offset(3),
                size: 2,
                content: x,
                content_offset: 7,
            },
            Write {
                file: file("f"),
                at: WriteAt::Offset(100),
                size: 0,
                content: x,
                content_offset: 0,
            },
            Truncate {
                file: file("f"),
                len: 4,
            },
            SetKey {
                key: key("a"),
                value: value("1"),
            },
            GetHead,
        ),
    )
    .await;
    assert_eq!(observed, Seq(6));
    assert_eq!(
        collect_entries(store, &volume, 1).await,
        numbered(
            2,
            vec![
                set_entry("a", "1"),
                delete_entry("present"),
                write_entry("f", 3, 2, x, 7),
                truncate_entry("f", 4),
                set_entry("a", "1"),
            ]
        )
    );
}
