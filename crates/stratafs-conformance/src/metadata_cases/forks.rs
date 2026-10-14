use stratafs_store::error::StoreError;
use stratafs_store::metadata::{FileInfo, ForkRejection, MetadataStore, Seq};

use crate::support::{
    blob, collect_entries, create, data, delete, delete_entry, file_info, fork, get_value, head,
    holes, listed, numbered, pairs, read_file, set, set_entry, stored_seq, truncate,
    truncate_entry, value, write_at, write_entry,
};

pub async fn fork_inherits_entries_up_to_position<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    set(store, &parent, "b", "2").await;
    write_at(store, &parent, "f", 0, 10, x, 0).await;
    set(store, &parent, "c", "3").await;
    truncate(store, &parent, "f", 5).await;
    let history = collect_entries(store, &parent, 0).await;
    assert_eq!(
        history,
        numbered(
            1,
            vec![
                set_entry("a", "1"),
                set_entry("b", "2"),
                write_entry("f", 0, 10, x, 0),
                set_entry("c", "3"),
                truncate_entry("f", 5),
            ]
        )
    );

    let child = fork(store, &parent, 3).await;
    assert_ne!(child, parent);
    assert_eq!(head(store, &child).await, Seq(3));
    assert_eq!(
        collect_entries(store, &child, 0).await,
        history[..3].to_vec()
    );
    assert_eq!(stored_seq(store, &child, "a").await, Some(Seq(1)));
    assert_eq!(stored_seq(store, &child, "b").await, Some(Seq(2)));
    assert_eq!(get_value(store, &child, "c").await, None);
    assert_eq!(
        listed(store, &child, "").await,
        pairs(&[("a", "1"), ("b", "2")])
    );
    assert_eq!(
        file_info(store, &child, "f").await,
        FileInfo {
            len: 10,
            seq: Some(Seq(3))
        }
    );
    assert_eq!(read_file(store, &child, "f").await, data(x, 0, 10));

    let whole = fork(store, &parent, 5).await;
    assert_eq!(head(store, &whole).await, Seq(5));
    assert_eq!(collect_entries(store, &whole, 0).await, history);
    assert_eq!(
        listed(store, &whole, "").await,
        pairs(&[("a", "1"), ("b", "2"), ("c", "3")])
    );
    assert_eq!(read_file(store, &whole, "f").await, data(x, 0, 5));
    assert_eq!(
        file_info(store, &whole, "f").await,
        FileInfo {
            len: 5,
            seq: Some(Seq(5))
        }
    );
}

pub async fn fork_and_parent_diverge_after_fork<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    write_at(store, &parent, "f", 0, 8, x, 0).await;
    let child = fork(store, &parent, 2).await;

    set(store, &child, "a", "child").await;
    set(store, &child, "b", "child-only").await;
    write_at(store, &child, "f", 2, 2, y, 0).await;

    set(store, &parent, "a", "parent").await;
    set(store, &parent, "c", "parent-only").await;
    write_at(store, &parent, "f", 6, 4, z, 0).await;

    assert_eq!(head(store, &child).await, Seq(5));
    assert_eq!(head(store, &parent).await, Seq(5));
    assert_eq!(
        listed(store, &child, "").await,
        pairs(&[("a", "child"), ("b", "child-only")])
    );
    assert_eq!(
        listed(store, &parent, "").await,
        pairs(&[("a", "parent"), ("c", "parent-only")])
    );
    assert_eq!(stored_seq(store, &child, "a").await, Some(Seq(3)));
    assert_eq!(stored_seq(store, &parent, "a").await, Some(Seq(3)));
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 2), data(y, 0, 2), data(x, 4, 4)].concat()
    );
    assert_eq!(
        read_file(store, &parent, "f").await,
        [data(x, 0, 6), data(z, 0, 4)].concat()
    );
    assert_eq!(
        collect_entries(store, &child, 2).await,
        numbered(
            3,
            vec![
                set_entry("a", "child"),
                set_entry("b", "child-only"),
                write_entry("f", 2, 2, y, 0),
            ]
        )
    );
    assert_eq!(
        collect_entries(store, &parent, 2).await,
        numbered(
            3,
            vec![
                set_entry("a", "parent"),
                set_entry("c", "parent-only"),
                write_entry("f", 6, 4, z, 0),
            ]
        )
    );
}

pub async fn fork_beyond_head_is_rejected<S: MetadataStore>(store: &S) {
    let empty = create(store).await;
    let result = store.fork(&empty, Seq(1)).await;
    assert!(
        matches!(
            result,
            Err(StoreError::Rejected(ForkRejection::BeyondHead {
                at: Seq(1),
                head: Seq(0)
            }))
        ),
        "{result:?}"
    );

    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    set(store, &parent, "b", "2").await;
    for at in [3, 4, u64::MAX] {
        let result = store.fork(&parent, Seq(at)).await;
        assert!(
            matches!(
                result,
                Err(StoreError::Rejected(ForkRejection::BeyondHead { at: rejected, head: Seq(2) })) if rejected == Seq(at)
            ),
            "fork at {at} gave {result:?}"
        );
    }

    let child = fork(store, &parent, 1).await;
    let result = store.fork(&child, Seq(2)).await;
    assert!(
        matches!(
            result,
            Err(StoreError::Rejected(ForkRejection::BeyondHead {
                at: Seq(2),
                head: Seq(1)
            }))
        ),
        "{result:?}"
    );
    assert_eq!(head(store, &parent).await, Seq(2));
}

pub async fn fork_at_zero_is_empty<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    write_at(store, &parent, "f", 0, 4, x, 0).await;
    let child = fork(store, &parent, 0).await;
    assert_eq!(head(store, &child).await, Seq(0));
    assert!(collect_entries(store, &child, 0).await.is_empty());
    assert!(listed(store, &child, "").await.is_empty());
    assert_eq!(
        file_info(store, &child, "f").await,
        FileInfo { len: 0, seq: None }
    );
    assert!(read_file(store, &child, "f").await.is_empty());

    set(store, &child, "b", "child").await;
    write_at(store, &child, "f", 2, 1, x, 9).await;
    assert_eq!(listed(store, &child, "").await, pairs(&[("b", "child")]));
    assert_eq!(
        read_file(store, &child, "f").await,
        [holes(2), data(x, 9, 1)].concat()
    );
    assert_eq!(listed(store, &parent, "").await, pairs(&[("a", "1")]));
    assert_eq!(read_file(store, &parent, "f").await, data(x, 0, 4));
}

pub async fn fork_of_empty_volume_at_zero<S: MetadataStore>(store: &S) {
    let parent = create(store).await;
    let child = fork(store, &parent, 0).await;
    assert_ne!(child, parent);
    assert_eq!(head(store, &child).await, Seq(0));
    set(store, &child, "a", "child").await;
    set(store, &parent, "b", "parent").await;
    set(store, &parent, "c", "parent").await;
    assert_eq!(listed(store, &child, "").await, pairs(&[("a", "child")]));
    assert_eq!(
        listed(store, &parent, "").await,
        pairs(&[("b", "parent"), ("c", "parent")])
    );
    assert_eq!(head(store, &child).await, Seq(1));
    assert_eq!(head(store, &parent).await, Seq(2));
}

pub async fn fork_at_older_position_ignores_later_parent_entries<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let parent = create(store).await;
    set(store, &parent, "a", "1").await;
    write_at(store, &parent, "f", 0, 10, x, 0).await;
    set(store, &parent, "b", "1").await;
    set(store, &parent, "a", "2").await;
    delete(store, &parent, "b").await;
    write_at(store, &parent, "f", 5, 10, y, 0).await;
    truncate(store, &parent, "f", 3).await;
    set(store, &parent, "c", "1").await;
    let parent_history = collect_entries(store, &parent, 0).await;
    assert_eq!(parent_history.len(), 8);

    let child = fork(store, &parent, 3).await;
    assert_eq!(head(store, &child).await, Seq(3));
    assert_eq!(
        collect_entries(store, &child, 0).await,
        parent_history[..3].to_vec()
    );
    assert_eq!(
        listed(store, &child, "").await,
        pairs(&[("a", "1"), ("b", "1")])
    );
    assert_eq!(stored_seq(store, &child, "a").await, Some(Seq(1)));
    assert_eq!(stored_seq(store, &child, "b").await, Some(Seq(3)));
    assert_eq!(
        file_info(store, &child, "f").await,
        FileInfo {
            len: 10,
            seq: Some(Seq(2))
        }
    );
    assert_eq!(read_file(store, &child, "f").await, data(x, 0, 10));

    let after_delete = fork(store, &parent, 5).await;
    assert_eq!(listed(store, &after_delete, "").await, pairs(&[("a", "2")]));
    assert_eq!(read_file(store, &after_delete, "f").await, data(x, 0, 10));

    let after_overwrite = fork(store, &parent, 6).await;
    assert_eq!(
        read_file(store, &after_overwrite, "f").await,
        [data(x, 0, 5), data(y, 0, 10)].concat()
    );
    assert_eq!(
        file_info(store, &after_overwrite, "f").await,
        FileInfo {
            len: 15,
            seq: Some(Seq(6))
        }
    );

    set(store, &child, "d", "child").await;
    assert_eq!(stored_seq(store, &child, "d").await, Some(Seq(4)));
    assert_eq!(get_value(store, &parent, "d").await, None);
    assert_eq!(get_value(store, &child, "c").await, None);
    assert_eq!(
        collect_entries(store, &child, 3).await,
        numbered(4, vec![set_entry("d", "child")])
    );
    assert_eq!(collect_entries(store, &parent, 0).await, parent_history);
}

pub async fn nested_forks_see_ancestors_within_bounds<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let grandparent = create(store).await;
    set(store, &grandparent, "a", "g1").await;
    write_at(store, &grandparent, "f", 0, 10, x, 0).await;
    set(store, &grandparent, "b", "g").await;
    set(store, &grandparent, "a", "g2").await;

    let parent = fork(store, &grandparent, 3).await;
    set(store, &parent, "c", "p").await;
    write_at(store, &parent, "f", 4, 2, y, 0).await;
    delete(store, &parent, "b").await;
    set(store, &parent, "a", "p").await;

    set(store, &grandparent, "d", "g").await;
    write_at(store, &grandparent, "f", 0, 10, z, 0).await;

    let child = fork(store, &parent, 5).await;
    assert_eq!(head(store, &child).await, Seq(5));
    assert_eq!(
        listed(store, &child, "").await,
        pairs(&[("a", "g1"), ("b", "g"), ("c", "p")])
    );
    assert_eq!(stored_seq(store, &child, "a").await, Some(Seq(1)));
    assert_eq!(stored_seq(store, &child, "b").await, Some(Seq(3)));
    assert_eq!(stored_seq(store, &child, "c").await, Some(Seq(4)));
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 4), data(y, 0, 2), data(x, 6, 4)].concat()
    );
    assert_eq!(
        file_info(store, &child, "f").await,
        FileInfo {
            len: 10,
            seq: Some(Seq(5))
        }
    );
    assert_eq!(
        collect_entries(store, &child, 0).await,
        numbered(
            1,
            vec![
                set_entry("a", "g1"),
                write_entry("f", 0, 10, x, 0),
                set_entry("b", "g"),
                set_entry("c", "p"),
                write_entry("f", 4, 2, y, 0),
            ]
        )
    );

    set(store, &child, "e", "c").await;
    delete(store, &child, "a").await;
    assert_eq!(
        listed(store, &child, "").await,
        pairs(&[("b", "g"), ("c", "p"), ("e", "c")])
    );
    assert_eq!(
        listed(store, &parent, "").await,
        pairs(&[("a", "p"), ("c", "p")])
    );
    assert_eq!(
        listed(store, &grandparent, "").await,
        pairs(&[("a", "g2"), ("b", "g"), ("d", "g")])
    );
    assert_eq!(read_file(store, &grandparent, "f").await, data(z, 0, 10));

    let great_grandchild = fork(store, &child, 7).await;
    assert_eq!(
        listed(store, &great_grandchild, "").await,
        pairs(&[("b", "g"), ("c", "p"), ("e", "c")])
    );
    assert_eq!(
        read_file(store, &great_grandchild, "f").await,
        [data(x, 0, 4), data(y, 0, 2), data(x, 6, 4)].concat()
    );
}

pub async fn fork_of_fork_inside_inherited_range<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let grandparent = create(store).await;
    set(store, &grandparent, "k1", "1").await;
    write_at(store, &grandparent, "f", 0, 6, x, 0).await;
    set(store, &grandparent, "k3", "3").await;
    set(store, &grandparent, "k4", "4").await;
    truncate(store, &grandparent, "f", 2).await;
    set(store, &grandparent, "k6", "6").await;
    let grandparent_history = collect_entries(store, &grandparent, 0).await;

    let parent = fork(store, &grandparent, 5).await;
    set(store, &parent, "p6", "6").await;
    write_at(store, &parent, "f", 0, 1, x, 100).await;

    let child = fork(store, &parent, 3).await;
    assert_eq!(head(store, &child).await, Seq(3));
    assert_eq!(
        collect_entries(store, &child, 0).await,
        grandparent_history[..3].to_vec()
    );
    assert_eq!(
        listed(store, &child, "").await,
        pairs(&[("k1", "1"), ("k3", "3")])
    );
    assert_eq!(read_file(store, &child, "f").await, data(x, 0, 6));
    assert_eq!(
        file_info(store, &child, "f").await,
        FileInfo {
            len: 6,
            seq: Some(Seq(2))
        }
    );

    let at_inherited_end = fork(store, &parent, 5).await;
    assert_eq!(
        collect_entries(store, &at_inherited_end, 0).await,
        grandparent_history[..5].to_vec()
    );
    assert_eq!(
        read_file(store, &at_inherited_end, "f").await,
        data(x, 0, 2)
    );
}

pub async fn fork_of_fork_inside_own_range<S: MetadataStore>(store: &S) {
    let grandparent = create(store).await;
    set(store, &grandparent, "g1", "1").await;
    set(store, &grandparent, "g2", "2").await;
    set(store, &grandparent, "g3", "3").await;

    let parent = fork(store, &grandparent, 2).await;
    set(store, &parent, "p3", "3").await;
    delete(store, &parent, "g1").await;
    set(store, &parent, "p5", "5").await;

    let child = fork(store, &parent, 4).await;
    assert_eq!(
        collect_entries(store, &child, 0).await,
        numbered(
            1,
            vec![
                set_entry("g1", "1"),
                set_entry("g2", "2"),
                set_entry("p3", "3"),
                delete_entry("g1"),
            ]
        )
    );
    assert_eq!(
        listed(store, &child, "").await,
        pairs(&[("g2", "2"), ("p3", "3")])
    );
    assert_eq!(get_value(store, &child, "g3").await, None);
    assert_eq!(get_value(store, &child, "p5").await, None);
    assert_eq!(get_value(store, &child, "g1").await, None);
    assert_eq!(get_value(store, &grandparent, "g1").await, Some(value("1")));
}

pub async fn sibling_forks_are_independent<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let parent = create(store).await;
    set(store, &parent, "shared", "0").await;
    write_at(store, &parent, "f", 0, 4, x, 0).await;
    let left = fork(store, &parent, 2).await;
    let right = fork(store, &parent, 2).await;
    assert_ne!(left, right);

    set(store, &left, "shared", "left").await;
    write_at(store, &left, "f", 1, 2, y, 0).await;
    delete(store, &right, "shared").await;
    truncate(store, &right, "f", 1).await;

    assert_eq!(listed(store, &left, "").await, pairs(&[("shared", "left")]));
    assert!(listed(store, &right, "").await.is_empty());
    assert_eq!(listed(store, &parent, "").await, pairs(&[("shared", "0")]));
    assert_eq!(
        read_file(store, &left, "f").await,
        [data(x, 0, 1), data(y, 0, 2), data(x, 3, 1)].concat()
    );
    assert_eq!(read_file(store, &right, "f").await, data(x, 0, 1));
    assert_eq!(read_file(store, &parent, "f").await, data(x, 0, 4));
}
