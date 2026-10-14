use stratafs_store::metadata::{FileInfo, MetadataStore, Seq, TransactRejection, Written};
use stratafs_store::transaction::{GetHead, SetKey, VisibleBlobs, Write, WriteAt};

use crate::support::{
    blob, collect_entries, create, data, file, file_info, fork, head, holes, key, listed, numbered,
    pairs, read_file, read_range, set, transact, transact_rejection, truncate, truncate_entry,
    value, write, write_at, write_entry,
};

fn info(len: u64, seq: u64) -> FileInfo {
    FileInfo {
        len,
        seq: Some(Seq(seq)),
    }
}

pub async fn unwritten_file_is_empty<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    set(store, &volume, "a", "1").await;
    assert_eq!(
        file_info(store, &volume, "f").await,
        FileInfo { len: 0, seq: None }
    );
    for (offset, size) in [
        (0, 0),
        (0, 10),
        (5, 10),
        (0, u64::MAX),
        (u64::MAX, u64::MAX),
    ] {
        let content = transact(
            store,
            &volume,
            VisibleBlobs {
                file: file("f"),
                offset,
                size,
            },
        )
        .await;
        assert_eq!(content.info, FileInfo { len: 0, seq: None });
        assert!(content.blobs.is_empty(), "{content:?}");
    }
}

pub async fn write_then_read_provenance<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 10, x, 3).await;
    assert_eq!(file_info(store, &volume, "f").await, info(10, 1));
    assert_eq!(read_file(store, &volume, "f").await, data(x, 3, 10));
    let content = transact(
        store,
        &volume,
        VisibleBlobs {
            file: file("f"),
            offset: 0,
            size: 10,
        },
    )
    .await;
    assert!(!content.blobs.is_empty());
    assert!(content.blobs.iter().all(|piece| piece.content == x));
}

pub async fn overwrite_in_middle_splits_piece<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 10, x, 0).await;
    write_at(store, &volume, "f", 3, 3, y, 0).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(x, 0, 3), data(y, 0, 3), data(x, 6, 4)].concat()
    );
    assert_eq!(file_info(store, &volume, "f").await, info(10, 2));
}

pub async fn overwrite_covering_several_pieces<S: MetadataStore>(store: &S) {
    let (x, y, z, w) = (blob("x"), blob("y"), blob("z"), blob("w"));
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 4, x, 0).await;
    write_at(store, &volume, "f", 4, 4, y, 0).await;
    write_at(store, &volume, "f", 8, 4, z, 0).await;
    write_at(store, &volume, "f", 2, 8, w, 100).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(x, 0, 2), data(w, 100, 8), data(z, 2, 2)].concat()
    );
    write_at(store, &volume, "f", 0, 12, y, 1).await;
    assert_eq!(read_file(store, &volume, "f").await, data(y, 1, 12));
    write_at(store, &volume, "f", 1, 20, x, 0).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(y, 1, 1), data(x, 0, 20)].concat()
    );
    assert_eq!(file_info(store, &volume, "f").await, info(21, 6));
}

pub async fn exact_overwrite_replaces_piece<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let volume = create(store).await;
    write_at(store, &volume, "f", 2, 4, x, 0).await;
    write_at(store, &volume, "f", 2, 4, y, 7).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [holes(2), data(y, 7, 4)].concat()
    );
    write_at(store, &volume, "f", 2, 4, x, 0).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [holes(2), data(x, 0, 4)].concat()
    );
    assert_eq!(file_info(store, &volume, "f").await, info(6, 3));
}

pub async fn write_past_end_leaves_zero_hole<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 3, x, 0).await;
    write_at(store, &volume, "f", 10, 2, y, 0).await;
    assert_eq!(file_info(store, &volume, "f").await, info(12, 2));
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(x, 0, 3), holes(7), data(y, 0, 2)].concat()
    );
    let hole = transact(
        store,
        &volume,
        VisibleBlobs {
            file: file("f"),
            offset: 4,
            size: 4,
        },
    )
    .await;
    assert!(hole.blobs.is_empty(), "{hole:?}");
    assert_eq!(hole.info, info(12, 2));
}

pub async fn write_at_end_appends_at_length<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let volume = create(store).await;
    assert_eq!(
        write(store, &volume, "f", WriteAt::End, 3, x, 0).await,
        Written { offset: 0 }
    );
    assert_eq!(
        write(store, &volume, "f", WriteAt::End, 2, y, 5).await,
        Written { offset: 3 }
    );
    truncate(store, &volume, "f", 10).await;
    assert_eq!(
        write(store, &volume, "f", WriteAt::End, 1, z, 0).await,
        Written { offset: 10 }
    );
    let (first, second, observed) = transact(
        store,
        &volume,
        (
            Write {
                file: file("f"),
                at: WriteAt::End,
                size: 2,
                content: x,
                content_offset: 1,
            },
            Write {
                file: file("f"),
                at: WriteAt::End,
                size: 1,
                content: y,
                content_offset: 0,
            },
            GetHead,
        ),
    )
    .await;
    assert_eq!(first, Written { offset: 11 });
    assert_eq!(second, Written { offset: 13 });
    assert_eq!(observed, Seq(6));
    assert_eq!(
        read_file(store, &volume, "f").await,
        [
            data(x, 0, 3),
            data(y, 5, 2),
            holes(5),
            data(z, 0, 1),
            data(x, 1, 2),
            data(y, 0, 1),
        ]
        .concat()
    );
    assert_eq!(
        collect_entries(store, &volume, 0).await,
        numbered(
            1,
            vec![
                write_entry("f", 0, 3, x, 0),
                write_entry("f", 3, 2, y, 5),
                truncate_entry("f", 10),
                write_entry("f", 10, 1, z, 0),
                write_entry("f", 11, 2, x, 1),
                write_entry("f", 13, 1, y, 0),
            ]
        )
    );
    let child = fork(store, &volume, 3).await;
    assert_eq!(
        write(store, &child, "f", WriteAt::End, 1, z, 9).await,
        Written { offset: 10 }
    );
}

pub async fn zero_size_write_is_noop<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 4, x, 0).await;
    assert_eq!(
        write(store, &volume, "f", WriteAt::Offset(100), 0, x, 0).await,
        Written { offset: 100 }
    );
    assert_eq!(
        write(store, &volume, "f", WriteAt::Offset(2), 0, x, 0).await,
        Written { offset: 2 }
    );
    assert_eq!(
        write(store, &volume, "f", WriteAt::End, 0, x, 0).await,
        Written { offset: 4 }
    );
    assert_eq!(
        write(store, &volume, "g", WriteAt::End, 0, x, 0).await,
        Written { offset: 0 }
    );
    assert_eq!(head(store, &volume).await, Seq(1));
    assert_eq!(file_info(store, &volume, "f").await, info(4, 1));
    assert_eq!(
        file_info(store, &volume, "g").await,
        FileInfo { len: 0, seq: None }
    );
    assert_eq!(read_file(store, &volume, "f").await, data(x, 0, 4));
    assert_eq!(collect_entries(store, &volume, 0).await.len(), 1);
}

pub async fn content_offset_is_honoured<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 10, x, 50).await;
    assert_eq!(read_file(store, &volume, "f").await, data(x, 50, 10));
    write_at(store, &volume, "f", 0, 2, y, 0).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(y, 0, 2), data(x, 52, 8)].concat()
    );
    write_at(store, &volume, "f", 8, 2, z, 1000).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(y, 0, 2), data(x, 52, 6), data(z, 1000, 2)].concat()
    );
    write_at(store, &volume, "f", 4, 1, y, 9).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [
            data(y, 0, 2),
            data(x, 52, 2),
            data(y, 9, 1),
            data(x, 55, 3),
            data(z, 1000, 2)
        ]
        .concat()
    );
    truncate(store, &volume, "f", 7).await;
    assert_eq!(
        read_range(store, &volume, "f", 3, 4).await.1,
        [data(x, 53, 1), data(y, 9, 1), data(x, 55, 2)].concat()
    );
    assert_eq!(
        read_range(store, &volume, "f", 6, 1).await.1,
        data(x, 56, 1)
    );
}

pub async fn truncate_shrink_then_grow_reads_zeros<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 10, x, 0).await;
    truncate(store, &volume, "f", 4).await;
    assert_eq!(read_file(store, &volume, "f").await, data(x, 0, 4));
    assert_eq!(file_info(store, &volume, "f").await, info(4, 2));
    truncate(store, &volume, "f", 8).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(x, 0, 4), holes(4)].concat()
    );
    assert_eq!(file_info(store, &volume, "f").await, info(8, 3));
    write_at(store, &volume, "f", 12, 1, y, 0).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(x, 0, 4), holes(8), data(y, 0, 1)].concat()
    );
    truncate(store, &volume, "f", 2).await;
    write_at(store, &volume, "f", 9, 1, y, 3).await;
    assert_eq!(
        read_file(store, &volume, "f").await,
        [data(x, 0, 2), holes(7), data(y, 3, 1)].concat()
    );
    truncate(store, &volume, "f", 0).await;
    assert!(read_file(store, &volume, "f").await.is_empty());
    assert_eq!(file_info(store, &volume, "f").await, info(0, 7));
    truncate(store, &volume, "f", 6).await;
    assert_eq!(read_file(store, &volume, "f").await, holes(6));
}

pub async fn truncate_to_same_length_appends_entry<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 4, x, 0).await;
    assert_eq!(truncate(store, &volume, "f", 4).await, Seq(2));
    assert_eq!(file_info(store, &volume, "f").await, info(4, 2));
    assert_eq!(read_file(store, &volume, "f").await, data(x, 0, 4));
    assert_eq!(
        collect_entries(store, &volume, 1).await,
        numbered(2, vec![truncate_entry("f", 4)])
    );
}

pub async fn truncate_of_unwritten_file_creates_hole<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    truncate(store, &volume, "g", 5).await;
    assert_eq!(file_info(store, &volume, "g").await, info(5, 1));
    assert_eq!(read_file(store, &volume, "g").await, holes(5));
    truncate(store, &volume, "h", 0).await;
    assert_eq!(file_info(store, &volume, "h").await, info(0, 2));
}

pub async fn truncate_in_fork_hides_parent_bytes_after_regrowth<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let parent = create(store).await;
    write_at(store, &parent, "f", 0, 10, x, 0).await;
    let child = fork(store, &parent, 1).await;
    truncate(store, &child, "f", 3).await;
    assert_eq!(read_file(store, &child, "f").await, data(x, 0, 3));
    truncate(store, &child, "f", 10).await;
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 3), holes(7)].concat()
    );
    write_at(store, &child, "f", 8, 1, y, 0).await;
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 3), holes(5), data(y, 0, 1), holes(1)].concat()
    );
    assert_eq!(read_file(store, &parent, "f").await, data(x, 0, 10));

    let other = fork(store, &parent, 1).await;
    truncate(store, &other, "f", 2).await;
    write_at(store, &other, "f", 5, 1, z, 0).await;
    assert_eq!(
        read_file(store, &other, "f").await,
        [data(x, 0, 2), holes(3), data(z, 0, 1)].concat()
    );

    write_at(store, &parent, "f", 10, 10, z, 0).await;
    let grandchild = fork(store, &child, 4).await;
    write_at(store, &grandchild, "f", 19, 1, y, 5).await;
    assert_eq!(
        read_file(store, &grandchild, "f").await,
        [
            data(x, 0, 3),
            holes(5),
            data(y, 0, 1),
            holes(10),
            data(y, 5, 1)
        ]
        .concat()
    );
    truncate(store, &grandchild, "f", 1).await;
    truncate(store, &grandchild, "f", 4).await;
    assert_eq!(
        read_file(store, &grandchild, "f").await,
        [data(x, 0, 1), holes(3)].concat()
    );
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 3), holes(5), data(y, 0, 1), holes(1)].concat()
    );
}

pub async fn write_in_fork_overlays_inherited_bytes<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let parent = create(store).await;
    write_at(store, &parent, "f", 0, 10, x, 0).await;
    let child = fork(store, &parent, 1).await;
    write_at(store, &child, "f", 3, 2, y, 0).await;
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 3), data(y, 0, 2), data(x, 5, 5)].concat()
    );
    assert_eq!(file_info(store, &child, "f").await, info(10, 2));
    assert_eq!(read_file(store, &parent, "f").await, data(x, 0, 10));
    assert_eq!(
        write(store, &child, "f", WriteAt::End, 2, z, 0).await,
        Written { offset: 10 }
    );
    let grandchild = fork(store, &child, 3).await;
    write_at(store, &grandchild, "f", 0, 4, z, 7).await;
    assert_eq!(
        read_file(store, &grandchild, "f").await,
        [data(z, 7, 4), data(y, 1, 1), data(x, 5, 5), data(z, 0, 2)].concat()
    );
    assert_eq!(
        read_range(store, &grandchild, "f", 2, 9).await.1,
        [data(z, 9, 2), data(y, 1, 1), data(x, 5, 5), data(z, 0, 1)].concat()
    );
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 3), data(y, 0, 2), data(x, 5, 5), data(z, 0, 2)].concat()
    );
}

pub async fn fork_at_older_position_sees_older_file<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let parent = create(store).await;
    write_at(store, &parent, "f", 0, 10, x, 0).await;
    write_at(store, &parent, "f", 5, 10, y, 0).await;
    truncate(store, &parent, "f", 3).await;
    write_at(store, &parent, "f", 0, 2, z, 0).await;

    let at_zero = fork(store, &parent, 0).await;
    assert_eq!(
        file_info(store, &at_zero, "f").await,
        FileInfo { len: 0, seq: None }
    );
    assert!(read_file(store, &at_zero, "f").await.is_empty());

    let at_one = fork(store, &parent, 1).await;
    assert_eq!(file_info(store, &at_one, "f").await, info(10, 1));
    assert_eq!(read_file(store, &at_one, "f").await, data(x, 0, 10));

    let at_two = fork(store, &parent, 2).await;
    assert_eq!(file_info(store, &at_two, "f").await, info(15, 2));
    assert_eq!(
        read_file(store, &at_two, "f").await,
        [data(x, 0, 5), data(y, 0, 10)].concat()
    );

    let at_three = fork(store, &parent, 3).await;
    assert_eq!(file_info(store, &at_three, "f").await, info(3, 3));
    assert_eq!(read_file(store, &at_three, "f").await, data(x, 0, 3));

    let at_four = fork(store, &parent, 4).await;
    assert_eq!(file_info(store, &at_four, "f").await, info(3, 4));
    assert_eq!(
        read_file(store, &at_four, "f").await,
        [data(z, 0, 2), data(x, 2, 1)].concat()
    );

    truncate(store, &at_two, "f", 7).await;
    truncate(store, &at_two, "f", 15).await;
    assert_eq!(
        read_file(store, &at_two, "f").await,
        [data(x, 0, 5), data(y, 0, 2), holes(8)].concat()
    );
    assert_eq!(
        read_file(store, &parent, "f").await,
        [data(z, 0, 2), data(x, 2, 1)].concat()
    );
}

pub async fn file_info_tracks_latest_write_or_truncate<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 4, x, 0).await;
    set(store, &volume, "a", "1").await;
    write_at(store, &volume, "g", 0, 2, x, 0).await;
    assert_eq!(file_info(store, &volume, "f").await, info(4, 1));
    assert_eq!(file_info(store, &volume, "g").await, info(2, 3));
    truncate(store, &volume, "f", 6).await;
    assert_eq!(file_info(store, &volume, "f").await, info(6, 4));
    write_at(store, &volume, "f", 1, 1, x, 0).await;
    assert_eq!(file_info(store, &volume, "f").await, info(6, 5));

    let child = fork(store, &volume, 4).await;
    assert_eq!(file_info(store, &child, "f").await, info(6, 4));
    assert_eq!(file_info(store, &child, "g").await, info(2, 3));
    set(store, &child, "b", "1").await;
    assert_eq!(file_info(store, &child, "f").await, info(6, 4));
    write_at(store, &child, "f", 0, 1, x, 0).await;
    assert_eq!(file_info(store, &child, "f").await, info(6, 6));
    assert_eq!(file_info(store, &child, "g").await, info(2, 3));
    assert_eq!(file_info(store, &volume, "f").await, info(6, 5));
}

pub async fn range_beyond_length_gives_no_blobs<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 10, x, 0).await;
    for (offset, size) in [
        (10, 5),
        (11, 1),
        (10, u64::MAX),
        (u64::MAX, u64::MAX),
        (u64::MAX - 1, 1),
        (3, 0),
        (0, 0),
    ] {
        let content = transact(
            store,
            &volume,
            VisibleBlobs {
                file: file("f"),
                offset,
                size,
            },
        )
        .await;
        assert_eq!(content.info, info(10, 1));
        assert!(
            content.blobs.is_empty(),
            "range ({offset}, {size}) gave {content:?}"
        );
    }
    truncate(store, &volume, "f", 4).await;
    let content = transact(
        store,
        &volume,
        VisibleBlobs {
            file: file("f"),
            offset: 4,
            size: 6,
        },
    )
    .await;
    assert!(content.blobs.is_empty(), "{content:?}");
}

pub async fn partial_range_clips_pieces<S: MetadataStore>(store: &S) {
    let (x, y, z, w) = (blob("x"), blob("y"), blob("z"), blob("w"));
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 4, x, 0).await;
    write_at(store, &volume, "f", 4, 4, y, 10).await;
    write_at(store, &volume, "f", 8, 4, z, 0).await;
    assert_eq!(
        read_range(store, &volume, "f", 2, 8).await.1,
        [data(x, 2, 2), data(y, 10, 4), data(z, 0, 2)].concat()
    );
    assert_eq!(
        read_range(store, &volume, "f", 5, 1).await.1,
        data(y, 11, 1)
    );
    assert_eq!(
        read_range(store, &volume, "f", 6, 100).await.1,
        [data(y, 12, 2), data(z, 0, 4)].concat()
    );
    assert_eq!(
        read_range(store, &volume, "f", 0, 12).await.1,
        [data(x, 0, 4), data(y, 10, 4), data(z, 0, 4)].concat()
    );
    write_at(store, &volume, "f", 20, 2, w, 0).await;
    assert_eq!(
        read_range(store, &volume, "f", 10, 15).await.1,
        [data(z, 2, 2), holes(8), data(w, 0, 2)].concat()
    );
    assert_eq!(
        read_range(store, &volume, "f", 13, 8).await.1,
        [holes(7), data(w, 0, 1)].concat()
    );
}

pub async fn files_are_independent<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let volume = create(store).await;
    write_at(store, &volume, "data", 0, 4, x, 0).await;
    write_at(store, &volume, "data2", 0, 2, y, 0).await;
    truncate(store, &volume, "dat", 3).await;
    write_at(store, &volume, "data/child", 1, 1, y, 1).await;
    truncate(store, &volume, "data", 1).await;
    assert_eq!(read_file(store, &volume, "data").await, data(x, 0, 1));
    assert_eq!(read_file(store, &volume, "data2").await, data(y, 0, 2));
    assert_eq!(read_file(store, &volume, "dat").await, holes(3));
    assert_eq!(
        read_file(store, &volume, "data/child").await,
        [holes(1), data(y, 1, 1)].concat()
    );
    assert_eq!(
        file_info(store, &volume, "Data").await,
        FileInfo { len: 0, seq: None }
    );
    assert_eq!(file_info(store, &volume, "data").await, info(1, 5));
    assert_eq!(file_info(store, &volume, "data2").await, info(2, 2));
    assert_eq!(file_info(store, &volume, "dat").await, info(3, 3));
    assert_eq!(file_info(store, &volume, "data/child").await, info(2, 4));
}

pub async fn fork_after_exact_overwrite_hides_dead_piece<S: MetadataStore>(store: &S) {
    let (x, y) = (blob("x"), blob("y"));
    let parent = create(store).await;
    write_at(store, &parent, "f", 5, 5, x, 0).await;
    write_at(store, &parent, "f", 5, 5, y, 0).await;
    let child = fork(store, &parent, 2).await;
    assert_eq!(
        read_file(store, &child, "f").await,
        [holes(5), data(y, 0, 5)].concat()
    );
    assert_eq!(read_range(store, &child, "f", 6, 3).await.1, data(y, 1, 3));
}

pub async fn older_fork_hides_dead_piece_starting_inside_range<S: MetadataStore>(store: &S) {
    let (x, y, z) = (blob("x"), blob("y"), blob("z"));
    let parent = create(store).await;
    write_at(store, &parent, "f", 0, 10, x, 0).await;
    write_at(store, &parent, "f", 4, 3, y, 0).await;
    write_at(store, &parent, "f", 4, 3, z, 0).await;
    write_at(store, &parent, "f", 0, 10, y, 0).await;
    let child = fork(store, &parent, 3).await;
    assert_eq!(
        read_file(store, &child, "f").await,
        [data(x, 0, 4), data(z, 0, 3), data(x, 7, 3)].concat()
    );
    assert_eq!(
        read_range(store, &child, "f", 2, 6).await.1,
        [data(x, 2, 2), data(z, 0, 3), data(x, 7, 1)].concat()
    );

    let other = create(store).await;
    write_at(store, &other, "f", 0, 10, x, 0).await;
    write_at(store, &other, "f", 4, 3, y, 0).await;
    write_at(store, &other, "f", 2, 6, z, 0).await;
    write_at(store, &other, "f", 0, 10, y, 0).await;
    let other_child = fork(store, &other, 3).await;
    assert_eq!(
        read_file(store, &other_child, "f").await,
        [data(x, 0, 2), data(z, 0, 6), data(x, 8, 2)].concat()
    );
    assert_eq!(
        read_range(store, &other_child, "f", 3, 7).await.1,
        [data(z, 1, 5), data(x, 8, 2)].concat()
    );

    let third = create(store).await;
    write_at(store, &third, "f", 0, 10, x, 0).await;
    write_at(store, &third, "f", 4, 4, y, 0).await;
    write_at(store, &third, "f", 2, 4, z, 0).await;
    write_at(store, &third, "f", 0, 10, y, 0).await;
    let third_child = fork(store, &third, 3).await;
    assert_eq!(
        read_range(store, &third_child, "f", 5, 5).await.1,
        [data(z, 3, 1), data(y, 2, 2), data(x, 8, 2)].concat()
    );
    assert_eq!(
        read_file(store, &third_child, "f").await,
        [data(x, 0, 2), data(z, 0, 4), data(y, 2, 2), data(x, 8, 2)].concat()
    );
}

pub async fn fork_reads_bytes_through_long_parent_history<S: MetadataStore>(store: &S) {
    let (base, hot, cold) = (blob("base"), blob("hot"), blob("cold"));
    let parent = create(store).await;
    write_at(store, &parent, "f", 0, 4000, base, 0).await;
    for index in 0..40_u64 {
        write_at(store, &parent, "f", (index % 8) * 50, 50, hot, index).await;
        write_at(store, &parent, "f", 1000 + index * 7, 3, cold, index).await;
    }
    let at = head(store, &parent).await.0;
    let expected = read_file(store, &parent, "f").await;
    let child = fork(store, &parent, at).await;
    for index in 0..200_u64 {
        write_at(store, &parent, "f", (index % 16) * 25, 25, cold, index).await;
    }
    assert_eq!(read_file(store, &child, "f").await, expected);
    for (offset, size) in [
        (0, 1),
        (10, 100),
        (399, 2),
        (1000, 300),
        (1100, 2),
        (3999, 5),
    ] {
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let end = usize::try_from(offset + size)
            .unwrap_or(usize::MAX)
            .min(expected.len());
        assert_eq!(
            read_range(store, &child, "f", offset, size).await.1,
            expected[start..end].to_vec(),
            "child range [{offset}, {})",
            offset + size
        );
    }
}

pub async fn write_overflow_is_rejected_as_too_large<S: MetadataStore>(store: &S) {
    let x = blob("x");
    let volume = create(store).await;
    write_at(store, &volume, "f", 0, 4, x, 0).await;
    set(store, &volume, "a", "1").await;
    let before_entries = collect_entries(store, &volume, 0).await;
    let before_file = read_file(store, &volume, "f").await;
    for (at, size, content_offset) in [
        (WriteAt::Offset(u64::MAX), 1, 0),
        (WriteAt::Offset(u64::MAX - 1), 2, 0),
        (WriteAt::Offset(1), u64::MAX, 0),
        (WriteAt::Offset(0), 2, u64::MAX - 1),
        (WriteAt::Offset(0), 1, u64::MAX),
        (WriteAt::Offset(u64::MAX), u64::MAX, u64::MAX),
        (WriteAt::End, u64::MAX - 3, 0),
        (WriteAt::End, 1, u64::MAX),
    ] {
        let rejection = transact_rejection(
            store,
            &volume,
            (
                SetKey {
                    key: key("a"),
                    value: value("changed"),
                },
                Write {
                    file: file("f"),
                    at,
                    size,
                    content: x,
                    content_offset,
                },
                GetHead,
            ),
        )
        .await;
        assert!(
            matches!(rejection, TransactRejection::TooLarge),
            "a write at {at:?} of {size} bytes from content offset {content_offset} gave {rejection:?}"
        );
    }
    assert_eq!(head(store, &volume).await, Seq(2));
    assert_eq!(collect_entries(store, &volume, 0).await, before_entries);
    assert_eq!(read_file(store, &volume, "f").await, before_file);
    assert_eq!(listed(store, &volume, "").await, pairs(&[("a", "1")]));

    let longest = create(store).await;
    truncate(store, &longest, "f", u64::MAX).await;
    let rejection = transact_rejection(
        store,
        &longest,
        Write {
            file: file("f"),
            at: WriteAt::End,
            size: 1,
            content: x,
            content_offset: 0,
        },
    )
    .await;
    assert!(
        matches!(rejection, TransactRejection::TooLarge),
        "appending to a file of length u64::MAX gave {rejection:?}"
    );
    assert_eq!(
        write(store, &longest, "f", WriteAt::End, 0, x, u64::MAX).await,
        Written { offset: u64::MAX }
    );
    assert_eq!(
        write(
            store,
            &volume,
            "f",
            WriteAt::Offset(u64::MAX),
            0,
            x,
            u64::MAX
        )
        .await,
        Written { offset: u64::MAX }
    );
    assert_eq!(head(store, &longest).await, Seq(1));
    assert_eq!(head(store, &volume).await, Seq(2));
}
