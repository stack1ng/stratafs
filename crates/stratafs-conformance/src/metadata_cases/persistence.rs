use std::pin::pin;

use futures_util::StreamExt;
use stratafs_store::metadata::{Entry, FileInfo, KeyValue, MetadataStore, Seq, Stored, VolumeId};

use crate::support::{
    Provenance, blob, collect_entries, collect_keys, create, delete, file_info, fork, head, key,
    read_file, set, truncate, write_at,
};

const FILES: [&str; 2] = ["f", "g"];
const PREFIXES: [&str; 4] = ["", "k/", "k/1", "z"];

#[derive(Debug, PartialEq)]
struct VolumeSnapshot {
    head: Seq,
    entries: Vec<Stored<Entry>>,
    listings: Vec<Vec<Stored<KeyValue>>>,
    files: Vec<(FileInfo, Provenance)>,
}

async fn snapshot<S: MetadataStore>(store: &S, volumes: &[VolumeId]) -> Vec<VolumeSnapshot> {
    let mut snapshots = Vec::new();
    for volume in volumes {
        let mut listings = Vec::new();
        for prefix in PREFIXES {
            listings.push(collect_keys(store, volume, &key(prefix)).await);
        }
        let mut files = Vec::new();
        for name in FILES {
            files.push((
                file_info(store, volume, name).await,
                read_file(store, volume, name).await,
            ));
        }
        snapshots.push(VolumeSnapshot {
            head: head(store, volume).await,
            entries: collect_entries(store, volume, 0).await,
            listings,
            files,
        });
    }
    snapshots
}

pub async fn state_survives_reopen<L, S: MetadataStore>(location: &L, open: impl Fn(&L) -> S) {
    let (x, y) = (blob("x"), blob("y"));
    let (volumes, before) = {
        let store = open(location);
        let root = create(&store).await;
        set(&store, &root, "k/1", "root").await;
        set(&store, &root, "k/2", "root").await;
        write_at(&store, &root, "f", 0, 20, x, 0).await;
        let child = fork(&store, &root, 3).await;
        set(&store, &root, "k/3", "root-later").await;
        truncate(&store, &root, "f", 5).await;
        delete(&store, &child, "k/1").await;
        write_at(&store, &child, "f", 10, 15, y, 2).await;
        truncate(&store, &child, "g", 7).await;
        let grandchild = fork(&store, &child, 4).await;
        set(&store, &grandchild, "z", "grandchild").await;
        write_at(&store, &grandchild, "g", 3, 2, x, 9).await;
        let empty = create(&store).await;
        let volumes = vec![root, child, grandchild, empty];
        let before = snapshot(&store, &volumes).await;
        (volumes, before)
    };
    let store = open(location);
    assert_eq!(snapshot(&store, &volumes).await, before);

    let fresh = create(&store).await;
    assert!(
        !volumes.contains(&fresh),
        "create after reopening reused {fresh:?}"
    );
    let forked = fork(&store, &volumes[1], 2).await;
    assert!(
        !volumes.contains(&forked) && forked != fresh,
        "fork after reopening reused {forked:?}"
    );
    let mut watch = pin!(store.watch(volumes[1].clone(), Seq(0)));
    let first = watch.next().await;
    assert!(
        matches!(first, Some(Ok(seq)) if seq == before[1].head),
        "watch after reopening gave {first:?}"
    );
    let appended = set(&store, &volumes[1], "k/4", "after").await;
    assert_eq!(appended, Seq(before[1].head.0 + 1));
    assert_eq!(
        snapshot(&store, &volumes[..1]).await,
        before[..1],
        "appending to a fork after reopening changed its parent"
    );
}
