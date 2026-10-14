use std::pin::pin;

use futures_util::StreamExt;
use stratafs_store::error::StoreError;
use stratafs_store::metadata::{
    ForkRejection, MetadataStore, Seq, TransactRejection, UnknownVolume,
};
use stratafs_store::transaction::GetHead;

use crate::support::{
    collect_entries, collect_keys, create, file_info, head, key, read_file, set,
    transact_rejection, unknown_volumes,
};

pub async fn create_returns_empty_volume<S: MetadataStore>(store: &S) {
    let volume = create(store).await;
    assert_eq!(head(store, &volume).await, Seq(0));
    assert!(collect_entries(store, &volume, 0).await.is_empty());
    assert!(collect_keys(store, &volume, &key("")).await.is_empty());
    assert_eq!(file_info(store, &volume, "file").await.len, 0);
    assert_eq!(file_info(store, &volume, "file").await.seq, None);
    assert!(read_file(store, &volume, "file").await.is_empty());
}

pub async fn create_returns_distinct_ids<S: MetadataStore>(store: &S) {
    let first = create(store).await;
    let second = create(store).await;
    let third = create(store).await;
    assert_ne!(first, second);
    assert_ne!(second, third);
    assert_ne!(first, third);
    set(store, &first, "only-in-first", "1").await;
    assert_eq!(head(store, &first).await, Seq(1));
    assert_eq!(head(store, &second).await, Seq(0));
    assert_eq!(head(store, &third).await, Seq(0));
}

pub async fn unknown_volume_is_rejected_by_execute<S: MetadataStore>(store: &S) {
    create(store).await;
    for unknown in unknown_volumes() {
        let rejection = transact_rejection(store, &unknown, GetHead).await;
        assert!(
            matches!(rejection, TransactRejection::UnknownVolume(UnknownVolume)),
            "execute on {unknown:?} gave {rejection:?}"
        );
    }
}

pub async fn unknown_volume_is_rejected_by_fork<S: MetadataStore>(store: &S) {
    create(store).await;
    for unknown in unknown_volumes() {
        let result = store.fork(&unknown, Seq(0)).await;
        assert!(
            matches!(
                result,
                Err(StoreError::Rejected(ForkRejection::UnknownVolume(
                    UnknownVolume
                )))
            ),
            "fork of {unknown:?} gave {result:?}"
        );
    }
}

pub async fn unknown_volume_is_rejected_by_entries<S: MetadataStore>(store: &S) {
    create(store).await;
    for unknown in unknown_volumes() {
        let items: Vec<_> = store.entries(unknown.clone(), Seq(0)).collect().await;
        assert!(
            matches!(items.as_slice(), [Err(StoreError::Rejected(UnknownVolume))]),
            "entries of {unknown:?} gave {items:?}"
        );
    }
}

pub async fn unknown_volume_is_rejected_by_list_keys<S: MetadataStore>(store: &S) {
    create(store).await;
    for unknown in unknown_volumes() {
        let items: Vec<_> = store.list_keys(unknown.clone(), key("")).collect().await;
        assert!(
            matches!(items.as_slice(), [Err(StoreError::Rejected(UnknownVolume))]),
            "list_keys of {unknown:?} gave {items:?}"
        );
    }
}

pub async fn unknown_volume_is_rejected_by_watch<S: MetadataStore>(store: &S) {
    create(store).await;
    for unknown in unknown_volumes() {
        let mut watch = pin!(store.watch(unknown.clone(), Seq(0)));
        let first = watch.next().await;
        assert!(
            matches!(first, Some(Err(StoreError::Rejected(UnknownVolume)))),
            "watch of {unknown:?} gave {first:?}"
        );
        let second = watch.next().await;
        assert!(
            second.is_none(),
            "watch of {unknown:?} kept going after the rejection with {second:?}"
        );
    }
}
