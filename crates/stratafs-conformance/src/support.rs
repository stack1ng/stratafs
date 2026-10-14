use std::fmt::Debug;

use bytes::Bytes;
use futures_util::StreamExt;
use stratafs_store::content::ContentHash;
use stratafs_store::error::StoreError;
use stratafs_store::metadata::{
    Entry, FileContent, FileId, FileInfo, Key, KeyValue, MetadataStore, Seq, Stored,
    TransactRejection, VolumeId, Written,
};
use stratafs_store::transaction::{
    DeleteKey, GetFileInfo, GetHead, GetKey, Operations, SetKey, Truncate, VisibleBlobs, Write,
    WriteAt,
};
use vec1::Vec1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobByte {
    pub content: ContentHash,
    pub content_offset: u64,
}

pub type Provenance = Vec<Option<BlobByte>>;

#[must_use]
pub fn key(name: &str) -> Key {
    Key(Bytes::copy_from_slice(name.as_bytes()))
}

#[must_use]
pub fn key_bytes(bytes: &[u8]) -> Key {
    Key(Bytes::copy_from_slice(bytes))
}

#[must_use]
pub fn value(text: &str) -> Bytes {
    Bytes::copy_from_slice(text.as_bytes())
}

#[must_use]
pub fn file(name: &str) -> FileId {
    FileId(name.to_owned())
}

#[must_use]
pub fn blob(name: &str) -> ContentHash {
    ContentHash::of(name.as_bytes())
}

#[must_use]
pub fn non_empty<T>(items: Vec<T>) -> Vec1<T> {
    match Vec1::try_from_vec(items) {
        Ok(items) => items,
        Err(error) => panic!("a case built an empty operation list: {error}"),
    }
}

#[must_use]
pub fn unknown_volumes() -> Vec<VolumeId> {
    vec![
        VolumeId("stratafs-conformance-unknown-volume".to_owned()),
        VolumeId(String::new()),
        VolumeId("18446744073709551615".to_owned()),
    ]
}

#[must_use]
pub fn data(content: ContentHash, content_offset: u64, size: u64) -> Provenance {
    (0..size)
        .map(|step| {
            Some(BlobByte {
                content,
                content_offset: content_offset + step,
            })
        })
        .collect()
}

#[must_use]
pub fn holes(size: u64) -> Provenance {
    (0..size).map(|_| None).collect()
}

#[must_use]
pub fn provenance(content: &FileContent, offset: u64, size: u64) -> Provenance {
    let range_end = offset.saturating_add(size).min(content.info.len);
    let window = range_end.saturating_sub(offset);
    let mut bytes = holes(window);
    let mut covered_until = offset;
    for blob in &content.blobs {
        assert!(blob.size > 0, "visible blob {blob:?} is empty");
        assert!(
            blob.offset >= covered_until,
            "visible blob {blob:?} is out of order or overlaps the previous one (covered until {covered_until}) in {content:?}"
        );
        let blob_end = blob.offset.checked_add(blob.size);
        assert!(
            blob_end.is_some_and(|end| end <= range_end),
            "visible blob {blob:?} lies outside the requested range [{offset}, {range_end}) in {content:?}"
        );
        assert!(
            blob.content_offset.checked_add(blob.size).is_some(),
            "visible blob {blob:?} has a content range that overflows"
        );
        for step in 0..blob.size {
            bytes[usize::try_from(blob.offset - offset + step).unwrap_or(usize::MAX)] =
                Some(BlobByte {
                    content: blob.content,
                    content_offset: blob.content_offset + step,
                });
        }
        covered_until = blob.offset + blob.size;
    }
    bytes
}

pub async fn transact<S: MetadataStore, T>(store: &S, volume: &VolumeId, operations: T) -> T::Output
where
    T: Operations + Send + Debug,
    T::Layout: Send,
{
    let description = format!("{operations:?}");
    match store.transact(volume, operations).await {
        Ok(output) => output,
        Err(error) => panic!("transaction {description} on volume {volume:?} failed: {error:?}"),
    }
}

pub async fn transact_rejection<S: MetadataStore, T>(
    store: &S,
    volume: &VolumeId,
    operations: T,
) -> TransactRejection
where
    T: Operations + Send + Debug,
    T::Layout: Send,
    T::Output: Debug,
{
    let description = format!("{operations:?}");
    match store.transact(volume, operations).await {
        Ok(output) => panic!(
            "transaction {description} on volume {volume:?} succeeded with {output:?} but should have been rejected"
        ),
        Err(StoreError::Rejected(rejection)) => rejection,
        Err(StoreError::Failed(error)) => {
            panic!("transaction {description} on volume {volume:?} failed: {error:?}")
        }
    }
}

pub async fn create<S: MetadataStore>(store: &S) -> VolumeId {
    match store.create().await {
        Ok(volume) => volume,
        Err(error) => panic!("create failed: {error:?}"),
    }
}

pub async fn fork<S: MetadataStore>(store: &S, parent: &VolumeId, at: u64) -> VolumeId {
    match store.fork(parent, Seq(at)).await {
        Ok(volume) => volume,
        Err(error) => panic!("fork of {parent:?} at {at} failed: {error:?}"),
    }
}

pub async fn head<S: MetadataStore>(store: &S, volume: &VolumeId) -> Seq {
    transact(store, volume, GetHead).await
}

pub async fn set<S: MetadataStore>(store: &S, volume: &VolumeId, name: &str, text: &str) -> Seq {
    transact(
        store,
        volume,
        (
            SetKey {
                key: key(name),
                value: value(text),
            },
            GetHead,
        ),
    )
    .await
    .1
}

pub async fn delete<S: MetadataStore>(store: &S, volume: &VolumeId, name: &str) -> Seq {
    transact(store, volume, (DeleteKey { key: key(name) }, GetHead))
        .await
        .1
}

pub async fn get<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    name: &str,
) -> Option<Stored<KeyValue>> {
    transact(store, volume, GetKey { key: key(name) }).await
}

pub async fn get_value<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    name: &str,
) -> Option<Bytes> {
    get(store, volume, name)
        .await
        .map(|stored| stored.entry.value)
}

pub async fn write<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    name: &str,
    at: WriteAt,
    size: u64,
    content: ContentHash,
    content_offset: u64,
) -> Written {
    transact(
        store,
        volume,
        Write {
            file: file(name),
            at,
            size,
            content,
            content_offset,
        },
    )
    .await
}

pub async fn write_at<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    name: &str,
    offset: u64,
    size: u64,
    content: ContentHash,
    content_offset: u64,
) -> Seq {
    transact(
        store,
        volume,
        (
            Write {
                file: file(name),
                at: WriteAt::Offset(offset),
                size,
                content,
                content_offset,
            },
            GetHead,
        ),
    )
    .await
    .1
}

pub async fn truncate<S: MetadataStore>(store: &S, volume: &VolumeId, name: &str, len: u64) -> Seq {
    transact(
        store,
        volume,
        (
            Truncate {
                file: file(name),
                len,
            },
            GetHead,
        ),
    )
    .await
    .1
}

pub async fn file_info<S: MetadataStore>(store: &S, volume: &VolumeId, name: &str) -> FileInfo {
    transact(store, volume, GetFileInfo { file: file(name) }).await
}

pub async fn read_range<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    name: &str,
    offset: u64,
    size: u64,
) -> (FileInfo, Provenance) {
    let content = transact(
        store,
        volume,
        VisibleBlobs {
            file: file(name),
            offset,
            size,
        },
    )
    .await;
    let bytes = provenance(&content, offset, size);
    (content.info, bytes)
}

pub async fn read_file<S: MetadataStore>(store: &S, volume: &VolumeId, name: &str) -> Provenance {
    let (info, bytes) = read_range(store, volume, name, 0, u64::MAX).await;
    assert_eq!(
        info,
        file_info(store, volume, name).await,
        "VisibleBlobs and GetFileInfo disagree for file {name}"
    );
    bytes
}

pub async fn collect_entries<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    after: u64,
) -> Vec<Stored<Entry>> {
    let items: Vec<_> = store.entries(volume.clone(), Seq(after)).collect().await;
    items
        .into_iter()
        .map(|item| match item {
            Ok(stored) => stored,
            Err(error) => panic!("entries({volume:?}, {after}) failed: {error:?}"),
        })
        .collect()
}

pub async fn collect_keys<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    prefix: &Key,
) -> Vec<Stored<KeyValue>> {
    let items: Vec<_> = store
        .list_keys(volume.clone(), prefix.clone())
        .collect()
        .await;
    items
        .into_iter()
        .map(|item| match item {
            Ok(stored) => stored,
            Err(error) => panic!("list_keys({volume:?}, {prefix:?}) failed: {error:?}"),
        })
        .collect()
}

pub async fn listed<S: MetadataStore>(
    store: &S,
    volume: &VolumeId,
    prefix: &str,
) -> Vec<(String, String)> {
    collect_keys(store, volume, &key(prefix))
        .await
        .into_iter()
        .map(|stored| {
            (
                String::from_utf8_lossy(&stored.entry.key.0).into_owned(),
                String::from_utf8_lossy(&stored.entry.value).into_owned(),
            )
        })
        .collect()
}

#[must_use]
pub fn pairs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(name, text)| ((*name).to_owned(), (*text).to_owned()))
        .collect()
}

#[must_use]
pub fn set_entry(name: &str, text: &str) -> Entry {
    Entry::SetKey(KeyValue {
        key: key(name),
        value: value(text),
    })
}

#[must_use]
pub fn delete_entry(name: &str) -> Entry {
    Entry::DeleteKey { key: key(name) }
}

#[must_use]
pub fn write_entry(
    name: &str,
    offset: u64,
    size: u64,
    content: ContentHash,
    content_offset: u64,
) -> Entry {
    Entry::Write {
        file: file(name),
        offset,
        size,
        content,
        content_offset,
    }
}

#[must_use]
pub fn truncate_entry(name: &str, len: u64) -> Entry {
    Entry::Truncate {
        file: file(name),
        len,
    }
}

#[must_use]
pub fn numbered(first: u64, entries: Vec<Entry>) -> Vec<Stored<Entry>> {
    entries
        .into_iter()
        .zip(first..)
        .map(|(entry, seq)| Stored {
            seq: Seq(seq),
            entry,
        })
        .collect()
}

pub async fn stored_seq<S: MetadataStore>(store: &S, volume: &VolumeId, name: &str) -> Option<Seq> {
    get(store, volume, name).await.map(|stored| stored.seq)
}
