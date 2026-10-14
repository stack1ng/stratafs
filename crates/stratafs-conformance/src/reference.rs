use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use event_listener::Event;
use futures_core::Stream;
use futures_util::stream::{self, StreamExt};
use stratafs_store::content::{ContentHash, ContentStore, GetRejection, UnknownContent};
use stratafs_store::error::StoreError;
use stratafs_store::metadata::{
    Entry, FileContent, FileId, FileInfo, ForkRejection, Key, KeyValue, MetadataStore, Seq, Stored,
    TransactRejection, UnknownVolume, VisibleBlob, VolumeId, Written,
};
use stratafs_store::transaction::{Condition, Op, Outcome, WriteAt};
use vec1::Vec1;

use crate::support::BlobByte;

pub const REFERENCE_MAX_FILE_BYTES: u64 = 1 << 22;

#[derive(Debug, Clone, Default)]
struct FileState {
    len: u64,
    bytes: Vec<Option<BlobByte>>,
    seq: Option<Seq>,
}

impl FileState {
    fn info(&self) -> FileInfo {
        FileInfo {
            len: self.len,
            seq: self.seq,
        }
    }

    fn visible_blobs(&self, offset: u64, size: u64) -> Vec<VisibleBlob> {
        let end = offset.saturating_add(size).min(self.len);
        let stored_end = end.min(self.bytes.len() as u64);
        let mut blobs: Vec<VisibleBlob> = Vec::new();
        let mut position = offset;
        while position < stored_end {
            if let Some(byte) = self.bytes[index(position)] {
                let extends_last = blobs.last().is_some_and(|last| {
                    last.offset + last.size == position
                        && last.content == byte.content
                        && last.content_offset + last.size == byte.content_offset
                });
                match blobs.last_mut() {
                    Some(last) if extends_last => last.size += 1,
                    _ => blobs.push(VisibleBlob {
                        offset: position,
                        size: 1,
                        content: byte.content,
                        content_offset: byte.content_offset,
                    }),
                }
            }
            position += 1;
        }
        blobs
    }
}

fn index(position: u64) -> usize {
    usize::try_from(position).unwrap_or(usize::MAX)
}

#[derive(Debug, Clone, Default)]
struct State {
    head: u64,
    keys: BTreeMap<Key, Stored<KeyValue>>,
    files: HashMap<FileId, FileState>,
}

impl State {
    fn replay(history: &[Entry]) -> Self {
        let mut state = Self::default();
        for entry in history {
            state.apply(entry);
        }
        state
    }

    fn apply(&mut self, entry: &Entry) {
        self.head += 1;
        let seq = Seq(self.head);
        match entry {
            Entry::SetKey(key_value) => {
                self.keys.insert(
                    key_value.key.clone(),
                    Stored {
                        seq,
                        entry: key_value.clone(),
                    },
                );
            }
            Entry::DeleteKey { key } => {
                self.keys.remove(key);
            }
            Entry::Write {
                file,
                offset,
                size,
                content,
                content_offset,
            } => {
                let state = self.files.entry(file.clone()).or_default();
                let end = offset + size;
                if (state.bytes.len() as u64) < end {
                    state.bytes.resize(index(end), None);
                }
                for step in 0..*size {
                    state.bytes[index(offset + step)] = Some(BlobByte {
                        content: *content,
                        content_offset: content_offset + step,
                    });
                }
                state.len = state.len.max(end);
                state.seq = Some(seq);
            }
            Entry::Truncate { file, len } => {
                let state = self.files.entry(file.clone()).or_default();
                if (state.bytes.len() as u64) > *len {
                    state.bytes.truncate(index(*len));
                }
                state.len = *len;
                state.seq = Some(seq);
            }
        }
    }

    fn file_info(&self, file: &FileId) -> FileInfo {
        self.files
            .get(file)
            .map_or(FileInfo { len: 0, seq: None }, FileState::info)
    }

    fn file_len(&self, file: &FileId) -> u64 {
        self.file_info(file).len
    }

    fn visible_blobs(&self, file: &FileId, offset: u64, size: u64) -> FileContent {
        match self.files.get(file) {
            Some(state) => FileContent {
                info: state.info(),
                blobs: state.visible_blobs(offset, size),
            },
            None => FileContent {
                info: FileInfo { len: 0, seq: None },
                blobs: Vec::new(),
            },
        }
    }

    fn holds(&self, condition: &Condition) -> bool {
        match condition {
            Condition::Equals { key, value } => {
                self.keys.get(key).map(|stored| &stored.entry.value) == value.as_ref()
            }
            Condition::Empty { prefix } => !self
                .keys
                .range(prefix.clone()..)
                .next()
                .is_some_and(|(key, _)| key.0.starts_with(&prefix.0)),
            Condition::FileVersion { file, seq } => self.file_info(file).seq == *seq,
            Condition::Head(seq) => Seq(self.head) == *seq,
            Condition::Not(inner) => !self.holds(inner),
            Condition::All(conditions) => conditions.iter().all(|inner| self.holds(inner)),
            Condition::Any(conditions) => conditions.iter().any(|inner| self.holds(inner)),
        }
    }

    fn listing(&self, prefix: &Key) -> Vec<Stored<KeyValue>> {
        self.keys
            .range(prefix.clone()..)
            .take_while(|(key, _)| key.0.starts_with(&prefix.0))
            .map(|(_, stored)| stored.clone())
            .collect()
    }
}

#[derive(Debug, Default)]
struct Volumes {
    created: u64,
    histories: HashMap<VolumeId, Vec<Entry>>,
}

#[derive(Debug, Default)]
struct Shared {
    volumes: Mutex<Volumes>,
    head_changed: Event,
}

impl Shared {
    fn volumes(&self) -> MutexGuard<'_, Volumes> {
        self.volumes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn history(&self, volume: &VolumeId) -> Result<Vec<Entry>, UnknownVolume> {
        self.volumes()
            .histories
            .get(volume)
            .cloned()
            .ok_or(UnknownVolume)
    }

    fn head(&self, volume: &VolumeId) -> Result<Seq, UnknownVolume> {
        self.volumes()
            .histories
            .get(volume)
            .map(|history| Seq(history.len() as u64))
            .ok_or(UnknownVolume)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReferenceMetadataStore {
    shared: Arc<Shared>,
}

impl ReferenceMetadataStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn add_volume(volumes: &mut Volumes, history: Vec<Entry>) -> VolumeId {
        volumes.created += 1;
        let id = VolumeId(format!("reference-{}", volumes.created));
        volumes.histories.insert(id.clone(), history);
        id
    }

    fn execute_now(
        &self,
        volume: &VolumeId,
        operations: &Vec1<Op>,
    ) -> Result<Vec1<Outcome>, TransactRejection> {
        let mut volumes = self.shared.volumes();
        let history = volumes
            .histories
            .get_mut(volume)
            .ok_or(TransactRejection::UnknownVolume(UnknownVolume))?;
        let mut state = State::replay(history);
        let mut appended = Vec::new();
        let mut position = 0;
        let outcomes = operations.try_mapped_ref(|operation| {
            let index = position;
            position += 1;
            let (entry, outcome) = match operation {
                Op::SetKey(set) => (
                    Some(Entry::SetKey(KeyValue {
                        key: set.key.clone(),
                        value: set.value.clone(),
                    })),
                    Outcome::Done,
                ),
                Op::DeleteKey(delete) => (
                    state
                        .keys
                        .contains_key(&delete.key)
                        .then(|| Entry::DeleteKey {
                            key: delete.key.clone(),
                        }),
                    Outcome::Done,
                ),
                Op::Write(write) => {
                    let offset = match write.at {
                        WriteAt::Offset(offset) => offset,
                        WriteAt::End => state.file_len(&write.file),
                    };
                    let fits = offset
                        .checked_add(write.size)
                        .is_some_and(|end| end <= REFERENCE_MAX_FILE_BYTES)
                        && write.content_offset.checked_add(write.size).is_some();
                    let entry = match (write.size, fits) {
                        (0, _) => None,
                        (_, false) => return Err(TransactRejection::TooLarge),
                        (_, true) => Some(Entry::Write {
                            file: write.file.clone(),
                            offset,
                            size: write.size,
                            content: write.content,
                            content_offset: write.content_offset,
                        }),
                    };
                    (entry, Outcome::Written(Written { offset }))
                }
                Op::Truncate(truncate) => (
                    Some(Entry::Truncate {
                        file: truncate.file.clone(),
                        len: truncate.len,
                    }),
                    Outcome::Done,
                ),
                Op::GetKey(get) => (None, Outcome::Value(state.keys.get(&get.key).cloned())),
                Op::Assert(assert) => {
                    if !state.holds(&assert.0) {
                        return Err(TransactRejection::AssertionFailed { index });
                    }
                    (None, Outcome::Done)
                }
                Op::VisibleBlobs(request) => (
                    None,
                    Outcome::FileContent(state.visible_blobs(
                        &request.file,
                        request.offset,
                        request.size,
                    )),
                ),
                Op::GetFileInfo(request) => {
                    (None, Outcome::FileInfo(state.file_info(&request.file)))
                }
                Op::GetHead(_) => (None, Outcome::Head(Seq(state.head))),
            };
            if let Some(entry) = entry {
                state.apply(&entry);
                appended.push(entry);
            }
            Ok(outcome)
        })?;
        let changed = !appended.is_empty();
        history.extend(appended);
        drop(volumes);
        if changed {
            self.shared.head_changed.notify(usize::MAX);
        }
        Ok(outcomes)
    }
}

impl MetadataStore for ReferenceMetadataStore {
    type Error = Infallible;

    async fn create(&self) -> Result<VolumeId, Infallible> {
        Ok(Self::add_volume(&mut self.shared.volumes(), Vec::new()))
    }

    async fn fork(
        &self,
        parent_volume: &VolumeId,
        at: Seq,
    ) -> Result<VolumeId, StoreError<ForkRejection, Infallible>> {
        let mut volumes = self.shared.volumes();
        let parent = volumes
            .histories
            .get(parent_volume)
            .ok_or(StoreError::Rejected(ForkRejection::UnknownVolume(
                UnknownVolume,
            )))?;
        let head = Seq(parent.len() as u64);
        if at > head {
            return Err(StoreError::Rejected(ForkRejection::BeyondHead { at, head }));
        }
        let inherited = parent[..index(at.0)].to_vec();
        Ok(Self::add_volume(&mut volumes, inherited))
    }

    async fn execute(
        &self,
        volume: &VolumeId,
        operations: Vec1<Op>,
    ) -> Result<Vec1<Outcome>, StoreError<TransactRejection, Infallible>> {
        self.execute_now(volume, &operations)
            .map_err(StoreError::Rejected)
    }

    fn entries(
        &self,
        volume: VolumeId,
        after: Seq,
    ) -> impl Stream<Item = Result<Stored<Entry>, StoreError<UnknownVolume, Infallible>>> + Send + 'static
    {
        let shared = Arc::clone(&self.shared);
        stream::once(async move {
            let items: Vec<Result<Stored<Entry>, StoreError<UnknownVolume, Infallible>>> =
                match shared.history(&volume) {
                    Ok(history) => history
                        .into_iter()
                        .zip(1..)
                        .filter(|(_, seq)| *seq > after.0)
                        .map(|(entry, seq)| {
                            Ok(Stored {
                                seq: Seq(seq),
                                entry,
                            })
                        })
                        .collect(),
                    Err(unknown) => vec![Err(StoreError::Rejected(unknown))],
                };
            stream::iter(items)
        })
        .flatten()
    }

    fn list_keys(
        &self,
        volume: VolumeId,
        prefix: Key,
    ) -> impl Stream<Item = Result<Stored<KeyValue>, StoreError<UnknownVolume, Infallible>>>
    + Send
    + 'static {
        let shared = Arc::clone(&self.shared);
        stream::once(async move {
            let items: Vec<Result<Stored<KeyValue>, StoreError<UnknownVolume, Infallible>>> =
                match shared.history(&volume) {
                    Ok(history) => State::replay(&history)
                        .listing(&prefix)
                        .into_iter()
                        .map(Ok)
                        .collect(),
                    Err(unknown) => vec![Err(StoreError::Rejected(unknown))],
                };
            stream::iter(items)
        })
        .flatten()
    }

    fn watch(
        &self,
        volume: VolumeId,
        after: Seq,
    ) -> impl Stream<Item = Result<Seq, StoreError<UnknownVolume, Infallible>>> + Send + 'static
    {
        let shared = Arc::clone(&self.shared);
        stream::unfold(Some(after), move |last| {
            let shared = Arc::clone(&shared);
            let volume = volume.clone();
            async move {
                let last = last?;
                loop {
                    let listener = shared.head_changed.listen();
                    match shared.head(&volume) {
                        Err(unknown) => return Some((Err(StoreError::Rejected(unknown)), None)),
                        Ok(head) if head > last => return Some((Ok(head), Some(head))),
                        Ok(_) => listener.await,
                    }
                }
            }
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReferenceContentStore {
    blobs: Arc<Mutex<HashMap<ContentHash, Bytes>>>,
}

impl ReferenceContentStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn blobs(&self) -> MutexGuard<'_, HashMap<ContentHash, Bytes>> {
        self.blobs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn replace_stored_object(&self, hash: ContentHash, bytes: Bytes) {
        self.blobs().insert(hash, bytes);
    }

    #[must_use]
    pub fn stored_object_len(&self, hash: &ContentHash) -> Option<u64> {
        self.blobs().get(hash).map(|blob| blob.len() as u64)
    }
}

impl ContentStore for ReferenceContentStore {
    type Error = Infallible;

    async fn put(&self, bytes: Bytes) -> Result<ContentHash, Infallible> {
        let hash = ContentHash::of(&bytes);
        self.blobs().insert(hash, bytes);
        Ok(hash)
    }

    async fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> Result<Bytes, StoreError<GetRejection, Infallible>> {
        let blob = self
            .blobs()
            .get(hash)
            .filter(|blob| ContentHash::of(blob) == *hash)
            .cloned()
            .ok_or(StoreError::Rejected(GetRejection::UnknownContent(
                UnknownContent,
            )))?;
        let len = blob.len() as u64;
        match offset.checked_add(size) {
            Some(end) if end <= len => Ok(blob.slice(index(offset)..index(end))),
            Some(_) | None => Err(StoreError::Rejected(GetRejection::OutOfBounds { len })),
        }
    }
}
