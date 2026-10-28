#[cfg(disk)]
mod disk;

use std::io;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use futures_util::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use stratafs_store::content::{
    ContentHash, ContentHasher, ContentStore, GetRejection, UnknownContent,
};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ObjectStoreError {
    #[error(transparent)]
    Store(#[from] object_store::Error),
    #[error("the content directory could not be made")]
    Directory(#[source] io::Error),
    #[error("the content could not be flushed to the disk")]
    Flush(#[source] io::Error),
}

// ObjectStoreContentStore keeps each blob in an object_store store, under its content hash.
#[derive(Debug, Clone)]
pub struct ObjectStoreContentStore {
    store: Arc<dyn ObjectStore>,
    #[cfg(disk)]
    unflushed: Option<Arc<disk::Unflushed>>,
}

impl ObjectStoreContentStore {
    // new keeps content in store, which makes each blob as durable as it is going to be when it is put.
    #[must_use]
    pub fn new(store: impl ObjectStore) -> Self {
        Self {
            store: Arc::new(store),
            #[cfg(disk)]
            unflushed: None,
        }
    }

    async fn store(&self, mut content: impl Buf + Send) -> Result<ContentHash, ObjectStoreError> {
        let len = content.remaining() as u64;
        let mut hasher = ContentHasher::default();
        let mut chunks = Vec::new();
        while content.has_remaining() {
            let chunk = content.copy_to_bytes(content.chunk().len());
            hasher.update(&chunk);
            chunks.push(chunk);
        }
        let hash = hasher.finish();
        let path = object_path(&hash);
        if !self.stored_object_matches(&path, &hash, len).await? {
            self.store.put(&path, PutPayload::from_iter(chunks)).await?;
        }
        // A blob that was already there is recorded too, since nothing says that copy reached the disk.
        #[cfg(disk)]
        if let Some(unflushed) = &self.unflushed {
            unflushed.record(&path);
        }
        Ok(hash)
    }

    async fn stored_object_matches(
        &self,
        path: &Path,
        hash: &ContentHash,
        len: u64,
    ) -> Result<bool, object_store::Error> {
        let metadata = match self.store.head(path).await {
            Ok(metadata) => metadata,
            Err(error) => return missing_or_failure(error),
        };
        match (metadata.size == len, len) {
            (false, _) => Ok(false),
            (true, 0) => Ok(true),
            (true, _) => match self.stored_hash(path).await {
                Ok(stored) => Ok(stored == *hash),
                Err(error) => missing_or_failure(error),
            },
        }
    }

    async fn stored_hash(&self, path: &Path) -> Result<ContentHash, object_store::Error> {
        let mut chunks = self.store.get(path).await?.into_stream();
        let mut hasher = ContentHasher::default();
        while let Some(chunk) = chunks.try_next().await? {
            hasher.update(&chunk);
        }
        Ok(hasher.finish())
    }

    async fn load(
        &self,
        hash: ContentHash,
        offset: u64,
        size: u64,
    ) -> Result<Bytes, StoreError<GetRejection, ObjectStoreError>> {
        let path = object_path(&hash);
        let len = self
            .store
            .head(&path)
            .await
            .map_err(rejection_or_failure)?
            .size;
        if len == 0 && hash != ContentHash::of(&[]) {
            return Err(StoreError::Rejected(GetRejection::UnknownContent(
                UnknownContent,
            )));
        }
        let end = match offset.checked_add(size) {
            Some(end) if end <= len => end,
            Some(_) | None => return Err(StoreError::Rejected(GetRejection::OutOfBounds { len })),
        };
        if size == 0 {
            return Ok(Bytes::new());
        }
        self.store
            .get_range(&path, offset..end)
            .await
            .map_err(rejection_or_failure)
    }
}

impl ContentStore for ObjectStoreContentStore {
    type Failure = ObjectStoreError;
    type Content = Bytes;

    fn put(
        &self,
        content: impl Buf + Send,
    ) -> impl Future<Output = Result<ContentHash, Self::Failure>> + Send {
        self.store(content)
    }

    fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<Bytes, StoreError<GetRejection, Self::Failure>>> + Send {
        self.load(*hash, offset, size)
    }

    async fn ensure_durable(&self, against: DurableAgainst) -> Result<(), Self::Failure> {
        #[cfg(disk)]
        if let Some(unflushed) = &self.unflushed {
            // The flush waits on the disk, so it runs on a thread of its own while this yields.
            let unflushed = Arc::clone(unflushed);
            blocking::unblock(move || unflushed.flush(against))
                .await
                .map_err(ObjectStoreError::Flush)?;
        }
        #[cfg(not(disk))]
        let _ = against;
        Ok(())
    }
}

fn object_path(hash: &ContentHash) -> Path {
    Path::from(format!(
        "blobs/{:02x}/{:032x}",
        hash.0[0],
        u128::from_be_bytes(hash.0)
    ))
}

fn missing_or_failure(error: object_store::Error) -> Result<bool, object_store::Error> {
    match error {
        object_store::Error::NotFound { .. } => Ok(false),
        error => Err(error),
    }
}

fn rejection_or_failure(error: object_store::Error) -> StoreError<GetRejection, ObjectStoreError> {
    match error {
        object_store::Error::NotFound { .. } => {
            StoreError::Rejected(GetRejection::UnknownContent(UnknownContent))
        }
        error => StoreError::Failed(ObjectStoreError::from(error)),
    }
}
