#[cfg(disk)]
mod disk;

use std::io;
use std::sync::Arc;

use bytes::{Buf, Bytes};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use stratafs_store::content::{ContentId, ContentStore, GetRejection, UnknownContent};
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

// ObjectStoreContentStore keeps each blob in an object_store store, under its content id.
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

    async fn store(
        &self,
        id: ContentId,
        mut content: impl Buf + Send,
    ) -> Result<(), ObjectStoreError> {
        let mut chunks = Vec::new();
        while content.has_remaining() {
            chunks.push(content.copy_to_bytes(content.chunk().len()));
        }
        let path = object_path(&id);
        self.store.put(&path, PutPayload::from_iter(chunks)).await?;
        #[cfg(disk)]
        if let Some(unflushed) = &self.unflushed {
            unflushed.record(&path);
        }
        Ok(())
    }

    async fn remove(&self, id: ContentId) -> Result<(), ObjectStoreError> {
        let path = object_path(&id);
        // A file no flush has handed to the disk yet is gone, so no flush looks for it.
        #[cfg(disk)]
        let _flushing = match &self.unflushed {
            Some(unflushed) => {
                let flushing = unflushed.flushing.lock_arc().await;
                unflushed.forget(&path);
                Some(flushing)
            }
            None => None,
        };
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(ObjectStoreError::from(error)),
        }
    }

    async fn load(
        &self,
        id: ContentId,
        offset: u64,
        size: u64,
    ) -> Result<Bytes, StoreError<GetRejection, ObjectStoreError>> {
        let path = object_path(&id);
        let len = self
            .store
            .head(&path)
            .await
            .map_err(rejection_or_failure)?
            .size;
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
        id: &ContentId,
        content: impl Buf + Send,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.store(*id, content)
    }

    fn get(
        &self,
        id: &ContentId,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<Bytes, StoreError<GetRejection, Self::Failure>>> + Send {
        self.load(*id, offset, size)
    }

    fn delete(&self, id: &ContentId) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.remove(*id)
    }

    async fn ensure_durable(&self, against: DurableAgainst) -> Result<(), Self::Failure> {
        #[cfg(disk)]
        if let Some(unflushed) = &self.unflushed {
            // The flush waits on the disk, so it runs on a thread of its own while this yields.
            let flushing = unflushed.flushing.lock_arc().await;
            let unflushed = Arc::clone(unflushed);
            blocking::unblock(move || {
                let _flushing = flushing;
                unflushed.flush(against)
            })
            .await
            .map_err(ObjectStoreError::Flush)?;
        }
        #[cfg(not(disk))]
        let _ = against;
        Ok(())
    }
}

fn object_path(id: &ContentId) -> Path {
    Path::from(format!("blobs/{:02x}/{:032x}", id.0 >> 120, id.0))
}

fn rejection_or_failure(error: object_store::Error) -> StoreError<GetRejection, ObjectStoreError> {
    match error {
        object_store::Error::NotFound { .. } => {
            StoreError::Rejected(GetRejection::UnknownContent(UnknownContent))
        }
        error => StoreError::Failed(ObjectStoreError::from(error)),
    }
}
