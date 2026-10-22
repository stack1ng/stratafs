#[cfg(all(feature = "fs", not(target_arch = "wasm32")))]
use std::path::Path;
use std::path::PathBuf;

use bytes::Buf;
use futures_util::TryStreamExt;
use opendal::{Buffer, ErrorKind, Operator};
use stratafs_store::content::{ContentHash, ContentStore, GetRejection, UnknownContent};
use stratafs_store::error::StoreError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum OpendalError {
    #[error(transparent)]
    Operator(#[from] opendal::Error),
    #[error("path {path:?} is not valid UTF-8")]
    NonUtf8Path { path: PathBuf },
}

const VERIFY_CHUNK_BYTES: usize = 4 << 20;

#[derive(Debug, Clone)]
pub struct OpendalContentStore {
    operator: Operator,
}

impl OpendalContentStore {
    #[must_use]
    pub fn new(operator: Operator) -> Self {
        Self { operator }
    }

    #[cfg(all(feature = "fs", not(target_arch = "wasm32")))]
    pub fn filesystem(
        root: impl AsRef<Path>,
        staging: impl AsRef<Path>,
    ) -> Result<Self, OpendalError> {
        let builder = opendal::services::Fs::default()
            .root(utf8_path(root.as_ref())?)
            .atomic_write_dir(utf8_path(staging.as_ref())?);
        Ok(Self::new(Operator::new(builder)?))
    }

    async fn store(&self, mut content: impl Buf + Send) -> Result<ContentHash, OpendalError> {
        let len = content.remaining() as u64;
        let mut hasher = blake3::Hasher::new();
        let mut chunks = Vec::new();
        while content.has_remaining() {
            let chunk = content.copy_to_bytes(content.chunk().len());
            hasher.update(&chunk);
            chunks.push(chunk);
        }
        let hash = ContentHash(*hasher.finalize().as_bytes());
        let path = object_path(&hash);
        if !self.stored_object_matches(&path, &hash, len).await? {
            self.operator.write(&path, Buffer::from(chunks)).await?;
        }
        Ok(hash)
    }

    async fn stored_object_matches(
        &self,
        path: &str,
        hash: &ContentHash,
        len: u64,
    ) -> Result<bool, opendal::Error> {
        let metadata = match self.operator.stat(path).await {
            Ok(metadata) => metadata,
            Err(error) => return missing_or_failure(error),
        };
        match (metadata.content_length() == len, len) {
            (false, _) => Ok(false),
            (true, 0) => Ok(true),
            (true, _) => match self.stored_hash(path, len).await {
                Ok(stored) => Ok(stored == *hash),
                Err(error) => missing_or_failure(error),
            },
        }
    }

    async fn stored_hash(&self, path: &str, len: u64) -> Result<ContentHash, opendal::Error> {
        let mut chunks = self
            .operator
            .reader_with(path)
            .chunk(VERIFY_CHUNK_BYTES)
            .await?
            .into_stream(0..len)
            .await?;
        let mut hasher = blake3::Hasher::new();
        while let Some(buffer) = chunks.try_next().await? {
            for chunk in buffer {
                hasher.update(&chunk);
            }
        }
        Ok(ContentHash(*hasher.finalize().as_bytes()))
    }

    async fn load(
        &self,
        hash: ContentHash,
        offset: u64,
        size: u64,
    ) -> Result<Buffer, StoreError<GetRejection, OpendalError>> {
        let path = object_path(&hash);
        let len = self
            .operator
            .stat(&path)
            .await
            .map_err(rejection_or_failure)?
            .content_length();
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
            return Ok(Buffer::new());
        }
        let buffer = self
            .operator
            .read_with(&path)
            .range(offset..end)
            .await
            .map_err(rejection_or_failure)?;
        Ok(buffer)
    }
}

impl ContentStore for OpendalContentStore {
    type Failure = OpendalError;

    fn put(
        &self,
        content: impl Buf + Send,
    ) -> impl Future<Output = Result<ContentHash, Self::Failure>> + Send {
        sendable(self.store(content))
    }

    fn get(
        &self,
        hash: &ContentHash,
        offset: u64,
        size: u64,
    ) -> impl Future<Output = Result<impl Buf + Send, StoreError<GetRejection, Self::Failure>>> + Send
    {
        sendable(self.load(*hash, offset, size))
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn sendable<F: Future>(future: F) -> F {
    future
}

#[cfg(target_arch = "wasm32")]
fn sendable<F: Future>(future: F) -> send_wrapper::SendWrapper<F> {
    send_wrapper::SendWrapper::new(future)
}

fn object_path(hash: &ContentHash) -> String {
    format!(
        "blobs/{:02x}/{}",
        hash.0[0],
        blake3::Hash::from_bytes(hash.0)
    )
}

fn missing_or_failure(error: opendal::Error) -> Result<bool, opendal::Error> {
    match error.kind() {
        ErrorKind::NotFound => Ok(false),
        _ => Err(error),
    }
}

#[cfg(all(feature = "fs", not(target_arch = "wasm32")))]
fn utf8_path(path: &Path) -> Result<&str, OpendalError> {
    path.to_str().ok_or_else(|| OpendalError::NonUtf8Path {
        path: PathBuf::from(path),
    })
}

fn rejection_or_failure(error: opendal::Error) -> StoreError<GetRejection, OpendalError> {
    match error.kind() {
        ErrorKind::NotFound => StoreError::Rejected(GetRejection::UnknownContent(UnknownContent)),
        _ => StoreError::Failed(OpendalError::from(error)),
    }
}
