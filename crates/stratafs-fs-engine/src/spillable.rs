// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
#![allow(clippy::manual_async_fn)]

use std::sync::PoisonError;

use bytes::buf::Chain;
use bytes::{Buf, Bytes};
use stratafs_store::content::{ContentId, ContentReference, ContentStore, GetRejection};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};
use xxhash_rust::xxh3::xxh3_128;
use zerocopy::little_endian::{U64, U128};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, TryReadError, Unaligned,
};

use crate::content::ExtentValue;
use crate::error::CorruptValue;
use crate::key::{FsKey, KeyKind};
use crate::{FsConfig, FsTransaction, WrittenBlob};

// ContentHash is the hash of a blob's content, as long as the file system's configuration makes it, up to LONGEST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentHash {
    bytes: [u8; Self::LONGEST],
    length: u8,
}

impl ContentHash {
    pub const LONGEST: usize = 32;

    // xxh3 is content's 128 bit xxh3 hash, which anyone can make other content match.
    #[must_use]
    pub fn xxh3(content: &[u8]) -> Self {
        Self::from(xxh3_128(content).to_le_bytes())
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.length)]
    }

    fn from_stored(stored: &[u8]) -> Option<Self> {
        let mut bytes = [0; Self::LONGEST];
        bytes.get_mut(..stored.len())?.copy_from_slice(stored);
        Some(Self {
            bytes,
            length: stored.len() as u8,
        })
    }
}

impl From<[u8; 16]> for ContentHash {
    fn from(hash: [u8; 16]) -> Self {
        Self::from_stored(&hash).expect("16 bytes are within the longest hash")
    }
}

impl From<[u8; 32]> for ContentHash {
    fn from(hash: [u8; 32]) -> Self {
        Self {
            bytes: hash,
            length: 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpillableValue {
    Inline(Bytes),
    Overflow {
        reference: ContentReference,
        length: u64,
        hash: ContentHash,
    },
}

pub enum SpillableContent<B> {
    Inline(Bytes),
    Overflow(B),
}

impl<B: Buf> Buf for SpillableContent<B> {
    fn remaining(&self) -> usize {
        match self {
            SpillableContent::Inline(content) => content.remaining(),
            SpillableContent::Overflow(content) => content.remaining(),
        }
    }

    fn chunk(&self) -> &[u8] {
        match self {
            SpillableContent::Inline(content) => content.chunk(),
            SpillableContent::Overflow(content) => content.chunk(),
        }
    }

    fn advance(&mut self, count: usize) {
        match self {
            SpillableContent::Inline(content) => content.advance(count),
            SpillableContent::Overflow(content) => content.advance(count),
        }
    }

    fn copy_to_bytes(&mut self, length: usize) -> Bytes {
        match self {
            SpillableContent::Inline(content) => content.copy_to_bytes(length),
            SpillableContent::Overflow(content) => content.copy_to_bytes(length),
        }
    }
}

#[derive(TryFromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(u8)]
enum SpillableKind {
    Inline,
    Overflow,
    // ExistingOverflow is an overflow whose blob already had a reference when it was written.
    ExistingOverflow,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct OverflowReference {
    id: U128,
    length: U64,
}

impl SpillableValue {
    const TAG_LENGTH: usize = size_of::<SpillableKind>();

    pub(crate) fn to_stored(&self) -> Chain<&'static [u8], Bytes> {
        match self {
            SpillableValue::Inline(content) => {
                SpillableKind::Inline.as_bytes().chain(content.clone())
            }
            SpillableValue::Overflow {
                reference,
                length,
                hash,
            } => {
                let kind = match reference {
                    ContentReference::New(_) => &SpillableKind::Overflow,
                    ContentReference::Existing(_) => &SpillableKind::ExistingOverflow,
                };
                let reference = OverflowReference {
                    id: U128::new(reference.id().0),
                    length: U64::new(*length),
                };
                kind.as_bytes().chain(Bytes::from(
                    [reference.as_bytes(), hash.as_bytes()].concat(),
                ))
            }
        }
    }

    pub(crate) fn from_stored(stored: Bytes) -> Result<Self, CorruptValue> {
        let (kind, rest) =
            SpillableKind::try_read_from_prefix(&stored).map_err(|error| match error {
                TryReadError::Size(_) => CorruptValue::WrongLength,
                TryReadError::Validity(_) => CorruptValue::UnknownSpillableKind,
            })?;
        let reference = match kind {
            SpillableKind::Inline => return Ok(SpillableValue::Inline(stored.slice_ref(rest))),
            SpillableKind::Overflow => ContentReference::New,
            SpillableKind::ExistingOverflow => ContentReference::Existing,
        };
        let (stored, hash) =
            OverflowReference::ref_from_prefix(rest).map_err(|_| CorruptValue::WrongLength)?;
        Ok(SpillableValue::Overflow {
            reference: reference(ContentId(stored.id.get())),
            length: stored.length.get(),
            hash: ContentHash::from_stored(hash).ok_or(CorruptValue::WrongLength)?,
        })
    }
}

// referenced_content is the stored entry of key and value's reference to a blob, if its value spilled.
// An entry it cannot read references none.
pub fn referenced_content(key: &[u8], value: &[u8]) -> Option<ContentReference> {
    let stored = match storekey::decode_borrow::<FsKey>(key).ok()?.kind {
        KeyKind::Annotation(_) | KeyKind::SymbolicLinkTarget => value,
        KeyKind::ContentExtent { .. } => value.get(ExtentValue::STORED_PREFIX_LENGTH..)?,
        KeyKind::Attributes
        | KeyKind::DirectoryEntry(_)
        | KeyKind::HardLink { .. }
        | KeyKind::PendingChangeVersion => return None,
    };
    let (kind, rest) = SpillableKind::try_read_from_prefix(stored).ok()?;
    let reference = match kind {
        SpillableKind::Inline => return None,
        SpillableKind::Overflow => ContentReference::New,
        SpillableKind::ExistingOverflow => ContentReference::Existing,
    };
    let (stored, _hash) = OverflowReference::ref_from_prefix(rest).ok()?;
    Some(reference(ContentId(stored.id.get())))
}

impl<T: KeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: FsConfig>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub(crate) fn make_spillable(
        &self,
        key: &[u8],
        prefix_length: usize,
        mut content: impl Buf + Send,
    ) -> impl Future<Output = Result<SpillableValue, C::Failure>> + Send {
        async move {
            let length = content.remaining();
            if key.len() + prefix_length + SpillableValue::TAG_LENGTH + length
                <= S::LARGEST_INLINE_ENTRY
            {
                return Ok(SpillableValue::Inline(content.copy_to_bytes(length)));
            }
            let content = content.copy_to_bytes(length);
            let hash = S::content_hash(&content);
            let length = length as u64;
            let id = match self.take_written(hash, length) {
                Some(id) => id,
                None => {
                    let id = (self.allocate_content_id)();
                    self.content.put(&id, content).await?;
                    self.record_written(hash, length, id);
                    id
                }
            };
            Ok(SpillableValue::Overflow {
                reference: ContentReference::New(id),
                length,
                hash,
            })
        }
    }
}

impl<T, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S> {
    // take_written is the id of a blob of this content that an earlier attempt wrote, and this attempt has not
    // referenced yet.
    fn take_written(&self, hash: ContentHash, length: u64) -> Option<ContentId> {
        let mut written = self.written?.lock().unwrap_or_else(PoisonError::into_inner);
        let blob = written.iter_mut().find(|blob| {
            blob.taken_by != self.attempt && blob.hash == hash && blob.length == length
        })?;
        blob.taken_by = self.attempt;
        Some(blob.id)
    }

    // record_written records a blob this attempt wrote and references.
    fn record_written(&self, hash: ContentHash, length: u64, id: ContentId) {
        if let Some(written) = self.written {
            written
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(WrittenBlob {
                    hash,
                    length,
                    id,
                    taken_by: self.attempt,
                });
        }
    }
}

impl<T: ReadKeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: Sync>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub(crate) fn read_spillable(
        &self,
        value: SpillableValue,
    ) -> impl Future<
        Output = Result<SpillableContent<C::Content>, StoreError<GetRejection, C::Failure>>,
    > + Send {
        async move {
            match value {
                SpillableValue::Inline(content) => Ok(SpillableContent::Inline(content)),
                SpillableValue::Overflow {
                    reference, length, ..
                } => self
                    .content
                    .get(&reference.id(), 0, length)
                    .await
                    .map(SpillableContent::Overflow),
            }
        }
    }
}

#[cfg(test)]
mod tests;
