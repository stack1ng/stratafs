// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
#![allow(clippy::manual_async_fn)]

use bytes::buf::Chain;
use bytes::{Buf, Bytes};
use stratafs_store::content::{ContentHash, ContentStore, GetRejection};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};
use zerocopy::little_endian::U64;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, TryReadError, Unaligned,
};

use crate::error::CorruptValue;
use crate::{FsConfig, FsTransaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpillableValue {
    Inline(Bytes),
    Overflow { hash: ContentHash, length: u64 },
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
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct OverflowReference {
    hash: [u8; 32],
    length: U64,
}

const INLINE: SpillableKind = SpillableKind::Inline;
const OVERFLOW: SpillableKind = SpillableKind::Overflow;

impl SpillableValue {
    const TAG_LENGTH: usize = size_of::<SpillableKind>();

    pub(crate) fn to_stored(&self) -> Chain<&'static [u8], Bytes> {
        match self {
            SpillableValue::Inline(content) => INLINE.as_bytes().chain(content.clone()),
            SpillableValue::Overflow { hash, length } => {
                let reference = OverflowReference {
                    hash: hash.0,
                    length: U64::new(*length),
                };
                OVERFLOW
                    .as_bytes()
                    .chain(Bytes::copy_from_slice(reference.as_bytes()))
            }
        }
    }

    pub(crate) fn from_stored(stored: Bytes) -> Result<Self, CorruptValue> {
        let (kind, rest) =
            SpillableKind::try_read_from_prefix(&stored).map_err(|error| match error {
                TryReadError::Size(_) => CorruptValue::WrongLength,
                TryReadError::Validity(_) => CorruptValue::UnknownSpillableKind,
            })?;
        match kind {
            SpillableKind::Inline => Ok(SpillableValue::Inline(stored.slice_ref(rest))),
            SpillableKind::Overflow => {
                let reference = OverflowReference::ref_from_bytes(rest)
                    .map_err(|_| CorruptValue::WrongLength)?;
                Ok(SpillableValue::Overflow {
                    hash: ContentHash(reference.hash),
                    length: reference.length.get(),
                })
            }
        }
    }
}

impl<T: KeyValueTransaction, C: ContentStore, A: Sync, N: Sync, S: FsConfig>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub(crate) fn make_spillable(
        &self,
        key: &[u8],
        mut content: impl Buf + Send,
    ) -> impl Future<Output = Result<SpillableValue, C::Failure>> + Send {
        async move {
            let length = content.remaining();
            if key.len() + SpillableValue::TAG_LENGTH + length <= S::LARGEST_INLINE_ENTRY {
                return Ok(SpillableValue::Inline(content.copy_to_bytes(length)));
            }
            let hash = self.content.put(content).await?;
            Ok(SpillableValue::Overflow {
                hash,
                length: length as u64,
            })
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
                SpillableValue::Overflow { hash, length } => self
                    .content
                    .get(&hash, 0, length)
                    .await
                    .map(SpillableContent::Overflow),
            }
        }
    }
}

#[cfg(test)]
mod tests;
