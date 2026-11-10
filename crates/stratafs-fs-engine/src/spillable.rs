// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
// As `async fn`, rustc checks Send at each call site and wrongly rejects stores whose types contain 'static lifetimes,
// such as redb's (https://github.com/rust-lang/rust/issues/110338).
#![allow(clippy::manual_async_fn)]

use bytes::buf::Chain;
use bytes::{Buf, Bytes};
use stratafs_store::content::{ContentId, ContentReference, ContentStore, GetRejection};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};
use zerocopy::little_endian::{U64, U128};
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, TryFromBytes, TryReadError, Unaligned,
};

use crate::content::ExtentValue;
use crate::error::CorruptValue;
use crate::key::{FsKey, KeyKind};
use crate::{FsConfig, FsTransaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpillableValue {
    Inline(Bytes),
    Overflow {
        reference: ContentReference,
        length: u64,
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
            SpillableValue::Overflow { reference, length } => {
                let kind = match reference {
                    ContentReference::New(_) => &SpillableKind::Overflow,
                    ContentReference::Existing(_) => &SpillableKind::ExistingOverflow,
                };
                let reference = OverflowReference {
                    id: U128::new(reference.id().0),
                    length: U64::new(*length),
                };
                kind.as_bytes()
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
        let reference = match kind {
            SpillableKind::Inline => return Ok(SpillableValue::Inline(stored.slice_ref(rest))),
            SpillableKind::Overflow => ContentReference::New,
            SpillableKind::ExistingOverflow => ContentReference::Existing,
        };
        let stored =
            OverflowReference::ref_from_bytes(rest).map_err(|_| CorruptValue::WrongLength)?;
        Ok(SpillableValue::Overflow {
            reference: reference(ContentId(stored.id.get())),
            length: stored.length.get(),
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
    let stored = OverflowReference::ref_from_bytes(rest).ok()?;
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
            let id = (self.allocate_content_id)();
            self.content.put(&id, content).await?;
            Ok(SpillableValue::Overflow {
                reference: ContentReference::New(id),
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
                SpillableValue::Overflow { reference, length } => self
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
