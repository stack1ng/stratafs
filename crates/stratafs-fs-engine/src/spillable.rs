use bytes::{Buf, Bytes};
use stratafs_store::content::{ContentHash, ContentStore};
use stratafs_store::kv::KeyValueTransaction;

use crate::{FsConfig, FsTransaction};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SpillableValue {
    Inline(Bytes),
    Overflow { hash: ContentHash, length: u64 },
}

impl SpillableValue {
    const TAG_LENGTH: usize = 1;
}

impl<T: KeyValueTransaction, C: ContentStore, A, N, S: FsConfig>
    FsTransaction<'_, '_, T, C, A, N, S>
{
    pub(crate) async fn make_spillable(
        &self,
        key: &[u8],
        mut content: impl Buf + Send,
    ) -> Result<SpillableValue, C::Failure> {
        let content = content.copy_to_bytes(content.remaining());
        if key.len() + SpillableValue::TAG_LENGTH + content.len() <= S::LARGEST_INLINE_ENTRY {
            return Ok(SpillableValue::Inline(content));
        }
        let length = content.len() as u64;
        let hash = self.content.put(content).await?;
        Ok(SpillableValue::Overflow { hash, length })
    }
}

#[cfg(test)]
mod tests;
