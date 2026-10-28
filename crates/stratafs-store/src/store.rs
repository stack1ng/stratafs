use crate::content::ContentStore;
use crate::durability::DurableAgainst;
use crate::kv::KeyValueStore;

// Store is one unit holding a file system's metadata and its file content, made durable together.
pub trait Store: Send + Sync + 'static {
    type Metadata: KeyValueStore;
    type Content: ContentStore;
    type Failure: std::error::Error + Send + Sync + 'static;

    fn metadata(&self) -> &Self::Metadata;

    fn content(&self) -> &Self::Content;

    // ensure_durable makes every metadata transaction committed and every content put finished before it is called
    // durable against a failure, and resolves once they are.
    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send;
}
