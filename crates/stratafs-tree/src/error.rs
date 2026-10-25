use std::sync::Arc;

use stratafs_store::kv::SetRejection;

use crate::serialized::CorruptValue;

#[derive(Debug, thiserror::Error)]
pub enum TreeFailure<F, R> {
    #[error(transparent)]
    Store(#[from] F),
    // Watch is the inner store failing a watch that several waiters share, so each of them holds the same failure.
    #[error(transparent)]
    Watch(Arc<F>),
    #[error(transparent)]
    Corrupt(CorruptValue),
    #[error("the inner store refused a node write")]
    RefusedWrite(#[source] SetRejection<R>),
    #[error("the inner store refused a transaction")]
    RefusedTransaction(#[source] R),
}
