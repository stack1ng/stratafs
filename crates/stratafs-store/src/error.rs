use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError<R, E> {
    // The store refused the operation, and it was not applied
    #[error(transparent)]
    Rejected(R),

    // The operation failed due to some backend failure, the operation may or may not have been applied
    #[error(transparent)]
    Failed(E),
}
