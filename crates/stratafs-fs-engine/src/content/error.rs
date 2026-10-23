use stratafs_store::error::StoreError;

use crate::error::IoFailure;

#[derive(Debug, thiserror::Error)]
pub enum ReadError<M, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EISDIR: Is a directory")]
    IsADirectory,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum WriteError<M, R, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EISDIR: Is a directory")]
    IsADirectory,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("EFBIG: File too large")]
    FileTooLarge,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

impl<M, R, C> From<StoreError<R, M>> for WriteError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReadSymbolicLinkError<M, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}
