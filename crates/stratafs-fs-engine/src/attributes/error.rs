use stratafs_store::error::StoreError;

use crate::error::{IoFailure, PosixError};

#[derive(Debug, thiserror::Error)]
pub enum SetattrError<M, R, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

impl<M, R, C> From<StoreError<R, M>> for SetattrError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, R, C> From<&SetattrError<M, R, C>> for PosixError {
    fn from(error: &SetattrError<M, R, C>) -> Self {
        match error {
            SetattrError::StaleFileHandle => PosixError::ESTALE,
            SetattrError::Refused(_) | SetattrError::InputOutputError(_) => PosixError::EIO,
        }
    }
}
