use stratafs_store::error::StoreError;

use crate::error::{IoFailure, PosixError};

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
    #[error("EPERM: Operation not permitted")]
    OperationNotPermitted,
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

impl<M, C> From<&ReadError<M, C>> for PosixError {
    fn from(error: &ReadError<M, C>) -> Self {
        match error {
            ReadError::StaleFileHandle => PosixError::ESTALE,
            ReadError::IsADirectory => PosixError::EISDIR,
            ReadError::InvalidInput => PosixError::EINVAL,
            ReadError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&WriteError<M, R, C>> for PosixError {
    fn from(error: &WriteError<M, R, C>) -> Self {
        match error {
            WriteError::StaleFileHandle => PosixError::ESTALE,
            WriteError::IsADirectory => PosixError::EISDIR,
            WriteError::InvalidInput => PosixError::EINVAL,
            WriteError::OperationNotPermitted => PosixError::EPERM,
            WriteError::FileTooLarge => PosixError::EFBIG,
            WriteError::Refused(_) | WriteError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, C> From<&ReadSymbolicLinkError<M, C>> for PosixError {
    fn from(error: &ReadSymbolicLinkError<M, C>) -> Self {
        match error {
            ReadSymbolicLinkError::StaleFileHandle => PosixError::ESTALE,
            ReadSymbolicLinkError::InvalidInput => PosixError::EINVAL,
            ReadSymbolicLinkError::InputOutputError(_) => PosixError::EIO,
        }
    }
}
