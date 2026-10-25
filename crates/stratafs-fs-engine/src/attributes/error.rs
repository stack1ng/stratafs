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

#[derive(Debug, thiserror::Error)]
pub enum GetxattrError<M, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum ListxattrError<M, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum SetxattrError<M, R, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("EEXIST: File exists")]
    AlreadyExists,
    #[error("ENOATTR: Attribute not found")]
    AttributeNotFound,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum RemovexattrError<M, R, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ENOATTR: Attribute not found")]
    AttributeNotFound,
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

impl<M, R, C> From<StoreError<R, M>> for SetxattrError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, R, C> From<StoreError<R, M>> for RemovexattrError<M, R, C> {
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

impl<M, C> From<&GetxattrError<M, C>> for PosixError {
    fn from(error: &GetxattrError<M, C>) -> Self {
        match error {
            GetxattrError::StaleFileHandle => PosixError::ESTALE,
            GetxattrError::InvalidInput => PosixError::EINVAL,
            GetxattrError::FileNameTooLong => PosixError::ENAMETOOLONG,
            GetxattrError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, C> From<&ListxattrError<M, C>> for PosixError {
    fn from(error: &ListxattrError<M, C>) -> Self {
        match error {
            ListxattrError::StaleFileHandle => PosixError::ESTALE,
            ListxattrError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&SetxattrError<M, R, C>> for PosixError {
    fn from(error: &SetxattrError<M, R, C>) -> Self {
        match error {
            SetxattrError::StaleFileHandle => PosixError::ESTALE,
            SetxattrError::InvalidInput => PosixError::EINVAL,
            SetxattrError::FileNameTooLong => PosixError::ENAMETOOLONG,
            SetxattrError::AlreadyExists => PosixError::EEXIST,
            SetxattrError::AttributeNotFound => PosixError::ENOATTR,
            SetxattrError::Refused(_) | SetxattrError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&RemovexattrError<M, R, C>> for PosixError {
    fn from(error: &RemovexattrError<M, R, C>) -> Self {
        match error {
            RemovexattrError::StaleFileHandle => PosixError::ESTALE,
            RemovexattrError::InvalidInput => PosixError::EINVAL,
            RemovexattrError::FileNameTooLong => PosixError::ENAMETOOLONG,
            RemovexattrError::AttributeNotFound => PosixError::ENOATTR,
            RemovexattrError::Refused(_) | RemovexattrError::InputOutputError(_) => PosixError::EIO,
        }
    }
}
