use stratafs_store::error::StoreError;

use crate::error::{IoFailure, PosixError};

#[derive(Debug, thiserror::Error)]
pub enum GetAnnotationError<M, C> {
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
pub enum ListAnnotationsError<M, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum SetAnnotationError<M, R, C> {
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
pub enum RemoveAnnotationError<M, R, C> {
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

impl<M, R, C> From<StoreError<R, M>> for SetAnnotationError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, R, C> From<StoreError<R, M>> for RemoveAnnotationError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, C> From<&GetAnnotationError<M, C>> for PosixError {
    fn from(error: &GetAnnotationError<M, C>) -> Self {
        match error {
            GetAnnotationError::StaleFileHandle => PosixError::ESTALE,
            GetAnnotationError::InvalidInput => PosixError::EINVAL,
            GetAnnotationError::FileNameTooLong => PosixError::ENAMETOOLONG,
            GetAnnotationError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, C> From<&ListAnnotationsError<M, C>> for PosixError {
    fn from(error: &ListAnnotationsError<M, C>) -> Self {
        match error {
            ListAnnotationsError::StaleFileHandle => PosixError::ESTALE,
            ListAnnotationsError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&SetAnnotationError<M, R, C>> for PosixError {
    fn from(error: &SetAnnotationError<M, R, C>) -> Self {
        match error {
            SetAnnotationError::StaleFileHandle => PosixError::ESTALE,
            SetAnnotationError::InvalidInput => PosixError::EINVAL,
            SetAnnotationError::FileNameTooLong => PosixError::ENAMETOOLONG,
            SetAnnotationError::AlreadyExists => PosixError::EEXIST,
            SetAnnotationError::AttributeNotFound => PosixError::ENOATTR,
            SetAnnotationError::Refused(_) | SetAnnotationError::InputOutputError(_) => {
                PosixError::EIO
            }
        }
    }
}

impl<M, R, C> From<&RemoveAnnotationError<M, R, C>> for PosixError {
    fn from(error: &RemoveAnnotationError<M, R, C>) -> Self {
        match error {
            RemoveAnnotationError::StaleFileHandle => PosixError::ESTALE,
            RemoveAnnotationError::InvalidInput => PosixError::EINVAL,
            RemoveAnnotationError::FileNameTooLong => PosixError::ENAMETOOLONG,
            RemoveAnnotationError::AttributeNotFound => PosixError::ENOATTR,
            RemoveAnnotationError::Refused(_) | RemoveAnnotationError::InputOutputError(_) => {
                PosixError::EIO
            }
        }
    }
}
