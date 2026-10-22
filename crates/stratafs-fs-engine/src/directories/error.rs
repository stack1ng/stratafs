use stratafs_store::error::StoreError;

use crate::error::IoFailure;

#[derive(Debug, thiserror::Error)]
pub enum LookupError<M, C> {
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum ReaddirError<M, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum CreateError<M, R, C> {
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("EEXIST: File exists")]
    AlreadyExists,
    #[error("EMLINK: Too many links")]
    TooManyLinks,
    #[error("ENOSPC: No space left on device")]
    StorageFull,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum HardLinkError<M, R, C> {
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("EPERM: Operation not permitted")]
    OperationNotPermitted,
    #[error("EEXIST: File exists")]
    AlreadyExists,
    #[error("EMLINK: Too many links")]
    TooManyLinks,
    #[error("ENOSPC: No space left on device")]
    StorageFull,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum HardUnlinkError<M, R, C> {
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("ENOENT: No such file or directory")]
    NotFound,
    #[error("EISDIR: Is a directory")]
    IsADirectory,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum RmdirError<M, R, C> {
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("ENOENT: No such file or directory")]
    NotFound,
    #[error("ENOTEMPTY: Directory not empty")]
    DirectoryNotEmpty,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum RenameError<M, R, C> {
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("EISDIR: Is a directory")]
    IsADirectory,
    #[error("ENOENT: No such file or directory")]
    NotFound,
    #[error("EEXIST: File exists")]
    AlreadyExists,
    #[error("ENOTEMPTY: Directory not empty")]
    DirectoryNotEmpty,
    #[error("EMLINK: Too many links")]
    TooManyLinks,
    #[error("ENOSPC: No space left on device")]
    StorageFull,
    #[error(transparent)]
    Refused(R),
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}

impl<M, R, C> From<StoreError<R, M>> for CreateError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, R, C> From<StoreError<R, M>> for HardLinkError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, R, C> From<StoreError<R, M>> for HardUnlinkError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, R, C> From<StoreError<R, M>> for RmdirError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}

impl<M, R, C> From<StoreError<R, M>> for RenameError<M, R, C> {
    fn from(error: StoreError<R, M>) -> Self {
        match error {
            StoreError::Rejected(rejection) => Self::Refused(rejection),
            StoreError::Failed(failure) => Self::InputOutputError(IoFailure::Metadata(failure)),
        }
    }
}
