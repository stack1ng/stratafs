use stratafs_store::error::StoreError;

use crate::error::{IoFailure, PosixError};

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
    #[error("ENOENT: No such file or directory")]
    NotFound,
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
pub enum HardLinkError<M, R, C> {
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
    #[error("EPERM: Operation not permitted")]
    OperationNotPermitted,
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
    #[error("EPERM: Operation not permitted")]
    OperationNotPermitted,
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
    #[error("EPERM: Operation not permitted")]
    OperationNotPermitted,
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

impl<M, C> From<&LookupError<M, C>> for PosixError {
    fn from(error: &LookupError<M, C>) -> Self {
        match error {
            LookupError::InvalidInput => PosixError::EINVAL,
            LookupError::FileNameTooLong => PosixError::ENAMETOOLONG,
            LookupError::StaleFileHandle => PosixError::ESTALE,
            LookupError::NotADirectory => PosixError::ENOTDIR,
            LookupError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, C> From<&ReaddirError<M, C>> for PosixError {
    fn from(error: &ReaddirError<M, C>) -> Self {
        match error {
            ReaddirError::StaleFileHandle => PosixError::ESTALE,
            ReaddirError::NotADirectory => PosixError::ENOTDIR,
            ReaddirError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&CreateError<M, R, C>> for PosixError {
    fn from(error: &CreateError<M, R, C>) -> Self {
        match error {
            CreateError::InvalidInput => PosixError::EINVAL,
            CreateError::FileNameTooLong => PosixError::ENAMETOOLONG,
            CreateError::StaleFileHandle => PosixError::ESTALE,
            CreateError::NotADirectory => PosixError::ENOTDIR,
            CreateError::NotFound => PosixError::ENOENT,
            CreateError::OperationNotPermitted => PosixError::EPERM,
            CreateError::AlreadyExists => PosixError::EEXIST,
            CreateError::TooManyLinks => PosixError::EMLINK,
            CreateError::StorageFull => PosixError::ENOSPC,
            CreateError::Refused(_) | CreateError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&HardLinkError<M, R, C>> for PosixError {
    fn from(error: &HardLinkError<M, R, C>) -> Self {
        match error {
            HardLinkError::InvalidInput => PosixError::EINVAL,
            HardLinkError::FileNameTooLong => PosixError::ENAMETOOLONG,
            HardLinkError::StaleFileHandle => PosixError::ESTALE,
            HardLinkError::NotADirectory => PosixError::ENOTDIR,
            HardLinkError::NotFound => PosixError::ENOENT,
            HardLinkError::OperationNotPermitted => PosixError::EPERM,
            HardLinkError::AlreadyExists => PosixError::EEXIST,
            HardLinkError::TooManyLinks => PosixError::EMLINK,
            HardLinkError::StorageFull => PosixError::ENOSPC,
            HardLinkError::Refused(_) | HardLinkError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&HardUnlinkError<M, R, C>> for PosixError {
    fn from(error: &HardUnlinkError<M, R, C>) -> Self {
        match error {
            HardUnlinkError::InvalidInput => PosixError::EINVAL,
            HardUnlinkError::FileNameTooLong => PosixError::ENAMETOOLONG,
            HardUnlinkError::StaleFileHandle => PosixError::ESTALE,
            HardUnlinkError::NotADirectory => PosixError::ENOTDIR,
            HardUnlinkError::OperationNotPermitted => PosixError::EPERM,
            HardUnlinkError::NotFound => PosixError::ENOENT,
            HardUnlinkError::IsADirectory => PosixError::EISDIR,
            HardUnlinkError::Refused(_) | HardUnlinkError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&RmdirError<M, R, C>> for PosixError {
    fn from(error: &RmdirError<M, R, C>) -> Self {
        match error {
            RmdirError::InvalidInput => PosixError::EINVAL,
            RmdirError::FileNameTooLong => PosixError::ENAMETOOLONG,
            RmdirError::StaleFileHandle => PosixError::ESTALE,
            RmdirError::NotADirectory => PosixError::ENOTDIR,
            RmdirError::OperationNotPermitted => PosixError::EPERM,
            RmdirError::NotFound => PosixError::ENOENT,
            RmdirError::DirectoryNotEmpty => PosixError::ENOTEMPTY,
            RmdirError::Refused(_) | RmdirError::InputOutputError(_) => PosixError::EIO,
        }
    }
}

impl<M, R, C> From<&RenameError<M, R, C>> for PosixError {
    fn from(error: &RenameError<M, R, C>) -> Self {
        match error {
            RenameError::InvalidInput => PosixError::EINVAL,
            RenameError::FileNameTooLong => PosixError::ENAMETOOLONG,
            RenameError::StaleFileHandle => PosixError::ESTALE,
            RenameError::NotADirectory => PosixError::ENOTDIR,
            RenameError::OperationNotPermitted => PosixError::EPERM,
            RenameError::IsADirectory => PosixError::EISDIR,
            RenameError::NotFound => PosixError::ENOENT,
            RenameError::AlreadyExists => PosixError::EEXIST,
            RenameError::DirectoryNotEmpty => PosixError::ENOTEMPTY,
            RenameError::TooManyLinks => PosixError::EMLINK,
            RenameError::StorageFull => PosixError::ENOSPC,
            RenameError::Refused(_) | RenameError::InputOutputError(_) => PosixError::EIO,
        }
    }
}
