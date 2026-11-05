use std::convert::Infallible;

use stratafs_serialized::ViewError;
use stratafs_store::content::GetRejection;
use stratafs_store::error::StoreError;
use stratafs_store::kv::SetRejection;

// PosixError is the POSIX error an engine error stands for, by its POSIX name. POSIX fixes the names
// but not their numbers, which differ between platforms, so each adapter turns these into its own.
#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PosixError {
    EBUSY,
    EEXIST,
    EFBIG,
    EINVAL,
    EIO,
    EISDIR,
    EMLINK,
    ENAMETOOLONG,
    ENOATTR,
    ENOENT,
    ENOSPC,
    ENOTDIR,
    ENOTEMPTY,
    EPERM,
    ESTALE,
}

// Posix is an error that says which POSIX error it stands for, so an adapter reports the right one even for an error
// whose type it does not know, such as a consistency's failure.
pub trait Posix {
    fn posix(&self) -> PosixError;
}

impl<E> Posix for E
where
    for<'e> PosixError: From<&'e E>,
{
    fn posix(&self) -> PosixError {
        PosixError::from(self)
    }
}

impl From<&Infallible> for PosixError {
    fn from(never: &Infallible) -> Self {
        match *never {}
    }
}

// EngineFailure is why a transaction failed outside its work: the store failed, or the consistency could not let it
// go ahead.
#[derive(Debug, thiserror::Error)]
pub enum EngineFailure<F, X> {
    #[error(transparent)]
    Store(F),
    #[error(transparent)]
    Consistency(X),
}

#[derive(Debug, thiserror::Error)]
pub enum IoFailure<M, C> {
    #[error(transparent)]
    Metadata(M),
    #[error(transparent)]
    ContentRead(StoreError<GetRejection, C>),
    #[error(transparent)]
    ContentWrite(C),
    #[error(transparent)]
    Corrupt(CorruptValue),
}

// A store failure is an I/O error; a consistency's failure stands for its own.
impl<F, X: Posix> From<&EngineFailure<F, X>> for PosixError {
    fn from(failure: &EngineFailure<F, X>) -> Self {
        match failure {
            EngineFailure::Store(_) => PosixError::EIO,
            EngineFailure::Consistency(failure) => failure.posix(),
        }
    }
}

impl<M, C> From<&IoFailure<M, C>> for PosixError {
    fn from(_: &IoFailure<M, C>) -> Self {
        PosixError::EIO
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CorruptValue {
    #[error("the stored value was written as a different type")]
    WrongFingerprint,
    #[error("the stored value has the wrong length for its type")]
    WrongLength,
    #[error("the stored value holds a value its type does not allow")]
    Invalid,
    #[error("the stored spillable kind is not a known kind")]
    UnknownSpillableKind,
    #[error("the stored file type is not a known file type")]
    UnknownFileType,
    #[error("a stored key could not be decoded")]
    UndecodableKey(#[source] storekey::DecodeError),
    #[error("a stored key is not the kind of key its position in the store implies")]
    UnexpectedKey,
    #[error("a directory entry names an inode that does not exist")]
    DanglingEntry,
    #[error("a directory's parent does not exist")]
    MissingParent,
    #[error("a symlink has no stored target")]
    MissingSymbolicLinkTarget,
    #[error("an extent's stored range is not a valid range")]
    InvalidExtent,
    #[error("a hard link exists that its inode's stored hard link count does not count")]
    UncountedHardLink,
}

impl From<ViewError> for CorruptValue {
    fn from(error: ViewError) -> Self {
        match error {
            ViewError::WrongFingerprint => CorruptValue::WrongFingerprint,
            ViewError::WrongLength => CorruptValue::WrongLength,
            ViewError::Invalid => CorruptValue::Invalid,
        }
    }
}

pub(crate) fn refused_or_failed<R: std::error::Error, M>(
    error: StoreError<SetRejection<R>, M>,
) -> StoreError<R, M> {
    match error {
        StoreError::Rejected(SetRejection::Store(rejection)) => StoreError::Rejected(rejection),
        StoreError::Rejected(rejection) => panic!(
            "the metadata store rejected an entry the engine sized to fit its largest accepted entry, which should be impossible: {rejection}"
        ),
        StoreError::Failed(failure) => StoreError::Failed(failure),
    }
}
