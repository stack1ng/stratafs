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

// ContentReadError is why a blob could not be read.
#[derive(Debug, thiserror::Error)]
pub enum ContentReadError<C> {
    #[error(transparent)]
    Store(StoreError<GetRejection, C>),
    #[error("a blob does not have the length and hash its reference holds")]
    Mismatched,
}

#[derive(Debug, thiserror::Error)]
pub enum IoFailure<M, C> {
    #[error(transparent)]
    Metadata(M),
    #[error(transparent)]
    ContentRead(ContentReadError<C>),
    #[error(transparent)]
    ContentWrite(C),
    #[error(transparent)]
    Corrupt(CorruptValue),
    // Unverified is something the store returned that no signer the volume trusts vouches for.
    #[error("the store returned what no trusted signer vouches for")]
    Unverified,
    #[error("signing or verifying an inode failed")]
    Verification(#[source] Box<dyn std::error::Error + Send + Sync>),
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
    #[error("a stored node of an inode's hash tree is not a key of digits with a hash")]
    InvalidVerificationNode,
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

// ChangeFailure is why an inode's records could not be changed: the store refused an entry, or the change failed.
#[derive(Debug, thiserror::Error)]
pub enum ChangeFailure<R, M, C> {
    #[error(transparent)]
    Refused(R),
    #[error(transparent)]
    Failed(#[from] IoFailure<M, C>),
}

// from_change_failure makes a failure to change records an operation's error, for each of the operations' errors.
macro_rules! from_change_failure {
    ($($error:ident),*) => {$(
        impl<M, R, C> From<$crate::error::ChangeFailure<R, M, C>> for $error<M, R, C> {
            fn from(failure: $crate::error::ChangeFailure<R, M, C>) -> Self {
                match failure {
                    $crate::error::ChangeFailure::Refused(rejection) => Self::Refused(rejection),
                    $crate::error::ChangeFailure::Failed(failure) => Self::InputOutputError(failure),
                }
            }
        }
    )*};
}
pub(crate) use from_change_failure;

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
