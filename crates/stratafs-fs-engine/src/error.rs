use stratafs_serialized::ViewError;
use stratafs_store::content::GetRejection;
use stratafs_store::error::StoreError;
use stratafs_store::kv::SetRejection;

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

#[derive(Debug, thiserror::Error)]
pub enum CorruptValue {
    #[error("the stored value was written as a different type")]
    WrongFingerprint,
    #[error("the stored value has the wrong length for its type")]
    WrongLength,
    #[error("the stored spillable kind is not a known kind")]
    UnknownSpillableKind,
    #[error("the stored file type is not a known file type")]
    UnknownFileType,
    #[error("the stored mode has bits that are not mode bits")]
    UnknownModeBits,
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
}

impl From<ViewError> for CorruptValue {
    fn from(error: ViewError) -> Self {
        match error {
            ViewError::WrongFingerprint => CorruptValue::WrongFingerprint,
            ViewError::WrongLength => CorruptValue::WrongLength,
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
