#[derive(Debug, thiserror::Error)]
pub enum FsError<M, C> {
    #[error("ENOENT: No such file or directory")]
    NotFound,
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EEXIST: File exists")]
    AlreadyExists,
    #[error("ENOTEMPTY: Directory not empty")]
    DirectoryNotEmpty,
    #[error("ENOTDIR: Not a directory")]
    NotADirectory,
    #[error("EISDIR: Is a directory")]
    IsADirectory,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("EMLINK: Too many links")]
    TooManyLinks,
    #[error("ENAMETOOLONG: File name too long")]
    FileNameTooLong,
    #[error("ENOATTR: Attribute not found")]
    AttributeNotFound,
    #[error("EIO: Input/output error")]
    InputOutputError(#[source] IoFailure<M, C>),
}

#[derive(Debug, thiserror::Error)]
pub enum IoFailure<M, C> {
    #[error(transparent)]
    Metadata(M),
    #[error(transparent)]
    Content(C),
    #[error(transparent)]
    Corrupt(CorruptValue),
}

#[derive(Debug, thiserror::Error)]
pub enum CorruptValue {
    #[error("the stored value has the wrong length for its type")]
    WrongLength,
    #[error("the stored spillable kind is not a known kind")]
    UnknownSpillableKind,
}
