#[derive(Debug, thiserror::Error)]
pub enum FsError<F> {
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
    InputOutputError(#[source] F),
}
