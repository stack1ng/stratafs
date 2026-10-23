use crate::error::IoFailure;

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
pub enum ReadSymbolicLinkError<M, C> {
    #[error("ESTALE: Stale file handle")]
    StaleFileHandle,
    #[error("EINVAL: Invalid argument")]
    InvalidInput,
    #[error("EIO: Input/output error")]
    InputOutputError(#[from] IoFailure<M, C>),
}
