use crate::serialized::CorruptValue;

#[derive(Debug, thiserror::Error)]
pub enum TreeFailure<F> {
    #[error(transparent)]
    Store(#[from] F),
    #[error(transparent)]
    Corrupt(CorruptValue),
}
