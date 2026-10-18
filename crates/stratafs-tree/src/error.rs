#[derive(Debug, thiserror::Error)]
pub enum TreeFailure<F> {
    #[error(transparent)]
    Store(F),
}
