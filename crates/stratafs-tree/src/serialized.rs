use stratafs_serialized::ViewError;

#[derive(Debug, thiserror::Error)]
pub enum CorruptValue {
    #[error("the stored value was written as a different type")]
    WrongFingerprint,
    #[error("the stored value has the wrong length for its type")]
    WrongLength,
    #[error("the stored entry positions do not describe the entry data")]
    InvalidEntryPositions,
    #[error("the stored node kind is not a known kind")]
    UnknownNodeKind,
    #[error("the stored key length is longer than its entry")]
    KeyLongerThanEntry,
    #[error("a node refers to a child node that is not in the store")]
    MissingNode,
    #[error("the volume's header is not in the store")]
    MissingVolumeHeader,
    #[error(
        "the key sorts before the internal node's first separator, which must be the empty key"
    )]
    KeyBeforeFirstSeparator,
    #[error("the path from the root is deeper than any valid tree")]
    TreeTooDeep,
    #[error("a leaf sits above the depth of the other leaves")]
    UnevenLeafDepth,
    #[error("an internal node has no children")]
    EmptyInternalNode,
}

impl From<ViewError> for CorruptValue {
    fn from(error: ViewError) -> Self {
        match error {
            ViewError::WrongFingerprint => CorruptValue::WrongFingerprint,
            ViewError::WrongLength => CorruptValue::WrongLength,
        }
    }
}
