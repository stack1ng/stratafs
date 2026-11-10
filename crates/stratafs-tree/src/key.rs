use bytes::Bytes;
use storekey::{Decode, Encode};

use crate::header::{JobId, NodeId, TreeContentId, TreeVolumeId};

// Referenced is a node or a blob that a reference count counts the pointers to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Encode, Decode)]
pub(crate) enum Referenced {
    Node(NodeId),
    // Content is a blob in the content store
    Content(TreeContentId),
}

#[derive(Encode, Decode)]
pub(crate) enum TreeKey {
    VolumeHeader(TreeVolumeId),
    Node(NodeId),
    Config,

    RefCount(Referenced),
    // Dereference is an asynchronous job, with an empty value,
    // saying that the subject "Referenced" was dereferenced once.
    Dereference(TreeVolumeId, Referenced, JobId),
    // ContentDeletion is a contentId that has been marked for deletion.
    // It should be created created atomically with the dereference, once it reaches 0 refs.
    ContentDeletion(TreeVolumeId, TreeContentId),
}

impl From<TreeKey> for Bytes {
    fn from(key: TreeKey) -> Bytes {
        Bytes::from(
            storekey::encode_vec(&key)
                .expect("a derived encoding only fails on custom errors, and TreeKey raises none"),
        )
    }
}

#[cfg(test)]
mod tests;
