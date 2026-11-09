use bytes::Bytes;
use storekey::{Decode, Encode};

use crate::header::{JobId, NodeId, TreeVolumeId};

#[derive(Encode, Decode)]
pub(crate) enum TreeKey {
    VolumeHeader(TreeVolumeId),
    Node(NodeId),
    Config,
    // NodeRefCount is a node's count of parent pointers, where a missing count is 1
    NodeRefCount(NodeId),
    // Dereference is a job, with an empty value, saying the node lost one parent in the volume's tree.
    // Deleting a volume must write one for its root and drain the volume's jobs.
    Dereference(TreeVolumeId, NodeId, JobId),
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
