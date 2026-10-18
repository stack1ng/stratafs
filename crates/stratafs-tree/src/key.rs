use bytes::Bytes;
use storekey::Encode;

use crate::header::{NodeId, TreeVolumeId};

#[derive(Encode)]
pub(crate) enum TreeKey {
    VolumeHeader(TreeVolumeId),
    Node(NodeId),
    Config,
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
