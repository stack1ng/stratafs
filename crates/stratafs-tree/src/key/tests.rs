use zerocopy::little_endian::U128;

use super::*;

#[test]
fn node_keys_end_with_the_node_id_in_big_endian() {
    let key = Bytes::from(TreeKey::Node(NodeId(U128::new(0x0102))));
    assert_eq!(key.len(), 1 + 16);
    assert_eq!(key[1..], 0x0102u128.to_be_bytes());
}
