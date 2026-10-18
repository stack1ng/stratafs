use proptest::collection::vec;
use proptest::prelude::*;

use super::*;

#[test]
fn plain_bytes_order_like_a_slice() {
    let all: [&[u8]; 5] = [b"", b"a", b"ab", b"b", b"ba"];
    for left in all {
        for right in all {
            assert_eq!(
                ConcatenatedBytes::from(left).cmp(&ConcatenatedBytes::from(right)),
                left.cmp(right)
            );
        }
    }
}

#[test]
fn the_default_is_empty() {
    assert_eq!(
        ConcatenatedBytes::default(),
        ConcatenatedBytes::from(&b""[..])
    );
    assert!(ConcatenatedBytes::default() < ConcatenatedBytes::from(&b"\0"[..]));
}

#[test]
fn a_split_point_does_not_change_the_order() {
    assert_eq!(
        ConcatenatedBytes::new(b"ab", b"c"),
        ConcatenatedBytes::new(b"a", b"bc")
    );
    assert_eq!(
        ConcatenatedBytes::new(b"m", b"\0"),
        ConcatenatedBytes::from(&b"m\0"[..])
    );
}

proptest! {
    #[test]
    fn concatenations_order_like_their_joined_bytes(
        left in (vec(0u8..3, 0..4), vec(0u8..3, 0..4)),
        right in (vec(0u8..3, 0..4), vec(0u8..3, 0..4)),
    ) {
        let left_joined = [left.0.as_slice(), left.1.as_slice()].concat();
        let right_joined = [right.0.as_slice(), right.1.as_slice()].concat();
        let left = ConcatenatedBytes::new(&left.0, &left.1);
        let right = ConcatenatedBytes::new(&right.0, &right.1);
        prop_assert_eq!(left.cmp(&right), left_joined.cmp(&right_joined));
        prop_assert_eq!(left == right, left_joined == right_joined);
    }
}
