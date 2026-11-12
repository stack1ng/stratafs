#![allow(unused_variables, dead_code)]

use std::marker::PhantomData;
use std::ops::RangeBounds;

use bytes::Bytes;
use digest::{Digest, Output};
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

// MerkleRadixTree is a radix tree, with digits as the keys, and hashes as the values.
// The 2 node types are discriminated like so:
// 1. Internal: Any key with a length < N
// 2. Leaf: Any key with a length == N
// Internal nodes represent digit prefixes, and store a hash of all their children's keys + hashes, in lexicographical order.
// A child of a given parent is defined as having a prefix that is the same as the parent's key, and having a key length of exactly the parent's key length +1.
// Leaf nodes represent full digit keys (left padded by zeros to fit the N size), and simply store the hash of the content that they represent.
//
// Its purpose is to allow for the verification of some data that sits within an unbounded domain, in a bounded amount of time.
pub struct MerkleRadixTree<H: Digest, const N: usize> {
    partition_prefix: Bytes,
    _digest: PhantomData<H>,
}

impl<H: Digest, const N: usize> MerkleRadixTree<H, N> {
    pub fn new(partition_prefix: Bytes) -> Self {
        todo!()
    }

    // initial_root is the root hash of a tree with no records: the digest of no bytes.
    pub fn initial_root() -> Output<H> {
        todo!()
    }

    // verify checks that the tree of root holds exactly records at the positions in range, and no others there. It
    // reads the store at most once.
    pub async fn verify<T: ReadKeyValueTransaction>(
        &self,
        transaction: &T,
        root: &Output<H>,
        range: impl RangeBounds<[u8; N]>,
        records: &[([u8; N], Output<H>)],
    ) -> Result<(), Error<T::Failure>> {
        todo!()
    }

    // update sets the record at each position, or removes it for None, and returns the root's hash of the tree after.
    // It checks every node it reads against root first, and reads the store at most once.
    pub async fn update<T: KeyValueTransaction>(
        &self,
        transaction: &mut T,
        root: &Output<H>,
        changes: &[([u8; N], Option<Output<H>>)],
    ) -> Result<Output<H>, Error<T::Failure>> {
        todo!()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error<F> {
    // Unverified is a record or node that root does not vouch for.
    #[error("the tree does not vouch for what the store returned")]
    Unverified,
    #[error("a stored hash is not as long as the digest's")]
    Corrupt,
    #[error(transparent)]
    Store(F),
}
