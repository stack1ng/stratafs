use std::convert::Infallible;
use std::iter::Peekable;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds, RangeInclusive};

use bytes::{BufMut, Bytes, BytesMut};
use digest::{Digest, Output};
use futures_util::TryFutureExt;
use futures_util::future::try_join_all;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction, SetRejection};

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

// Record is a record of a tree: its position and the hash of its content.
type Record<H, const N: usize> = ([u8; N], Output<H>);

// Change is a change to the record at a position: its new hash, or None to remove it.
type Change<H, const N: usize> = ([u8; N], Option<Output<H>>);

// Rebuilt is the hash a node's children gave it as they were read, and the hash they give it after an update's changes,
// each None for no children.
type Rebuilt<H> = (Option<Output<H>>, Option<Output<H>>);

// UpdateError is why an update in a transaction of T failed.
pub type UpdateError<T> = Error<
    <T as ReadKeyValueTransaction>::Failure,
    SetRejection<<T as KeyValueTransaction>::Rejection>,
>;

impl<H: Digest, const N: usize> MerkleRadixTree<H, N> {
    pub fn new(partition_prefix: Bytes) -> Self {
        const { assert!(N >= 1 && N <= 255, "a position is 1 to 255 digits") };
        Self {
            partition_prefix,
            _digest: PhantomData,
        }
    }

    // initial_root is the root hash of a tree with no records: the digest of no bytes.
    pub fn initial_root() -> Output<H> {
        H::new().finalize()
    }

    // verify checks that the tree of root holds exactly records at the positions in range, and no others there. The
    // records are in order of position. It reads the store at most once.
    pub async fn verify<T: ReadKeyValueTransaction>(
        &self,
        transaction: &T,
        root: &Output<H>,
        range: impl RangeBounds<[u8; N]>,
        mut records: &[Record<H, N>],
    ) -> Result<(), Error<T::Failure>> {
        let Some((first, last)) = inclusive(range) else {
            return match records {
                [] => Ok(()),
                _ => Err(Error::Unverified),
            };
        };
        let mut keys = Keys::new(&self.partition_prefix, 2 * N, N);
        // Each level is every child of the nodes of one length between first and last, from the root's on down.
        let mut levels = try_join_all((0..N).map(|length| {
            let children = keys.children(&first[..length], &last[..length]);
            transaction
                .get_range(children, usize::MAX)
                .map_ok(Iterator::peekable)
        }))
        .await
        .map_err(Error::Store)?;
        let vouched = self
            .vouched(&mut levels, &[], &first, &last, &mut records)?
            .unwrap_or_else(Self::initial_root);
        let unread = levels.iter_mut().any(|level| level.peek().is_some());
        if vouched != *root || !records.is_empty() || unread {
            return Err(Error::Unverified);
        }
        Ok(())
    }

    // vouched takes parent's children off levels and returns the hash they give parent, or None when it has none. It
    // checks each child between first and last: a record against the next of records, and a node against its own
    // children.
    fn vouched(
        &self,
        levels: &mut [Peekable<impl Iterator<Item = (Bytes, Bytes)>>],
        parent: &[u8],
        first: &[u8; N],
        last: &[u8; N],
        records: &mut &[Record<H, N>],
    ) -> Result<Option<Output<H>>, Refused> {
        let (level, deeper) = levels
            .split_first_mut()
            .expect("a parent is shorter than a position");
        let length = parent.len() + 1;
        let mut hasher = None;
        while let Some((key, _)) = level.peek() {
            if self.digits(key, length)?[..parent.len()] != *parent {
                break;
            }
            let (key, hash) = level.next().expect("the entry was peeked");
            let digits = self.digits(&key, length)?;
            let hash = stored_hash::<H>(&hash)?;
            add_child(&mut hasher, digits, hash);
            if first[..length] > *digits || *digits > last[..length] {
                continue;
            }
            if length < N {
                match self.vouched(deeper, digits, first, last, records)? {
                    Some(children) if children.as_slice() == hash => {}
                    _ => return Err(Refused::Unverified),
                }
                continue;
            }
            match records.split_first() {
                Some(((position, content), rest))
                    if position[..] == *digits && content.as_slice() == hash =>
                {
                    *records = rest;
                }
                _ => return Err(Refused::Unverified),
            }
        }
        Ok(hasher.map(H::finalize))
    }

    // update sets the record at each position, or removes it for None, and returns the root's hash of the tree after.
    // The changes are in order of position, one for each. It checks every node it reads against root, and reads the
    // store at most once. A transaction whose update failed may hold some of its writes, and must not commit.
    pub async fn update<T: KeyValueTransaction>(
        &self,
        transaction: &mut T,
        root: &Output<H>,
        changes: &[Change<H, N>],
    ) -> Result<Output<H>, UpdateError<T>> {
        if changes.is_empty() {
            return Ok(root.clone());
        }
        assert!(
            changes.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "the changes are in order of position, one for each"
        );
        // The parents are every node above a changed record, in the order rebuilt comes to them: each change adds the
        // ones the change before does not share.
        let parents = || {
            changes
                .iter()
                .enumerate()
                .flat_map(|(index, (position, _))| {
                    let unshared = match index.checked_sub(1) {
                        Some(before) => shared(&changes[before].0, position) + 1,
                        None => 0,
                    };
                    (unshared..N).map(move |length| &position[..length])
                })
        };
        let written = changes.len() * N;
        let mut keys = Keys::new(&self.partition_prefix, 2 * parents().count() + written, N);
        let read = try_join_all(parents().map(|parent| {
            let children = keys.children(parent, parent);
            transaction.get_range(children, usize::MAX)
        }))
        .await
        .map_err(Error::Store)?;
        let (before, after) =
            self.rebuilt(transaction, &mut keys, &mut read.into_iter(), &[], changes)?;
        if before.unwrap_or_else(Self::initial_root) != *root {
            return Err(Error::Unverified);
        }
        Ok(after.unwrap_or_else(Self::initial_root))
    }

    // rebuilt applies changes, which all lie below parent, to parent's children, which are the next of read, and writes
    // each child's new hash. It returns the hash the children it read give parent, and the hash its children give it
    // after the changes, each None for no children. It checks every changed node below parent against what it read.
    fn rebuilt<T: KeyValueTransaction, I: Iterator<Item = (Bytes, Bytes)>>(
        &self,
        transaction: &mut T,
        keys: &mut Keys,
        read: &mut impl Iterator<Item = I>,
        parent: &[u8],
        mut changes: &[Change<H, N>],
    ) -> Result<Rebuilt<H>, UpdateError<T>> {
        let length = parent.len() + 1;
        let mut children = read
            .next()
            .expect("the children of every changed node were read")
            .peekable();
        let (mut before, mut after) = (None, None);
        loop {
            let next_stored = match children.peek() {
                Some((key, _)) => {
                    let digits = self.digits(key, length)?;
                    if digits[..parent.len()] != *parent {
                        return Err(Error::Corrupt);
                    }
                    Some(digits[parent.len()])
                }
                None => None,
            };
            let changed = changes.first().map(|(position, _)| position[parent.len()]);
            let Some(digit) =
                changed.filter(|digit| next_stored.is_none_or(|stored| *digit <= stored))
            else {
                // The next stored child lies before the next changed one, or nothing more changes: it stays.
                let Some((key, hash)) = children.next() else {
                    break;
                };
                let digits = self.digits(&key, length)?;
                let hash = stored_hash::<H>(&hash)?;
                add_child(&mut before, digits, hash);
                add_child(&mut after, digits, hash);
                continue;
            };
            let below = changes
                .iter()
                .take_while(|(position, _)| position[parent.len()] == digit)
                .count();
            let (below, rest) = changes.split_at(below);
            changes = rest;
            let child = &below[0].0[..length];
            let stored = match next_stored == Some(digit) {
                true => children.next().map(|(_, hash)| hash),
                false => None,
            };
            let stored = stored.as_deref().map(stored_hash::<H>).transpose()?;
            let new = match length < N {
                true => {
                    let (before, after) = self.rebuilt(transaction, keys, read, child, below)?;
                    if before.as_ref().map(|before| before.as_slice()) != stored {
                        return Err(Error::Unverified);
                    }
                    after
                }
                false => below[0].1.clone(),
            };
            if let Some(stored) = stored {
                add_child(&mut before, child, stored);
            }
            match &new {
                Some(new) => {
                    add_child(&mut after, child, new);
                    if stored != Some(new.as_slice()) {
                        transaction
                            .set(keys.node(child), new.as_slice())
                            .map_err(|error| match error {
                                StoreError::Rejected(rejection) => Error::Rejected(rejection),
                                StoreError::Failed(failure) => Error::Store(failure),
                            })?;
                    }
                }
                None if stored.is_some() => {
                    transaction.clear(keys.node(child)).map_err(Error::Store)?;
                }
                None => {}
            }
        }
        Ok((before.map(H::finalize), after.map(H::finalize)))
    }

    // digits is the digits of the node a stored key names, which are length of them.
    fn digits<'key>(&self, key: &'key [u8], length: usize) -> Result<&'key [u8], Refused> {
        match key.strip_prefix(&self.partition_prefix[..]) {
            Some([stored, digits @ ..])
                if usize::from(*stored) == length && digits.len() == length =>
            {
                Ok(digits)
            }
            _ => Err(Refused::Corrupt),
        }
    }
}

// stored_hash is a stored value as the hash it has to be.
fn stored_hash<H: Digest>(stored: &[u8]) -> Result<&[u8], Refused> {
    match stored.len() == <H as Digest>::output_size() {
        true => Ok(stored),
        false => Err(Refused::Corrupt),
    }
}

// add_child adds a child, by its digits and its hash, to the hash of its parent.
fn add_child<H: Digest>(parent: &mut Option<H>, digits: &[u8], hash: &[u8]) {
    let parent = parent.get_or_insert_with(H::new);
    parent.update(digits);
    parent.update(hash);
}

// shared is how many leading digits two positions share.
fn shared<const N: usize>(one: &[u8; N], other: &[u8; N]) -> usize {
    one.iter()
        .zip(other)
        .take_while(|(one, other)| one == other)
        .count()
}

// inclusive is the first and last position of a range, or None for a range of no positions.
fn inclusive<const N: usize>(range: impl RangeBounds<[u8; N]>) -> Option<([u8; N], [u8; N])> {
    let first = match range.start_bound() {
        Bound::Included(first) => *first,
        Bound::Excluded(before) => next(*before, 1)?,
        Bound::Unbounded => [u8::MIN; N],
    };
    let last = match range.end_bound() {
        Bound::Included(last) => *last,
        Bound::Excluded(after) => next(*after, -1)?,
        Bound::Unbounded => [u8::MAX; N],
    };
    (first <= last).then_some((first, last))
}

// next is the position one step after position, or None when there is none.
fn next<const N: usize>(mut position: [u8; N], step: i8) -> Option<[u8; N]> {
    for digit in position.iter_mut().rev() {
        let (stepped, wrapped) = digit.overflowing_add_signed(step);
        *digit = stepped;
        if !wrapped {
            return Some(position);
        }
    }
    None
}

// Keys makes the store's keys of a tree's nodes, in one allocation for all of them.
struct Keys<'tree> {
    partition_prefix: &'tree [u8],
    unused: BytesMut,
}

impl<'tree> Keys<'tree> {
    // new makes room for count keys of nodes up to longest digits long.
    fn new(partition_prefix: &'tree [u8], count: usize, longest: usize) -> Self {
        Keys {
            partition_prefix,
            unused: BytesMut::with_capacity(count * (partition_prefix.len() + 1 + longest)),
        }
    }

    // node is the key of the node with digits.
    fn node(&mut self, digits: &[u8]) -> Bytes {
        self.key(digits, &[])
    }

    // children is the keys of every child of the nodes from first to last, which are as long as each other.
    fn children(&mut self, first: &[u8], last: &[u8]) -> RangeInclusive<Bytes> {
        self.key(first, &[u8::MIN])..=self.key(last, &[u8::MAX])
    }

    // key is the key of the node whose digits are parent's and then digit's.
    fn key(&mut self, parent: &[u8], digit: &[u8]) -> Bytes {
        let length = parent.len() + digit.len();
        debug_assert!(
            self.unused.capacity() > self.partition_prefix.len() + length,
            "new made room for every key"
        );
        self.unused.put_slice(self.partition_prefix);
        self.unused.put_u8(length as u8);
        self.unused.put_slice(parent);
        self.unused.put_slice(digit);
        self.unused.split().freeze()
    }
}

// Refused is why what the store returned was not taken.
enum Refused {
    Unverified,
    Corrupt,
}

impl<F, R> From<Refused> for Error<F, R> {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::Unverified => Error::Unverified,
            Refused::Corrupt => Error::Corrupt,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error<F, R = Infallible> {
    // Unverified is a record or node that root does not vouch for.
    #[error("the tree does not vouch for what the store returned")]
    Unverified,
    #[error("a stored node is not a key of digits with a hash")]
    Corrupt,
    #[error(transparent)]
    Rejected(R),
    #[error(transparent)]
    Store(F),
}

#[cfg(test)]
mod tests;
