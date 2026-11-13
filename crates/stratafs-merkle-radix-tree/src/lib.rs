use std::collections::BTreeMap;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::ops::{Bound, RangeInclusive};

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
pub struct Record<H: Digest, const N: usize> {
    pub position: [u8; N],
    pub hash: Output<H>,
}

// Change is a change to the record at a position: its new hash, or None to remove it.
pub struct Change<H: Digest, const N: usize> {
    pub position: [u8; N],
    pub hash: Option<Output<H>>,
}

// Claim says that records are every record of a tree at the positions from first to last, in order of position. One
// whose first is after its last says nothing, and has no records.
pub struct Claim<'a, H: Digest, const N: usize> {
    pub first: [u8; N],
    pub last: [u8; N],
    pub records: &'a [Record<H, N>],
}

// Prefetched is nodes of a tree read ahead of the verify or update that needs them. Each uses the nodes it finds
// here, reads the ones it does not, and keeps them here for the next. An update keeps the nodes it changes current.
pub struct Prefetched<const N: usize> {
    // covered is the ranges of positions that every node on the way to is here for.
    covered: Vec<([u8; N], [u8; N])>,
    // nodes is the hash of each node read, by its digits, for each length a node's digits have: N of them.
    nodes: Vec<BTreeMap<Bytes, Bytes>>,
}

impl<const N: usize> Default for Prefetched<N> {
    // The default is no nodes, of no positions.
    fn default() -> Self {
        Prefetched {
            covered: Vec::new(),
            nodes: vec![BTreeMap::new(); N],
        }
    }
}

impl<const N: usize> Prefetched<N> {
    // of_no_records is every node of a tree that holds no records: none.
    pub fn of_no_records() -> Self {
        Prefetched {
            covered: vec![([u8::MIN; N], [u8::MAX; N])],
            ..Prefetched::default()
        }
    }

    // extend adds the nodes of other, which is of the same tree.
    pub fn extend(&mut self, other: Prefetched<N>) {
        self.covered.extend(other.covered);
        for (nodes, others) in self.nodes.iter_mut().zip(other.nodes) {
            nodes.extend(others);
        }
    }

    // covers says whether every node a verify or update of the positions from first to last needs is here.
    pub fn covers(&self, first: &[u8; N], last: &[u8; N]) -> bool {
        let within = |(covered_first, covered_last): &([u8; N], [u8; N])| {
            covered_first <= first && last <= covered_last
        };
        self.covered.iter().any(within)
    }

    // children is every child of parent, in order: its digits and its hash.
    fn children(&self, parent: &[u8]) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        let (mut first, mut last) = ([u8::MIN; N], [u8::MAX; N]);
        first[..parent.len()].copy_from_slice(parent);
        last[..parent.len()].copy_from_slice(parent);
        let length = parent.len() + 1;
        let children = (
            Bound::Included(&first[..length]),
            Bound::Included(&last[..length]),
        );
        self.nodes[parent.len()].range::<[u8], _>(children)
    }
}

// Planned is what an update does once the nodes it read are verified: the node it sets or removes at each of its
// digits, and the tree's root afterwards.
struct Planned<H: Digest> {
    writes: Vec<(Vec<u8>, Option<Output<H>>)>,
    root: Output<H>,
}

// Pending is a change to the node of digits that an update has worked out: its hash afterwards, or None for a node
// left with no children, and for a node that is not a record the hash its children gave it before.
struct Pending<H: Digest> {
    digits: Vec<u8>,
    after: Option<Output<H>>,
    before: Option<Option<Output<H>>>,
}

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

    // prefetch reads the nodes a verify or update of the positions from first to last needs.
    pub async fn prefetch<T: ReadKeyValueTransaction>(
        &self,
        transaction: &T,
        first: [u8; N],
        last: [u8; N],
    ) -> Result<Prefetched<N>, Error<T::Failure>> {
        match first <= last {
            true => self.read(transaction, &[(first, last)]).await,
            false => Ok(Prefetched::default()),
        }
    }

    // verify checks every claim against the tree of root. It reads the store at most once, for the nodes prefetched
    // lacks, and only what root vouches for counts: nodes prefetched from an earlier tree are read again.
    pub async fn verify<T: ReadKeyValueTransaction>(
        &self,
        transaction: &T,
        root: &Output<H>,
        claims: &[Claim<'_, H, N>],
        prefetched: &mut Prefetched<N>,
    ) -> Result<(), Error<T::Failure>> {
        let needed: Vec<_> = claims
            .iter()
            .filter(|claim| claim.first <= claim.last)
            .map(|claim| (claim.first, claim.last))
            .collect();
        let read_ahead = !prefetched.covered.is_empty();
        self.fill(transaction, &needed, prefetched).await?;
        match Self::check(root, claims, prefetched) {
            Err(Refused::Unverified) if read_ahead => {
                *prefetched = self.read(transaction, &needed).await?;
                Ok(Self::check(root, claims, prefetched)?)
            }
            checked => Ok(checked?),
        }
    }

    // update checks every claim against the tree of root, then sets the record at each change's position, or removes
    // it for None, and returns the root's hash of the tree after. The changes are in order of position, one for each.
    // It reads the store at most once, for the nodes prefetched lacks, and checks every node it uses against root.
    // A transaction whose update failed may hold some of its writes, and must not commit.
    pub async fn update<T: KeyValueTransaction>(
        &self,
        transaction: &mut T,
        root: &Output<H>,
        claims: &[Claim<'_, H, N>],
        changes: &[Change<H, N>],
        prefetched: &mut Prefetched<N>,
    ) -> Result<Output<H>, UpdateError<T>> {
        assert!(
            changes
                .windows(2)
                .all(|pair| pair[0].position < pair[1].position),
            "the changes are in order of position, one for each"
        );
        let claimed = claims.iter().filter(|claim| claim.first <= claim.last);
        let needed: Vec<_> = claimed
            .map(|claim| (claim.first, claim.last))
            .chain(
                changes
                    .iter()
                    .map(|change| (change.position, change.position)),
            )
            .collect();
        let read_ahead = !prefetched.covered.is_empty();
        let read = |error| match error {
            Error::Unverified => Error::Unverified,
            Error::Corrupt => Error::Corrupt,
            Error::Store(failure) => Error::Store(failure),
        };
        self.fill(&*transaction, &needed, prefetched)
            .await
            .map_err(read)?;
        let planned = match Self::plan(root, claims, changes, prefetched) {
            Err(Refused::Unverified) if read_ahead => {
                *prefetched = self.read(&*transaction, &needed).await.map_err(read)?;
                Self::plan(root, claims, changes, prefetched)?
            }
            planned => planned?,
        };
        let mut keys = Keys::new(&self.partition_prefix, planned.writes.len(), N);
        for (digits, hash) in planned.writes {
            let key = keys.node(&digits);
            let nodes = &mut prefetched.nodes[digits.len() - 1];
            match hash {
                Some(hash) => {
                    transaction
                        .set(key, hash.as_slice())
                        .map_err(|error| match error {
                            StoreError::Rejected(rejection) => Error::Rejected(rejection),
                            StoreError::Failed(failure) => Error::Store(failure),
                        })?;
                    nodes.insert(Bytes::from(digits), Bytes::copy_from_slice(&hash));
                }
                None => {
                    transaction.clear(key).map_err(Error::Store)?;
                    nodes.remove(&digits[..]);
                }
            }
        }
        Ok(planned.root)
    }

    // fill reads the nodes of every needed range of positions that prefetched has not all of.
    async fn fill<T: ReadKeyValueTransaction>(
        &self,
        transaction: &T,
        needed: &[([u8; N], [u8; N])],
        prefetched: &mut Prefetched<N>,
    ) -> Result<(), Error<T::Failure>> {
        let missing: Vec<_> = needed
            .iter()
            .filter(|(first, last)| !prefetched.covers(first, last))
            .copied()
            .collect();
        if !missing.is_empty() {
            prefetched.extend(self.read(transaction, &missing).await?);
        }
        Ok(())
    }

    // read reads, for each range of positions, every child of each node on the way to one of them, all at once.
    async fn read<T: ReadKeyValueTransaction>(
        &self,
        transaction: &T,
        ranges: &[([u8; N], [u8; N])],
    ) -> Result<Prefetched<N>, Error<T::Failure>> {
        // Ranges that share the start of their positions share the nodes there, which are read once.
        let mut parents: Vec<(usize, &[u8], &[u8])> = ranges
            .iter()
            .flat_map(|(first, last)| {
                (0..N).map(move |length| (length, &first[..length], &last[..length]))
            })
            .collect();
        parents.sort_unstable();
        parents.dedup();
        let mut keys = Keys::new(&self.partition_prefix, 2 * parents.len(), N);
        let read = try_join_all(parents.into_iter().map(|(length, first, last)| {
            let children = keys.children(first, last);
            transaction
                .get_range(children, usize::MAX)
                .map_ok(move |children| (length, children))
        }))
        .await
        .map_err(Error::Store)?;
        let mut prefetched = Prefetched {
            covered: ranges.to_vec(),
            ..Prefetched::default()
        };
        for (length, children) in read {
            for (key, hash) in children {
                let digits = self.digits(&key, length + 1)?;
                if hash.len() != <H as Digest>::output_size() {
                    return Err(Error::Corrupt);
                }
                prefetched.nodes[length].insert(key.slice_ref(digits), hash);
            }
        }
        Ok(prefetched)
    }

    // check checks every claim against the tree of root, whose nodes on the way to the claims' positions are all in
    // nodes.
    fn check(
        root: &Output<H>,
        claims: &[Claim<'_, H, N>],
        nodes: &Prefetched<N>,
    ) -> Result<(), Refused> {
        for claim in claims {
            let mut records = claim.records;
            if claim.first <= claim.last {
                let vouched = Self::vouched(nodes, &[], &claim.first, &claim.last, &mut records)?
                    .unwrap_or_else(Self::initial_root);
                if vouched != *root {
                    return Err(Refused::Unverified);
                }
            }
            if !records.is_empty() {
                return Err(Refused::Unverified);
            }
        }
        Ok(())
    }

    // vouched returns the hash parent's children give it, or None when it has none. It checks each child between
    // first and last: a record against the next of records, and a node against its own children.
    fn vouched(
        nodes: &Prefetched<N>,
        parent: &[u8],
        first: &[u8; N],
        last: &[u8; N],
        records: &mut &[Record<H, N>],
    ) -> Result<Option<Output<H>>, Refused> {
        let length = parent.len() + 1;
        let mut hasher = None;
        for (digits, hash) in nodes.children(parent) {
            add_child::<H>(&mut hasher, digits, hash);
            if first[..length] > digits[..] || digits[..] > last[..length] {
                continue;
            }
            if length < N {
                match Self::vouched(nodes, digits, first, last, records)? {
                    Some(children) if children.as_slice() == &hash[..] => {}
                    _ => return Err(Refused::Unverified),
                }
                continue;
            }
            match records.split_first() {
                Some((record, rest))
                    if record.position[..] == digits[..] && record.hash.as_slice() == &hash[..] =>
                {
                    *records = rest;
                }
                _ => return Err(Refused::Unverified),
            }
        }
        Ok(hasher.map(H::finalize))
    }

    // plan checks the claims and works out the changes against the tree of root, whose nodes on the way to the
    // claims' and the changes' positions are all in nodes. It checks every node it changes against the hash its
    // parent held for it, up to root.
    fn plan(
        root: &Output<H>,
        claims: &[Claim<'_, H, N>],
        changes: &[Change<H, N>],
        nodes: &Prefetched<N>,
    ) -> Result<Planned<H>, Refused> {
        Self::check(root, claims, nodes)?;
        let mut writes = Vec::new();
        if changes.is_empty() {
            let root = root.clone();
            return Ok(Planned { writes, root });
        }
        let mut pending: Vec<Pending<H>> = changes
            .iter()
            .map(|change| Pending {
                digits: change.position.to_vec(),
                after: change.hash.clone(),
                before: None,
            })
            .collect();
        // The changes move up the tree a length at a time: each node whose children changed is a change to its parent.
        for length in (1..=N).rev() {
            let mut parents = Vec::new();
            for changed in pending
                .chunk_by(|one, other| one.digits[..length - 1] == other.digits[..length - 1])
            {
                let parent = &changed[0].digits[..length - 1];
                let (mut before, mut after) = (None, None);
                let mut changed = changed.iter().peekable();
                let mut children = nodes.children(parent).peekable();
                loop {
                    let next = changed.peek().map(|change| &change.digits[..]);
                    let child = match (children.peek(), next) {
                        (Some((digits, _)), Some(next)) if digits[..] < *next => children.next(),
                        (Some(_), None) => children.next(),
                        (_, Some(_)) => None,
                        (None, None) => break,
                    };
                    // A child no change is for stays as it is.
                    if let Some((digits, hash)) = child {
                        add_child::<H>(&mut before, digits, hash);
                        add_child::<H>(&mut after, digits, hash);
                        continue;
                    }
                    let change = changed.next().expect("the change was peeked");
                    let stored = children
                        .next_if(|(digits, _)| digits[..] == change.digits[..])
                        .map(|(_, hash)| &hash[..]);
                    if let Some(read) = &change.before
                        && read.as_ref().map(|read| read.as_slice()) != stored
                    {
                        return Err(Refused::Unverified);
                    }
                    if let Some(stored) = stored {
                        add_child::<H>(&mut before, &change.digits, stored);
                    }
                    if let Some(hash) = &change.after {
                        add_child::<H>(&mut after, &change.digits, hash);
                    }
                    if change.after.as_ref().map(|hash| hash.as_slice()) != stored {
                        writes.push((change.digits.clone(), change.after.clone()));
                    }
                }
                parents.push(Pending {
                    digits: parent.to_vec(),
                    after: after.map(H::finalize),
                    before: Some(before.map(H::finalize)),
                });
            }
            pending = parents;
        }
        let [top] = &pending[..] else {
            unreachable!("every change is below the root");
        };
        let before = top.before.clone().flatten();
        if before.unwrap_or_else(Self::initial_root) != *root {
            return Err(Refused::Unverified);
        }
        let root = top.after.clone().unwrap_or_else(Self::initial_root);
        Ok(Planned { writes, root })
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

// add_child adds a child, by its digits and its hash, to the hash of its parent.
fn add_child<H: Digest>(parent: &mut Option<H>, digits: &[u8], hash: &[u8]) {
    let parent = parent.get_or_insert_with(H::new);
    parent.update(digits);
    parent.update(hash);
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
