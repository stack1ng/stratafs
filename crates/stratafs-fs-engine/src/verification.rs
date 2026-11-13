// A hash tree holds every record of an inode, and the inode's attributes are signed with the tree's root. An engine
// reads only what a signer the volume trusts signed, and signs what it writes.

// These functions return `impl Future + Send` instead of being `async fn`, so Send is proven here once, for every store.
#![allow(clippy::manual_async_fn, clippy::type_complexity)]

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::sync::{Arc, PoisonError};

use bytes::{Buf, Bytes};
use digest::{Digest, Output};
use futures_core::future::BoxFuture;
use futures_util::future::{join, try_join_all};
use quick_cache::sync::Cache;
use stratafs_merkle_radix_tree::MerkleRadixTree;
use stratafs_store::content::ContentStore;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use crate::attributes::Attributes;
use crate::error::{ChangeFailure, CorruptValue, IoFailure, refused_or_failed};
use crate::key::{FsKey, KeyKind};
use crate::{Change, FsConfig, FsTransaction, Inode};

// Signer signs the statements an engine makes about what it writes, and says whether a statement it reads was signed
// by a signer the volume trusts. A statement is an inode's number, its stored attributes, and the root of its hash
// tree.
pub trait Signer: Send + Sync + 'static {
    // VERIFIES says whether the volume is signed at all. A volume that is not keeps no signatures and no hash trees.
    const VERIFIES: bool;

    type Failure: Error + Send + Sync + 'static;

    fn sign(&self, statement: &[u8]) -> impl Future<Output = Result<Bytes, Self::Failure>> + Send;

    fn verify(
        &self,
        statement: &[u8],
        signature: &[u8],
    ) -> impl Future<Output = Result<bool, Self::Failure>> + Send;
}

// Unsigned is the signer of a volume that is not signed.
pub struct Unsigned;

impl Signer for Unsigned {
    const VERIFIES: bool = false;

    type Failure = Infallible;

    async fn sign(&self, _: &[u8]) -> Result<Bytes, Infallible> {
        Ok(Bytes::new())
    }

    async fn verify(&self, _: &[u8], _: &[u8]) -> Result<bool, Infallible> {
        Ok(true)
    }
}

type SignerFailure = Box<dyn Error + Send + Sync>;

// AnySigner is a Signer of any type.
trait AnySigner: Send + Sync {
    fn sign<'a>(&'a self, statement: &'a [u8]) -> BoxFuture<'a, Result<Bytes, SignerFailure>>;

    fn verify<'a>(
        &'a self,
        statement: &'a [u8],
        signature: &'a [u8],
    ) -> BoxFuture<'a, Result<bool, SignerFailure>>;
}

impl<G: Signer> AnySigner for G {
    fn sign<'a>(&'a self, statement: &'a [u8]) -> BoxFuture<'a, Result<Bytes, SignerFailure>> {
        Box::pin(async move {
            let signed = Signer::sign(self, statement).await;
            signed.map_err(|failure| Box::new(failure) as SignerFailure)
        })
    }

    fn verify<'a>(
        &'a self,
        statement: &'a [u8],
        signature: &'a [u8],
    ) -> BoxFuture<'a, Result<bool, SignerFailure>> {
        Box::pin(async move {
            let verified = Signer::verify(self, statement, signature).await;
            verified.map_err(|failure| Box::new(failure) as SignerFailure)
        })
    }
}

// CHECKED_INODES is how many inodes' attributes a mount keeps as verified.
const CHECKED_INODES: usize = 16 * 1024;

// Verifier is a signed volume's signer, with the attributes it verified last.
pub(crate) struct Verifier {
    signer: Box<dyn AnySigner>,
    // checked is the stored attributes and signature of each inode that the signer verified last.
    checked: Cache<(Inode, Bytes), ()>,
}

impl Verifier {
    // of is signer's verifier, or None for a signer of a volume that is not signed.
    pub(crate) fn of<G: Signer>(signer: G) -> Option<Arc<Self>> {
        G::VERIFIES.then(|| {
            Arc::new(Verifier {
                signer: Box::new(signer),
                checked: Cache::new(CHECKED_INODES),
            })
        })
    }

    // read is inode's attributes and the root of its hash tree as metadata reads them, once their signature is
    // verified, or None when the inode does not exist.
    pub(crate) fn read<'a, T: ReadKeyValueTransaction, C: 'a>(
        &'a self,
        metadata: &'a T,
        inode: Inode,
    ) -> impl Future<Output = Result<Option<(Attributes, Bytes)>, IoFailure<T::Failure, C>>> + Send + 'a
    {
        async move {
            let (attributes, signed) = join(
                metadata.get(FsKey::new_attributes(inode).into()),
                metadata.get(FsKey::new_signature(inode).into()),
            )
            .await;
            let Some(stored) = attributes.map_err(IoFailure::Metadata)? else {
                return Ok(None);
            };
            let attributes =
                Attributes::view(&stored).map_err(|corrupt| IoFailure::Corrupt(corrupt.into()))?;
            let Some(signed) = signed.map_err(IoFailure::Metadata)? else {
                return Err(unverified(inode, "its attributes are not signed"));
            };
            let root_length = usize::from(
                *signed
                    .first()
                    .ok_or(IoFailure::Corrupt(CorruptValue::WrongLength))?,
            );
            if signed.len() <= root_length {
                return Err(IoFailure::Corrupt(CorruptValue::WrongLength));
            }
            let root = signed.slice(1..=root_length);
            let checked = (inode, Bytes::from([&stored[..], &signed[..]].concat()));
            if self.checked.get(&checked).is_none() {
                let statement = statement(inode, &stored, &root);
                let signature = &signed[1 + root_length..];
                match self.signer.verify(&statement, signature).await {
                    Ok(true) => self.checked.insert(checked, ()),
                    Ok(false) => {
                        return Err(unverified(inode, "no trusted signer signed its attributes"));
                    }
                    Err(failure) => return Err(IoFailure::Verification(failure)),
                }
            }
            Ok(Some((attributes, root)))
        }
    }

    // signed is inode's stored signature for attributes and the root of its hash tree: the root, then the signer's
    // signature of the two.
    pub(crate) fn signed<'a>(
        &'a self,
        inode: Inode,
        attributes: &Attributes,
        root: &'a [u8],
    ) -> impl Future<Output = Result<Bytes, SignerFailure>> + Send + 'a {
        let stored = Bytes::from(attributes);
        async move {
            let signature = self.signer.sign(&statement(inode, &stored, root)).await?;
            let root_length = u8::try_from(root.len()).expect("a hash is at most 255 bytes");
            let signed = Bytes::from([&[root_length], root, &signature[..]].concat());
            let checked = Bytes::from([&stored[..], &signed[..]].concat());
            self.checked.insert((inode, checked), ());
            Ok(signed)
        }
    }
}

// statement is what a signer signs for inode: its number, its stored attributes, and the root of its hash tree.
fn statement(inode: Inode, attributes: &[u8], root: &[u8]) -> Vec<u8> {
    [&inode.0.to_le_bytes()[..], attributes, root].concat()
}

// unverified is the failure of a read of inode that no trusted signer vouches for.
fn unverified<M, C>(inode: Inode, why: &'static str) -> IoFailure<M, C> {
    tracing::error!(
        inode = inode.0,
        why,
        "the store returned what no trusted signer vouches for"
    );
    IoFailure::Unverified
}

// POSITION_LENGTH is how many digits a record's position in its inode's hash tree has: its kind, then its number.
const POSITION_LENGTH: usize = 9;

// Position is where a record is in its inode's hash tree.
pub(crate) type Position = [u8; POSITION_LENGTH];

// RecordKind is a kind of record an inode's hash tree holds.
#[derive(Clone, Copy)]
pub(crate) enum RecordKind {
    // Content is a file's extents by their ends, a directory's entries by their positions, or a symbolic link's
    // target.
    Content,
    Annotation,
    // Name is a file's or symbolic link's record of a name it has in a directory.
    Name,
}

impl RecordKind {
    // at is the position of the kind's record with number.
    pub(crate) fn at(self, number: u64) -> Position {
        let mut position = [self as u8; POSITION_LENGTH];
        position[1..].copy_from_slice(&number.to_be_bytes());
        position
    }

    // of is the position of the kind's record with key, when its records have no number of their own: the start of the
    // key's hash.
    pub(crate) fn of<D: Digest>(self, key: &[u8]) -> Position {
        let mut position = [self as u8; POSITION_LENGTH];
        position[1..].copy_from_slice(&D::digest(key)[..POSITION_LENGTH - 1]);
        position
    }
}

// Record is a stored record of an inode: its key and value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

impl From<(Bytes, Bytes)> for Record {
    fn from((key, value): (Bytes, Bytes)) -> Self {
        Record { key, value }
    }
}

// RecordChange is a change to the record of a key: its new value, or None to remove it.
pub(crate) struct RecordChange<B> {
    pub(crate) key: Bytes,
    pub(crate) value: Option<B>,
}

// position is where the record of key is in its inode's hash tree: a record with a number of its own at that number,
// and any other at the start of its key's hash.
fn position<D: Digest>(key: &[u8]) -> Result<Position, CorruptValue> {
    let decoded = storekey::decode_borrow::<FsKey>(key).map_err(CorruptValue::UndecodableKey)?;
    match decoded.kind {
        KeyKind::ContentExtent { end } => Ok(RecordKind::Content.at(end)),
        KeyKind::DirectoryEntry(position) => Ok(RecordKind::Content.at(position)),
        // A symbolic link's target is its one record of content.
        KeyKind::SymbolicLinkTarget => Ok(RecordKind::Content.at(0)),
        KeyKind::Annotation(_) => Ok(RecordKind::Annotation.of::<D>(key)),
        KeyKind::HardLink { .. } => Ok(RecordKind::Name.of::<D>(key)),
        KeyKind::Attributes
        | KeyKind::Signature
        | KeyKind::VerificationNode
        | KeyKind::PendingChangeVersion => Err(CorruptValue::UnexpectedKey),
    }
}

// hash is the hash an inode's hash tree holds for the record of key and value.
fn hash<D: Digest>(key: &[u8], value: &[u8]) -> Output<D> {
    let key_length = u32::try_from(key.len()).expect("a key is shorter than 4 GiB");
    D::new()
        .chain_update(key_length.to_le_bytes())
        .chain_update(key)
        .chain_update(value)
        .finalize()
}

fn tree<D: Digest>(inode: Inode) -> MerkleRadixTree<D, POSITION_LENGTH> {
    MerkleRadixTree::new(FsKey::new_verification_node(inode).into())
}

impl<T, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S>
where
    T: ReadKeyValueTransaction,
    C: ContentStore,
    A: Sync,
    N: Sync,
    S: FsConfig,
{
    // verifier is the volume's verifier, or None when it is not signed.
    pub(crate) fn verifier(&self) -> Option<&Verifier> {
        match <S::Signer as Signer>::VERIFIES {
            true => self.verifier,
            false => None,
        }
    }

    // verified_attributes is inode's attributes once their signature is verified, or None when the inode does not
    // exist. Attributes this transaction wrote are its own, and wait for it to sign them.
    pub(crate) fn verified_attributes<'a>(
        &'a self,
        verifier: &'a Verifier,
        inode: Inode,
    ) -> impl Future<Output = Result<Option<Attributes>, IoFailure<T::Failure, C::Failure>>> + Send + 'a
    {
        async move {
            if self.changes.contains_key(&inode) {
                return crate::attributes::read_attributes(&*self.metadata, inode).await;
            }
            let Some((attributes, root)) = verifier.read(&*self.metadata, inode).await? else {
                return Ok(None);
            };
            let root = Output::<S::HashFunction>::try_from(&root[..])
                .map_err(|_| IoFailure::Corrupt(CorruptValue::WrongLength))?;
            // A root the transaction moved since stays.
            self.roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(inode)
                .or_insert(root);
            Ok(Some(attributes))
        }
    }

    // root is the root of inode's hash tree as the transaction left it: the one it read or moved to, the stored one,
    // or that of a tree with no records for an inode that does not exist.
    fn root<'a>(
        &'a self,
        verifier: &'a Verifier,
        inode: Inode,
    ) -> impl Future<Output = Result<Output<S::HashFunction>, IoFailure<T::Failure, C::Failure>>>
    + Send
    + 'a {
        async move {
            let read = self
                .roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&inode)
                .cloned();
            if let Some(root) = read {
                return Ok(root);
            }
            if self.changes.contains_key(&inode) {
                return Err(unverified(
                    inode,
                    "the transaction wrote its attributes without reading them",
                ));
            }
            match self.verified_attributes(verifier, inode).await? {
                Some(_) => {
                    let roots = self.roots.lock().unwrap_or_else(PoisonError::into_inner);
                    let read = roots.get(&inode);
                    Ok(read
                        .expect("reading verified attributes leaves their root")
                        .clone())
                }
                None => Ok(MerkleRadixTree::<S::HashFunction, POSITION_LENGTH>::initial_root()),
            }
        }
    }

    // vouched checks that records are every record of inode at the positions from first to last.
    pub(crate) fn vouched<'a>(
        &'a self,
        inode: Inode,
        first: Position,
        last: Position,
        records: &'a [Record],
    ) -> impl Future<Output = Result<(), IoFailure<T::Failure, C::Failure>>> + Send + 'a {
        async move {
            let Some(verifier) = self.verifier() else {
                return Ok(());
            };
            let hashed = |record: &Record| {
                let position = position::<S::HashFunction>(&record.key)?;
                Ok((
                    position,
                    hash::<S::HashFunction>(&record.key, &record.value),
                ))
            };
            let mut hashed = records
                .iter()
                .map(hashed)
                .collect::<Result<Vec<_>, _>>()
                .map_err(IoFailure::Corrupt)?;
            hashed.sort_unstable_by_key(|(position, _)| *position);
            let root = self.root(verifier, inode).await?;
            let vouched = tree::<S::HashFunction>(inode)
                .verify(&*self.metadata, &root, first..=last, &hashed)
                .await;
            vouched.map_err(|error| match error {
                stratafs_merkle_radix_tree::Error::Unverified => {
                    unverified(inode, "its hash tree does not hold what was read")
                }
                stratafs_merkle_radix_tree::Error::Corrupt => {
                    IoFailure::Corrupt(CorruptValue::InvalidVerificationNode)
                }
                stratafs_merkle_radix_tree::Error::Store(failure) => IoFailure::Metadata(failure),
            })
        }
    }

    // vouched_record checks that inode's record of key has value, or that it has no such record for None.
    pub(crate) fn vouched_record<'a>(
        &'a self,
        inode: Inode,
        key: &'a Bytes,
        value: Option<&'a Bytes>,
    ) -> impl Future<Output = Result<(), IoFailure<T::Failure, C::Failure>>> + Send + 'a {
        async move {
            if self.verifier().is_none() {
                return Ok(());
            }
            let at = position::<S::HashFunction>(key).map_err(IoFailure::Corrupt)?;
            let record = value.map(|value| Record {
                key: key.clone(),
                value: value.clone(),
            });
            self.vouched(inode, at, at, record.as_slice()).await
        }
    }
}

impl<T, C, A, N, S> FsTransaction<'_, '_, T, C, A, N, S>
where
    T: KeyValueTransaction,
    C: ContentStore,
    A: Sync,
    N: Sync,
    S: FsConfig,
{
    // made notes that the transaction made inode, whose hash tree holds no records.
    pub(crate) fn made(&self, inode: Inode) {
        if self.verifier().is_some() {
            let root = MerkleRadixTree::<S::HashFunction, POSITION_LENGTH>::initial_root();
            self.roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(inode, root);
        }
    }

    // unmade forgets the root of an inode the transaction deleted.
    pub(crate) fn unmade(&self, inode: Inode) {
        self.roots
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&inode);
    }

    // change_records makes each change to inode's records, in the store and in the inode's hash tree.
    pub(crate) fn change_records<B: Buf + Send>(
        &mut self,
        inode: Inode,
        changes: Vec<RecordChange<B>>,
    ) -> impl Future<Output = Result<(), ChangeFailure<T::Rejection, T::Failure, C::Failure>>> + Send
    {
        async move {
            let metadata_failed = |failure| ChangeFailure::Failed(IoFailure::Metadata(failure));
            let verifies = self.verifier().is_some();
            let mut records = Vec::with_capacity(if verifies { changes.len() } else { 0 });
            for RecordChange { key, value } in changes {
                if verifies {
                    let at = position::<S::HashFunction>(&key).map_err(IoFailure::Corrupt)?;
                    let value = value.map(|mut value| value.copy_to_bytes(value.remaining()));
                    let hash = value
                        .as_ref()
                        .map(|value| hash::<S::HashFunction>(&key, value));
                    records.push((at, hash));
                    match value {
                        Some(value) => self.metadata.set(key, value),
                        None => self.metadata.clear(key).map_err(StoreError::Failed),
                    }
                } else {
                    match value {
                        Some(value) => self.metadata.set(key, value),
                        None => self.metadata.clear(key).map_err(StoreError::Failed),
                    }
                }
                .map_err(refused_or_failed)
                .map_err(|error| match error {
                    StoreError::Rejected(rejection) => ChangeFailure::Refused(rejection),
                    StoreError::Failed(failure) => metadata_failed(failure),
                })?;
            }
            let Some(verifier) = self.verifier() else {
                return Ok(());
            };
            // The last change to a position is the one that stands.
            records.reverse();
            records.sort_by_key(|(position, _)| *position);
            records.dedup_by_key(|(position, _)| *position);
            let root = self.root(verifier, inode).await?;
            let updated = tree::<S::HashFunction>(inode)
                .update(&mut *self.metadata, &root, &records)
                .await;
            let root = updated.map_err(|error| match error {
                stratafs_merkle_radix_tree::Error::Unverified => ChangeFailure::Failed(unverified(
                    inode,
                    "its hash tree is not the one its attributes are signed with",
                )),
                stratafs_merkle_radix_tree::Error::Corrupt => {
                    ChangeFailure::Failed(IoFailure::Corrupt(CorruptValue::InvalidVerificationNode))
                }
                stratafs_merkle_radix_tree::Error::Rejected(rejection) => {
                    match refused_or_failed::<_, T::Failure>(StoreError::Rejected(rejection)) {
                        StoreError::Rejected(rejection) => ChangeFailure::Refused(rejection),
                        StoreError::Failed(failure) => metadata_failed(failure),
                    }
                }
                stratafs_merkle_radix_tree::Error::Store(failure) => metadata_failed(failure),
            })?;
            self.roots
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(inode, root);
            Ok(())
        }
    }

    // sign signs the attributes of every inode the transaction wrote with the root its hash tree has now. The engine
    // calls it once the transaction's changes may commit.
    pub(crate) fn sign(
        &mut self,
    ) -> impl Future<Output = Result<(), IoFailure<T::Failure, Infallible>>> + Send {
        async move {
            let Some(verifier) = self.verifier else {
                return Ok(());
            };
            if !<S::Signer as Signer>::VERIFIES {
                return Ok(());
            }
            let mut written = Vec::new();
            for (inode, change) in &self.changes {
                let Change::Written(attributes) = change else {
                    continue;
                };
                let read = self
                    .roots
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(inode)
                    .cloned();
                let root = match read {
                    Some(root) => root,
                    // Only an inode the transaction made has attributes it wrote without reading them.
                    None => {
                        let signed = self
                            .metadata
                            .get(FsKey::new_signature(*inode).into())
                            .await
                            .map_err(IoFailure::Metadata)?;
                        if signed.is_some() {
                            return Err(unverified(
                                *inode,
                                "the transaction wrote its attributes without reading them",
                            ));
                        }
                        MerkleRadixTree::<S::HashFunction, POSITION_LENGTH>::initial_root()
                    }
                };
                written.push((*inode, attributes, root));
            }
            let signed = try_join_all(
                written
                    .iter()
                    .map(|(inode, attributes, root)| verifier.signed(*inode, attributes, root)),
            )
            .await
            .map_err(IoFailure::Verification)?;
            let keys: Vec<Inode> = written.iter().map(|(inode, _, _)| *inode).collect();
            for (inode, signed) in keys.into_iter().zip(signed) {
                self.metadata
                    .set(FsKey::new_signature(inode).into(), signed)
                    .map_err(|error| match error {
                        StoreError::Rejected(rejection) => {
                            IoFailure::Verification(Box::new(rejection))
                        }
                        StoreError::Failed(failure) => IoFailure::Metadata(failure),
                    })?;
            }
            Ok(())
        }
    }
}

// Roots is the root of the hash tree of each inode a transaction read or changed, as the transaction left it. D is
// the hash function that makes the roots.
pub(crate) type Roots<D> = std::sync::Mutex<HashMap<Inode, Output<D>>>;

#[cfg(test)]
mod tests;
