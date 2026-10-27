use bytes::{Buf, BytesMut};
use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use stratafs_redb::RedbStore;
use stratafs_store::error::StoreError;
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, SetRejection};
use stratafs_store::spawn::Inline;
use zerocopy::little_endian::{U64, U128};

use super::*;
use crate::header::TreeVolumeId;
use crate::node::{NodeHeader, NodeKind};
use crate::tree::Tree;
use crate::volume::TreeVolume;

struct SmallConfig;

impl TreeConfig for SmallConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = 4096;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

type Volume<S> = TreeVolume<S, fn() -> u128, SmallConfig>;

const VOLUME: TreeVolumeId = TreeVolumeId(1);
const ROOT: u128 = 100;
const LEFT: u128 = 110;
const RIGHT: u128 = 120;
const LEAVES: [u128; 4] = [111, 112, 121, 122];

// Path is what a descent found: each step's node and position, then the leaf and the target's position in it.
type Path = (Vec<(u128, usize)>, u128, Result<usize, usize>);

fn leaf(keys: &[&[u8]]) -> Bytes {
    let mut entries = SlottedList::<BytesMut, LeafEntry<u16>, u16>::empty();
    for (index, key) in keys.iter().enumerate() {
        entries.insert_entry(index, key, &b"value"[..]);
    }
    stored(NodeKind::Leaf, &entries.into_untyped())
}

fn internal(children: &[(&[u8], u128)]) -> Bytes {
    let mut entries = SlottedList::<BytesMut, InternalEntry, u16>::empty();
    for (index, (separator, child)) in children.iter().enumerate() {
        entries.insert_entry(index, NodeId(U128::new(*child)), separator);
    }
    stored(NodeKind::Internal, &entries.into_untyped())
}

fn stored(kind: NodeKind, entries: &SlottedList<BytesMut, [u8], u16>) -> Bytes {
    let header = NodeHeader {
        kind,
        epoch: VolumeEpoch(U64::new(0)),
    };
    let mut node = Node::<Bytes, u16>::write(&header, entries);
    node.copy_to_bytes(node.remaining())
}

// three_level_volume stores this tree directly, then opens it as a volume:
//                 root ["" -> left, "f" -> right]
//   left ["" -> 111, "c" -> 112]          right ["" -> 121, "i" -> 122]
//   111 [a b]      112 [d e]              121 [g h]      122 [j k]
fn three_level_volume() -> Volume<RedbStore<Inline>> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let store = RedbStore::new(database, Inline).unwrap();
    store_three_levels(&store);
    let tree = block_on(Tree::<_, fn() -> u128, SmallConfig>::new(store, || 1)).unwrap();
    block_on(tree.open(VOLUME)).unwrap()
}

fn store_three_levels<S: KeyValueStore>(store: &S) {
    let nodes = [
        (ROOT, internal(&[(b"", LEFT), (b"f", RIGHT)])),
        (LEFT, internal(&[(b"", LEAVES[0]), (b"c", LEAVES[1])])),
        (RIGHT, internal(&[(b"", LEAVES[2]), (b"i", LEAVES[3])])),
        (LEAVES[0], leaf(&[b"a", b"b"])),
        (LEAVES[1], leaf(&[b"d", b"e"])),
        (LEAVES[2], leaf(&[b"g", b"h"])),
        (LEAVES[3], leaf(&[b"j", b"k"])),
    ];
    block_on(store.transact(|mut transaction| {
        let nodes = nodes.clone();
        Box::pin(async move {
            for (id, node) in nodes {
                transaction.set(TreeKey::Node(NodeId(U128::new(id))).into(), node)?;
            }
            let header = VolumeHeader {
                root: NodeId(U128::new(ROOT)),
                epoch: VolumeEpoch(U64::new(0)),
            };
            transaction.set(TreeKey::VolumeHeader(VOLUME).into(), Bytes::from(&header))?;
            Ok::<_, StoreError<SetRejection<S::Rejection>, S::Failure>>(((), transaction))
        })
    }))
    .unwrap();
}

fn path_of(path: &DescentPath<u16>) -> Path {
    (
        path.steps
            .iter()
            .map(|step| (step.node.id.0.get(), step.position))
            .collect(),
        path.destination.node.id.0.get(),
        path.destination.position,
    )
}

fn descend_many<S: KeyValueStore>(volume: &Volume<S>, targets: &[&[u8]]) -> Vec<Path> {
    let targets: Vec<Bytes> = targets
        .iter()
        .map(|target| Bytes::copy_from_slice(target))
        .collect();
    block_on(volume.read_transact(|transaction| {
        let targets = targets.clone();
        Box::pin(async move {
            let paths = transaction.descend_many(&targets).await?;
            Ok::<_, TreeFailure<S::Failure, S::Rejection>>(paths.iter().map(path_of).collect())
        })
    }))
    .unwrap()
}

fn descend<S: KeyValueStore>(volume: &Volume<S>, target: &[u8]) -> Path {
    let target = Bytes::copy_from_slice(target);
    block_on(volume.read_transact(|transaction| {
        let target = target.clone();
        Box::pin(async move {
            let path = transaction.descend(&target).await?;
            Ok::<_, TreeFailure<S::Failure, S::Rejection>>(path_of(&path))
        })
    }))
    .unwrap()
}

fn expected(side: usize, child: usize, position: Result<usize, usize>) -> Path {
    (
        vec![(ROOT, side), ([LEFT, RIGHT][side], child)],
        LEAVES[side * 2 + child],
        position,
    )
}

#[test]
fn every_target_gets_the_path_a_lone_descent_finds() {
    let volume = three_level_volume();
    let targets: [&[u8]; 10] = [b"", b"a", b"b", b"bb", b"c", b"e", b"f", b"g", b"i", b"z"];
    assert_eq!(
        descend_many(&volume, &targets),
        [
            expected(0, 0, Err(0)),
            expected(0, 0, Ok(0)),
            expected(0, 0, Ok(1)),
            expected(0, 0, Err(2)),
            expected(0, 1, Err(0)),
            expected(0, 1, Ok(1)),
            expected(1, 0, Err(0)),
            expected(1, 0, Ok(0)),
            expected(1, 1, Err(0)),
            expected(1, 1, Err(2)),
        ]
    );
    for target in targets {
        assert_eq!(descend_many(&volume, &[target]), [descend(&volume, target)]);
    }
}

#[test]
fn targets_in_one_leaf_share_its_path() {
    let volume = three_level_volume();
    assert_eq!(
        descend_many(&volume, &[b"d", b"d", b"e"]),
        [
            expected(0, 1, Ok(0)),
            expected(0, 1, Ok(0)),
            expected(0, 1, Ok(1)),
        ]
    );
}

#[test]
fn targets_that_split_at_the_root_and_below_each_reach_their_leaf() {
    let volume = three_level_volume();
    assert_eq!(
        descend_many(&volume, &[b"b", b"k"]),
        [expected(0, 0, Ok(1)), expected(1, 1, Ok(1))]
    );
    assert_eq!(
        descend_many(&volume, &[b"d", b"j"]),
        [expected(0, 1, Ok(0)), expected(1, 1, Ok(0))]
    );
    assert_eq!(
        descend_many(&volume, &[b"a", b"d", b"g", b"j"]),
        [
            expected(0, 0, Ok(0)),
            expected(0, 1, Ok(0)),
            expected(1, 0, Ok(0)),
            expected(1, 1, Ok(0)),
        ]
    );
}

#[test]
fn no_targets_descend_nowhere() {
    let volume = three_level_volume();
    assert_eq!(descend_many(&volume, &[]), []);
}
