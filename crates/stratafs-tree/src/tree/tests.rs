use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use stratafs_redb::{Inline, RedbStore};

use super::*;
use crate::node::Node;

type Optimal<R, const NODE_CAPACITY: usize> = fn(u8) -> TreeConfig<R, NODE_CAPACITY>;

#[test]
fn each_width_accepts_its_optimal_capacities() {
    let _: Optimal<u8, 255> = TreeConfig::new;
    let _: Optimal<u16, 256> = TreeConfig::new;
    let _: Optimal<u16, 65535> = TreeConfig::new;
    let _: Optimal<u32, 65536> = TreeConfig::new;
}

fn memory_store() -> RedbStore<Inline> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    RedbStore::new(database, Inline).unwrap()
}

fn into_store<S, A, R, const NODE_CAPACITY: usize>(tree: Tree<S, A, R, NODE_CAPACITY>) -> S {
    Arc::into_inner(tree.0)
        .expect("the test holds the only reference to the tree")
        .store
}

#[test]
fn a_store_accepts_the_config_it_was_first_given() {
    let tree = block_on(Tree::new(
        memory_store(),
        TreeConfig::<u16, 16355>::new(25),
        || 1,
    ))
    .unwrap();
    block_on(Tree::new(
        into_store(tree),
        TreeConfig::<u16, 16355>::new(25),
        || 1,
    ))
    .unwrap();
}

#[test]
fn a_store_rejects_a_different_node_capacity() {
    let tree = block_on(Tree::new(
        memory_store(),
        TreeConfig::<u16, 16355>::new(25),
        || 1,
    ))
    .unwrap();
    let reopened = block_on(Tree::new(
        into_store(tree),
        TreeConfig::<u16, 4067>::new(25),
        || 1,
    ));
    assert!(matches!(
        reopened,
        Err(TransactError::Aborted(PinnedConfigError::Mismatch { stored, configured }))
            if stored.node_capacity.get() == 16355 && configured.node_capacity.get() == 4067
    ));
}

#[test]
fn a_store_rejects_a_different_address_length() {
    let tree = block_on(Tree::new(
        memory_store(),
        TreeConfig::<u16, 65535>::new(25),
        || 1,
    ))
    .unwrap();
    let reopened = block_on(Tree::new(
        into_store(tree),
        TreeConfig::<u32, 65536>::new(25),
        || 1,
    ));
    assert!(matches!(
        reopened,
        Err(TransactError::Aborted(PinnedConfigError::Mismatch { stored, configured }))
            if stored.address_length == 2 && configured.address_length == 4
    ));
}

#[test]
fn a_store_rejects_a_different_merge_threshold() {
    let tree = block_on(Tree::new(
        memory_store(),
        TreeConfig::<u16, 16355>::new(25),
        || 1,
    ))
    .unwrap();
    let reopened = block_on(Tree::new(
        into_store(tree),
        TreeConfig::<u16, 16355>::new(30),
        || 1,
    ));
    assert!(matches!(
        reopened,
        Err(TransactError::Aborted(PinnedConfigError::Mismatch { stored, configured }))
            if stored.merge_below_percent == 25 && configured.merge_below_percent == 30
    ));
}

#[test]
fn a_corrupt_pinned_config_is_reported() {
    let store = memory_store();
    block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction
                .set(TreeKey::Config.into(), Bytes::from_static(b"not a config"))
                .await
        })
    }))
    .unwrap();
    let tree = block_on(Tree::new(store, TreeConfig::<u16, 16355>::new(25), || 1));
    assert!(matches!(
        tree,
        Err(TransactError::Aborted(PinnedConfigError::Corrupt(
            CorruptValue::WrongFingerprint
        )))
    ));
}

#[test]
fn create_writes_a_header_and_an_empty_root() {
    let tree = block_on(Tree::new(
        memory_store(),
        TreeConfig::<u16, 16355>::new(25),
        || 7,
    ))
    .unwrap();
    let volume = block_on(tree.create()).unwrap();
    assert_eq!(
        volume,
        VolumeId(Bytes::copy_from_slice(&7u128.to_be_bytes()))
    );
    let (header, root) = block_on(tree.0.store.read_transact(|transaction| {
        Box::pin(async move {
            let header = transaction
                .get(TreeKey::VolumeHeader(TreeVolumeId(7)).into())
                .await?
                .unwrap();
            let root = transaction
                .get(TreeKey::Node(NodeId(U128::new(7))).into())
                .await?
                .unwrap();
            Ok::<_, stratafs_redb::RedbFailure>((header, root))
        })
    }))
    .unwrap();
    let header = VolumeHeader::view(&header).unwrap();
    assert_eq!(
        (header.root, header.epoch),
        (NodeId(U128::new(7)), VolumeEpoch(U64::new(0)))
    );
    let (root_epoch, root) = Node::<Bytes, u16>::read(root).unwrap();
    assert_eq!(root_epoch, VolumeEpoch(U64::new(0)));
    assert!(matches!(root, Node::Leaf(list) if list.len() == 0));
}

fn split_halves_fit<R: AddressableRange, const NODE_CAPACITY: usize>() {
    let per_node = size_of::<NodeHeader>() + size_of::<R::Stored>();
    let largest = TreeConfig::<R, NODE_CAPACITY>::LARGEST_ENTRY_SIZE + size_of::<R::Stored>();
    let overfull = NODE_CAPACITY - per_node + largest;
    assert!(overfull.div_ceil(2) + largest <= NODE_CAPACITY - per_node);
}

#[test]
fn the_largest_entry_leaves_room_for_both_halves_of_a_split() {
    assert_eq!(TreeConfig::<u16, 4067>::LARGEST_ENTRY_SIZE, 1350);
    split_halves_fit::<u8, 255>();
    split_halves_fit::<u16, 256>();
    split_halves_fit::<u16, 4067>();
    split_halves_fit::<u16, 16355>();
    split_halves_fit::<u32, 65536>();
    split_halves_fit::<u32, 1048576>();
}
