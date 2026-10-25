use futures_lite::future::block_on;
use redb::backends::InMemoryBackend;
use stratafs_redb::{Inline, RedbStore};

use super::*;
use crate::node::Node;

struct TestConfig<R, const NODE_CAPACITY: usize, const MERGE_BELOW_PERCENT: u8>(PhantomData<R>);

impl<R: AddressableRange, const NODE_CAPACITY: usize, const MERGE_BELOW_PERCENT: u8> TreeConfig
    for TestConfig<R, NODE_CAPACITY, MERGE_BELOW_PERCENT>
{
    type Range = R;
    const NODE_CAPACITY: usize = NODE_CAPACITY;
    const MERGE_BELOW_PERCENT: u8 = MERGE_BELOW_PERCENT;
}

type Config<R, const NODE_CAPACITY: usize> = TestConfig<R, NODE_CAPACITY, 25>;

fn accepts_config<C: TreeConfig>() {
    drop(Tree::<RedbStore<Inline>, fn() -> u128, C>::new(
        memory_store(),
        || 1,
    ));
}

#[test]
fn each_width_accepts_its_optimal_capacities() {
    accepts_config::<Config<u8, 255>>();
    accepts_config::<Config<u16, 256>>();
    accepts_config::<Config<u16, 65535>>();
    accepts_config::<Config<u32, 65536>>();
}

fn memory_store() -> RedbStore<Inline> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    RedbStore::new(database, Inline).unwrap()
}

fn into_store<S, A, C>(tree: Tree<S, A, C>) -> S {
    Arc::into_inner(tree.0)
        .expect("the test holds the only reference to the tree")
        .store
}

#[test]
fn a_store_accepts_the_config_it_was_first_given() {
    let tree = block_on(Tree::<_, _, TestConfig<u16, 16355, 25>>::new(
        memory_store(),
        || 1,
    ))
    .unwrap();
    block_on(Tree::<_, _, TestConfig<u16, 16355, 25>>::new(
        into_store(tree),
        || 1,
    ))
    .unwrap();
}

#[test]
fn a_store_rejects_a_different_node_capacity() {
    let tree = block_on(Tree::<_, _, TestConfig<u16, 16355, 25>>::new(
        memory_store(),
        || 1,
    ))
    .unwrap();
    let reopened = block_on(Tree::<_, _, TestConfig<u16, 4067, 25>>::new(
        into_store(tree),
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
    let tree = block_on(Tree::<_, _, TestConfig<u16, 65535, 25>>::new(
        memory_store(),
        || 1,
    ))
    .unwrap();
    let reopened = block_on(Tree::<_, _, TestConfig<u32, 65536, 25>>::new(
        into_store(tree),
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
    let tree = block_on(Tree::<_, _, TestConfig<u16, 16355, 25>>::new(
        memory_store(),
        || 1,
    ))
    .unwrap();
    let reopened = block_on(Tree::<_, _, TestConfig<u16, 16355, 30>>::new(
        into_store(tree),
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
            let set = transaction
                .set(TreeKey::Config.into(), Bytes::from_static(b"not a config"))
                .await;
            set.map(|()| ((), transaction))
        })
    }))
    .unwrap();
    let tree = block_on(Tree::<_, _, TestConfig<u16, 16355, 25>>::new(store, || 1));
    assert!(matches!(
        tree,
        Err(TransactError::Aborted(PinnedConfigError::Corrupt(
            CorruptValue::WrongFingerprint
        )))
    ));
}

#[test]
fn create_writes_a_header_and_an_empty_root() {
    let tree = block_on(Tree::<_, _, TestConfig<u16, 16355, 25>>::new(
        memory_store(),
        || 7,
    ))
    .unwrap();
    let volume = block_on(tree.create()).unwrap();
    assert_eq!(volume, TreeVolumeId(7));
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

fn split_halves_fit<C: TreeConfig>() {
    let address = size_of::<<C::Range as AddressableRange>::Stored>();
    let per_node = size_of::<NodeHeader>() + address;
    let largest = C::LARGEST_ENTRY_SIZE + address;
    let overfull = C::NODE_CAPACITY - per_node + largest;
    assert!(overfull.div_ceil(2) + largest <= C::NODE_CAPACITY - per_node);
}

#[test]
fn the_largest_entry_leaves_room_for_both_halves_of_a_split() {
    assert_eq!(Config::<u16, 4067>::LARGEST_ENTRY_SIZE, 1350);
    split_halves_fit::<Config<u8, 255>>();
    split_halves_fit::<Config<u16, 256>>();
    split_halves_fit::<Config<u16, 4067>>();
    split_halves_fit::<Config<u16, 16355>>();
    split_halves_fit::<Config<u32, 65536>>();
    split_halves_fit::<Config<u32, 1048576>>();
}

#[test]
fn the_merge_threshold_is_its_percentage_of_the_node_capacity() {
    assert_eq!(TestConfig::<u16, 16355, 25>::MERGE_BELOW_SIZE, 4088);
    assert_eq!(
        TestConfig::<u32, 4294967295, 49>::MERGE_BELOW_SIZE,
        2104533974
    );
}
