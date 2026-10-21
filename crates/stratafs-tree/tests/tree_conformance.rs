use std::collections::BTreeMap;
use std::convert::Infallible;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_lite::future::block_on;
use proptest::collection::vec;
use proptest::proptest;
use redb::backends::InMemoryBackend;
use stratafs_kv_conformance::activity::{
    SimulatedOperation, SimulatedRead, SimulatedTransaction, simulated_client_transactions,
};
use stratafs_redb::{Inline, RedbStore};
use stratafs_store::error::StoreError;
use stratafs_store::forkable::{ForkableKeyValueStore, UnknownVolume};
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, TransactError,
};
use stratafs_tree::header::TreeVolumeId;
use stratafs_tree::serialized::CorruptValue;
use stratafs_tree::tree::{Tree, TreeConfig};
use stratafs_tree::volume::TreeVolume;

struct SingleLeafConfig;

impl TreeConfig for SingleLeafConfig {
    type Range = u16;
    const NODE_CAPACITY: usize = 4096;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

struct SmallNodeConfig;

impl TreeConfig for SmallNodeConfig {
    type Range = u8;
    const NODE_CAPACITY: usize = 128;
    const MERGE_BELOW_PERCENT: u8 = 25;
}

fn new_tree<C: TreeConfig>() -> Tree<RedbStore<Inline>, impl Fn() -> u128, C> {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let next_id = AtomicU64::new(1);
    let allocate_id = move || u128::from(next_id.fetch_add(1, Ordering::Relaxed));
    block_on(Tree::new(
        RedbStore::new(database, Inline).unwrap(),
        allocate_id,
    ))
    .unwrap()
}

fn open_volume<C: TreeConfig>() -> TreeVolume<RedbStore<Inline>, impl Fn() -> u128, C> {
    let tree = new_tree::<C>();
    block_on(async {
        let id = tree.create().await.unwrap();
        tree.open(id).await.unwrap()
    })
}

#[test]
fn opening_a_volume_from_another_tree_is_rejected() {
    let id = block_on(new_tree::<SingleLeafConfig>().create()).unwrap();
    assert!(matches!(
        block_on(new_tree::<SingleLeafConfig>().open(id)),
        Err(StoreError::Rejected(UnknownVolume))
    ));
}

#[test]
fn a_volume_id_round_trips_through_bytes() {
    let id = block_on(new_tree::<SingleLeafConfig>().create()).unwrap();
    assert_eq!(TreeVolumeId::try_from(Bytes::from(id)).unwrap(), id);
    assert!(matches!(
        TreeVolumeId::try_from(Bytes::from_static(b"not a tree volume id")),
        Err(CorruptValue::WrongLength)
    ));
}

#[test]
fn panics_on_invalid_ranges() {
    stratafs_kv_conformance::panics_on_invalid_ranges(open_volume::<SingleLeafConfig>);
}

type Model = BTreeMap<Bytes, Bytes>;

#[derive(Debug)]
struct SimulatedAbort;

async fn expect_reads<T: ReadKeyValueTransaction>(
    transaction: &T,
    model: &Model,
    reads: &[SimulatedRead],
) {
    for read in reads {
        match read {
            SimulatedRead::Get(key) => assert_eq!(
                transaction.get(key.clone()).await.unwrap(),
                model.get(key).cloned(),
                "get {key:?}"
            ),
            SimulatedRead::GetRange(start, end, limit) => {
                let expected: Vec<(Bytes, Bytes)> = model
                    .range((start.clone(), end.clone()))
                    .take(*limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                assert_eq!(
                    transaction
                        .get_range((start.clone(), end.clone()), *limit)
                        .await
                        .unwrap(),
                    expected,
                    "get_range {start:?} {end:?} {limit}"
                );
            }
            SimulatedRead::Observe(_) => {}
        }
    }
}

async fn run_read<S: KeyValueStore>(store: &S, model: &Model, groups: Vec<Vec<SimulatedRead>>) {
    let model = model.clone();
    store
        .read_transact::<_, Infallible>(move |transaction| {
            let (groups, model) = (groups.clone(), model.clone());
            Box::pin(async move {
                for reads in &groups {
                    expect_reads(&*transaction, &model, reads).await;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn run_write<S: KeyValueStore>(
    store: &S,
    model: &mut Model,
    operations: Vec<SimulatedOperation>,
) {
    let start = model.clone();
    let committed = store
        .transact::<_, SimulatedAbort>(move |mut transaction| {
            let (operations, mut working) = (operations.clone(), start.clone());
            Box::pin(async move {
                for operation in operations {
                    match operation {
                        SimulatedOperation::Read(reads) => {
                            expect_reads(&*transaction, &working, &reads).await;
                        }
                        SimulatedOperation::Set(key, value) => {
                            transaction.set(key.clone(), value.clone()).await.unwrap();
                            working.insert(key, value);
                        }
                        SimulatedOperation::Clear(key) => {
                            transaction.clear(key.clone()).await.unwrap();
                            working.remove(&key);
                        }
                        SimulatedOperation::Abort => return Err(SimulatedAbort),
                        SimulatedOperation::Panic => {}
                    }
                }
                Ok(working)
            })
        })
        .await;
    match committed {
        Ok(working) => *model = working,
        Err(TransactError::Aborted(SimulatedAbort)) => {}
        Err(error) => panic!("the transaction failed: {error:?}"),
    }
}

fn sequential_transactions_match_a_model<C: TreeConfig>(clients: Vec<Vec<SimulatedTransaction>>) {
    let volume = open_volume::<C>();
    let mut model = Model::new();
    for transaction in clients.into_iter().flatten() {
        match transaction {
            SimulatedTransaction::Read(groups) => block_on(run_read(&volume, &model, groups)),
            SimulatedTransaction::Write(operations) => {
                block_on(run_write(&volume, &mut model, operations));
            }
        }
    }
}

fn filling_then_emptying_matches_a_model<C: TreeConfig>(sets: Vec<usize>, clears: Vec<usize>) {
    let volume = open_volume::<C>();
    let mut model = Model::new();
    let key = |index: usize| Bytes::from(format!("key/{index:03}"));
    let operations: Vec<SimulatedOperation> = sets
        .into_iter()
        .map(|index| SimulatedOperation::Set(key(index), Bytes::from_static(b"value")))
        .chain(
            clears
                .into_iter()
                .map(|index| SimulatedOperation::Clear(key(index))),
        )
        .collect();
    for batch in operations.chunks(32) {
        let mut batch = batch.to_vec();
        batch.push(SimulatedOperation::Read(vec![SimulatedRead::GetRange(
            Bound::Unbounded,
            Bound::Unbounded,
            usize::MAX,
        )]));
        block_on(run_write(&volume, &mut model, batch));
    }
}

proptest! {
    #[test]
    fn filling_then_emptying_matches_a_model_across_small_nodes(
        sets in vec(0..400usize, 0..400),
        clears in vec(0..400usize, 0..400),
    ) {
        filling_then_emptying_matches_a_model::<SmallNodeConfig>(sets, clears);
    }

    #[test]
    fn sequential_transactions_match_a_model_in_a_single_leaf(clients in simulated_client_transactions()) {
        sequential_transactions_match_a_model::<SingleLeafConfig>(clients);
    }

    #[test]
    fn sequential_transactions_match_a_model_across_small_nodes(clients in simulated_client_transactions()) {
        sequential_transactions_match_a_model::<SmallNodeConfig>(clients);
    }
}
