use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_lite::future::block_on;
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

fn new_tree() -> Tree<RedbStore<Inline>, impl Fn() -> u128, SingleLeafConfig> {
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

fn open_volume() -> TreeVolume<RedbStore<Inline>, impl Fn() -> u128, SingleLeafConfig> {
    let tree = new_tree();
    block_on(async {
        let id = tree.create().await.unwrap();
        tree.open(id).await.unwrap()
    })
}

#[test]
fn opening_a_volume_from_another_tree_is_rejected() {
    let id = block_on(new_tree().create()).unwrap();
    assert!(matches!(
        block_on(new_tree().open(id)),
        Err(StoreError::Rejected(UnknownVolume))
    ));
}

#[test]
fn a_volume_id_round_trips_through_bytes() {
    let id = block_on(new_tree().create()).unwrap();
    assert_eq!(TreeVolumeId::try_from(Bytes::from(id)).unwrap(), id);
    assert!(matches!(
        TreeVolumeId::try_from(Bytes::from_static(b"not a tree volume id")),
        Err(CorruptValue::WrongLength)
    ));
}

#[test]
fn panics_on_invalid_ranges() {
    stratafs_kv_conformance::panics_on_invalid_ranges(open_volume);
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

proptest! {
    #[test]
    fn sequential_transactions_match_a_model(clients in simulated_client_transactions()) {
        let volume = open_volume();
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
}
