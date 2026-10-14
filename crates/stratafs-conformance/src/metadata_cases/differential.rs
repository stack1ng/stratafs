use std::fmt::Debug;
use std::pin::pin;

use bytes::Bytes;
use fastrand::Rng;
use futures_util::StreamExt;
use stratafs_store::content::ContentHash;
use stratafs_store::error::StoreError;
use stratafs_store::metadata::{
    FileId, FileInfo, ForkRejection, Key, KeyValue, MetadataStore, Seq, Stored, TransactRejection,
    VolumeId, Written,
};
use stratafs_store::transaction::{
    Assert, Condition, DeleteKey, GetFileInfo, GetHead, GetKey, Op, Outcome, SetKey, Truncate,
    VisibleBlobs, Write, WriteAt,
};
use vec1::Vec1;

use crate::reference::ReferenceMetadataStore;
use crate::support::{Provenance, collect_entries, collect_keys, create, key_bytes, provenance};

const SEEDS: u64 = 64;
const STEPS: usize = 150;
const MAX_VOLUMES: usize = 8;
const COMPARE_EVERY: usize = 15;
const FILES: [&str; 3] = ["f0", "f1", "f2"];
const KEY_ALPHABET: [u8; 3] = [b'a', b'b', 0xff];
const VALUES: [&str; 4] = ["", "x", "y", "xy"];
const PREFIXES: [&[u8]; 8] = [
    b"",
    b"a",
    b"b",
    b"ab",
    &[0xff],
    &[b'a', 0xff],
    b"ba",
    b"aaa",
];

#[derive(Debug, Clone)]
struct VolumePair {
    backend: VolumeId,
    reference: VolumeId,
}

#[derive(Debug, PartialEq, Eq)]
enum ObservedRejection {
    UnknownVolume,
    AssertionFailed { index: usize },
    TooLarge,
}

#[derive(Debug, PartialEq, Eq)]
enum ObservedOutcome {
    Done,
    Written(Written),
    Value(Option<Stored<KeyValue>>),
    FileContent { info: FileInfo, bytes: Provenance },
    FileInfo(FileInfo),
    Head(Seq),
}

#[derive(Debug, PartialEq, Eq)]
enum Observed {
    Applied(Vec<ObservedOutcome>),
    Rejected(ObservedRejection),
}

struct Hints {
    head: u64,
    file_seqs: Vec<Option<Seq>>,
    live: Vec<(Key, Bytes)>,
}

fn observe<E: Debug>(
    operations: &Vec1<Op>,
    result: Result<Vec1<Outcome>, StoreError<TransactRejection, E>>,
    context: &str,
) -> Observed {
    match result {
        Ok(outcomes) => {
            assert_eq!(
                outcomes.len(),
                operations.len(),
                "{context}: execute returned {} outcomes for {} operations",
                outcomes.len(),
                operations.len()
            );
            Observed::Applied(
                operations
                    .iter()
                    .zip(outcomes)
                    .map(|(operation, outcome)| observe_outcome(operation, outcome))
                    .collect(),
            )
        }
        Err(StoreError::Rejected(TransactRejection::UnknownVolume(_))) => {
            Observed::Rejected(ObservedRejection::UnknownVolume)
        }
        Err(StoreError::Rejected(TransactRejection::AssertionFailed { index })) => {
            Observed::Rejected(ObservedRejection::AssertionFailed { index })
        }
        Err(StoreError::Rejected(TransactRejection::TooLarge)) => {
            Observed::Rejected(ObservedRejection::TooLarge)
        }
        Err(StoreError::Failed(error)) => panic!("{context}: execute failed: {error:?}"),
    }
}

fn observe_outcome(operation: &Op, outcome: Outcome) -> ObservedOutcome {
    match outcome {
        Outcome::Done => ObservedOutcome::Done,
        Outcome::Written(written) => ObservedOutcome::Written(written),
        Outcome::Value(value) => ObservedOutcome::Value(value),
        Outcome::FileContent(content) => {
            let (offset, size) = match operation {
                Op::VisibleBlobs(request) => (request.offset, request.size),
                _ => (0, u64::MAX),
            };
            ObservedOutcome::FileContent {
                info: content.info,
                bytes: provenance(&content, offset, size),
            }
        }
        Outcome::FileInfo(info) => ObservedOutcome::FileInfo(info),
        Outcome::Head(seq) => ObservedOutcome::Head(seq),
    }
}

fn observe_fork<T, E: Debug>(
    result: Result<T, StoreError<ForkRejection, E>>,
    context: &str,
) -> Result<T, (Seq, Seq)> {
    match result {
        Ok(volume) => Ok(volume),
        Err(StoreError::Rejected(ForkRejection::BeyondHead { at, head })) => Err((at, head)),
        Err(StoreError::Rejected(ForkRejection::UnknownVolume(_))) => {
            panic!("{context}: fork rejected a known volume as unknown")
        }
        Err(StoreError::Failed(error)) => panic!("{context}: fork failed: {error:?}"),
    }
}

fn random_key(rng: &mut Rng) -> Key {
    let len = rng.usize(1..=3);
    let bytes: Vec<u8> = (0..len)
        .map(|_| KEY_ALPHABET[rng.usize(..KEY_ALPHABET.len())])
        .collect();
    key_bytes(&bytes)
}

fn random_prefix(rng: &mut Rng) -> Key {
    let len = rng.usize(0..=2);
    let bytes: Vec<u8> = (0..len)
        .map(|_| KEY_ALPHABET[rng.usize(..KEY_ALPHABET.len())])
        .collect();
    key_bytes(&bytes)
}

fn random_value(rng: &mut Rng) -> Bytes {
    Bytes::from_static(VALUES[rng.usize(..VALUES.len())].as_bytes())
}

fn random_file_index(rng: &mut Rng) -> usize {
    rng.usize(..FILES.len())
}

fn random_blob(rng: &mut Rng) -> ContentHash {
    ContentHash::of(&[rng.u8(0..4)])
}

fn key_for_lookup(rng: &mut Rng, hints: &Hints) -> Key {
    match (rng.bool(), hints.live.is_empty()) {
        (true, false) => hints.live[rng.usize(..hints.live.len())].0.clone(),
        _ => random_key(rng),
    }
}

fn random_conditions(rng: &mut Rng, hints: &Hints, depth: u32) -> Vec1<Condition> {
    let first = random_condition(rng, hints, depth);
    let mut conditions = Vec1::new(first);
    for _ in 0..rng.usize(0..=2) {
        conditions.push(random_condition(rng, hints, depth));
    }
    conditions
}

fn random_condition(rng: &mut Rng, hints: &Hints, depth: u32) -> Condition {
    let kinds = if depth == 0 { 4 } else { 7 };
    match rng.u32(0..kinds) {
        0 => {
            let (key, live_value) = match (rng.bool(), hints.live.is_empty()) {
                (true, false) => {
                    let (key, value) = &hints.live[rng.usize(..hints.live.len())];
                    (key.clone(), Some(value.clone()))
                }
                _ => (random_key(rng), None),
            };
            let value = match rng.u32(0..10) {
                0..4 => None,
                4..7 => live_value.or_else(|| Some(random_value(rng))),
                _ => Some(random_value(rng)),
            };
            Condition::Equals { key, value }
        }
        1 => Condition::Empty {
            prefix: random_prefix(rng),
        },
        2 => {
            let file_index = random_file_index(rng);
            let seq = match rng.u32(0..4) {
                0 | 1 => hints.file_seqs[file_index],
                2 => None,
                _ => Some(Seq(rng.u64(0..=hints.head + 1))),
            };
            Condition::FileVersion {
                file: file_id(file_index),
                seq,
            }
        }
        3 => Condition::Head(Seq(match rng.u32(0..10) {
            0..7 => hints.head,
            _ => rng.u64(0..=hints.head + 2),
        })),
        4 => Condition::Not(Box::new(random_condition(rng, hints, depth - 1))),
        5 => Condition::All(random_conditions(rng, hints, depth - 1)),
        _ => Condition::Any(random_conditions(rng, hints, depth - 1)),
    }
}

fn file_id(index: usize) -> FileId {
    FileId(FILES[index].to_owned())
}

fn random_operation(rng: &mut Rng, hints: &Hints) -> Op {
    match rng.u32(0..100) {
        0..22 => Op::SetKey(SetKey {
            key: random_key(rng),
            value: random_value(rng),
        }),
        22..32 => Op::DeleteKey(DeleteKey {
            key: key_for_lookup(rng, hints),
        }),
        32..40 => Op::GetKey(GetKey {
            key: key_for_lookup(rng, hints),
        }),
        40..48 => Op::Assert(Assert(random_condition(rng, hints, 2))),
        48..70 => Op::Write(Write {
            file: file_id(random_file_index(rng)),
            at: match rng.u32(0..5) {
                0 => WriteAt::End,
                _ => WriteAt::Offset(rng.u64(0..300)),
            },
            size: match rng.u32(0..10) {
                0 => 0,
                _ => rng.u64(1..=60),
            },
            content: random_blob(rng),
            content_offset: rng.u64(0..200),
        }),
        70..78 => Op::Truncate(Truncate {
            file: file_id(random_file_index(rng)),
            len: rng.u64(0..320),
        }),
        78..88 => Op::VisibleBlobs(VisibleBlobs {
            file: file_id(random_file_index(rng)),
            offset: rng.u64(0..330),
            size: match rng.u32(0..20) {
                0..3 => u64::MAX,
                3..5 => 0,
                _ => rng.u64(1..340),
            },
        }),
        88..94 => Op::GetFileInfo(GetFileInfo {
            file: file_id(random_file_index(rng)),
        }),
        _ => Op::GetHead(GetHead),
    }
}

async fn reference_execute(
    reference: &ReferenceMetadataStore,
    volume: &VolumeId,
    operations: Vec1<Op>,
) -> Vec1<Outcome> {
    match reference.execute(volume, operations).await {
        Ok(outcomes) => outcomes,
        Err(error) => panic!("the reference rejected a read-only transaction: {error:?}"),
    }
}

async fn hints(reference: &ReferenceMetadataStore, volume: &VolumeId) -> Hints {
    let mut operations = Vec1::new(Op::GetHead(GetHead));
    for index in 0..FILES.len() {
        operations.push(Op::GetFileInfo(GetFileInfo {
            file: file_id(index),
        }));
    }
    let outcomes = reference_execute(reference, volume, operations).await;
    let mut head = 0;
    let mut file_seqs = Vec::new();
    for outcome in outcomes {
        match outcome {
            Outcome::Head(seq) => head = seq.0,
            Outcome::FileInfo(info) => file_seqs.push(info.seq),
            other => panic!("the reference answered a read with {other:?}"),
        }
    }
    let live = collect_keys(reference, volume, &key_bytes(b""))
        .await
        .into_iter()
        .map(|stored| (stored.entry.key, stored.entry.value))
        .collect();
    Hints {
        head,
        file_seqs,
        live,
    }
}

async fn compare_volume<S: MetadataStore>(
    store: &S,
    reference: &ReferenceMetadataStore,
    pair: &VolumePair,
    rng: &mut Rng,
    context: &str,
) {
    let expected_entries = collect_entries(reference, &pair.reference, 0).await;
    assert_eq!(
        collect_entries(store, &pair.backend, 0).await,
        expected_entries,
        "{context}: entries of {pair:?} differ"
    );
    let head = expected_entries.len() as u64;
    let after = rng.u64(0..=head + 1);
    assert_eq!(
        collect_entries(store, &pair.backend, after).await,
        collect_entries(reference, &pair.reference, after).await,
        "{context}: entries after {after} of {pair:?} differ"
    );
    for prefix in PREFIXES {
        assert_eq!(
            collect_keys(store, &pair.backend, &key_bytes(prefix)).await,
            collect_keys(reference, &pair.reference, &key_bytes(prefix)).await,
            "{context}: list_keys with prefix {prefix:?} of {pair:?} differs"
        );
    }
    let mut operations = Vec1::new(Op::GetHead(GetHead));
    for index in 0..FILES.len() {
        operations.push(Op::GetFileInfo(GetFileInfo {
            file: file_id(index),
        }));
        operations.push(Op::VisibleBlobs(VisibleBlobs {
            file: file_id(index),
            offset: 0,
            size: u64::MAX,
        }));
        operations.push(Op::VisibleBlobs(VisibleBlobs {
            file: file_id(index),
            offset: rng.u64(0..300),
            size: rng.u64(0..300),
        }));
    }
    let context = format!("{context}: reading {pair:?} with {operations:?}");
    let backend = observe(
        &operations,
        store.execute(&pair.backend, operations.clone()).await,
        &context,
    );
    let expected = observe(
        &operations,
        reference.execute(&pair.reference, operations.clone()).await,
        &context,
    );
    assert_eq!(backend, expected, "{context}");
    if head > 0 {
        let mut watch = pin!(store.watch(pair.backend.clone(), Seq(head - 1)));
        let first = watch.next().await;
        assert!(
            matches!(first, Some(Ok(seq)) if seq == Seq(head)),
            "{context}: watch after {} gave {first:?}",
            head - 1
        );
    }
}

async fn compare_all<S: MetadataStore>(
    store: &S,
    reference: &ReferenceMetadataStore,
    volumes: &[VolumePair],
    rng: &mut Rng,
    context: &str,
) {
    for pair in volumes {
        compare_volume(store, reference, pair, rng, context).await;
    }
}

async fn reference_head(reference: &ReferenceMetadataStore, volume: &VolumeId) -> u64 {
    match reference_execute(reference, volume, Vec1::new(Op::GetHead(GetHead)))
        .await
        .into_vec()
        .as_slice()
    {
        [Outcome::Head(seq)] => seq.0,
        other => panic!("the reference answered GetHead with {other:?}"),
    }
}

async fn run_seed<S: MetadataStore>(store: &S, seed: u64) {
    let reference = ReferenceMetadataStore::new();
    let mut rng = Rng::with_seed(seed);
    let mut volumes = vec![VolumePair {
        backend: create(store).await,
        reference: create(&reference).await,
    }];
    for step in 0..STEPS {
        let context = format!("seed {seed} step {step}");
        let pair = volumes[rng.usize(..volumes.len())].clone();
        match rng.u32(0..100) {
            0..9 if volumes.len() < MAX_VOLUMES => {
                let head = reference_head(&reference, &pair.reference).await;
                let at = match rng.u32(0..4) {
                    0 => head,
                    _ => rng.u64(0..=head),
                };
                let context = format!("{context}: fork {pair:?} at {at}");
                let backend = observe_fork(store.fork(&pair.backend, Seq(at)).await, &context);
                let expected =
                    observe_fork(reference.fork(&pair.reference, Seq(at)).await, &context);
                match (backend, expected) {
                    (Ok(backend), Ok(reference)) => volumes.push(VolumePair { backend, reference }),
                    (backend, expected) => {
                        panic!("{context}: backend gave {backend:?}, reference gave {expected:?}")
                    }
                }
            }
            9..11 if volumes.len() < MAX_VOLUMES => volumes.push(VolumePair {
                backend: create(store).await,
                reference: create(&reference).await,
            }),
            11..14 => {
                let head = reference_head(&reference, &pair.reference).await;
                let at = head + rng.u64(1..=3);
                let context = format!("{context}: fork {pair:?} beyond head at {at}");
                let backend = observe_fork(store.fork(&pair.backend, Seq(at)).await, &context);
                let expected =
                    observe_fork(reference.fork(&pair.reference, Seq(at)).await, &context);
                assert!(
                    matches!((&backend, &expected), (Err(rejected), Err(wanted)) if rejected == wanted),
                    "{context}: backend gave {backend:?}, reference gave {expected:?}"
                );
            }
            _ => {
                let hints = hints(&reference, &pair.reference).await;
                let mut operations = Vec1::new(random_operation(&mut rng, &hints));
                for _ in 1..rng.usize(1..=6) {
                    operations.push(random_operation(&mut rng, &hints));
                }
                let context = format!("{context}: execute {operations:?} on {pair:?}");
                let backend = observe(
                    &operations,
                    store.execute(&pair.backend, operations.clone()).await,
                    &context,
                );
                let expected = observe(
                    &operations,
                    reference.execute(&pair.reference, operations.clone()).await,
                    &context,
                );
                assert_eq!(backend, expected, "{context}");
            }
        }
        if step % COMPARE_EVERY == COMPARE_EVERY - 1 {
            compare_all(store, &reference, &volumes, &mut rng, &context).await;
        }
    }
    let context = format!("seed {seed} end");
    compare_all(store, &reference, &volumes, &mut rng, &context).await;
}

pub async fn differential_against_reference<S: MetadataStore>(store: &S) {
    for seed in 0..SEEDS {
        run_seed(store, seed).await;
    }
}
