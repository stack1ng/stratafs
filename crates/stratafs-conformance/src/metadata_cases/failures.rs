use std::fmt::Debug;

use futures_util::StreamExt;
use stratafs_store::error::StoreError;
use stratafs_store::metadata::{Entry, MetadataStore, Seq, Stored, UnknownVolume, VolumeId};

use crate::support::{create, key, numbered, set, set_entry};

const ITEMS_TAKEN: usize = 5;
const ENTRIES_WRITTEN: u64 = 6;
const UNREADABLE_SEQ: u64 = 4;

async fn volume_with_keys<S: MetadataStore>(store: &S) -> VolumeId {
    let volume = create(store).await;
    for index in 1..=ENTRIES_WRITTEN {
        set(store, &volume, &format!("k{index}"), "v").await;
    }
    volume
}

fn written_entries() -> Vec<Stored<Entry>> {
    numbered(
        1,
        (1..=ENTRIES_WRITTEN)
            .map(|index| set_entry(&format!("k{index}"), "v"))
            .collect(),
    )
}

async fn unreadable_volume<L, S, Open, Break, Broken>(
    location: &L,
    open: &Open,
    make_unreadable: Break,
) -> (S, VolumeId)
where
    S: MetadataStore,
    Open: Fn(&L) -> S,
    Break: Fn(&L, &VolumeId) -> Broken,
    Broken: Future<Output = ()>,
{
    let volume = volume_with_keys(&open(location)).await;
    make_unreadable(location, &volume).await;
    (open(location), volume)
}

fn assert_single_failure<T: Debug, E: Debug>(
    stream: &str,
    items: &[Result<T, StoreError<UnknownVolume, E>>],
    expected: impl Fn(&E) -> bool,
) {
    match items {
        [Err(StoreError::Failed(error))] => assert!(
            expected(error),
            "{stream} on an unreadable volume failed with an unexpected error: {error:?}"
        ),
        _ => panic!(
            "{stream} on an unreadable volume gave {items:?} where exactly one failure and then the end of the stream were expected"
        ),
    }
}

pub async fn entries_end_after_failing_to_read_the_volume<L, S, Open, Break, Broken, Expected>(
    location: &L,
    open: Open,
    make_unreadable: Break,
    expected: Expected,
) where
    S: MetadataStore,
    Open: Fn(&L) -> S,
    Break: Fn(&L, &VolumeId) -> Broken,
    Broken: Future<Output = ()>,
    Expected: Fn(&S::Error) -> bool,
{
    let (store, volume) = unreadable_volume(location, &open, make_unreadable).await;
    let items: Vec<_> = store
        .entries(volume, Seq(0))
        .take(ITEMS_TAKEN)
        .collect()
        .await;
    assert_single_failure("entries", &items, expected);
}

pub async fn list_keys_ends_after_failing_to_read_the_volume<L, S, Open, Break, Broken, Expected>(
    location: &L,
    open: Open,
    make_unreadable: Break,
    expected: Expected,
) where
    S: MetadataStore,
    Open: Fn(&L) -> S,
    Break: Fn(&L, &VolumeId) -> Broken,
    Broken: Future<Output = ()>,
    Expected: Fn(&S::Error) -> bool,
{
    let (store, volume) = unreadable_volume(location, &open, make_unreadable).await;
    let items: Vec<_> = store
        .list_keys(volume, key(""))
        .take(ITEMS_TAKEN)
        .collect()
        .await;
    assert_single_failure("list_keys", &items, expected);
}

pub async fn watch_ends_after_failing_to_read_the_volume<L, S, Open, Break, Broken, Expected>(
    location: &L,
    open: Open,
    make_unreadable: Break,
    expected: Expected,
) where
    S: MetadataStore,
    Open: Fn(&L) -> S,
    Break: Fn(&L, &VolumeId) -> Broken,
    Broken: Future<Output = ()>,
    Expected: Fn(&S::Error) -> bool,
{
    let (store, volume) = unreadable_volume(location, &open, make_unreadable).await;
    let items: Vec<_> = store
        .watch(volume, Seq(0))
        .take(ITEMS_TAKEN)
        .collect()
        .await;
    assert_single_failure("watch", &items, expected);
}

pub async fn entries_end_after_failing_to_read_an_entry<L, S, Open, Break, Broken, Expected>(
    location: &L,
    open: Open,
    make_unreadable: Break,
    expected: Expected,
) where
    S: MetadataStore,
    Open: Fn(&L) -> S,
    Break: Fn(&L, &VolumeId, Seq) -> Broken,
    Broken: Future<Output = ()>,
    Expected: Fn(&S::Error) -> bool,
{
    let volume = volume_with_keys(&open(location)).await;
    make_unreadable(location, &volume, Seq(UNREADABLE_SEQ)).await;
    let store = open(location);
    let items: Vec<_> = store
        .entries(volume.clone(), Seq(0))
        .take(ITEMS_TAKEN + usize::try_from(ENTRIES_WRITTEN).unwrap_or(usize::MAX))
        .collect()
        .await;
    let readable = written_entries();
    let failure = items.iter().position(Result::is_err);
    let yielded: Vec<_> = items
        .iter()
        .take(failure.unwrap_or(items.len()))
        .filter_map(|item| item.as_ref().ok().cloned())
        .collect();
    assert!(
        yielded.len() < usize::try_from(UNREADABLE_SEQ).unwrap_or(usize::MAX)
            && yielded[..] == readable[..yielded.len()],
        "entries({volume:?}, 0) with seq {UNREADABLE_SEQ} unreadable yielded {yielded:?} before failing"
    );
    assert_single_failure("entries", &items[yielded.len()..], expected);
}
