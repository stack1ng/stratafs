use std::cell::Cell;
use std::fmt::Debug;

use futures_lite::future::block_on;
use opendal::Operator;
use redb::backends::InMemoryBackend;
use stratafs_opendal::OpendalContentStore;
use stratafs_redb::{Inline, RedbStore};
use stratafs_store::kv::TransactError;

use crate::{FsConfig, FsEngine, Timestamp};

pub(crate) struct OneKibInline;

impl FsConfig for OneKibInline {
    const LARGEST_INLINE_ENTRY: usize = 1024;
}

pub(crate) type InMemoryEngine =
    FsEngine<RedbStore<Inline>, OpendalContentStore, fn() -> u64, fn() -> Timestamp, OneKibInline>;

thread_local! {
    static CLOCK: Cell<i64> = const { Cell::new(0) };
}

pub(crate) fn set_clock(at: i64) {
    CLOCK.set(at);
}

fn clock() -> Timestamp {
    Timestamp(CLOCK.get())
}

pub(crate) fn new_test_engine() -> InMemoryEngine {
    let database = redb::Database::builder()
        .create_with_backend(InMemoryBackend::new())
        .unwrap();
    let allocate_inode: fn() -> u64 = || 0;
    let now: fn() -> Timestamp = clock;
    block_on(FsEngine::new(
        RedbStore::new(database, Inline).unwrap(),
        OpendalContentStore::new(Operator::new(opendal::services::Memory::default()).unwrap()),
        allocate_inode,
        now,
    ))
    .unwrap()
}

pub(crate) fn aborted<R: Debug, F: Debug, E>(error: TransactError<R, F, E>) -> E {
    match error {
        TransactError::Aborted(error) => error,
        TransactError::Rejected(rejection) => {
            panic!("the store refused the transaction: {rejection:?}")
        }
        TransactError::Failed(failure) => panic!("the transaction failed: {failure:?}"),
    }
}
