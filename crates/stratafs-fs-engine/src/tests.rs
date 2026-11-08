use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};

use futures_lite::future::block_on;
use stratafs_store::kv::{KeyValueTransaction, ReadKeyValueTransaction};

use super::*;
use crate::attributes::Mode;
use crate::directories::NewItem;
use crate::error::PosixError;
use crate::kernel_cache::NoKernel;
use crate::test_support::{InMemoryStore, TestEngine, new_test_engine_with};

// Scripted lets every write commit at once. Its holds fail while fail_holds says so, its result handling is what
// handling says, and cached is every inode the engine told it the kernel caches.
struct Scripted {
    fail_holds: AtomicBool,
    handling: Mutex<ResultHandling>,
    cached: Mutex<Vec<Inode>>,
}

#[derive(Debug, thiserror::Error)]
#[error("holding failed on purpose")]
struct HoldFailed;

impl From<&HoldFailed> for PosixError {
    fn from(_: &HoldFailed) -> Self {
        PosixError::EIO
    }
}

impl Scripted {
    fn set_handling(&self, handling: ResultHandling) {
        *self.handling.lock().unwrap_or_else(PoisonError::into_inner) = handling;
    }
}

impl Consistency for Scripted {}

impl Cached<InMemoryStore> for Scripted {
    type Failure = HoldFailed;
    type Prepared = ();
    type Ticket = u64;
    type InodeState = ();

    async fn inode_state<T: ReadKeyValueTransaction<Failure = MetadataFailure<InMemoryStore>>>(
        &self,
        _: &T,
        _: &[Inode],
    ) -> Result<(), HoldFailed> {
        Ok(())
    }

    async fn may_commit<T: KeyValueTransaction<Failure = MetadataFailure<InMemoryStore>>>(
        &self,
        _: &Mount<InMemoryStore>,
        _: Option<&()>,
        _: &HashMap<Inode, Change>,
        _: &mut T,
    ) -> Result<bool, HoldFailed> {
        Ok(true)
    }

    async fn prepare(
        &self,
        _: &Mount<InMemoryStore>,
        _: Option<()>,
        _: HashMap<Inode, Change>,
        _: PurgeWaits,
    ) -> Result<(), HoldFailed> {
        Ok(())
    }

    async fn finish(&self, _: &Mount<InMemoryStore>, (): (), _: Option<&HashMap<Inode, Change>>) {}

    fn reading(&self) -> u64 {
        0
    }

    async fn hold(
        &self,
        _: &Mount<InMemoryStore>,
        _: &[Inode],
        _: u64,
    ) -> Result<bool, HoldFailed> {
        match self.fail_holds.load(Ordering::SeqCst) {
            true => Err(HoldFailed),
            false => Ok(true),
        }
    }

    fn result_handling(
        &self,
        _: &Mount<InMemoryStore>,
        _: &[(Inode, Attributes)],
        (): &(),
        _: u64,
        _: u64,
        _: PurgeWaits,
        _: bool,
    ) -> ResultHandling {
        *self.handling.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn result_handling_listed(
        &self,
        _: &Mount<InMemoryStore>,
        _: &[(Inode, Attributes)],
        (): &(),
        _: u64,
    ) -> ResultHandling {
        *self.handling.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn settled(
        &self,
        _: &Mount<InMemoryStore>,
        _: &[(Inode, Attributes)],
        (): &(),
        _: PurgeWaits,
    ) -> Result<(), HoldFailed> {
        Ok(())
    }

    async fn reread(
        &self,
        _: &Mount<InMemoryStore>,
        committed: &[(Inode, Attributes)],
    ) -> Result<(Vec<(Inode, Attributes)>, ()), HoldFailed> {
        Ok((committed.to_vec(), ()))
    }

    fn cached(&self, _: &Mount<InMemoryStore>, inode: Inode, _: &Attributes) {
        self.cached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(inode);
    }

    fn forget(&self, _: &Mount<InMemoryStore>, _: Inode) {}

    fn write_back(&self) -> bool {
        false
    }
}

type ScriptedEngine = TestEngine<Scripted>;

fn scripted_engine(handling: ResultHandling) -> ScriptedEngine {
    let scripted = Scripted {
        fail_holds: AtomicBool::new(false),
        handling: Mutex::new(handling),
        cached: Mutex::default(),
    };
    new_test_engine_with(scripted, NoKernel)
}

fn new_file(engine: &ScriptedEngine) -> Inode {
    let created = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn
                .create(Inode::ROOT, b"file", NewItem::File, |_| {})
                .await;
            result.map(|result| (result, txn))
        })
    }));
    created.unwrap().inode
}

// getattr serves inode's attributes, handing them over as hand does, with the result handling.
fn getattr(
    engine: &ScriptedEngine,
    inode: Inode,
    mut hand: impl FnMut(ResultHandling) -> bool + Send,
) -> ResultHandling {
    let served = block_on(engine.serve(
        PurgeWaits::OfOwnInode,
        false,
        |txn| {
            Box::pin(async move {
                let found = txn.getattr(inode).await;
                found.map(|found| (found, txn))
            })
        },
        |found| found.iter().map(|found| (inode, found.clone())).collect(),
        |_, _, result_handling| {
            let handling = result_handling();
            hand(handling).then_some(handling)
        },
    ));
    served.unwrap()
}

// chmod sets inode's mode, handing the kernel what it committed as hand does, with the result handling.
fn chmod(
    engine: &ScriptedEngine,
    inode: Inode,
    mode: Mode,
    mut hand: impl FnMut(ResultHandling) -> bool + Send,
) -> Result<(ResultHandling, Attributes), impl std::fmt::Debug> {
    block_on(engine.transact_and_serve(
        PurgeWaits::OfOwnInode,
        |mut txn| {
            Box::pin(async move {
                let result = txn.setattr(inode, |settable| settable.mode = mode).await;
                result.map(|result| (result, txn))
            })
        },
        |attributes| vec![(inode, attributes.clone())],
        |_, reads, result_handling| {
            let handling = result_handling();
            hand(handling).then(|| (handling, reads[0].1.clone()))
        },
    ))
}

#[test]
fn a_result_handled_with_no_cache_is_not_taken_as_cached() {
    let engine = scripted_engine(ResultHandling::NoCache);
    let inode = new_file(&engine);
    let cached = || {
        let cached = &engine.consistency().cached;
        cached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    };
    assert_eq!(getattr(&engine, inode, |_| true), ResultHandling::NoCache);
    assert_eq!(cached(), []);
    engine.consistency().set_handling(ResultHandling::Cache);
    assert_eq!(getattr(&engine, inode, |_| true), ResultHandling::Cache);
    assert_eq!(cached(), [inode]);
}

#[test]
fn a_read_an_integration_cannot_reply_uncached_is_taken_again() {
    let engine = scripted_engine(ResultHandling::NoCache);
    let inode = new_file(&engine);
    let mut handed = Vec::new();
    let served = getattr(&engine, inode, |handling| {
        handed.push(handling);
        engine.consistency().set_handling(ResultHandling::Cache);
        handling == ResultHandling::Cache
    });
    assert_eq!(served, ResultHandling::Cache);
    assert_eq!(handed, [ResultHandling::NoCache, ResultHandling::Cache]);
}

#[test]
fn a_change_whose_serving_fails_once_committed_hands_over_what_it_committed_uncached() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    engine
        .consistency()
        .fail_holds
        .store(true, Ordering::SeqCst);
    let (handling, handed) = chmod(&engine, inode, Mode::OWNER_READ, |_| true).unwrap();
    assert_eq!(handling, ResultHandling::NoCache);
    assert_eq!(handed.settable.mode, Mode::OWNER_READ);
}

#[test]
fn a_change_whose_serving_fails_once_committed_fails_when_it_cannot_reply_uncached() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    engine
        .consistency()
        .fail_holds
        .store(true, Ordering::SeqCst);
    let changed = chmod(&engine, inode, Mode::OWNER_READ, |handling| {
        handling == ResultHandling::Cache
    });
    assert!(changed.is_err());
    let stored =
        block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })));
    assert_eq!(stored.unwrap().unwrap().settable.mode, Mode::OWNER_READ);
}
