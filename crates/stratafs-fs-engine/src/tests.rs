use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use futures_lite::future::block_on;
use stratafs_pubsub::PubSubError;
use stratafs_pubsub_conformance::MemoryClient;

use super::*;
use crate::attributes::Mode;
use crate::directories::NewItem;
use crate::kernel_cache::NoKernel;
use crate::test_support::{InMemoryStore, TestEngine, new_test_engine_with};

// Scripted's holds fail once holds_left of them succeeded, its result handling is what handling says, and cached is
// every inode the engine told it the kernel caches.
struct Scripted {
    holds_left: AtomicUsize,
    handling: Mutex<ResultHandling>,
    cached: Mutex<Vec<Inode>>,
}

impl Scripted {
    fn set_handling(&self, handling: ResultHandling) {
        *self.handling.lock().unwrap_or_else(PoisonError::into_inner) = handling;
    }
}

impl Consistency for Scripted {}

type Memory = MemoryClient<u64>;

impl Cached<InMemoryStore, Memory> for Scripted {
    type Ticket = u64;

    fn reading(&self, _: Mount<'_, InMemoryStore, Memory>) -> u64 {
        0
    }

    async fn before_caching(
        &self,
        _: Mount<'_, InMemoryStore, Memory>,
        _: &[Inode],
        _: u64,
    ) -> Result<bool, Failure<Memory, InMemoryStore>> {
        let held = self
            .holds_left
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            });
        match held {
            Ok(_) => Ok(true),
            Err(_) => Err(ConsistencyFailure::Hold(PubSubError::TimedOut)),
        }
    }

    fn result_handling(
        &self,
        _: Mount<'_, InMemoryStore, Memory>,
        _: &[(Inode, Attributes)],
        _: &Pending,
        _: u64,
        _: u64,
        _: PurgeWaits,
        _: bool,
    ) -> ResultHandling {
        *self.handling.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn result_handling_listed(
        &self,
        _: Mount<'_, InMemoryStore, Memory>,
        _: &[(Inode, Attributes)],
        _: &Pending,
        _: u64,
    ) -> ResultHandling {
        *self.handling.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn settled(
        &self,
        _: Mount<'_, InMemoryStore, Memory>,
        _: &[(Inode, Attributes)],
        _: &Pending,
        _: u64,
        _: PurgeWaits,
    ) -> Result<(), Failure<Memory, InMemoryStore>> {
        Ok(())
    }

    fn cached(&self, _: Mount<'_, InMemoryStore, Memory>, inode: Inode, _: &Attributes) {
        self.cached
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(inode);
    }

    fn forget(&self, _: Mount<'_, InMemoryStore, Memory>, _: Inode) {}

    fn write_back(&self) -> bool {
        false
    }

    fn holds(&self) -> bool {
        false
    }
}

type ScriptedEngine = TestEngine<Scripted>;

fn scripted_engine(handling: ResultHandling) -> ScriptedEngine {
    let scripted = Scripted {
        holds_left: AtomicUsize::new(usize::MAX),
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
    engine.consistency().holds_left.store(0, Ordering::SeqCst);
    let (handling, handed) = chmod(&engine, inode, Mode::OWNER_READ, |_| true).unwrap();
    assert_eq!(handling, ResultHandling::NoCache);
    assert_eq!(handed.settable.mode, Mode::OWNER_READ);
}

#[test]
fn a_change_whose_serving_fails_once_committed_fails_when_it_cannot_reply_uncached() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    engine.consistency().holds_left.store(0, Ordering::SeqCst);
    let changed = chmod(&engine, inode, Mode::OWNER_READ, |handling| {
        handling == ResultHandling::Cache
    });
    assert!(changed.is_err());
    let stored =
        block_on(engine.read_transact(|txn| Box::pin(async move { txn.getattr(inode).await })));
    assert_eq!(stored.unwrap().unwrap().settable.mode, Mode::OWNER_READ);
}

// write_spilling writes content, long enough to spill, at the start of inode, and from its second attempt on writes also
// to each of others, which the attempt before did not announce. It returns how many attempts ran.
fn write_spilling(
    engine: &ScriptedEngine,
    inode: Inode,
    others: &[Inode],
    content: impl Fn() -> Vec<u8> + Sync,
) -> usize {
    let attempts = AtomicUsize::new(0);
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
        let content = bytes::Bytes::from(content());
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        let others = if attempt == 0 { &[][..] } else { others };
        Box::pin(async move {
            txn.write(inode, 0, content).await.unwrap();
            for other in others {
                txn.write(*other, 0, bytes::Bytes::from_static(b"x"))
                    .await
                    .unwrap();
            }
            Ok::<_, std::convert::Infallible>(((), txn))
        })
    }))
    .unwrap();
    attempts.into_inner()
}

// new_named_file makes a file named name in the root.
fn new_named_file(engine: &ScriptedEngine, name: &'static [u8]) -> Inode {
    let created = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            let result = txn.create(Inode::ROOT, name, NewItem::File, |_| {}).await;
            result.map(|result| (result, txn))
        })
    }));
    created.unwrap().inode
}

#[test]
fn a_write_refused_once_puts_its_blob_once() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    // A write to a file that exists commits only once it announced the file's next version, so it runs twice.
    assert_eq!(write_spilling(&engine, inode, &[], || vec![7; 2000]), 2);
    // the second attempt took the first one's blob, so no second id was allocated
    assert_eq!(
        engine.content().store().ids(),
        BTreeSet::from([ContentId(1)])
    );
}

#[test]
fn a_write_refused_twice_puts_its_blob_once() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    let other = new_named_file(&engine, b"other");
    // The second attempt also changes a file the first did not, which it announces before a third attempt commits.
    assert_eq!(
        write_spilling(&engine, inode, &[other], || vec![7; 2000]),
        3
    );
    assert_eq!(
        engine.content().store().ids(),
        BTreeSet::from([ContentId(1)])
    );
}

#[test]
fn an_attempt_that_spills_other_content_than_the_one_before_keeps_only_its_own_blob() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    let attempts = AtomicUsize::new(0);
    let ran = write_spilling(&engine, inode, &[], || {
        vec![attempts.fetch_add(1, Ordering::SeqCst) as u8; 2000]
    });
    assert_eq!(ran, 2);
    assert_eq!(
        engine.content().store().ids(),
        BTreeSet::from([ContentId(2)])
    );
}

#[test]
fn a_transaction_that_gives_up_after_spilling_keeps_no_blob() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    let given_up = block_on(engine.transact(PurgeWaits::No, |mut txn| {
        Box::pin(async move {
            txn.write(inode, 0, bytes::Bytes::from(vec![7; 2000]))
                .await
                .unwrap();
            Err::<((), _), _>("the work gave up after spilling")
        })
    }));
    assert!(given_up.is_err());
    assert!(engine.content().store().ids().is_empty());
}

#[test]
fn the_same_content_spilled_twice_by_one_attempt_is_two_blobs() {
    let engine = scripted_engine(ResultHandling::Cache);
    let inode = new_file(&engine);
    let attempts = AtomicUsize::new(0);
    block_on(engine.transact(PurgeWaits::No, |mut txn| {
        attempts.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            let content = bytes::Bytes::from(vec![7; 2000]);
            txn.write(inode, 0, content.clone()).await.unwrap();
            let result = txn.write(inode, 2000, content).await;
            result.map(|_| ((), txn))
        })
    }))
    .unwrap();
    assert_eq!(attempts.into_inner(), 2);
    // each attempt references two blobs, and the second attempt took both of the first one's
    assert_eq!(
        engine.content().store().ids(),
        BTreeSet::from([ContentId(1), ContentId(2)])
    );
}
