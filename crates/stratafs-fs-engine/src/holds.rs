// A mount's connection to the volume's pub/sub, and the inodes it holds through it. A publish to a held inode is a
// delivery, which drops the kernel's copy and then acknowledges. A mount whose connection fails drops the kernel's copy
// of every inode it held.

use std::collections::HashMap;
use std::convert::Infallible;
use std::error::Error;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::{Duration, Instant};

use event_listener::Event;
use futures_core::future::BoxFuture;
use futures_util::future::{AbortHandle, Abortable, join_all};
use stratafs_pubsub::{Key, PubSub, PubSubError};
use stratafs_runtime::Runtime;
use stratafs_store::kv::TransactError;
use stratafs_store::store::Store;

use crate::error::{IoFailure, PosixError};
use crate::kernel_cache::{Answered, Asked, KernelCache, Uninvalidated};
use crate::writes::StoreWork;
use crate::{Inode, MetadataFailure, MetadataRejection, Mount, MountId, PurgeWaits};

// CONNECT_TIMEOUT is how long connecting to the volume's pub/sub may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

// PURGE_RETRY is how soon a purge tries again to drop a copy the kernel would not, doubling up to PURGE_RETRY_MOST.
const PURGE_RETRY: Duration = Duration::from_millis(10);
const PURGE_RETRY_MOST: Duration = Duration::from_secs(1);

// LATE_READS_WAIT is how long after a round asks the kernel a read begun since waits for the round to end instead of
// making it invalidate again.
pub(crate) const LATE_READS_WAIT: Duration = Duration::from_millis(3);

// ConnectFailure is why connecting to the volume's pub/sub failed.
pub type ConnectFailure = Box<dyn Error + Send + Sync>;

// ConsistencyFailure is why the engine could not let a write or a reply go ahead with every mount's kernel cache in
// line with the store.
#[derive(Debug, thiserror::Error)]
pub enum ConsistencyFailure<F, R, M> {
    #[error("connecting to the volume's pub/sub failed")]
    Connect(#[source] Arc<dyn Error + Send + Sync>),
    #[error("connecting to the volume's pub/sub took longer than its time limit")]
    ConnectTimedOut,
    #[error("holding an inode failed")]
    Hold(#[source] PubSubError<F>),
    #[error("publishing a write failed")]
    Publish(#[source] PubSubError<F>),
    #[error("announcing or settling a write in the store failed")]
    Store(#[source] TransactError<R, M, StoreWork<R, M>>),
    #[error("reading or clearing pending change versions in the engine's transaction failed")]
    Pending(#[source] IoFailure<M, Infallible>),
    #[error("another write changed what a write committed before its reply could hand it over")]
    Overtaken,
    #[error("the mount is dropping the kernel's cache after its pub/sub connection failed")]
    Purging,
    #[error("dropping the kernel's copy of the attributes a write changes failed")]
    Uninvalidated(#[source] Uninvalidated),
    #[error(
        "another write is changing an inode this write changes, and cannot end before this mount drops a copy, which \
         the kernel may hold back while this write is in flight"
    )]
    Contended,
    #[error("watching the store for the end of a write in flight failed")]
    Watch(#[source] M),
}

// A write another write in flight holds back is busy; every other failure is an I/O error.
impl<F, R, M> From<&ConsistencyFailure<F, R, M>> for PosixError {
    fn from(failure: &ConsistencyFailure<F, R, M>) -> Self {
        match failure {
            ConsistencyFailure::Contended => PosixError::EBUSY,
            ConsistencyFailure::Connect(_)
            | ConsistencyFailure::ConnectTimedOut
            | ConsistencyFailure::Hold(_)
            | ConsistencyFailure::Publish(_)
            | ConsistencyFailure::Store(_)
            | ConsistencyFailure::Pending(_)
            | ConsistencyFailure::Overtaken
            | ConsistencyFailure::Purging
            | ConsistencyFailure::Uninvalidated(_)
            | ConsistencyFailure::Watch(_) => PosixError::EIO,
        }
    }
}

// Failure is why a mount of a store St, reaching the volume's pub/sub through P, could not let a write or a reply go
// ahead.
pub type Failure<P, St> =
    ConsistencyFailure<<P as PubSub>::Failure, MetadataRejection<St>, MetadataFailure<St>>;

pub(crate) struct State<P: PubSub> {
    inner: Mutex<Inner<P>>,
    // watching stops the task watching the connected client for failure.
    watching: Mutex<Option<AbortHandle>>,
    // changed wakes reads waiting on a hold, a delivery or a connection.
    pub(crate) changed: Event,
    // clock numbers holds as they start and reads as they begin, so a read can tell a hold made after it began.
    pub(crate) clock: AtomicU64,
    // woken is the clock at the latest wake, so a read can tell the state changed after it began.
    pub(crate) woken: AtomicU64,
}

pub(crate) struct Inner<P: PubSub> {
    pub(crate) connection: Connection<P>,
    pub(crate) holds: HashMap<Inode, Holding<P>>,
    // seen is when this mount first waited on each write in flight by another mount, by inode and announcement.
    pub(crate) seen: HashMap<(Inode, MountId, u64), Instant>,
    // claims is the write of this mount announcing each inode, until it finishes.
    pub(crate) claims: HashMap<Inode, u64>,
    // delivering is each write of this mount whose own delivery runs, and whether a write its purge may wait on asked
    // it to yield.
    pub(crate) delivering: HashMap<u64, bool>,
    // publishing counts this mount's writes a purge of any inode may wait for publishing now.
    pub(crate) publishing: usize,
    // generations counts the clients connected so far.
    generations: u64,
    // connects counts the connects begun so far, and unconnected is the last that failed, by its count.
    connects: u64,
    unconnected: Option<(u64, Unconnected)>,
}

// Unconnected is why a connect failed, which every operation that waited on it fails with too.
#[derive(Clone)]
enum Unconnected {
    Failed(Arc<dyn Error + Send + Sync>),
    TimedOut,
}

impl<F, R, M> From<Unconnected> for ConsistencyFailure<F, R, M> {
    fn from(unconnected: Unconnected) -> Self {
        match unconnected {
            Unconnected::Failed(failure) => ConsistencyFailure::Connect(failure),
            Unconnected::TimedOut => ConsistencyFailure::ConnectTimedOut,
        }
    }
}

pub(crate) enum Connection<P> {
    // Down has no client, and one may be connecting. A mount purging is dropping the kernel's copies after its
    // connection failed, and connects again only once it is done.
    Down { connecting: bool, purging: bool },
    Up { client: Arc<P>, generation: u64 },
}

pub(crate) enum Holding<P: PubSub> {
    // Subscribing is an inode being held by one attempt: lost when the kernel may still have an item for it, and
    // forgotten when the kernel let go of it since.
    Subscribing {
        attempt: u64,
        lost: bool,
        forgotten: bool,
    },
    Held(Hold<P>),
    // Lost is an inode held through a client whose connection failed. The kernel may still hold its item, so a listing
    // hands over its attributes only once it is held again.
    Lost,
}

// Hold is a held inode: later publishes to it reach this mount.
pub(crate) struct Hold<P: PubSub> {
    // subscription holds the inode until it is dropped.
    _subscription: P::Subscription,
    // generation is the client the hold was made through.
    generation: u64,
    // since is the clock when the hold began, which also tells it apart from any other hold of the inode.
    pub(crate) since: u64,
    // replying counts this mount's writes that committed changes to the inode and have not replied yet.
    pub(crate) replying: u32,
    // confirmed is the highest version whose delivery this mount acknowledged, which a write may have committed.
    pub(crate) confirmed: u64,
    // pending counts deliveries still being handled, and pending_highest is the highest version among them.
    pub(crate) pending: u32,
    pub(crate) pending_highest: u64,
    // replies counts replies handed to the kernel cached with data older than a pending delivery, so a delivery
    // invalidates again after any that happened while it invalidated.
    pub(crate) replies: u64,
    // asked counts the deliveries invalidating now that have asked the kernel to drop its copy, and round and asked_at
    // are when the first of them asked, by the clock and in time. again counts the deliveries invalidating again.
    pub(crate) asked: u32,
    pub(crate) round: u64,
    pub(crate) asked_at: Option<Instant>,
    pub(crate) again: u32,
}

impl<P: PubSub> Inner<P> {
    fn generation(&self) -> Option<u64> {
        match &self.connection {
            Connection::Up { generation, .. } => Some(*generation),
            Connection::Down { .. } => None,
        }
    }

    pub(crate) fn held(&mut self, inode: Inode) -> Option<&mut Hold<P>> {
        let generation = self.generation()?;
        match self.holds.get_mut(&inode) {
            Some(Holding::Held(hold)) if hold.generation == generation => Some(hold),
            _ => None,
        }
    }
}

impl<P: PubSub> State<P> {
    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner<P>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn wake(&self) {
        let now = self.clock.fetch_add(1, Ordering::SeqCst);
        self.woken.fetch_max(now, Ordering::SeqCst);
        self.changed.notify(usize::MAX);
    }
}

// Next is what hold does next for an inode: nothing more when it is held, wait for a hold another read is making,
// or make the hold, as the attempt it numbers.
enum Next<L> {
    Held(bool),
    Wait(L),
    Subscribe(u64),
}

// handling_delivery says whether a delivery of any inode is being handled here.
pub(crate) fn handling_delivery<P: PubSub>(inner: &Inner<P>) -> bool {
    let generation = inner.generation();
    inner.holds.values().any(|holding| {
        matches!(holding, Holding::Held(hold) if Some(hold.generation) == generation && hold.pending > 0)
    })
}

// no_room says whether an operation is one of the requests the kernel keeps a limited number of in flight, with no
// room for another, which a purge may need. It fails rather than wait while a delivery is handled here, so a purge
// always gets one.
pub(crate) fn no_room(purge_waits: PurgeWaits, kernel: &dyn KernelCache) -> bool {
    purge_waits == PurgeWaits::OfAnyInode && !kernel.in_flight_room()
}

impl<St, P: PubSub> Mount<St, P> {
    // new is the mount id of store, whose inodes kernel caches and whose tasks run on runtime, of a volume whose
    // pub/sub connect reaches and whose publishes wait timeout for holders.
    pub(crate) fn new(
        store: Arc<St>,
        kernel: Arc<dyn KernelCache>,
        runtime: Arc<dyn Runtime>,
        id: MountId,
        connect: impl Fn() -> BoxFuture<'static, Result<P, ConnectFailure>> + Send + Sync + 'static,
        timeout: Duration,
    ) -> Self {
        Mount {
            store,
            kernel,
            runtime,
            id,
            connect: Box::new(connect),
            timeout,
            state: Arc::new(State {
                inner: Mutex::new(Inner {
                    connection: Connection::Down {
                        connecting: false,
                        purging: false,
                    },
                    holds: HashMap::new(),
                    seen: HashMap::new(),
                    claims: HashMap::new(),
                    delivering: HashMap::new(),
                    publishing: 0,
                    generations: 0,
                    connects: 0,
                    unconnected: None,
                }),
                watching: Mutex::new(None),
                changed: Event::new(),
                clock: AtomicU64::new(1),
                woken: AtomicU64::new(0),
            }),
        }
    }
}

impl<St: Store, P: PubSub<Message = u64>> Mount<St, P> {
    // client is the connected client, connecting one first when there is none.
    // waited is the first connect that may have run while this operation waited, on a hold another operation was
    // making or here: one that failed fails this operation too, rather than it trying again in turn.
    pub(crate) async fn client(
        &self,
        mut waited: Option<u64>,
    ) -> Result<(Arc<P>, u64), Failure<P, St>> {
        let attempt = loop {
            let listener = {
                let mut inner = self.state.lock();
                match &inner.connection {
                    Connection::Up { client, generation } => {
                        return Ok((Arc::clone(client), *generation));
                    }
                    // An operation that waited for the purge could hold it up, since the purge waits for operations
                    // in flight on the items it drops, so it fails at once instead.
                    Connection::Down { purging: true, .. } => {
                        return Err(ConsistencyFailure::Purging);
                    }
                    _ => {}
                }
                if let (Some(from), Some((failed, unconnected))) = (waited, &inner.unconnected)
                    && *failed >= from
                {
                    return Err(unconnected.clone().into());
                }
                match &inner.connection {
                    Connection::Down {
                        connecting: true, ..
                    } => {
                        waited = Some(waited.unwrap_or(inner.connects).min(inner.connects));
                        self.state.changed.listen()
                    }
                    _ => {
                        inner.connection = Connection::Down {
                            connecting: true,
                            purging: false,
                        };
                        inner.connects += 1;
                        break inner.connects;
                    }
                }
            };
            listener.await;
        };
        let connecting = self.runtime.timeout(CONNECT_TIMEOUT, (self.connect)());
        let connected = match connecting.await {
            Some(connected) => connected.map_err(|failure| Unconnected::Failed(failure.into())),
            None => Err(Unconnected::TimedOut),
        };
        let mut inner = self.state.lock();
        // The watcher is started and recorded before the lock is let go of, so its own disconnect, which takes the
        // lock, finds it recorded.
        let generation = inner.generations + 1;
        let watched = connected.map(|client| {
            let client = Arc::new(client);
            let (state, kernel, runtime, watched) = (
                Arc::downgrade(&self.state),
                Arc::clone(&self.kernel),
                Arc::clone(&self.runtime),
                Arc::clone(&client),
            );
            let (stop, stopped) = AbortHandle::new_pair();
            self.runtime.spawn(Box::pin(async move {
                let _ = Abortable::new(
                    async move {
                        let failure = watched.failed().await;
                        tracing::warn!(%failure, "the volume's pub/sub connection failed, so the kernel's cache is dropped");
                        if let Some(state) = state.upgrade() {
                            disconnected(&state, &*kernel, &*runtime, generation).await;
                        }
                    },
                    stopped,
                )
                .await;
            }));
            (client, stop)
        });
        let result = match watched {
            Ok((client, stop)) => {
                inner.generations = generation;
                inner.connection = Connection::Up {
                    client: Arc::clone(&client),
                    generation,
                };
                if let Some(earlier) = self
                    .state
                    .watching
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .replace(stop)
                {
                    earlier.abort();
                }
                Ok((client, generation))
            }
            Err(unconnected) => {
                inner.connection = Connection::Down {
                    connecting: false,
                    purging: false,
                };
                inner.unconnected = Some((attempt, unconnected.clone()));
                Err(unconnected.into())
            }
        };
        drop(inner);
        self.state.wake();
        result
    }

    // subscribe holds inode through client, for the hold attempt left Subscribing.
    async fn subscribe(
        &self,
        inode: Inode,
        attempt: u64,
        waited: Option<u64>,
    ) -> Result<(), Failure<P, St>> {
        // A delivery reaches only the hold it was published to, once that hold has begun.
        let id = Arc::new(AtomicU64::new(0));
        let held = async {
            let (client, generation) = self.client(waited).await?;
            // The handler holds the state weakly, so the client the state holds does not keep it alive.
            let (state, kernel, runtime, id) = (
                Arc::downgrade(&self.state),
                Arc::clone(&self.kernel),
                Arc::clone(&self.runtime),
                Arc::clone(&id),
            );
            let subscription = client
                .subscribe(Key(inode.0), move |version| {
                    let (state, kernel, runtime, id) = (
                        Weak::clone(&state),
                        Arc::clone(&kernel),
                        Arc::clone(&runtime),
                        Arc::clone(&id),
                    );
                    async move {
                        if let Some(state) = state.upgrade() {
                            delivered(&state, &*kernel, &runtime, inode, &id, version).await;
                        }
                    }
                })
                .await
                .map_err(ConsistencyFailure::Hold)?;
            Ok((subscription, generation))
        }
        .await;
        let mut inner = self.state.lock();
        let subscribing = match inner.holds.get(&inode) {
            Some(Holding::Subscribing {
                attempt: made,
                lost,
                forgotten,
            }) if *made == attempt => Some((*lost, *forgotten)),
            _ => None,
        };
        let current = inner.generation();
        let result = match (held, subscribing) {
            // A hold lost or replaced while it was made is let go of, and the operation holds again.
            (Ok(_), None) => Ok(()),
            // So is one forgotten while it was made, which the kernel has no item for.
            (Ok(_), Some((_, true))) => {
                inner.holds.remove(&inode);
                Ok(())
            }
            (Ok((_, generation)), Some(_)) if current != Some(generation) => {
                inner.holds.insert(inode, Holding::Lost);
                Ok(())
            }
            (Ok((subscription, generation)), Some((_, false))) => {
                let since = self.state.clock.fetch_add(1, Ordering::SeqCst);
                id.store(since, Ordering::SeqCst);
                inner.holds.insert(
                    inode,
                    Holding::Held(Hold {
                        _subscription: subscription,
                        generation,
                        since,
                        replying: 0,
                        confirmed: 0,
                        pending: 0,
                        pending_highest: 0,
                        replies: 0,
                        asked: 0,
                        round: 0,
                        asked_at: None,
                        again: 0,
                    }),
                );
                Ok(())
            }
            // An inode lost stays lost until it is held again.
            (Err(failure), Some((lost, forgotten))) => {
                match lost && !forgotten {
                    true => inner.holds.insert(inode, Holding::Lost),
                    false => inner.holds.remove(&inode),
                };
                Err(failure)
            }
            (Err(failure), None) => Err(failure),
        };
        drop(inner);
        self.state.wake();
        result
    }

    // hold makes this mount a holder of inodes, so every later write to one waits for this mount to drop its copy, and
    // says whether every one was held already when ticket was taken.
    pub(crate) async fn hold(&self, inodes: &[Inode], ticket: u64) -> Result<bool, Failure<P, St>> {
        let mut held_before = true;
        // waited is the first connect that may have run while this operation waited on a hold another was making.
        let mut waited = None;
        for &inode in inodes {
            loop {
                let next = {
                    let mut inner = self.state.lock();
                    if let Some(hold) = inner.held(inode) {
                        Next::Held(hold.since < ticket)
                    } else if let Some(Holding::Subscribing { .. }) = inner.holds.get(&inode) {
                        let from = inner.connects
                            + u64::from(!matches!(
                                inner.connection,
                                Connection::Down {
                                    connecting: true,
                                    ..
                                }
                            ));
                        waited = Some(waited.unwrap_or(from).min(from));
                        Next::Wait(self.state.changed.listen())
                    } else {
                        // A hold through a client that failed is lost too.
                        let lost = inner.holds.contains_key(&inode);
                        let attempt = self.state.clock.fetch_add(1, Ordering::SeqCst);
                        inner.holds.insert(
                            inode,
                            Holding::Subscribing {
                                attempt,
                                lost,
                                forgotten: false,
                            },
                        );
                        Next::Subscribe(attempt)
                    }
                };
                match next {
                    Next::Held(before) => {
                        held_before &= before;
                        break;
                    }
                    Next::Wait(listener) => listener.await,
                    Next::Subscribe(attempt) => {
                        self.subscribe(inode, attempt, waited).await?;
                        held_before = false;
                        break;
                    }
                }
            }
        }
        Ok(held_before)
    }

    // release lets go of the holds of inodes, under the lock, so no hold of the same key is made through the client
    // before the old one is gone. One still being made is let go of once it is made.
    pub(crate) fn release(&self, inodes: &[Inode]) {
        if inodes.is_empty() {
            return;
        }
        {
            let mut inner = self.state.lock();
            for inode in inodes {
                match inner.holds.get_mut(inode) {
                    Some(Holding::Subscribing { forgotten, .. }) => *forgotten = true,
                    _ => {
                        inner.holds.remove(inode);
                    }
                }
            }
        }
        self.state.wake();
    }
}

// delivered handles a publish of version to the hold of inode whose id is in hold, and then acknowledges it by
// returning. A purge keeps this mount's holds until it has dropped the kernel's copies, so a delivery is acknowledged
// only once no purge is running.
async fn delivered<P: PubSub>(
    state: &Arc<State<P>>,
    kernel: &dyn KernelCache,
    runtime: &Arc<dyn Runtime>,
    inode: Inode,
    hold: &AtomicU64,
    version: u64,
) {
    // A hold's id is set as the hold begins. A delivery before then finds nothing cached through it to drop, and any
    // read through it comes after the write's announcement, so it sees that write.
    match hold.load(Ordering::SeqCst) {
        0 => {}
        id => handled(state, kernel, runtime, inode, id, version).await,
    }
    purged(state).await;
}

// Round is how a delivery's round of invalidation ended: the delivery acknowledged, it goes again since a reply of
// older data came during the round, it waits for this mount's own write to the inode to reply first, or its hold is
// gone.
enum Round {
    Acknowledged,
    Again,
    Replying,
    Gone,
}

// end_round ends a round of the delivery of version to inode's hold id, begun when the hold had counted replies, of a
// delivery that invalidates again when again says so.
fn end_round<P: PubSub>(
    inner: &mut Inner<P>,
    inode: Inode,
    id: u64,
    replies: u64,
    again: bool,
    version: u64,
) -> Round {
    let Some(hold) = inner.held(inode).filter(|hold| hold.since == id) else {
        return Round::Gone;
    };
    hold.asked -= 1;
    if hold.asked == 0 {
        hold.asked_at = None;
    }
    if hold.replies != replies {
        if !again {
            hold.again += 1;
        }
        return Round::Again;
    }
    if hold.replying > 0 {
        return Round::Replying;
    }
    if again {
        hold.again -= 1;
    }
    hold.pending -= 1;
    hold.confirmed = hold.confirmed.max(version);
    if hold.pending == 0 {
        hold.pending_highest = 0;
    }
    Round::Acknowledged
}

// purged resolves once no purge is running.
async fn purged<P: PubSub>(state: &State<P>) {
    loop {
        let listener = {
            let inner = state.lock();
            if !matches!(inner.connection, Connection::Down { purging: true, .. }) {
                return;
            }
            state.changed.listen()
        };
        listener.await;
    }
}

// handled drops the kernel's copy of inode for a publish of version to its hold id, again until no reply of older data
// happened while it did and none of this mount's writes to the inode is still to reply.
pub(crate) async fn handled<P: PubSub>(
    state: &Arc<State<P>>,
    kernel: &dyn KernelCache,
    runtime: &Arc<dyn Runtime>,
    inode: Inode,
    id: u64,
    version: u64,
) {
    let generation = {
        let mut inner = state.lock();
        let Some(hold) = inner.held(inode).filter(|hold| hold.since == id) else {
            return;
        };
        // A delivery no newer than one this mount acknowledged, or than a write of its own announced here, has nothing
        // left to drop: reads older than that already go no further.
        if version <= hold.confirmed {
            return;
        }
        hold.pending += 1;
        hold.pending_highest = hold.pending_highest.max(version);
        hold.generation
    };
    state.wake();
    // again says whether this delivery invalidates again, which the hold counts.
    let mut again = false;
    loop {
        let replies = match state.lock().held(inode).filter(|hold| hold.since == id) {
            Some(hold) => hold.replies,
            None => break,
        };
        // The hold notes when the kernel was asked, since the purge drops replies of the operations it waits for begun
        // before then. The round ends under the lock as the kernel answers, so no reply comes between purge and check.
        let ended = Arc::new(Mutex::new(None));
        let (asked, answered): (Asked, Answered) = {
            let (asking, clock, answering, ending) = (
                Arc::downgrade(state),
                Arc::clone(runtime),
                Arc::downgrade(state),
                Arc::clone(&ended),
            );
            (
                Box::new(move || {
                    if let Some(state) = asking.upgrade() {
                        let now = state.clock.fetch_add(1, Ordering::SeqCst);
                        if let Some(hold) = state.lock().held(inode).filter(|hold| hold.since == id)
                        {
                            if hold.asked == 0 {
                                hold.round = now;
                                hold.asked_at = Some(clock.now());
                            }
                            hold.asked += 1;
                        }
                    }
                }),
                Box::new(move |dropped| {
                    if let Some(state) = answering.upgrade()
                        && dropped
                    {
                        let round =
                            end_round(&mut state.lock(), inode, id, replies, again, version);
                        *ending.lock().unwrap_or_else(PoisonError::into_inner) = Some(round);
                    }
                }),
            )
        };
        let round = match kernel.invalidate_noting(inode, asked, answered).await {
            Ok(()) => ended
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
                .unwrap_or(Round::Gone),
            // A copy the kernel keeps cannot be acknowledged as dropped. When a second try fails too, the mount drops
            // the connection the hold was made through, so the coordinator stops counting it as a holder.
            Err(_) => match kernel.invalidate(inode).await {
                Ok(()) => end_round(&mut state.lock(), inode, id, replies, again, version),
                Err(error) => {
                    tracing::warn!(inode = inode.0, %error, "invalidating the kernel's copy failed, so the pub/sub connection is dropped");
                    disconnected(state, kernel, &**runtime, generation).await;
                    break;
                }
            },
        };
        match round {
            Round::Acknowledged | Round::Gone => break,
            // The round's end lets a late read waiting on it go on.
            Round::Again => {
                again = true;
                state.wake();
            }
            // A write of this mount committed a change to the inode and has yet to hand it over, which it does
            // without waiting on this delivery, so the delivery invalidates again once it has.
            Round::Replying => {
                let listener = {
                    let mut inner = state.lock();
                    inner
                        .held(inode)
                        .filter(|hold| hold.since == id && hold.replying > 0)
                        .map(|_| state.changed.listen())
                };
                if let Some(listener) = listener {
                    listener.await;
                }
            }
        }
    }
    state.wake();
}

// disconnected drops everything held through client generation, whose connection failed, and the kernel's copy of
// every inode it held, so the mount serves nothing it may have missed a write to. Each inode it held is kept as lost.
async fn disconnected<P: PubSub>(
    state: &State<P>,
    kernel: &dyn KernelCache,
    runtime: &dyn Runtime,
    generation: u64,
) {
    let dropped: Vec<(Inode, Holding<P>)> = {
        let mut inner = state.lock();
        if inner.generation() != Some(generation) {
            return;
        }
        inner.connection = Connection::Down {
            connecting: false,
            purging: true,
        };
        inner.seen.clear();
        let mut dropped = Vec::new();
        // An inode still being held is kept as lost only if it was lost before: the kernel has no item for any other.
        inner.holds.retain(|inode, holding| {
            let lost = match holding {
                Holding::Held(_) => {
                    dropped.push((*inode, std::mem::replace(holding, Holding::Lost)));
                    true
                }
                Holding::Subscribing {
                    lost, forgotten, ..
                } => *lost && !*forgotten,
                Holding::Lost => true,
            };
            if lost {
                *holding = Holding::Lost;
            }
            lost
        });
        // The purges go in order, so a mount purges the same way each time it runs.
        dropped.sort_by_key(|(inode, _)| inode.0);
        dropped
    };
    state.wake();
    // The holds go only once the purge is done, so the coordinator counts this mount as a holder of each inode while
    // the kernel may still have a copy of it.
    let (dropped, holds): (Vec<Inode>, Vec<Holding<P>>) = dropped.into_iter().unzip();
    join_all(dropped.iter().map(|inode| purge(runtime, kernel, *inode))).await;
    // The watcher of the failed client is stopped only now, since this may be it. No client connects while purging,
    // so the watcher is still that client's.
    let watching = state
        .watching
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .take();
    drop(holds);
    if let Connection::Down { purging, .. } = &mut state.lock().connection {
        *purging = false;
    }
    state.wake();
    if let Some(watching) = watching {
        watching.abort();
    }
}

// purge drops the kernel's copy of inode for a purge, trying again until the kernel does: a copy the kernel keeps
// cannot be let go of, so the purge, and with it the mount, waits instead.
async fn purge(runtime: &dyn Runtime, kernel: &dyn KernelCache, inode: Inode) {
    let mut wait = PURGE_RETRY;
    while let Err(error) = kernel.invalidate(inode).await {
        tracing::warn!(inode = inode.0, %error, "dropping the kernel's copy after the connection failed failed, so it is tried again");
        runtime.sleep(wait).await;
        wait = (wait * 2).min(PURGE_RETRY_MOST);
    }
}

// Dropping the mount stops watching its client, which it no longer holds, so the connection closes.
impl<St, P: PubSub> Drop for Mount<St, P> {
    fn drop(&mut self) {
        if let Some(watching) = self
            .state
            .watching
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            watching.abort();
        }
    }
}
