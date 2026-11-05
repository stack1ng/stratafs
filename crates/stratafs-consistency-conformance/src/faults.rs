use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use stratafs_fs_engine::strong::ConnectFailure;
use stratafs_pubsub_conformance::{MemoryClient, MemoryVolume, Volume};

use crate::activity::{FaultKind, REFUSALS};
use crate::simulation::{Event, History};

// Faults is what goes wrong with one mount, at the store transactions, kernel invalidations and pub/sub connects it
// counts.
pub(crate) struct Faults {
    planned: Mutex<Vec<(FaultKind, usize)>>,
    counted: Mutex<Counted>,
    dead: AtomicBool,
    // failed says whether a store transaction failed on purpose.
    failed: AtomicBool,
    link: Arc<Link>,
}

// Counted is how many store transactions, kernel invalidations and pub/sub connects a mount began since it was planned.
#[derive(Clone, Copy, Default)]
pub(crate) struct Counted {
    transactions: usize,
    invalidations: usize,
    connects: usize,
}

// Link is a mount's connection to the volume's pub/sub, as the network carries it.
pub(crate) struct Link {
    mount: usize,
    pubsub: Arc<MemoryVolume<u64>>,
    connection: Mutex<Option<u64>>,
    history: History,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Counted {
    // of is the count a fault of kind is placed in.
    pub(crate) fn of(self, kind: FaultKind) -> usize {
        match kind {
            FaultKind::Refused => self.invalidations,
            FaultKind::ConnectFails => self.connects,
            FaultKind::Cut | FaultKind::StoreFails | FaultKind::Dies => self.transactions,
        }
    }
}

impl Faults {
    pub(crate) fn new(link: &Arc<Link>) -> Self {
        Faults {
            planned: Mutex::default(),
            counted: Mutex::default(),
            dead: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            link: Arc::clone(link),
        }
    }

    // plan counts from now, and makes planned go wrong, each at its count.
    pub(crate) fn plan(&self, planned: Vec<(FaultKind, usize)>) {
        *lock(&self.planned) = planned;
        *lock(&self.counted) = Counted::default();
    }

    pub(crate) fn counted(&self) -> Counted {
        *lock(&self.counted)
    }

    pub(crate) fn dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    // failed says whether a store transaction failed on purpose, as every one does once the mount died.
    pub(crate) fn failed(&self) -> bool {
        self.failed.load(Ordering::SeqCst)
    }

    // begin counts a store transaction, and is its count, or None when it fails.
    pub(crate) fn begin(&self) -> Option<usize> {
        let transaction = self.count(|counted| &mut counted.transactions);
        if self.planned(FaultKind::Cut, transaction) {
            self.link.cut();
        }
        if self.dead() || self.planned(FaultKind::StoreFails, transaction) {
            self.failed.store(true, Ordering::SeqCst);
            return None;
        }
        Some(transaction)
    }

    // end is the end of the store transaction counted transaction.
    pub(crate) fn end(&self, transaction: usize) {
        if self.planned(FaultKind::Dies, transaction) && !self.dead.swap(true, Ordering::SeqCst) {
            self.link.history.push(Event::Died(self.link.mount));
            self.link.cut();
        }
    }

    // connect connects the mount to the volume's pub/sub, unless this connect fails.
    pub(crate) async fn connect(&self) -> Result<MemoryClient<u64>, ConnectFailure> {
        let connect = self.count(|counted| &mut counted.connects);
        if self.planned(FaultKind::ConnectFails, connect) {
            return Err("the connect failed on purpose".into());
        }
        Ok(self.link.connect().await)
    }

    // refuses counts an invalidation the kernel was asked for, and says whether it refuses it.
    pub(crate) fn refuses(&self) -> bool {
        let invalidation = self.count(|counted| &mut counted.invalidations);
        let refused = (invalidation.saturating_sub(REFUSALS - 1)..=invalidation)
            .any(|at| self.planned(FaultKind::Refused, at));
        // A mount drops its connection once a delivery's invalidation is refused twice, so it is cut from here on.
        if refused {
            self.link.history.push(Event::Cut(self.link.mount));
        }
        refused
    }

    // count counts one more of what counter picks, and is how many there were before.
    fn count(&self, counter: impl FnOnce(&mut Counted) -> &mut usize) -> usize {
        let mut counted = lock(&self.counted);
        let count = counter(&mut counted);
        *count += 1;
        *count - 1
    }

    fn planned(&self, kind: FaultKind, at: usize) -> bool {
        lock(&self.planned).contains(&(kind, at))
    }
}

impl Link {
    pub(crate) fn new(mount: usize, pubsub: &Arc<MemoryVolume<u64>>, history: &History) -> Self {
        Link {
            mount,
            pubsub: Arc::clone(pubsub),
            connection: Mutex::default(),
            history: History::clone(history),
        }
    }

    async fn connect(&self) -> MemoryClient<u64> {
        let (client, connection) = Volume::connect(&*self.pubsub).await;
        *lock(&self.connection) = Some(connection);
        self.history.push(Event::Connected(self.mount));
        client
    }

    // cut ends the mount's connection the way a network failure does.
    fn cut(&self) {
        let connection = lock(&self.connection).take();
        if let Some(connection) = connection {
            self.history.push(Event::Cut(self.mount));
            self.pubsub.cut(&connection);
        }
    }
}
