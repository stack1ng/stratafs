use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use futures_util::future::{AbortHandle, Abortable};
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};
use stratafs_store::store::Store;

use crate::attributes::{Attributes, read_attributes};
use crate::key::FsKey;
use crate::{
    Cached, Change, Consistency, Inode, MetadataFailure, Mount, PurgeWaits, ResultHandling,
};

// Eventual serves reads from a cache that may lag behind other clients' writes.
pub struct Eventual {
    // auto_invalidators invalidate the kernel's cache of each inode it caches once the inode changes.
    auto_invalidators: Arc<AutoInvalidators>,
    // clock numbers each read as it begins, and forgotten holds, per bucket of inodes, the number taken when the kernel
    // last let go of one of them.
    clock: AtomicU64,
    forgotten: [AtomicU64; FORGOTTEN_BUCKETS],
}

const FORGOTTEN_BUCKETS: usize = 1024;

impl Default for Eventual {
    fn default() -> Self {
        Self {
            auto_invalidators: Arc::default(),
            clock: AtomicU64::new(0),
            forgotten: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

impl Consistency for Eventual {}

// AutoInvalidators runs an auto invalidator for each inode the kernel caches: a task that invalidates the kernel's
// copy once the inode changes in a way the kernel did not make itself.
#[derive(Default)]
pub(crate) struct AutoInvalidators {
    // running is the running auto invalidator of each inode.
    pub(crate) running: Mutex<HashMap<Inode, AutoInvalidator>>,
    // started numbers each auto invalidator as it starts.
    started: AtomicU64,
}

// AutoInvalidator is an inode's running auto invalidator.
pub(crate) struct AutoInvalidator {
    // id tells it apart from one that replaced it.
    id: u64,
    // seen is the change number it watches the inode from.
    seen: u64,
    pub(crate) stop: AbortHandle,
}

impl AutoInvalidators {
    // advance moves inode's auto invalidator id on to watch from seen, and says whether it still runs.
    fn advance(&self, inode: Inode, id: u64, seen: u64) -> bool {
        match self
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_mut(&inode)
        {
            Some(running) if running.id == id => {
                running.seen = seen;
                true
            }
            _ => false,
        }
    }

    // end takes inode's auto invalidator id out of running, and says whether it still ran.
    fn end(&self, inode: Inode, id: u64) -> bool {
        let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        let ran = running.get(&inode).is_some_and(|running| running.id == id);
        if ran {
            running.remove(&inode);
        }
        ran
    }
}

// invalidated resolves once an inode cached as seen needs to be invalidated, at once if seen is already out of date.
// Every change to an inode's content, attributes, annotations or entries records itself in its attributes, and
// deleting it removes them.
pub(crate) fn invalidated<St: Store>(
    store: &St,
    inode: Inode,
    seen: &Attributes,
) -> impl Future<Output = Result<(), MetadataFailure<St>>> + Send {
    store
        .metadata()
        .changed(FsKey::new_attributes(inode).into(), Some(Bytes::from(seen)))
}

// Eventual invalidates an inode once a write to it commits: once its stored attributes differ from seen. It lets
// every write commit, and takes again a read begun before the kernel last let go of one of its inodes.
impl Eventual {
    // forgotten is the bucket holding the number taken when the kernel last let go of inode.
    fn forgotten(&self, inode: Inode) -> &AtomicU64 {
        &self.forgotten[(inode.0 % FORGOTTEN_BUCKETS as u64) as usize]
    }

    // taken_after says whether a read with ticket began after the kernel last let go of any of inodes.
    fn taken_after(&self, ticket: u64, mut inodes: impl Iterator<Item = Inode>) -> bool {
        inodes.all(|inode| ticket > self.forgotten(inode).load(Ordering::SeqCst))
    }
}

impl<St: Store> Cached<St> for Eventual {
    type Failure = Infallible;
    type Prepared = ();
    type Ticket = u64;
    type InodeState = ();

    async fn inode_state<T: ReadKeyValueTransaction<Failure = MetadataFailure<St>>>(
        &self,
        _: &T,
        _: &[Inode],
    ) -> Result<(), Infallible> {
        Ok(())
    }

    async fn may_commit<T: KeyValueTransaction<Failure = MetadataFailure<St>>>(
        &self,
        _: &Mount<St>,
        _: Option<&()>,
        _: &HashMap<Inode, Change>,
        _: &mut T,
    ) -> Result<bool, Infallible> {
        Ok(true)
    }

    async fn prepare(
        &self,
        _: &Mount<St>,
        _: Option<()>,
        _: HashMap<Inode, Change>,
        _: PurgeWaits,
    ) -> Result<(), Infallible> {
        Ok(())
    }

    async fn finish(&self, _: &Mount<St>, (): (), _: Option<&HashMap<Inode, Change>>) {}

    fn reading(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst) + 1
    }

    async fn hold(&self, _: &Mount<St>, inodes: &[Inode], ticket: u64) -> Result<bool, Infallible> {
        Ok(self.taken_after(ticket, inodes.iter().copied()))
    }

    // A read begun before the kernel let go of one of its inodes is taken again: once the item is gone, its reply could
    // land after a newer one this mount handed over, which the auto invalidator then takes for the kernel's own.
    fn result_handling(
        &self,
        _: &Mount<St>,
        reads: &[(Inode, Attributes)],
        (): &(),
        ticket: u64,
        _: u64,
        _: PurgeWaits,
        _: bool,
    ) -> ResultHandling {
        match self.taken_after(ticket, reads.iter().map(|(inode, _)| *inode)) {
            true => ResultHandling::Cache,
            false => ResultHandling::Retry,
        }
    }

    fn result_handling_listed(
        &self,
        _: &Mount<St>,
        _: &[(Inode, Attributes)],
        (): &(),
        _: u64,
    ) -> ResultHandling {
        ResultHandling::Cache
    }

    async fn settled(
        &self,
        _: &Mount<St>,
        _: &[(Inode, Attributes)],
        (): &(),
        _: PurgeWaits,
    ) -> Result<(), Infallible> {
        Ok(())
    }

    async fn reread(
        &self,
        _: &Mount<St>,
        committed: &[(Inode, Attributes)],
    ) -> Result<(Vec<(Inode, Attributes)>, ()), Infallible> {
        Ok((committed.to_vec(), ()))
    }

    // cached tells Eventual the kernel now caches the given inode as seen.
    // This is then used to coordinate an auto invalidator for this inode.
    fn cached(&self, mount: &Mount<St>, inode: Inode, seen: &Attributes) {
        let (stop, stopped) = AbortHandle::new_pair();
        let id = self
            .auto_invalidators
            .started
            .fetch_add(1, Ordering::Relaxed);
        let starting = AutoInvalidator {
            id,
            seen: seen.change_version.number,
            stop,
        };
        match self
            .auto_invalidators
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(inode)
        {
            Entry::Occupied(mut running) => {
                if running.get().seen <= starting.seen {
                    return;
                }
                running.insert(starting).stop.abort();
            }
            Entry::Vacant(vacant) => {
                vacant.insert(starting);
            }
        }
        let (store, kernel, auto_invalidators, mount_id, mut seen) = (
            Arc::clone(&mount.store),
            Arc::clone(&mount.kernel),
            Arc::clone(&self.auto_invalidators),
            mount.id,
            seen.clone(),
        );
        let auto_invalidator = Abortable::new(
            async move {
                loop {
                    if let Err(error) = invalidated(&*store, inode, &seen).await {
                        tracing::warn!(inode = inode.0, %error, "watching an inode for changes failed, so the kernel's copy is invalidated");
                        break;
                    }
                    let current = store
                        .metadata()
                        .read_transact(move |metadata| {
                            Box::pin(async move {
                                read_attributes::<_, Infallible>(&*metadata, inode).await
                            })
                        })
                        .await;
                    match current {
                        // The kernel made the change itself, through this engine.
                        Ok(Some(current))
                            if current.change_version == seen.change_version.after(mount_id) =>
                        {
                            if !auto_invalidators.advance(inode, id, current.change_version.number)
                            {
                                return;
                            }
                            seen = current;
                        }
                        Ok(_) => break,
                        Err(error) => {
                            tracing::warn!(inode = inode.0, %error, "reading a changed inode failed, so the kernel's copy is invalidated");
                            break;
                        }
                    }
                }
                if auto_invalidators.end(inode, id)
                    && let Err(error) = kernel.invalidate(inode).await
                {
                    tracing::warn!(inode = inode.0, %error, "invalidating the kernel's copy failed");
                }
            },
            stopped,
        );
        mount.runtime.spawn(Box::pin(async move {
            let _ = auto_invalidator.await;
        }));
    }

    // forget tells Eventual the kernel no longer caches inode.
    fn forget(&self, _: &Mount<St>, inode: Inode) {
        let at = self.clock.fetch_add(1, Ordering::SeqCst) + 1;
        self.forgotten(inode).fetch_max(at, Ordering::SeqCst);
        if let Some(running) = self
            .auto_invalidators
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&inode)
        {
            running.stop.abort();
        }
    }

    fn write_back(&self) -> bool {
        true
    }
}

// Dropping Eventual stops its auto invalidators.
impl Drop for Eventual {
    fn drop(&mut self) {
        let running = std::mem::take(
            &mut *self
                .auto_invalidators
                .running
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for auto_invalidator in running.into_values() {
            auto_invalidator.stop.abort();
        }
    }
}

#[cfg(test)]
mod tests;
