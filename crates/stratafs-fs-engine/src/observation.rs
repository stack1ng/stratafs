use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use futures_core::future::BoxFuture;
use futures_util::future::{AbortHandle, Abortable};
use futures_util::task::{Spawn, SpawnExt};
use stratafs_store::kv::KeyValueStore;
use stratafs_store::store::Store;

use crate::attributes::{Attributes, read_attributes};
use crate::key::FsKey;
use crate::{Cached, Eventual, FsEngine, Inode, MetadataFailure};

// KernelCache is a kernel's cache of the engine's inodes, as the integration serving that kernel reaches it.
// invalidate drops the kernel's copy of an inode, and resolves once it has.
pub trait KernelCache: Send + Sync + 'static {
    fn invalidate(&self, inode: Inode) -> BoxFuture<'static, ()>;
}

// NoKernel is the kernel of an engine whose inodes no kernel caches, so there is nothing to invalidate.
pub struct NoKernel;

impl KernelCache for NoKernel {
    fn invalidate(&self, _inode: Inode) -> BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}

// AutoInvalidators runs an auto invalidator for each inode the kernel caches: a task that invalidates the kernel's
// copy once the inode changes in a way the kernel did not make itself.
pub(crate) struct AutoInvalidators {
    kernel: Box<dyn KernelCache>,
    spawner: Box<dyn Spawn + Send + Sync>,
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
    pub(crate) fn new(
        kernel: impl KernelCache,
        spawner: impl Spawn + Send + Sync + 'static,
    ) -> Self {
        AutoInvalidators {
            kernel: Box::new(kernel),
            spawner: Box::new(spawner),
            running: Mutex::default(),
            started: AtomicU64::new(0),
        }
    }

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

impl<St: Store + 'static, A, N, S, C: Cached<St>> FsEngine<St, A, N, S, C> {
    // invalidated resolves once an inode cached as seen needs to be invalidated, at once if seen is already out
    // of date.
    pub fn invalidated(
        &self,
        inode: Inode,
        seen: &Attributes,
    ) -> impl Future<Output = Result<(), C::Failure>> + Send {
        self.consistency.invalidated(&self.store, inode, seen)
    }

    // cached tells the engine the kernel now caches the given inode as seen.
    // This is then used to coordinate an auto invalidator for this inode.
    pub fn cached(&self, inode: Inode, seen: &Attributes) {
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
        let (store, consistency, auto_invalidators, mount, mut seen) = (
            Arc::clone(&self.store),
            Arc::clone(&self.consistency),
            Arc::clone(&self.auto_invalidators),
            self.mount,
            seen.clone(),
        );
        let auto_invalidator = Abortable::new(
            async move {
                loop {
                    if let Err(error) = consistency.invalidated(&store, inode, &seen).await {
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
                            if current.change_version == seen.change_version.after(mount) =>
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
                if auto_invalidators.end(inode, id) {
                    auto_invalidators.kernel.invalidate(inode).await;
                }
            },
            stopped,
        );
        if let Err(error) = self.auto_invalidators.spawner.spawn(async move {
            let _ = auto_invalidator.await;
        }) {
            self.auto_invalidators.end(inode, id);
            tracing::warn!(inode = inode.0, %error, "an auto invalidator could not start");
        }
    }

    // forget tells the engine the kernel no longer caches inode.
    pub fn forget(&self, inode: Inode) {
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
}

// Eventual invalidates an inode once a write to it commits: once its stored attributes differ from seen. Every
// change to an inode's content, attributes, annotations or entries records itself in its attributes, and deleting
// it removes them.
impl<St: Store> Cached<St> for Eventual {
    type Failure = MetadataFailure<St>;

    fn invalidated(
        &self,
        store: &St,
        inode: Inode,
        seen: &Attributes,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        store
            .metadata()
            .changed(FsKey::new_attributes(inode).into(), Some(Bytes::from(seen)))
    }
}

#[cfg(test)]
mod tests;
