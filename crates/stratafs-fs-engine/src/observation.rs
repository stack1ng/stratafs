use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use futures_core::future::BoxFuture;
use futures_util::future::{AbortHandle, Abortable};
use futures_util::task::{Spawn, SpawnExt};
use stratafs_store::kv::KeyValueStore;
use stratafs_store::store::Store;

use crate::attributes::{Attributes, StoredAttributes};
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
// copy once the inode changes.
pub(crate) struct AutoInvalidators {
    kernel: Box<dyn KernelCache>,
    spawner: Box<dyn Spawn + Send + Sync>,
    // running stops the running auto invalidator of each inode.
    pub(crate) running: Mutex<HashMap<Inode, AbortHandle>>,
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
        }
    }

    // stop stops the auto invalidators of inodes.
    pub(crate) fn stop(&self, inodes: impl IntoIterator<Item = Inode>) {
        let mut running = self.running.lock().unwrap_or_else(PoisonError::into_inner);
        for inode in inodes {
            if let Some(auto_invalidator) = running.remove(&inode) {
                auto_invalidator.abort();
            }
        }
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
        match self
            .auto_invalidators
            .running
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(inode)
        {
            Entry::Occupied(_) => return,
            Entry::Vacant(vacant) => {
                vacant.insert(stop);
            }
        }
        let (store, consistency, auto_invalidators, seen) = (
            Arc::clone(&self.store),
            Arc::clone(&self.consistency),
            Arc::clone(&self.auto_invalidators),
            seen.clone(),
        );
        let auto_invalidator = Abortable::new(
            async move {
                if let Err(error) = consistency.invalidated(&store, inode, &seen).await {
                    tracing::warn!(inode = inode.0, %error, "watching an inode for changes failed, so the kernel's copy is invalidated");
                }
                auto_invalidators
                    .running
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&inode);
                auto_invalidators.kernel.invalidate(inode).await;
            },
            stopped,
        );
        if let Err(error) = self.auto_invalidators.spawner.spawn(async move {
            let _ = auto_invalidator.await;
        }) {
            self.auto_invalidators
                .running
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&inode);
            tracing::warn!(inode = inode.0, %error, "an auto invalidator could not start");
        }
    }

    // forget tells the engine the kernel no longer caches inode.
    pub fn forget(&self, inode: Inode) {
        self.auto_invalidators.stop([inode]);
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
        store.metadata().changed(
            FsKey::new_attributes(inode).into(),
            Some(Bytes::from(&StoredAttributes::from(seen))),
        )
    }
}

#[cfg(test)]
mod tests;
