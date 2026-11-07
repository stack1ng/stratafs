use std::io;
use std::sync::Arc;

use fuser::Notifier;
use futures_util::future::BoxFuture;
use stratafs_fs_engine::Inode;
use stratafs_fs_engine::kernel_cache::{Answered, Asked, KernelCache, Uninvalidated};
use stratafs_store::spawn::{SpawnBlocking, ThreadPool};

use super::item::{file_name, node_id};
use super::items::Items;

// FuseCache is the kernel's cache of a volume's inodes, reached through the session's notifier, which items keeps.
#[derive(Clone)]
pub struct FuseCache {
    items: Arc<Items>,
}

impl FuseCache {
    pub(crate) fn new(items: Arc<Items>) -> Self {
        FuseCache { items }
    }

    // connect invalidates through the session's notifier from now on.
    pub(crate) fn connect(&self, notifier: Notifier) {
        if self.items.notifier.set(notifier).is_err() {
            panic!("a volume is mounted once");
        }
    }

    // invalidate_now drops the kernel's copy of inode: the names in it the kernel was handed, which takes its lock as a
    // directory's, then its attributes and cached contents. It waits for the kernel, so it holds no lock.
    fn invalidate_now(&self, inode: Inode) -> Result<(), Uninvalidated> {
        let Some(notifier) = self.items.notifier.get() else {
            return Ok(());
        };
        let Some(names) = self.items.to_invalidate(inode) else {
            return Ok(());
        };
        for (name, reply) in names {
            notifier
                .inval_entry(node_id(inode), file_name(&name))
                .map_err(uninvalidated)?;
            self.items.dropped_name(inode, &name, reply);
        }
        // Offset 0 and length 0 drop all of the inode's cached contents.
        notifier
            .inval_inode(node_id(inode), 0, 0)
            .map_err(uninvalidated)
    }
}

// The kernel can wait on operations in flight while it invalidates, so invalidating waits on a thread of its own.
impl KernelCache for FuseCache {
    fn invalidate(&self, inode: Inode) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        let cache = self.clone();
        Box::pin(ThreadPool.spawn_blocking(move || cache.invalidate_now(inode)))
    }

    fn invalidate_noting(
        &self,
        inode: Inode,
        asked: Asked,
        answered: Answered,
    ) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        let cache = self.clone();
        Box::pin(ThreadPool.spawn_blocking(move || {
            asked();
            let invalidated = cache.invalidate_now(inode);
            answered(invalidated.is_ok());
            invalidated
        }))
    }

    // invalidate_attributes asks the kernel at once, as dropping only the attributes waits for no request. The
    // notifier takes an item the kernel holds nothing of as nothing left to drop.
    fn invalidate_attributes(&self, inode: Inode) -> Result<(), Uninvalidated> {
        let Some(notifier) = self.items.notifier.get() else {
            return Ok(());
        };
        // Offset -1 drops the attributes and none of the cached contents.
        notifier
            .inval_inode(node_id(inode), -1, 0)
            .map_err(uninvalidated)
    }

    // A directory's purge takes its lock, which an unlink there holds while it waits for the lock of a file another
    // change holds.
    fn purge_waits_on_others(&self) -> bool {
        true
    }
}

fn uninvalidated(error: io::Error) -> Uninvalidated {
    Uninvalidated(error.to_string())
}
