use futures_core::future::BoxFuture;

use crate::Inode;

// KernelCache is a kernel's cache of the engine's inodes, as the integration serving that kernel reaches it.
// invalidate drops the kernel's copy of an inode, and resolves once it has, or with why it could not.
pub trait KernelCache: Send + Sync + 'static {
    fn invalidate(&self, inode: Inode) -> BoxFuture<'static, Result<(), Uninvalidated>>;

    // invalidate_noting invalidates as invalidate does, calling asked and answered once each before it resolves. The
    // kernel may be asked later than the call, such as on a thread of its own.
    fn invalidate_noting(
        &self,
        inode: Inode,
        asked: Asked,
        answered: Answered,
    ) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        asked();
        let invalidated = self.invalidate(inode);
        Box::pin(async move {
            let invalidated = invalidated.await;
            answered(invalidated.is_ok());
            invalidated
        })
    }
}

// Asked is called right as the kernel is asked to drop its copy of an inode, and Answered right as it answers, with
// whether it dropped it.
pub type Asked = Box<dyn FnOnce() + Send>;
pub type Answered = Box<dyn FnOnce(bool) + Send>;

// Uninvalidated is why the kernel's copy of an inode could not be dropped, as the kernel said.
#[derive(Debug, thiserror::Error)]
#[error("the kernel's copy could not be dropped: {0}")]
pub struct Uninvalidated(pub String);

// NoKernel is the kernel of an engine whose inodes no kernel caches, so there is nothing to invalidate.
pub struct NoKernel;

impl KernelCache for NoKernel {
    fn invalidate(&self, _inode: Inode) -> BoxFuture<'static, Result<(), Uninvalidated>> {
        Box::pin(async { Ok(()) })
    }
}
