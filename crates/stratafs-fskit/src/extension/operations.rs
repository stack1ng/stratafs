use std::fmt::Display;
use std::ptr::null_mut;
use std::sync::Arc;

use block2::RcBlock;
use futures_util::task::{Spawn, SpawnExt};
use objc2::AllocAnyThread;
use objc2::rc::Retained;
use objc2_foundation::NSError;
use objc2_fs_kit::FSGetAttributesResult;
use stratafs_fs_engine::error::PosixError;
use stratafs_fs_engine::{FsConfig, FsEngine, Inode, Timestamp};
use stratafs_store::content::ContentStore;
use stratafs_store::kv::{KeyValueStore, TransactError};

use super::snapshots::Snapshots;
use super::{errno, posix_error};

// Reply is an FSKit reply block, copied so an operation can call it once it finishes.
pub(crate) type Reply<R> = RcBlock<'static, fn(*mut R, *mut NSError), dyn Send + Sync>;

// VolumeOperations are a mounted volume's FSKit operations. StrataVolume, which FSKit calls, cannot be
// generic, so it reaches the Volume holding the consumer's engine through this trait.
pub(crate) trait VolumeOperations: Send + Sync {
    fn get_attributes(self: Arc<Self>, inode: Inode, reply: Reply<FSGetAttributesResult>);
}

// Volume is a mounted volume's engine. Each operation runs on spawner and replies when it finishes.
pub(crate) struct Volume<M, C, A, N, S> {
    pub(crate) engine: FsEngine<M, C, A, N, S>,
    pub(crate) spawner: Arc<dyn Spawn + Send + Sync>,
    pub(crate) snapshots: Snapshots,
}

impl<M, C, A, N, S> VolumeOperations for Volume<M, C, A, N, S>
where
    M: KeyValueStore,
    C: ContentStore + 'static,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
{
    fn get_attributes(self: Arc<Self>, inode: Inode, reply: Reply<FSGetAttributesResult>) {
        let spawner = self.spawner.clone();
        spawn(&*spawner, async move {
            let found = self
                .engine
                .read_transact(|transaction| {
                    Box::pin(async move { transaction.getattr(inode).await })
                })
                .await;
            let error = match found {
                Ok(Some(attributes)) => {
                    return self.snapshots.reply(inode, attributes, |snapshot| {
                        // SAFETY: the snapshot is a live, fully populated object.
                        let result = unsafe {
                            FSGetAttributesResult::initWithAttributes(
                                FSGetAttributesResult::alloc(),
                                snapshot,
                            )
                        }
                        .expect("a get attributes result holds any attributes");
                        reply.call(Retained::as_ptr(&result).cast_mut(), null_mut());
                    });
                }
                Ok(None) => posix_error(errno(PosixError::ESTALE), "the item no longer exists"),
                Err(error) => failed(error),
            };
            reply.call(null_mut(), Retained::as_ptr(&error).cast_mut());
        });
    }
}

// spawn runs an operation on spawner. The spawner is taken separately from the volume, since the
// operation moves the volume in.
fn spawn(
    spawner: &(dyn Spawn + Send + Sync),
    operation: impl Future<Output = ()> + Send + 'static,
) {
    spawner
        .spawn(operation)
        .expect("the spawner accepts work while a volume is mounted");
}

// failed is the error FSKit reports for a failed transaction: the work's own POSIX error when it aborted,
// and EIO when the store refused or failed the transaction itself.
fn failed<R: Display, F: Display, E: Display>(error: TransactError<R, F, E>) -> Retained<NSError>
where
    for<'e> PosixError: From<&'e E>,
{
    let posix = match &error {
        TransactError::Aborted(work) => PosixError::from(work),
        TransactError::Rejected(_) | TransactError::Failed(_) => PosixError::EIO,
    };
    posix_error(errno(posix), &error.to_string())
}
