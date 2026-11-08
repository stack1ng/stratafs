// A mount of a test volume: a real engine over the volume's shared store with a model of its kernel, whose operations
// call the engine as that kernel's integration does. What every kernel's mount shares is here.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_util::future::try_join_all;
use stratafs_fs_engine::attributes::{Attributes, ChangeVersion};
use stratafs_fs_engine::content::error::ReadError;
use stratafs_fs_engine::directories::NewItem;
use stratafs_fs_engine::error::{EngineFailure, Posix, PosixError};
use stratafs_fs_engine::kernel_cache::KernelCache;
use stratafs_fs_engine::strong::{Pending, StrongFailure};
use stratafs_fs_engine::{
    Cached, Eventual, FsConfig, FsEngine, Inode, MountId, Purge, StrongUntilDisconnectCached,
    Timestamp,
};
use stratafs_pubsub_conformance::{MemoryClient, MemoryVolume};
use stratafs_runtime::Runtime;
use stratafs_store::kv::TransactError;

use crate::faults::{Faults, Link};
use crate::simulation::History;
use crate::store::{InMemoryStore, SharedStore, new_store};

// PUBLISH_TIMEOUT is how long a strong mount's publish waits for every other holder to handle it.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);

// READ_LENGTH is how much a read asks for, more than any file holds, and LISTING_BATCH how many entries a listing
// reads, more than the directory holds.
pub(crate) const READ_LENGTH: u64 = 4096;
pub(crate) const LISTING_BATCH: usize = 64;

pub(crate) struct OneKibInline;

impl FsConfig for OneKibInline {
    const LARGEST_INLINE_ENTRY: usize = 1024;
}

type AllocateInode = Box<dyn Fn() -> u64 + Send + Sync>;

pub(crate) type Engine<C> =
    FsEngine<SharedStore, AllocateInode, fn() -> Timestamp, OneKibInline, C>;

// Consistency is a consistency under test, which each mount of a volume gets its own of.
pub(crate) trait Consistency: Cached<SharedStore, Failure: Uncommitted> {
    fn new(volume: &Volume, faults: &Arc<Faults>) -> Self;
    // pending is every pending change version of state, the consistency's own state of some inodes.
    fn pending(state: &Self::InodeState) -> Vec<(Inode, ChangeVersion)>;
}

// Uncommitted is a consistency's failure, which says whether the write it failed committed nothing.
pub(crate) trait Uncommitted {
    fn uncommitted(&self) -> bool;
}

impl Consistency for StrongUntilDisconnectCached<MemoryClient<u64>> {
    fn new(_: &Volume, faults: &Arc<Faults>) -> Self {
        let faults = Arc::clone(faults);
        StrongUntilDisconnectCached::new(
            move || {
                let faults = Arc::clone(&faults);
                Box::pin(async move { faults.connect().await })
            },
            PUBLISH_TIMEOUT,
        )
    }

    fn pending(state: &Pending) -> Vec<(Inode, ChangeVersion)> {
        state
            .iter()
            .map(|(inode, version)| (*inode, *version))
            .collect()
    }
}

impl<F, R, M> Uncommitted for StrongFailure<F, R, M> {
    fn uncommitted(&self) -> bool {
        matches!(
            self,
            StrongFailure::Contended | StrongFailure::Publish(_) | StrongFailure::Uninvalidated(_)
        )
    }
}

impl Consistency for Eventual {
    fn new(_: &Volume, _: &Arc<Faults>) -> Self {
        Eventual::default()
    }

    fn pending((): &()) -> Vec<(Inode, ChangeVersion)> {
        Vec::new()
    }
}

impl Uncommitted for Infallible {
    fn uncommitted(&self) -> bool {
        match *self {}
    }
}

// KernelModel is a model of one mount's kernel.
pub(crate) trait KernelModel: Sized + Send + Sync + 'static {
    // UPSTREAM KERNEL BUG PIN: TAINTED is what the reads the check leaves out were answered with.
    const TAINTED: &'static str;
    // FINDS_NAMES says whether the kernel finds the names a change works on under their directories' locks, failing
    // it before it asks the volume.
    const FINDS_NAMES: bool;
    // LISTS_ATTRIBUTES says whether a listing shows the app each entry's attributes, or only its name and inode.
    const LISTS_ATTRIBUTES: bool;
    // STATS_BEFORE_COPYING says whether a read stats the file at one moment and copies the data at a later one, rather
    // than taking both from one reply.
    const STATS_BEFORE_COPYING: bool;
    // REGRESSIONS is the file the checks of mounts with this kernel keep the failing cases they found in, so they
    // replay only their own.
    const REGRESSIONS: &'static str;

    fn new(faults: &Arc<Faults>, references: &Arc<References>) -> Self;
    // cache is the kernel as the engine reaches it.
    fn cache(self: &Arc<Self>) -> impl KernelCache;
}

// Operations is what an app asks of a mount, which calls the engine as its kernel's integration does.
pub(crate) trait Operations: Send + Sync {
    // resolve is the file name refers to in directory, or None for a free name, and whether the check leaves it out.
    fn resolve(
        &self,
        directory: Inode,
        name: &'static str,
    ) -> impl Future<Output = Result<(Option<Inode>, bool), Failure>> + Send;
    fn list(
        &self,
        directory: Inode,
    ) -> impl Future<Output = Result<Vec<(Bytes, Inode, Attributes)>, Failure>> + Send;
    // stat is a file's attributes, and whether the check leaves them out.
    fn stat(
        &self,
        inode: Inode,
    ) -> impl Future<Output = Result<(Attributes, bool), Failure>> + Send;
    // read is a file's whole content and the attributes handed over with it, and whether the check leaves it out.
    fn read(
        &self,
        inode: Inode,
    ) -> impl Future<Output = Result<(Bytes, Attributes, bool), Failure>> + Send;
    fn write(
        &self,
        inode: Inode,
        data: &'static [u8],
    ) -> impl Future<Output = Result<Attributes, Failure>> + Send;
    fn touch(
        &self,
        inode: Inode,
        at: i64,
    ) -> impl Future<Output = Result<Attributes, Failure>> + Send;
    fn chmod(
        &self,
        inode: Inode,
        mode: u16,
    ) -> impl Future<Output = Result<Attributes, Failure>> + Send;
    // create is the inode its last attempt made, if it got as far.
    fn create(
        &self,
        directory: Inode,
        name: &'static str,
        item: NewItem,
    ) -> impl Future<Output = (Option<Inode>, Result<(Inode, Attributes), Failure>)> + Send;
    // unlink removes name from directory, which refers to inode.
    fn unlink(
        &self,
        directory: Inode,
        name: &'static str,
        inode: Inode,
    ) -> impl Future<Output = Result<Attributes, Failure>> + Send;
    // rename moves inode from one name in a directory to another, where the app found replaced, and is the file it
    // replaced, which the integration may find there instead.
    fn rename(
        &self,
        from: (Inode, &'static str),
        to: (Inode, &'static str),
        inode: Inode,
        replaced: Option<Inode>,
    ) -> impl Future<Output = Result<(Attributes, Option<Inode>), Failure>> + Send;
    // link gives inode another name, name in directory.
    fn link(
        &self,
        inode: Inode,
        directory: Inode,
        name: &'static str,
    ) -> impl Future<Output = Result<Attributes, Failure>> + Send;
    // let_go lets go of a file once no operation refers to it, and says whether the volume forgot it.
    fn let_go(&self, inode: Inode) -> bool;
}

// Volume is a test volume: its store and pub/sub, the runtime its mounts run on, and the history they write.
pub(crate) struct Volume {
    store: Arc<InMemoryStore>,
    pubsub: Arc<MemoryVolume<u64>>,
    pub(crate) runtime: Arc<dyn Runtime>,
    history: History,
    next_inode: Arc<AtomicU64>,
}

pub(crate) struct Mount<C: Consistency, K> {
    pub(crate) engine: Engine<C>,
    pub(crate) kernel: Arc<K>,
    references: Arc<References>,
    pub(crate) faults: Arc<Faults>,
    pub(crate) runtime: Arc<dyn Runtime>,
}

// Failure is how an operation failed: with the error its work failed with, committing nothing for another reason, or
// in a way that may have committed.
pub(crate) enum Failure {
    Posix(PosixError),
    Unchanged(PosixError),
    Unknown(PosixError),
}

// References counts the operations referring to each item, which the kernel does not reclaim meanwhile.
#[derive(Default)]
pub(crate) struct References(Mutex<HashMap<Inode, usize>>);

// Reference marks an operation referring to an item, until it is dropped.
pub(crate) struct Reference {
    references: Arc<References>,
    inode: Inode,
}

impl Volume {
    pub(crate) async fn new(runtime: Arc<dyn Runtime>, history: &History) -> Self {
        Volume {
            store: new_store().await,
            pubsub: Arc::new(MemoryVolume::new(Arc::clone(&runtime))),
            runtime,
            history: History::clone(history),
            next_inode: Arc::new(AtomicU64::new(1)),
        }
    }

    pub(crate) async fn mount<C: Consistency, K: KernelModel>(&self, index: usize) -> Mount<C, K> {
        let link = Arc::new(Link::new(index, &self.pubsub, &self.history));
        let faults = Arc::new(Faults::new(&link));
        let references = Arc::new(References::default());
        let kernel = Arc::new(K::new(&faults, &references));
        let next_inode = Arc::clone(&self.next_inode);
        let allocate_inode: AllocateInode =
            Box::new(move || next_inode.fetch_add(1, Ordering::SeqCst));
        let engine = FsEngine::new(
            SharedStore::new(&self.store, &faults),
            allocate_inode,
            epoch as fn() -> Timestamp,
            MountId(index as u64 + 1),
            None,
            C::new(self, &faults),
            kernel.cache(),
            Arc::clone(&self.runtime),
        )
        .await
        .unwrap_or_else(|_| panic!("mount {index} failed to mount"));
        Mount {
            engine,
            kernel,
            references,
            faults,
            runtime: Arc::clone(&self.runtime),
        }
    }

    // inodes is the root and every inode the volume's mounts allocated.
    pub(crate) fn inodes(&self) -> Vec<Inode> {
        (0..self.next_inode.load(Ordering::SeqCst))
            .map(Inode)
            .collect()
    }
}

// epoch is every time the engine stamps, so the times an app sees are the ones it set.
fn epoch() -> Timestamp {
    Timestamp(0)
}

// failed is how an operation that failed with error failed.
pub(crate) fn failed<R, F, X: Posix + Uncommitted, E>(
    error: TransactError<R, EngineFailure<F, X>, E>,
) -> Failure
where
    for<'e> PosixError: From<&'e E>,
{
    match error {
        TransactError::Aborted(work) => match PosixError::from(&work) {
            PosixError::EIO => Failure::Unchanged(PosixError::EIO),
            posix => Failure::Posix(posix),
        },
        TransactError::Failed(EngineFailure::Consistency(failure)) if failure.uncommitted() => {
            Failure::Unchanged(failure.posix())
        }
        TransactError::Failed(EngineFailure::Consistency(failure)) => {
            Failure::Unknown(failure.posix())
        }
        TransactError::Failed(EngineFailure::Store(_)) | TransactError::Rejected(_) => {
            Failure::Unknown(PosixError::EIO)
        }
    }
}

impl<C: Consistency, K> Mount<C, K> {
    pub(crate) fn refer(&self, inode: Inode) -> Reference {
        self.references.refer(inode)
    }

    // delete_if_unlinked deletes a file the kernel let go of if no name refers to it, as the integration does.
    pub(crate) async fn delete_if_unlinked(&self, inode: Inode) -> Result<(), Failure> {
        self.engine
            .transact(Purge::GoesAhead, |mut transaction| {
                Box::pin(async move {
                    let result = transaction.delete_if_unlinked(inode).await;
                    result.map(|()| ((), transaction))
                })
            })
            .await
            .map_err(failed)
    }

    // live_pending is each of inodes whose pending change version is ahead of its stored change version, with the mount
    // that announced it, read past the kernel.
    pub(crate) async fn live_pending(
        &self,
        inodes: &[Inode],
    ) -> Result<Vec<(Inode, MountId)>, Failure> {
        let state = self.engine.inode_state(inodes).await;
        let state = state.map_err(|_| Failure::Unknown(PosixError::EIO))?;
        let mut live = Vec::new();
        for (inode, version) in C::pending(&state) {
            let stored = self
                .engine
                .read_transact(|transaction| {
                    Box::pin(async move { transaction.getattr(inode).await })
                })
                .await
                .map_err(|_| Failure::Unknown(PosixError::EIO))?;
            if stored.is_some_and(|stored| version.number > stored.change_version.number) {
                live.push((inode, version.mount));
            }
        }
        Ok(live)
    }

    // stored_lookup is the file name refers to in directory, read past the kernel and the consistency.
    pub(crate) async fn stored_lookup(
        &self,
        directory: Inode,
        name: &'static str,
    ) -> Result<Option<Inode>, Failure> {
        self.engine
            .read_transact(|transaction| {
                Box::pin(async move {
                    let lookup = transaction.lookup(directory, name.as_bytes()).await;
                    lookup.map(|lookup| lookup.found.map(|(inode, _)| inode))
                })
            })
            .await
            .map_err(|_| Failure::Unknown(PosixError::EIO))
    }

    // stored_read is a file's whole content and attributes, read past the kernel and the consistency.
    pub(crate) async fn stored_read(&self, inode: Inode) -> Result<(Bytes, Attributes), Failure> {
        let content = self.engine.content();
        let (plan, attributes) = self
            .engine
            .read_transact(|transaction| {
                Box::pin(async move {
                    let attributes = transaction.getattr(inode).await?;
                    let attributes = attributes.ok_or(ReadError::StaleFileHandle)?;
                    let plan = transaction.read(inode, 0, READ_LENGTH).await?;
                    Ok::<_, ReadError<_, _>>((plan, attributes))
                })
            })
            .await
            .map_err(|_| Failure::Unknown(PosixError::EIO))?;
        let pieces = try_join_all(plan.load(content))
            .await
            .map_err(|_| Failure::Unknown(PosixError::EIO))?;
        let read: Vec<u8> = pieces
            .into_iter()
            .flat_map(|mut piece| piece.copy_to_bytes(piece.remaining()))
            .collect();
        Ok((Bytes::from(read), attributes))
    }
}

impl References {
    fn refer(self: &Arc<Self>, inode: Inode) -> Reference {
        *self
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(inode)
            .or_default() += 1;
        Reference {
            references: Arc::clone(self),
            inode,
        }
    }

    // referred says whether an operation refers to inode.
    pub(crate) fn referred(&self, inode: Inode) -> bool {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&inode)
    }
}

impl Drop for Reference {
    fn drop(&mut self) {
        let mut references = self
            .references
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let counted = references
            .get_mut(&self.inode)
            .expect("a reference is counted until it is dropped");
        *counted -= 1;
        if *counted == 0 {
            references.remove(&self.inode);
        }
    }
}
