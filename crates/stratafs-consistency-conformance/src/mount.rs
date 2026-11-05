// A mount of a test volume: a real engine over the volume's shared store with a model of its kernel, whose operations
// call the engine as the FSKit volume does.

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_channel::oneshot;
use futures_util::future::try_join_all;
use stratafs_fs_engine::attributes::{Attributes, ChangeVersion, Mode};
use stratafs_fs_engine::content::error::ReadError;
use stratafs_fs_engine::directories::error::{LookupError, ReaddirError, RenameError};
use stratafs_fs_engine::directories::{NewItem, RenameMode};
use stratafs_fs_engine::error::{CorruptValue, EngineFailure, IoFailure, Posix, PosixError};
use stratafs_fs_engine::strong::{Pending, StrongFailure};
use stratafs_fs_engine::{
    Cached, Eventual, FsConfig, FsEngine, Inode, MountId, Purge, StrongUntilDisconnectCached,
    Timestamp,
};
use stratafs_pubsub_conformance::{MemoryClient, MemoryVolume};
use stratafs_runtime::Runtime;
use stratafs_store::kv::TransactError;

use crate::faults::{Faults, Link};
use crate::kernel::{Access, Kernel, KernelHandle, Reference, Update, Work};
use crate::simulation::History;
use crate::store::{InMemoryStore, SharedStore, new_store};

// PUBLISH_TIMEOUT is how long a strong mount's publish waits for every other holder to handle it.
const PUBLISH_TIMEOUT: Duration = Duration::from_secs(1);

// READ_LENGTH is how much a read asks for, more than any file holds, and LISTING_BATCH how many entries a listing
// reads, more than the directory holds.
const READ_LENGTH: u64 = 4096;
const LISTING_BATCH: usize = 64;

pub(crate) struct OneKibInline;

impl FsConfig for OneKibInline {
    const LARGEST_INLINE_ENTRY: usize = 1024;
}

type AllocateInode = Box<dyn Fn() -> u64 + Send + Sync>;

type Engine<C> = FsEngine<SharedStore, AllocateInode, fn() -> Timestamp, OneKibInline, C>;

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
        matches!(self, StrongFailure::Contended | StrongFailure::Publish(_))
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

// Volume is a test volume: its store and pub/sub, the runtime its mounts run on, and the history they write.
pub(crate) struct Volume {
    store: Arc<InMemoryStore>,
    pubsub: Arc<MemoryVolume<u64>>,
    pub(crate) runtime: Arc<dyn Runtime>,
    history: History,
    next_inode: Arc<AtomicU64>,
}

pub(crate) struct Mount<C: Consistency> {
    engine: Engine<C>,
    kernel: Arc<Kernel>,
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

    pub(crate) async fn mount<C: Consistency>(&self, index: usize) -> Mount<C> {
        let link = Arc::new(Link::new(index, &self.pubsub, &self.history));
        let faults = Arc::new(Faults::new(&link));
        let kernel = Arc::new(Kernel::new(&faults));
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
            KernelHandle(Arc::clone(&kernel)),
            Arc::clone(&self.runtime),
        )
        .await
        .unwrap_or_else(|_| panic!("mount {index} failed to mount"));
        Mount {
            engine,
            kernel,
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
fn failed<R, F, X: Posix + Uncommitted, E>(
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

// RenameFailure is why a rename's work failed, as the FSKit volume's: looking up the destination, finding something
// other than what the kernel found there, or the rename itself.
#[derive(Debug, thiserror::Error)]
enum RenameFailure<M, R, C> {
    #[error(transparent)]
    Lookup(LookupError<M, C>),
    #[error("the destination changed since the kernel looked it up")]
    DestinationChanged,
    #[error(transparent)]
    Rename(RenameError<M, R, C>),
}

impl<M, R, C> From<&RenameFailure<M, R, C>> for PosixError {
    fn from(failure: &RenameFailure<M, R, C>) -> Self {
        match failure {
            RenameFailure::Lookup(error) => PosixError::from(error),
            RenameFailure::DestinationChanged => PosixError::ESTALE,
            RenameFailure::Rename(error) => PosixError::from(error),
        }
    }
}

impl<C: Consistency> Mount<C> {
    // returned is what an operation returns once its reply took effect, as the kernel returns it only then.
    async fn returned<T>(&self, replied: Result<(T, u64), Failure>) -> Result<T, Failure> {
        let (returned, reply) = replied?;
        self.kernel.took_effect(reply).await;
        Ok(returned)
    }

    // resolve is the file name refers to in directory, or None for a free name, from the kernel's cache when it has
    // the name, or as the volume finds it, which the kernel caches with the file's attributes.
    pub(crate) async fn resolve(
        &self,
        directory: Inode,
        name: &'static str,
    ) -> Result<Option<Inode>, Failure> {
        if let Some(bound) = self.kernel.bound(directory, name).await {
            return Ok(bound);
        }
        let _looking_up = self.kernel.enter(Work::Lookup(directory)).await;
        let served = self
            .engine
            .serve(
                Purge::GoesAhead,
                |transaction| {
                    Box::pin(async move {
                        let lookup = transaction.lookup(directory, name.as_bytes()).await;
                        lookup.map(|lookup| (lookup, transaction))
                    })
                },
                |lookup| {
                    lookup
                        .found
                        .iter()
                        .cloned()
                        .chain([(directory, lookup.directory.clone())])
                        .collect()
                },
                |lookup, reads, vouch| {
                    let found = lookup.found.as_ref().map(|(inode, _)| *inode);
                    let replied = self.kernel.reply(
                        reads,
                        &[(directory, Update::Name(name, found))],
                        None,
                        vouch,
                    )?;
                    Some((found, replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // list is every entry of directory, by name, with the attributes handed over for it.
    pub(crate) async fn list(
        &self,
        directory: Inode,
    ) -> Result<Vec<(Bytes, Inode, Attributes)>, Failure> {
        self.kernel.enter(Work::Listing(directory)).await;
        let listed = self
            .engine
            .serve_listed(
                |transaction| {
                    Box::pin(async move {
                        transaction
                            .getattr(directory)
                            .await?
                            .ok_or(ReaddirError::StaleFileHandle)?;
                        let entries = transaction.readdir(directory, 0, LISTING_BATCH).await?;
                        let mut listed = Vec::new();
                        for entry in entries {
                            let attributes = transaction
                                .getattr(entry.inode)
                                .await?
                                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
                            listed.push((entry.name, entry.inode, attributes));
                        }
                        Ok::<_, ReaddirError<_, _>>((listed, transaction))
                    })
                },
                |listed| {
                    listed
                        .iter()
                        .map(|(_, inode, attributes)| (*inode, attributes.clone()))
                        .collect()
                },
                |listed, reads, vouch| {
                    let cached =
                        |inode, attributes: &Attributes| self.engine.cached(inode, attributes);
                    let (handed, reply) = self.kernel.reply_listed(reads, vouch, cached)?;
                    let listed = listed.iter().zip(handed);
                    let listed =
                        listed.map(|((name, inode, _), handed)| (name.clone(), *inode, handed));
                    Some((listed.collect(), reply))
                },
            )
            .await;
        self.returned(listed.map_err(failed)).await
    }

    // stat is a file's attributes, from the kernel's cache when it has them.
    pub(crate) async fn stat(&self, inode: Inode) -> Result<Attributes, Failure> {
        if let Some(cached) = self.kernel.cached(inode).await {
            return Ok(cached);
        }
        let operation = self.kernel.begin(inode, Access::Stat).await;
        let served = self
            .engine
            .serve(
                Purge::Waits,
                |transaction| {
                    Box::pin(async move {
                        let found = transaction.getattr(inode).await;
                        found.map(|found| (found, transaction))
                    })
                },
                |found| {
                    found
                        .iter()
                        .map(|attributes| (inode, attributes.clone()))
                        .collect()
                },
                |found, reads, vouch| match found {
                    Some(_) => self
                        .kernel
                        .reply(reads, &[], Some(&operation), vouch)
                        .map(|replied| Ok((replied.handed[&inode].clone(), replied.reply))),
                    None => Some(Err(Failure::Posix(PosixError::ESTALE))),
                },
            )
            .await;
        self.returned(served.map_err(failed)?).await
    }

    // read is a file's whole content, and the attributes handed over with it, from the kernel's cache when it has them.
    // UPSTREAM KERNEL BUG PIN: and whether it is a failed write's data the read is excused for.
    pub(crate) async fn read(&self, inode: Inode) -> Result<(Bytes, Attributes, bool), Failure> {
        if let Some(cached) = self.kernel.cached_content(inode).await {
            return Ok(cached);
        }
        let operation = self.kernel.begin(inode, Access::Read).await;
        let content = self.engine.content();
        let served = self
            .engine
            .serve_loaded(
                Purge::Waits,
                |transaction| {
                    Box::pin(async move {
                        let attributes = transaction.getattr(inode).await?;
                        let attributes = attributes.ok_or(ReadError::StaleFileHandle)?;
                        let plan = transaction.read(inode, 0, READ_LENGTH).await?;
                        Ok::<_, ReadError<_, _>>(((plan, attributes), transaction))
                    })
                },
                |(plan, attributes)| async move {
                    let pieces = try_join_all(plan.load(content))
                        .await
                        .map_err(|error| ReadError::from(IoFailure::ContentRead(error)))?;
                    let read: Vec<u8> = pieces
                        .into_iter()
                        .flat_map(|mut piece| piece.copy_to_bytes(piece.remaining()))
                        .collect();
                    Ok((Bytes::from(read), attributes))
                },
                |(_, attributes)| vec![(inode, attributes.clone())],
                |(read, _), reads, vouch| {
                    let content = [(inode, Update::Content(read.clone()))];
                    let replied = self
                        .kernel
                        .reply(reads, &content, Some(&operation), vouch)?;
                    Some((
                        (read.clone(), replied.handed[&inode].clone(), false),
                        replied.reply,
                    ))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // write writes data at the start of a file.
    pub(crate) async fn write(
        &self,
        inode: Inode,
        data: &'static [u8],
    ) -> Result<Attributes, Failure> {
        let operation = self.kernel.begin(inode, Access::Write(data)).await;
        let served = self
            .engine
            .transact_and_serve(
                Purge::Waits,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction.write(inode, 0, Bytes::from_static(data)).await;
                        result.map(|attributes| (attributes, transaction))
                    })
                },
                |attributes| vec![(inode, attributes.clone())],
                |_, reads, vouch| {
                    let replied = self.kernel.reply(reads, &[], Some(&operation), vouch)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        // UPSTREAM KERNEL BUG PIN: as the FSKit volume does, a failure returns as the purge dropping the write's data
        // is asked. This kernel holds no writes back.
        if served.is_err() {
            let (asked, replied) = oneshot::channel();
            let kernel = Arc::clone(&self.kernel);
            let asked = Box::new(move || {
                let _ = asked.send(());
            });
            self.runtime.spawn(Box::pin(async move {
                let _ = kernel
                    .invalidate(inode, Some((asked, Box::new(|_| {}))), true)
                    .await;
            }));
            let _ = replied.await;
        }
        self.returned(served.map_err(failed)).await
    }

    // touch sets a file's last access time.
    pub(crate) async fn touch(&self, inode: Inode, at: i64) -> Result<Attributes, Failure> {
        let operation = self.kernel.begin(inode, Access::Times).await;
        let served = self
            .engine
            .transact_and_serve(
                Purge::Waits,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction
                            .setattr(inode, |settable| settable.last_accessed_at = Timestamp(at))
                            .await;
                        result.map(|attributes| (attributes, transaction))
                    })
                },
                |attributes| vec![(inode, attributes.clone())],
                |_, reads, vouch| {
                    let replied = self.kernel.reply(reads, &[], Some(&operation), vouch)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // chmod sets a file's mode.
    pub(crate) async fn chmod(&self, inode: Inode, mode: u16) -> Result<Attributes, Failure> {
        let served = self
            .engine
            .transact_and_serve(
                Purge::GoesAhead,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction
                            .setattr(inode, |settable| {
                                settable.mode = Mode::from_bits_retain(mode)
                            })
                            .await;
                        result.map(|attributes| (attributes, transaction))
                    })
                },
                |attributes| vec![(inode, attributes.clone())],
                |_, reads, vouch| {
                    let replied = self.kernel.reply(reads, &[], None, vouch)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // create makes an item named name in directory, and is the inode its last attempt made, if it got as far.
    pub(crate) async fn create(
        &self,
        directory: Inode,
        name: &'static str,
        item: NewItem,
    ) -> (Option<Inode>, Result<(Inode, Attributes), Failure>) {
        let _changing = self.kernel.enter(Work::Change(vec![directory])).await;
        let made = Arc::new(Mutex::new(None));
        let created = self
            .engine
            .transact_and_serve(
                Purge::GoesAhead,
                |mut transaction| {
                    let (item, made) = (item.clone(), Arc::clone(&made));
                    Box::pin(async move {
                        let result = transaction
                            .create(directory, name.as_bytes(), item, |_| {})
                            .await;
                        let inode = result.as_ref().ok().map(|created| created.inode);
                        *made.lock().unwrap_or_else(PoisonError::into_inner) = inode;
                        result.map(|created| (created, transaction))
                    })
                },
                |created| {
                    vec![
                        (created.inode, created.item.clone()),
                        (directory, created.parent.clone()),
                    ]
                },
                |created, reads, vouch| {
                    let inode = created.inode;
                    let binds = [(directory, Update::Name(name, Some(inode)))];
                    let replied = self.kernel.reply(reads, &binds, None, vouch)?;
                    Some(((inode, replied.handed[&inode].clone()), replied.reply))
                },
            )
            .await;
        let created = self.returned(created.map_err(failed)).await;
        let made = *made.lock().unwrap_or_else(PoisonError::into_inner);
        (made, created)
    }

    // unlink removes name from directory, which refers to inode.
    pub(crate) async fn unlink(
        &self,
        directory: Inode,
        name: &'static str,
        inode: Inode,
    ) -> Result<Attributes, Failure> {
        let _changing = self.kernel.enter(Work::Change(vec![directory])).await;
        let _unlinking = self.kernel.begin(inode, Access::Unlink).await;
        let served = self
            .engine
            .transact_and_serve(
                Purge::GoesAhead,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction
                            .hard_unlink(directory, name.as_bytes(), Some(inode))
                            .await;
                        result.map(|removed| (removed, transaction))
                    })
                },
                |removed| {
                    vec![
                        (inode, removed.item.clone()),
                        (directory, removed.parent.clone()),
                    ]
                },
                |_, reads, vouch| {
                    let replied = self.kernel.reply(
                        reads,
                        &[(directory, Update::Name(name, None))],
                        None,
                        vouch,
                    )?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // rename moves inode from one name in a directory to another, replacing the file the kernel found there.
    pub(crate) async fn rename(
        &self,
        (from_directory, from): (Inode, &'static str),
        (to_directory, to): (Inode, &'static str),
        inode: Inode,
        replaced: Option<Inode>,
    ) -> Result<Attributes, Failure> {
        let directories = vec![from_directory, to_directory];
        let _changing = self.kernel.enter(Work::Change(directories)).await;
        let _renaming = self.kernel.begin(inode, Access::Unlink).await;
        let _replacing = match replaced {
            Some(replaced) => Some(self.kernel.begin(replaced, Access::Unlink).await),
            None => None,
        };
        let served = self
            .engine
            .transact_and_serve(
                Purge::GoesAhead,
                |mut transaction| {
                    Box::pin(async move {
                        let at_destination = transaction
                            .lookup(to_directory, to.as_bytes())
                            .await
                            .map_err(RenameFailure::Lookup)?;
                        if at_destination.found.map(|(inode, _)| inode) != replaced {
                            return Err(RenameFailure::DestinationChanged);
                        }
                        let result = transaction
                            .rename(
                                from_directory,
                                from.as_bytes(),
                                to_directory,
                                to.as_bytes(),
                                RenameMode::Replace,
                                Some(inode),
                            )
                            .await
                            .map_err(RenameFailure::Rename);
                        result.map(|renamed| (renamed, transaction))
                    })
                },
                |renamed| {
                    [
                        (inode, renamed.item.clone()),
                        (from_directory, renamed.from.clone()),
                        (to_directory, renamed.to.clone()),
                    ]
                    .into_iter()
                    .chain(renamed.replaced.clone())
                    .collect()
                },
                |_, reads, vouch| {
                    let binds = [
                        (from_directory, Update::Name(from, None)),
                        (to_directory, Update::Name(to, Some(inode))),
                    ];
                    let replied = self.kernel.reply(reads, &binds, None, vouch)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // link gives inode another name, name in directory.
    pub(crate) async fn link(
        &self,
        inode: Inode,
        directory: Inode,
        name: &'static str,
    ) -> Result<Attributes, Failure> {
        let _changing = self.kernel.enter(Work::Change(vec![directory])).await;
        let served = self
            .engine
            .transact_and_serve(
                Purge::GoesAhead,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction
                            .hard_link(inode, directory, name.as_bytes())
                            .await;
                        result.map(|linked| (linked, transaction))
                    })
                },
                |linked| {
                    vec![
                        (inode, linked.item.clone()),
                        (directory, linked.parent.clone()),
                    ]
                },
                |_, reads, vouch| {
                    let binds = [(directory, Update::Name(name, Some(inode)))];
                    let replied = self.kernel.reply(reads, &binds, None, vouch)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    pub(crate) fn refer(&self, inode: Inode) -> Reference {
        self.kernel.refer(inode)
    }

    // let_go lets go of a file as the kernel does once no operation refers to it, and says whether it did.
    pub(crate) fn let_go(&self, inode: Inode) -> bool {
        self.kernel.reclaim(inode, || self.engine.forget(inode))
    }

    // delete_if_unlinked deletes a file the kernel let go of if no name refers to it, as the FSKit volume does.
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
