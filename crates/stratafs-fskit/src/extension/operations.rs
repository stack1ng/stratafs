use std::fmt::Display;
use std::ptr::null_mut;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Weak};
use std::task::{Context, Waker};
use std::time::Duration;

use block2::RcBlock;
use bytes::{Buf, Bytes};
use futures_lite::future::block_on;
use futures_util::future::{join, try_join_all};
use futures_util::task::{Spawn, SpawnExt};
use objc2::rc::Retained;
use objc2::{AllocAnyThread, Message, msg_send};
use objc2_foundation::{NSArray, NSData, NSError};
use objc2_fs_kit::{
    FSCreateItemResult, FSCreateLinkResult, FSCreateSymlinkResult, FSDirectoryCookie,
    FSDirectoryEntryPacker, FSDirectoryVerifier, FSEnumerateDirectoryResult, FSFreeSpace,
    FSGetAttributesResult, FSGetXattrResult, FSItemType, FSListXattrsResult, FSLookupItemResult,
    FSMutableFileDataBuffer, FSReadFileResult, FSReadSymlinkResult, FSRemoveItemResult,
    FSRenameItemResult, FSSetAttributesResult, FSSetXattrPolicy, FSSetXattrResult,
    FSWriteFileResult,
};
use stratafs_fs_engine::annotations::error::{GetAnnotationError, ListAnnotationsError};
use stratafs_fs_engine::annotations::{AnnotationKey, AnnotationScope, SetAnnotationMode};
use stratafs_fs_engine::attributes::Attributes;
use stratafs_fs_engine::attributes::FileType;
use stratafs_fs_engine::content::error::{ReadError, ReadSymbolicLinkError};
use stratafs_fs_engine::directories::error::{LookupError, ReaddirError, RenameError};
use stratafs_fs_engine::directories::{NewItem, RenameMode};
use stratafs_fs_engine::error::{CorruptValue, IoFailure, PosixError};
use stratafs_fs_engine::{Cached, FsConfig, FsEngine, Inode, Timestamp};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::kv::TransactError;
use stratafs_store::store::Store;

use super::changes::AttributeChange;
use super::item::{file_name, item_id};
use super::items::{Items, Locked};
use super::{errno, posix_error};

// Reply is an FSKit reply block, copied so an operation can call it once it finishes.
pub(crate) type Reply<R> = RcBlock<'static, fn(*mut R, *mut NSError), dyn Send + Sync>;

// Handed carries an object FSKit handed an operation to fill, such as a directory entry packer or a read
// buffer, to the thread the operation finishes on.
pub(crate) struct Handed<T>(pub(crate) Retained<T>);

// SAFETY: FSKit keeps these objects valid until the operation replies, and only that one operation uses
// them, from whichever thread it runs on, as Swift FSKit modules do from their task threads.
unsafe impl<T> Send for Handed<T> {}
unsafe impl<T> Sync for Handed<T> {}

impl<T> Handed<T> {
    // object is the object FSKit handed over. Going through a method makes a future capture the whole
    // Handed, not just the object, which is not Send.
    pub(crate) fn object(&self) -> &T {
        &self.0
    }
}

// VolumeOperations are a mounted volume's FSKit operations. StrataVolume, which FSKit calls, cannot be
// generic, so it reaches the Volume holding the consumer's engine through this trait.
pub(crate) trait VolumeOperations: Send + Sync {
    fn items(&self) -> &Items;

    fn get_attributes(self: Arc<Self>, inode: Inode, reply: Reply<FSGetAttributesResult>);

    fn set_attributes(
        self: Arc<Self>,
        inode: Inode,
        change: AttributeChange,
        reply: Reply<FSSetAttributesResult>,
    );

    fn lookup(self: Arc<Self>, directory: Inode, name: Bytes, reply: Reply<FSLookupItemResult>);

    fn read_symbolic_link(self: Arc<Self>, inode: Inode, reply: Reply<FSReadSymlinkResult>);

    fn create(
        self: Arc<Self>,
        directory: Inode,
        name: Bytes,
        item: NewItem,
        change: AttributeChange,
        reply: Reply<FSCreateItemResult>,
    );

    fn create_symbolic_link(
        self: Arc<Self>,
        directory: Inode,
        name: Bytes,
        target: Bytes,
        change: AttributeChange,
        reply: Reply<FSCreateSymlinkResult>,
    );

    fn create_link(
        self: Arc<Self>,
        inode: Inode,
        directory: Inode,
        name: Bytes,
        reply: Reply<FSCreateLinkResult>,
    );

    fn remove(
        self: Arc<Self>,
        inode: Inode,
        kind: FileType,
        directory: Inode,
        name: Bytes,
        reply: Reply<FSRemoveItemResult>,
    );

    // rename moves inode from one name to another, replacing the item FSKit expects at the destination,
    // or nothing if it expects nothing there.
    fn rename(
        self: Arc<Self>,
        inode: Inode,
        from: (Inode, Bytes),
        to: (Inode, Bytes),
        replaced: Option<Inode>,
        reply: Reply<FSRenameItemResult>,
    );

    fn enumerate_directory(
        self: Arc<Self>,
        directory: Inode,
        cookie: FSDirectoryCookie,
        with_attributes: bool,
        packer: Handed<FSDirectoryEntryPacker>,
        reply: Reply<FSEnumerateDirectoryResult>,
    );

    fn read(
        self: Arc<Self>,
        inode: Inode,
        offset: u64,
        length: usize,
        buffer: Handed<FSMutableFileDataBuffer>,
        reply: Reply<FSReadFileResult>,
    );

    fn write(
        self: Arc<Self>,
        inode: Inode,
        offset: u64,
        contents: Bytes,
        reply: Reply<FSWriteFileResult>,
    );

    fn get_xattr(self: Arc<Self>, inode: Inode, name: Bytes, reply: Reply<FSGetXattrResult>);

    fn set_xattr(
        self: Arc<Self>,
        inode: Inode,
        name: Bytes,
        value: Option<Bytes>,
        policy: FSSetXattrPolicy,
        reply: Reply<FSSetXattrResult>,
    );

    fn list_xattrs(self: Arc<Self>, inode: Inode, reply: Reply<FSListXattrsResult>);

    // delete_if_unlinked deletes inode, which FSKit no longer holds, if no name refers to it. Nothing waits on it,
    // so it logs a failure.
    fn delete_if_unlinked(self: Arc<Self>, inode: Inode);

    // forget tells the engine the kernel no longer holds inode.
    fn forget(&self, inode: Inode);
}

// Volume is a mounted volume's engine. Each operation replies when it finishes, on FSKit's thread unless it
// has to wait, and then on spawner.
pub(crate) struct Volume<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    pub(crate) engine: FsEngine<St, A, N, S, C>,
    pub(crate) spawner: Arc<dyn Spawn + Send + Sync>,
    pub(crate) items: Items,
    // stops_durability_cadence stops the volume's durability cadence when the volume is dropped.
    pub(crate) stops_durability_cadence: mpsc::Sender<()>,
}

// make_durable_on_cadence makes the volume durable against a power failure every period, on a thread of its own,
// until the volume is dropped.
pub(crate) fn make_durable_on_cadence<St, A, N, S, C>(
    volume: Weak<Volume<St, A, N, S, C>>,
    stopped: mpsc::Receiver<()>,
    period: Duration,
) where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    std::thread::Builder::new()
        .name("durability cadence".to_owned())
        .spawn(move || {
            while let Err(RecvTimeoutError::Timeout) = stopped.recv_timeout(period) {
                let Some(volume) = volume.upgrade() else {
                    return;
                };
                if let Err(error) =
                    block_on(volume.engine.ensure_durable(DurableAgainst::PowerFailure))
                {
                    tracing::warn!(%error, "making the volume durable on its cadence failed");
                }
            }
        })
        .expect("the operating system starts the durability cadence's thread");
}

// Dropping the volume, which happens after its last operation, makes every commit durable, up to the engine's
// durability limit.
impl<St, A, N, S, C> Drop for Volume<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    fn drop(&mut self) {
        // The cadence's thread has stopped when the send fails.
        let _ = self.stops_durability_cadence.send(());
        if let Err(error) = block_on(self.engine.ensure_durable(DurableAgainst::PowerFailure)) {
            tracing::warn!(%error, "making the volume durable before closing it failed");
        }
    }
}

// RenameFailure is why a rename's transaction failed: looking up the destination, finding something other
// than what FSKit expects there, or the rename itself.
#[derive(Debug, thiserror::Error)]
enum RenameFailure<M, R, C> {
    #[error(transparent)]
    Lookup(LookupError<M, C>),
    #[error("ESTALE: the destination changed since FSKit last saw it")]
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

// LISTING_BATCH is how many directory entries enumerate reads per transaction while it fills a packer.
const LISTING_BATCH: usize = 64;

// VERIFIER is every listing's verifier, nonzero as FSKit requires. A cookie is an entry's position, which never
// moves while the entry exists, so a listing can carry on from its cookie however the directory changes, and
// FSKit never needs to start it over.
const VERIFIER: FSDirectoryVerifier = 1;

impl<St, A, N, S, C> VolumeOperations for Volume<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    fn items(&self) -> &Items {
        &self.items
    }

    fn get_attributes(self: Arc<Self>, inode: Inode, reply: Reply<FSGetAttributesResult>) {
        self.run(move |volume| async move {
            let found = volume
                .engine
                .read_transact(|transaction| {
                    Box::pin(async move { transaction.getattr(inode).await })
                })
                .await;
            match found {
                Ok(Some(attributes)) => volume.reply([(inode, attributes)], |locked| {
                    // SAFETY: the attributes are a live, fully populated object.
                    succeed(&reply, unsafe {
                        FSGetAttributesResult::initWithAttributes(
                            FSGetAttributesResult::alloc(),
                            &locked.attributes(inode),
                        )
                    })
                }),
                Ok(None) => fail(&reply, stale()),
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn set_attributes(
        self: Arc<Self>,
        inode: Inode,
        change: AttributeChange,
        reply: Reply<FSSetAttributesResult>,
    ) {
        self.run(move |volume| async move {
            let changed = volume
                .engine
                .transact(|mut transaction| {
                    let change = change.clone();
                    Box::pin(async move {
                        let result = transaction
                            .setattr(inode, move |settable| change(settable))
                            .await;
                        result.map(|attributes| (attributes, transaction))
                    })
                })
                .await;
            match changed {
                Ok(attributes) => volume.reply([(inode, attributes)], |locked| {
                    // SAFETY: the attributes are a live, fully populated object.
                    succeed(&reply, unsafe {
                        FSSetAttributesResult::initWithAttributes_freeSpace(
                            FSSetAttributesResult::alloc(),
                            &locked.attributes(inode),
                            Some(&no_free_space_update()),
                        )
                    })
                }),
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn lookup(self: Arc<Self>, directory: Inode, name: Bytes, reply: Reply<FSLookupItemResult>) {
        self.run(move |volume| async move {
            let found = volume
                .engine
                .read_transact(|transaction| {
                    let name = name.clone();
                    Box::pin(async move { transaction.lookup(directory, &name).await })
                })
                .await;
            match found {
                Ok(lookup) => {
                    match lookup.found {
                        Some((inode, attributes)) => {
                            volume.reply([(inode, attributes)], |locked| {
                                let item = locked.item(inode);
                                // SAFETY: the item, name and attributes are live objects.
                                succeed(&reply, unsafe {
                                    FSLookupItemResult::initWithFoundItem_itemName_itemAttributes(
                                        FSLookupItemResult::alloc(),
                                        &item,
                                        &file_name(&name),
                                        &locked.attributes(inode),
                                    )
                                })
                            })
                        }
                        None => fail(
                            &reply,
                            posix_error(errno(PosixError::ENOENT), "no item has that name"),
                        ),
                    }
                    // The engine learns the kernel caches the directory once the kernel has the answer, so an
                    // invalidation reaches the kernel after it.
                    volume.engine.cached(directory, &lookup.directory);
                }
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn read_symbolic_link(self: Arc<Self>, inode: Inode, reply: Reply<FSReadSymlinkResult>) {
        self.run(move |volume| async move {
            let read = volume
                .engine
                .read_transact(|transaction| {
                    Box::pin(async move {
                        let (target, attributes) = join(
                            transaction.read_symbolic_link(inode),
                            transaction.getattr(inode),
                        )
                        .await;
                        let mut target = target?;
                        let attributes =
                            attributes?.ok_or(ReadSymbolicLinkError::StaleFileHandle)?;
                        Ok::<_, ReadSymbolicLinkError<_, _>>((
                            target.copy_to_bytes(target.remaining()),
                            attributes,
                        ))
                    })
                })
                .await;
            match read {
                Ok((target, attributes)) => volume.reply([(inode, attributes)], |locked| {
                    // SAFETY: the target and attributes are live objects.
                    succeed(&reply, unsafe {
                        FSReadSymlinkResult::initWithContents_symlinkAttributes(
                            FSReadSymlinkResult::alloc(),
                            &file_name(&target),
                            &locked.attributes(inode),
                        )
                    })
                }),
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn create(
        self: Arc<Self>,
        directory: Inode,
        name: Bytes,
        item: NewItem,
        change: AttributeChange,
        reply: Reply<FSCreateItemResult>,
    ) {
        self.run(move |volume| async move {
            let created = volume
                .engine
                .transact(|mut transaction| {
                    let (name, item, change) = (name.clone(), item.clone(), change.clone());
                    Box::pin(async move {
                        let result = transaction
                            .create(directory, &name, item, move |settable| change(settable))
                            .await;
                        result.map(|created| (created, transaction))
                    })
                })
                .await;
            match created {
                Ok(created) => {
                    let inode = created.inode;
                    volume.reply(
                        [(inode, created.item), (directory, created.parent)],
                        |locked| {
                            let item = locked.item(inode);
                            // SAFETY: the item, name and attributes are live objects.
                            succeed(&reply, unsafe {
                                FSCreateItemResult::initWithNewItem_newItemName_newItemAttributes_directoryAttributes_freeSpace(
                                    FSCreateItemResult::alloc(),
                                    &item,
                                    &file_name(&name),
                                    &locked.attributes(inode),
                                    &locked.attributes(directory),
                                    Some(&no_free_space_update()),
                                )
                            })
                        },
                    )
                }
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn create_symbolic_link(
        self: Arc<Self>,
        directory: Inode,
        name: Bytes,
        target: Bytes,
        change: AttributeChange,
        reply: Reply<FSCreateSymlinkResult>,
    ) {
        self.run(move |volume| async move {
            let created = volume
                .engine
                .transact(|mut transaction| {
                    let (name, target, change) = (name.clone(), target.clone(), change.clone());
                    Box::pin(async move {
                        let result = transaction
                            .create(directory, &name, NewItem::SymbolicLink(target), move |settable| change(settable))
                            .await;
                        result.map(|created| (created, transaction))
                    })
                })
                .await;
            match created {
                Ok(created) => {
                    let inode = created.inode;
                    volume.reply(
                        [(inode, created.item), (directory, created.parent)],
                        |locked| {
                            let item = locked.item(inode);
                            // SAFETY: the item, name and attributes are live objects.
                            succeed(&reply, unsafe {
                                FSCreateSymlinkResult::initWithNewItem_newItemName_newItemAttributes_directoryAttributes_freeSpace(
                                    FSCreateSymlinkResult::alloc(),
                                    &item,
                                    &file_name(&name),
                                    &locked.attributes(inode),
                                    &locked.attributes(directory),
                                    Some(&no_free_space_update()),
                                )
                            })
                        },
                    )
                }
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn create_link(
        self: Arc<Self>,
        inode: Inode,
        directory: Inode,
        name: Bytes,
        reply: Reply<FSCreateLinkResult>,
    ) {
        self.run(move |volume| async move {
            let linked = volume
                .engine
                .transact(|mut transaction| {
                    let name = name.clone();
                    Box::pin(async move {
                        let result = transaction.hard_link(inode, directory, &name).await;
                        result.map(|linked| (linked, transaction))
                    })
                })
                .await;
            match linked {
                Ok(linked) => volume.reply(
                    [(inode, linked.item), (directory, linked.parent)],
                    |locked| {
                        // SAFETY: the name and attributes are live objects.
                        succeed(&reply, unsafe {
                            FSCreateLinkResult::initWithLinkName_linkAttributes_directoryAttributes_freeSpace(
                                FSCreateLinkResult::alloc(),
                                &file_name(&name),
                                &locked.attributes(inode),
                                &locked.attributes(directory),
                                Some(&no_free_space_update()),
                            )
                        })
                    },
                ),
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn remove(
        self: Arc<Self>,
        inode: Inode,
        kind: FileType,
        directory: Inode,
        name: Bytes,
        reply: Reply<FSRemoveItemResult>,
    ) {
        self.run(move |volume| async move {
            let removed = match kind {
                FileType::Directory => volume
                    .engine
                    .transact(|mut transaction| {
                        let name = name.clone();
                        Box::pin(async move {
                            let result = transaction.rmdir(directory, &name, Some(inode)).await;
                            result.map(|removed| (removed, transaction))
                        })
                    })
                    .await
                    .map_err(failed),
                FileType::File | FileType::SymbolicLink => volume
                    .engine
                    .transact(|mut transaction| {
                        let name = name.clone();
                        Box::pin(async move {
                            let result =
                                transaction.hard_unlink(directory, &name, Some(inode)).await;
                            result.map(|removed| (removed, transaction))
                        })
                    })
                    .await
                    .map_err(failed),
            };
            match removed {
                Ok(removed) => volume.reply(
                    [(inode, removed.item), (directory, removed.parent)],
                    |locked| {
                        // SAFETY: the attributes are live objects.
                        succeed(&reply, unsafe {
                            FSRemoveItemResult::initWithItemAttributes_directoryAttributes_freeSpace(
                                FSRemoveItemResult::alloc(),
                                &locked.attributes(inode),
                                &locked.attributes(directory),
                                Some(&no_free_space_update()),
                            )
                        })
                    },
                ),
                Err(error) => fail(&reply, error),
            }
        });
    }

    fn rename(
        self: Arc<Self>,
        inode: Inode,
        (from, from_name): (Inode, Bytes),
        (to, to_name): (Inode, Bytes),
        expected_replaced: Option<Inode>,
        reply: Reply<FSRenameItemResult>,
    ) {
        self.run(move |volume| async move {
            let renamed = volume
                .engine
                .transact(|mut transaction| {
                    let (from_name, to_name) = (from_name.clone(), to_name.clone());
                    Box::pin(async move {
                        let at_destination = transaction
                            .lookup(to, &to_name)
                            .await
                            .map_err(RenameFailure::Lookup)?;
                        if at_destination.found.map(|(inode, _)| inode) != expected_replaced {
                            return Err(RenameFailure::DestinationChanged);
                        }
                        let result = transaction
                            .rename(from, &from_name, to, &to_name, RenameMode::Replace, Some(inode))
                            .await
                            .map_err(RenameFailure::Rename);
                        result.map(|renamed| (renamed, transaction))
                    })
                })
                .await;
            match renamed {
                Ok(renamed) => {
                    let replaced = renamed.replaced.as_ref().map(|(replaced, _)| *replaced);
                    let reads = [(inode, renamed.item), (from, renamed.from), (to, renamed.to)]
                        .into_iter()
                        .chain(renamed.replaced);
                    volume.reply(reads, |locked| {
                        let replaced = replaced.map(|replaced| locked.attributes(replaced));
                        // SAFETY: the name and attributes are live objects.
                        succeed(&reply, unsafe {
                            FSRenameItemResult::initWithNewName_renamedItemAttributes_sourceDirectoryAttributes_destinationDirectoryAttributes_overItemAttributes_freeSpace(
                                FSRenameItemResult::alloc(),
                                &file_name(&to_name),
                                &locked.attributes(inode),
                                &locked.attributes(from),
                                &locked.attributes(to),
                                replaced.as_deref(),
                                Some(&no_free_space_update()),
                            )
                        })
                    })
                }
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn enumerate_directory(
        self: Arc<Self>,
        directory: Inode,
        cookie: FSDirectoryCookie,
        with_attributes: bool,
        packer: Handed<FSDirectoryEntryPacker>,
        reply: Reply<FSEnumerateDirectoryResult>,
    ) {
        self.run(move |volume| async move {
            // Engine positions start at 3, which leaves cookie 0 for the start, 1 for "." and 2 for "..".
            let mut after = cookie;
            let mut dots_packed = false;
            loop {
                let batch = volume
                    .engine
                    .read_transact(|transaction| {
                        Box::pin(async move {
                            let directory_attributes = transaction
                                .getattr(directory)
                                .await?
                                .ok_or(ReaddirError::StaleFileHandle)?;
                            let entries =
                                transaction.readdir(directory, after, LISTING_BATCH).await?;
                            let attributes = match with_attributes {
                                true => try_join_all(entries.iter().map(|entry| async {
                                    transaction
                                        .getattr(entry.inode)
                                        .await?
                                        .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))
                                }))
                                .await
                                .map(|attributes| attributes.into_iter().map(Some).collect())?,
                                false => vec![None; entries.len()],
                            };
                            Ok::<_, ReaddirError<_, _>>((directory_attributes, entries, attributes))
                        })
                    })
                    .await;
                let (directory_attributes, entries, attributes) = match batch {
                    Ok(batch) => batch,
                    Err(error) => return fail(&reply, failed(error)),
                };
                if !dots_packed {
                    dots_packed = true;
                    if !with_attributes {
                        let parent = match directory {
                            Inode::ROOT => Inode::ROOT,
                            _ => directory_attributes.parent,
                        };
                        let dots = [
                            (b".".as_slice(), directory, 1),
                            (b"..".as_slice(), parent, 2),
                        ];
                        for (name, inode, next) in dots {
                            // SAFETY: the name is a live object, and the packer is valid until the reply.
                            if cookie < next
                                && !unsafe {
                                    packer
                                        .object()
                                        .packEntryWithName_itemType_itemID_nextCookie_attributes(
                                            &file_name(name),
                                            FSItemType::Directory,
                                            item_id(inode),
                                            next,
                                            None,
                                        )
                                }
                            {
                                return listed(&reply);
                            }
                        }
                    }
                }
                let complete = entries.len() < LISTING_BATCH;
                for (entry, attributes) in entries.into_iter().zip(attributes) {
                    let pack = |attributes: Option<&_>| {
                        // SAFETY: the name and attributes are live objects, and the packer is valid until
                        // the reply.
                        unsafe {
                            packer
                                .object()
                                .packEntryWithName_itemType_itemID_nextCookie_attributes(
                                    &file_name(&entry.name),
                                    item_type(entry.kind),
                                    item_id(entry.inode),
                                    entry.position,
                                    attributes,
                                )
                        }
                    };
                    let packed = match attributes {
                        Some(attributes) => {
                            volume.items.reply([(entry.inode, attributes)], |locked| {
                                pack(Some(&locked.attributes(entry.inode)))
                            })
                        }
                        None => pack(None),
                    };
                    if !packed {
                        return listed(&reply);
                    }
                    after = entry.position;
                }
                if complete {
                    return listed(&reply);
                }
            }
        });
    }

    fn read(
        self: Arc<Self>,
        inode: Inode,
        offset: u64,
        length: usize,
        buffer: Handed<FSMutableFileDataBuffer>,
        reply: Reply<FSReadFileResult>,
    ) {
        self.run(move |volume| async move {
            let planned = volume
                .engine
                .read_transact(|transaction| {
                    Box::pin(async move {
                        let (plan, attributes) = join(
                            transaction.read(inode, offset, length as u64),
                            transaction.getattr(inode),
                        )
                        .await;
                        Ok::<_, ReadError<_, _>>((
                            plan?,
                            attributes?.ok_or(ReadError::StaleFileHandle)?,
                        ))
                    })
                })
                .await;
            let (plan, attributes) = match planned {
                Ok(planned) => planned,
                Err(error) => return fail(&reply, failed(error)),
            };
            let pieces = match try_join_all(plan.load(volume.engine.content())).await {
                Ok(pieces) => pieces,
                Err(error) => {
                    return fail(
                        &reply,
                        posix_error(errno(PosixError::EIO), &error.to_string()),
                    );
                }
            };
            // SAFETY: FSKit's buffer holds length() writable bytes until the reply, and only this
            // operation writes to it.
            let destination = unsafe {
                std::slice::from_raw_parts_mut(
                    buffer.object().mutableBytes().as_ptr().cast::<u8>(),
                    buffer.object().length(),
                )
            };
            let mut filled = 0;
            for mut piece in pieces {
                let length = piece.remaining().min(destination.len() - filled);
                piece.copy_to_slice(&mut destination[filled..filled + length]);
                filled += length;
            }
            volume.reply([(inode, attributes)], |locked| {
                // SAFETY: the attributes are a live, fully populated object.
                succeed(&reply, unsafe {
                    FSReadFileResult::initWithBytesRead_itemAttributes(
                        FSReadFileResult::alloc(),
                        filled,
                        &locked.attributes(inode),
                    )
                })
            })
        });
    }

    fn write(
        self: Arc<Self>,
        inode: Inode,
        offset: u64,
        contents: Bytes,
        reply: Reply<FSWriteFileResult>,
    ) {
        self.run(move |volume| async move {
            let written = volume
                .engine
                .transact(|mut transaction| {
                    let contents = contents.clone();
                    Box::pin(async move {
                        let result = transaction.write(inode, offset, contents).await;
                        result.map(|attributes| (attributes, transaction))
                    })
                })
                .await;
            match written {
                Ok(attributes) => volume.reply([(inode, attributes)], |locked| {
                    // SAFETY: the attributes are a live, fully populated object.
                    succeed(&reply, unsafe {
                        FSWriteFileResult::initWithBytesWritten_itemAttributes_freeSpace(
                            FSWriteFileResult::alloc(),
                            contents.len(),
                            &locked.attributes(inode),
                            Some(&no_free_space_update()),
                        )
                    })
                }),
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn get_xattr(self: Arc<Self>, inode: Inode, name: Bytes, reply: Reply<FSGetXattrResult>) {
        self.run(move |volume| async move {
            let found = volume
                .engine
                .read_transact(|transaction| {
                    let name = name.clone();
                    Box::pin(async move {
                        let value = transaction
                            .get_annotation(inode, annotation_key(&name))
                            .await?;
                        Ok::<_, GetAnnotationError<_, _>>(
                            value.map(|mut value| value.copy_to_bytes(value.remaining())),
                        )
                    })
                })
                .await;
            match found {
                // SAFETY: the value is a live object.
                Ok(Some(value)) => succeed(&reply, unsafe {
                    FSGetXattrResult::initWithXattrValue(
                        FSGetXattrResult::alloc(),
                        &NSData::with_bytes(&value),
                    )
                }),
                Ok(None) => fail(
                    &reply,
                    posix_error(
                        errno(PosixError::ENOATTR),
                        "the item has no attribute by that name",
                    ),
                ),
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn set_xattr(
        self: Arc<Self>,
        inode: Inode,
        name: Bytes,
        value: Option<Bytes>,
        policy: FSSetXattrPolicy,
        reply: Reply<FSSetXattrResult>,
    ) {
        self.run(move |volume| async move {
            let mode = match policy {
                FSSetXattrPolicy::MustCreate => Some(SetAnnotationMode::Create),
                FSSetXattrPolicy::MustReplace => Some(SetAnnotationMode::Replace),
                FSSetXattrPolicy::Delete => None,
                _ => Some(SetAnnotationMode::CreateOrReplace),
            };
            let changed = match (mode, value) {
                (Some(mode), Some(value)) => volume
                    .engine
                    .transact(|mut transaction| {
                        let (name, value) = (name.clone(), value.clone());
                        Box::pin(async move {
                            let result = transaction
                                .set_annotation(inode, annotation_key(&name), value, mode)
                                .await;
                            result.map(|attributes| (attributes, transaction))
                        })
                    })
                    .await
                    .map_err(failed),
                (None, _) => volume
                    .engine
                    .transact(|mut transaction| {
                        let name = name.clone();
                        Box::pin(async move {
                            let result = transaction
                                .remove_annotation(inode, annotation_key(&name))
                                .await;
                            result.map(|attributes| (attributes, transaction))
                        })
                    })
                    .await
                    .map_err(failed),
                (Some(_), None) => Err(posix_error(
                    errno(PosixError::EINVAL),
                    "setting an attribute needs a value",
                )),
            };
            match changed {
                Ok(attributes) => {
                    volume.engine.cached(inode, &attributes);
                    // SAFETY: the free space is FSKit's sentinel for no change.
                    succeed(&reply, unsafe {
                        FSSetXattrResult::initWithFreeSpace(
                            FSSetXattrResult::alloc(),
                            Some(&no_free_space_update()),
                        )
                    })
                }
                Err(error) => fail(&reply, error),
            }
        });
    }

    fn list_xattrs(self: Arc<Self>, inode: Inode, reply: Reply<FSListXattrsResult>) {
        self.run(move |volume| async move {
            let listed = volume
                .engine
                .read_transact(|transaction| {
                    Box::pin(async move {
                        let (portable, macos) = join(
                            transaction.list_annotations(inode, AnnotationScope::Portable),
                            transaction.list_annotations(inode, AnnotationScope::Platform(MACOS)),
                        )
                        .await;
                        Ok::<_, ListAnnotationsError<_, _>>([portable?, macos?].concat())
                    })
                })
                .await;
            match listed {
                Ok(names) => {
                    let names: Vec<_> = names.iter().map(|name| file_name(name)).collect();
                    // SAFETY: the names are live objects.
                    succeed(&reply, unsafe {
                        FSListXattrsResult::initWithXattrNames(
                            FSListXattrsResult::alloc(),
                            &NSArray::from_retained_slice(&names),
                        )
                    })
                }
                Err(error) => fail(&reply, failed(error)),
            }
        });
    }

    fn delete_if_unlinked(self: Arc<Self>, inode: Inode) {
        self.run(move |volume| async move {
            let deleted = volume
                .engine
                .transact(|mut transaction| {
                    Box::pin(async move {
                        let result = transaction.delete_if_unlinked(inode).await;
                        result.map(|()| ((), transaction))
                    })
                })
                .await;
            if let Err(error) = deleted {
                tracing::warn!(inode = inode.0, %error, "deleting an item with no name failed");
            }
        });
    }

    fn forget(&self, inode: Inode) {
        self.engine.forget(inode);
    }
}

impl<St, A, N, S, C> Volume<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    // run runs an operation, handing it the volume, on FSKit's thread until it finishes or has to wait, such
    // as while the store's thread pool runs its transaction. An operation that has to wait finishes on the spawner, so
    // FSKit can hand over the next one meanwhile.
    fn run<O: Future<Output = ()> + Send + 'static>(
        self: Arc<Self>,
        operation: impl FnOnce(Arc<Self>) -> O,
    ) {
        let spawner = self.spawner.clone();
        let mut operation = Box::pin(operation(self));
        // The spawner polls the operation again with a waker of its own, so this first poll needs none.
        if operation
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
        {
            spawner
                .spawn(operation)
                .expect("the spawner accepts work while a volume is mounted");
        }
    }

    // reply replies with reads as Items::reply does, then tells the engine the kernel caches each item it read.
    fn reply<R>(
        self: &Arc<Self>,
        reads: impl IntoIterator<Item = (Inode, Attributes)>,
        reply: impl FnOnce(&mut Locked<'_>) -> R,
    ) -> R {
        let reads: Vec<_> = reads.into_iter().collect();
        let replied = self.items.reply(reads.iter().cloned(), reply);
        for (inode, read) in &reads {
            self.engine.cached(*inode, read);
        }
        replied
    }
}

// MACOS is the platform of the annotations only macOS gives meaning to.
const MACOS: &[u8] = b"macos";

// annotation_key is the annotation an extended attribute's name refers to. Apple's own com.apple. names
// mean something only to macOS, and every other name is portable.
fn annotation_key(name: &[u8]) -> AnnotationKey<'_> {
    if name.starts_with(b"com.apple.") {
        AnnotationKey::Platform {
            platform: MACOS,
            name,
        }
    } else {
        AnnotationKey::Portable(name)
    }
}

fn item_type(kind: FileType) -> FSItemType {
    match kind {
        FileType::File => FSItemType::File,
        FileType::Directory => FSItemType::Directory,
        FileType::SymbolicLink => FSItemType::Symlink,
    }
}

// succeed replies with result, or with EIO if FSKit would not make it.
fn succeed<R: Message>(reply: &Reply<R>, result: Option<Retained<R>>) {
    match result {
        Some(result) => reply.call(Retained::as_ptr(&result).cast_mut(), null_mut()),
        None => fail(
            reply,
            posix_error(errno(PosixError::EIO), "FSKit would not make the reply"),
        ),
    }
}

// fail replies with error, and logs it. A name or extended attribute that does not exist is ordinary, so it is
// only logged for debugging.
fn fail<R: Message>(reply: &Reply<R>, error: Retained<NSError>) {
    let code = error.code();
    if code == libc::ENOENT as isize || code == libc::ENOATTR as isize {
        tracing::debug!(code, description = %error.localizedDescription(), "operation failed");
    } else {
        tracing::warn!(code, description = %error.localizedDescription(), "operation failed");
    }
    reply.call(null_mut(), Retained::as_ptr(&error).cast_mut());
}

fn stale() -> Retained<NSError> {
    posix_error(errno(PosixError::ESTALE), "the item no longer exists")
}

// listed replies to an enumeration, with the one verifier every listing has.
fn listed(reply: &Reply<FSEnumerateDirectoryResult>) {
    // SAFETY: initWithVerifier: takes a plain value. The bindings lack it, though the macOS 27.2 SDK has it.
    let result: Option<Retained<FSEnumerateDirectoryResult>> =
        unsafe { msg_send![FSEnumerateDirectoryResult::alloc(), initWithVerifier: VERIFIER] };
    succeed(reply, result);
}

// no_free_space_update is what a result says of the volume's free space, which no operation changes: the volume reports
// a fixed one.
fn no_free_space_update() -> Retained<FSFreeSpace> {
    // SAFETY: noUpdate takes nothing and returns FSKit's sentinel.
    unsafe { FSFreeSpace::noUpdate() }
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
