use std::convert::Infallible;
use std::error::Error;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Weak};
use std::task::{Context, Waker};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use fuser::{
    Errno, FileAttr, FileHandle, FopenFlags, Generation, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyWrite, ReplyXattr,
};
use futures_lite::future::block_on;
use futures_util::future::{join, try_join_all};
use stratafs_fs_engine::annotations::error::{GetAnnotationError, ListAnnotationsError};
use stratafs_fs_engine::annotations::{AnnotationKey, AnnotationScope, SetAnnotationMode};
use stratafs_fs_engine::attributes::Attributes;
use stratafs_fs_engine::content::error::{ReadError, ReadSymbolicLinkError, WriteError};
use stratafs_fs_engine::directories::error::ReaddirError;
use stratafs_fs_engine::directories::{EntryChange, NewItem, RenameMode};
use stratafs_fs_engine::error::{CorruptValue, EngineFailure, IoFailure, PosixError};
use stratafs_fs_engine::kernel_cache::KernelCache;
use stratafs_fs_engine::{Cached, FsConfig, FsEngine, Inode, ResultHandling, Timestamp};
use stratafs_runtime::Runtime;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::kv::TransactError;
use stratafs_store::store::Store;

use super::changes::{AttributeChange, Requested};
use super::item::{file_name, file_type, node_id};
use super::items::{Items, Locked};
use super::purges;
use super::volume::FuseCache;
use super::{errno, error_chain};

// CACHED_FOR is how long the kernel keeps the attributes and names it is handed before asking again. The volume drops
// them itself when they change, so it is long.
pub(crate) const CACHED_FOR: Duration = Duration::from_secs(24 * 60 * 60);

// CHANGE_CACHED_FOR is how long the kernel keeps the attributes a change replies with: a setattr, a link, or an item a
// change makes. It takes them in whatever purge came since the volume replied, so it keeps them for no time and asks
// again at the next stat.
const CHANGE_CACHED_FOR: Duration = Duration::ZERO;

// cached_for is how long the kernel keeps what a reply hands over: CACHED_FOR, or no time when the reply is uncached,
// so the kernel asks again before it uses what a lookup, listing or getattr hands over uncached.
fn cached_for(locked: &Locked<'_>) -> Duration {
    match locked.cached {
        true => CACHED_FOR,
        false => Duration::ZERO,
    }
}

// attributes_cached_for is how long the kernel keeps the attributes a lookup or listing hands over for inode: as long
// as the name once it asked for them since it last forgot a lookup of inode, and no time before. It drops attributes
// replied to a request sent before a purge only for an item it holds, and no purge waits until it takes a reply in.
fn attributes_cached_for(locked: &Locked<'_>, inode: Inode) -> Duration {
    match locked.asked(inode) {
        true => cached_for(locked),
        false => Duration::ZERO,
    }
}

// opened is how a file opens with flags. The kernel keeps its cached contents from one open to the next, since the
// volume drops them itself when they change. A file opened to append goes around them, so each write of up to
// max_write bytes reaches the volume as one request, which the volume moves to the stored end as a whole.
pub(crate) fn opened(flags: i32) -> FopenFlags {
    match flags & libc::O_APPEND {
        0 => FopenFlags::FOPEN_KEEP_CACHE,
        _ => FopenFlags::FOPEN_KEEP_CACHE | FopenFlags::FOPEN_DIRECT_IO,
    }
}

// GENERATION is every inode's generation: the engine allocates inode numbers at random from 64 bits, so it never gives
// a number the kernel holds to a new item.
const GENERATION: Generation = Generation(0);

// Volume is a mounted volume's engine. Each operation replies when it finishes, on FUSE's thread unless it has to
// wait, and then on runtime.
pub(crate) struct Volume<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    pub(crate) engine: FsEngine<St, A, N, S, C>,
    pub(crate) runtime: Arc<dyn Runtime>,
    pub(crate) items: Arc<Items>,
    // kernel is the kernel's cache of the volume's inodes, the one the engine invalidates.
    pub(crate) kernel: FuseCache,
    // stops_durability_cadence stops the volume's durability cadence when the volume is dropped.
    pub(crate) stops_durability_cadence: mpsc::Sender<()>,
    // closes says whether the last commits became durable when the volume is dropped.
    pub(crate) closes: mpsc::Sender<Result<(), St::Failure>>,
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

// Dropping the volume, once its last operation ends, makes every commit durable, up to the engine's durability limit,
// and says whether that worked.
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
        // mount waits for this once the session ends, unless it already returned.
        let _ = self.closes.send(block_on(
            self.engine.ensure_durable(DurableAgainst::PowerFailure),
        ));
    }
}

// Made is the reply to an operation that makes an item: create, which opens the file it makes with flags, or mkdir,
// symlink or mknod.
pub(crate) enum Made {
    Opened { reply: ReplyCreate, flags: i32 },
    Entry(ReplyEntry),
}

impl Made {
    // made replies with attr, whose name the kernel keeps for cached_for. fuser hands over a create's name for as long
    // as its attributes, CHANGE_CACHED_FOR.
    fn made(self, cached_for: &Duration, attr: &FileAttr) {
        match self {
            Made::Opened { reply, flags } => reply.created(
                &CHANGE_CACHED_FOR,
                attr,
                GENERATION,
                FileHandle(0),
                opened(flags),
            ),
            Made::Entry(reply) => {
                reply.entry_with_ttls(&CHANGE_CACHED_FOR, cached_for, attr, GENERATION)
            }
        }
    }

    // error replies with error. The kernel creates only once its lookup found the name free, so a create without
    // O_EXCL that finds the name taken gets ESTALE, and the kernel looks the name up again and opens what it finds.
    fn error(self, error: Errno) {
        match self {
            Made::Opened { reply, flags }
                if error == Errno::EEXIST && flags & libc::O_EXCL == 0 =>
            {
                reply.error(Errno::ESTALE)
            }
            Made::Opened { reply, .. } => reply.error(error),
            Made::Entry(reply) => reply.error(error),
        }
    }
}

// Removal is the call that removes a name: unlink, a file's or a symbolic link's, or rmdir, a directory's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Removal {
    Unlink,
    Rmdir,
}

// Packed is how far a batch of a listing got into the reply: it filled the reply, or added every entry through the
// last, and whether the listing is complete. Replied is a batch that ended the call by replying.
enum Packed {
    Full,
    Through { last: Option<u64>, complete: bool },
    Replied,
}

// LISTING_BATCH is how many directory entries a listing reads per transaction while it fills a reply.
const LISTING_BATCH: usize = 64;

// LINUX is the platform of the extended attributes only Linux gives meaning to.
const LINUX: &[u8] = b"linux";

// PORTABLE_PREFIX is the namespace Linux names the extended attributes of users in, which mean the same on every
// platform.
const PORTABLE_PREFIX: &[u8] = b"user.";

impl<St, A, N, S, C> Volume<St, A, N, S, C>
where
    St: Store,
    A: Fn() -> u64 + Send + Sync + 'static,
    N: Fn() -> Timestamp + Send + Sync + 'static,
    S: FsConfig,
    C: Cached<St>,
{
    pub(crate) fn getattr(self: Arc<Self>, inode: Inode, reply: ReplyAttr) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let served = volume
                .engine
                .serve(
                    purges::GETATTR,
                    true,
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
                    |found, reads, result_handling| match found {
                        Some(_) => {
                            volume
                                .items
                                .reply(reads.iter().cloned(), result_handling, |locked| {
                                    locked.ask(inode);
                                    sent(&mut reply).attr(&cached_for(locked), &locked.attr(inode))
                                })
                        }
                        None => {
                            sent(&mut reply).error(Errno::ESTALE);
                            Some(())
                        }
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    pub(crate) fn setattr(self: Arc<Self>, inode: Inode, requested: Requested, reply: ReplyAttr) {
        let change = requested.change();
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let served = volume
                .engine
                .transact_and_serve(
                    purges::ITEM_CHANGES,
                    |mut transaction| {
                        let change = change.clone();
                        Box::pin(async move {
                            let result = transaction
                                .setattr(inode, move |settable| change(settable))
                                .await;
                            result.map(|attributes| (attributes, transaction))
                        })
                    },
                    |attributes| vec![(inode, attributes.clone())],
                    |_, reads, result_handling| {
                        volume
                            .items
                            .reply(reads.iter().cloned(), result_handling, |locked| {
                                sent(&mut reply).attr(&CHANGE_CACHED_FOR, &locked.attr(inode))
                            })
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    pub(crate) fn lookup(self: Arc<Self>, directory: Inode, name: Bytes, reply: ReplyEntry) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            // The kernel keeps the name it is handed, so the reply hands over the directory too. A name not found is
            // refused, which the kernel keeps nothing of. An item look_up keeps back gets ESTALE, and the kernel looks
            // the name up once more.
            let served = volume
                .engine
                .serve(
                    purges::NAME_READS,
                    true,
                    |transaction| {
                        let name = name.clone();
                        Box::pin(async move {
                            let lookup = transaction.lookup(directory, &name).await;
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
                    |lookup, reads, result_handling| match &lookup.found {
                        Some((inode, attributes)) => {
                            volume
                                .items
                                .reply(reads.iter().cloned(), result_handling, |locked| {
                                    let found = attributes.change_version.number;
                                    if !locked.look_up(directory, &name, *inode, found) {
                                        return sent(&mut reply).error(Errno::ESTALE);
                                    }
                                    sent(&mut reply).entry_with_ttls(
                                        &attributes_cached_for(locked, *inode),
                                        &cached_for(locked),
                                        &locked.attr(*inode),
                                        GENERATION,
                                    )
                                })
                        }
                        None => volume
                            .items
                            .reply(reads.iter().cloned(), result_handling, |_| {
                                sent(&mut reply).error(Errno::ENOENT)
                            }),
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    pub(crate) fn readlink(self: Arc<Self>, inode: Inode, reply: ReplyData) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let served = volume
                .engine
                .serve(
                    purges::READLINK,
                    false,
                    |transaction| {
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
                                (target.copy_to_bytes(target.remaining()), attributes),
                                transaction,
                            ))
                        })
                    },
                    |(_, attributes)| vec![(inode, attributes.clone())],
                    |(target, _), reads, result_handling| {
                        volume
                            .items
                            .reply(reads.iter().cloned(), result_handling, |_| {
                                sent(&mut reply).data(target)
                            })
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    // make makes item with name in directory, its attributes changed as change says, and hands the kernel a lookup of
    // it.
    pub(crate) fn make(
        self: Arc<Self>,
        directory: Inode,
        name: Bytes,
        item: NewItem,
        change: AttributeChange,
        reply: Made,
    ) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let served = volume
                .engine
                .transact_and_serve(
                    purges::NAME_CHANGES,
                    |mut transaction| {
                        let (name, item, change) = (name.clone(), item.clone(), change.clone());
                        Box::pin(async move {
                            let result = transaction
                                .create(directory, &name, item, move |settable| change(settable))
                                .await;
                            result.map(|created| (created, transaction))
                        })
                    },
                    |created| {
                        vec![
                            (created.inode, created.item.clone()),
                            (directory, created.parent.clone()),
                        ]
                    },
                    |created, reads, result_handling| {
                        let inode = created.inode;
                        volume.invalidate_attributes([directory]);
                        volume
                            .items
                            .reply(reads.iter().cloned(), result_handling, |locked| {
                                locked.hand(directory, &name, inode);
                                sent(&mut reply).made(&cached_for(locked), &locked.attr(inode))
                            })
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    pub(crate) fn link(
        self: Arc<Self>,
        inode: Inode,
        directory: Inode,
        name: Bytes,
        reply: ReplyEntry,
    ) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let served = volume
                .engine
                .transact_and_serve(
                    purges::NAME_CHANGES,
                    |mut transaction| {
                        let name = name.clone();
                        Box::pin(async move {
                            let result = transaction.hard_link(inode, directory, &name).await;
                            result.map(|linked| (linked, transaction))
                        })
                    },
                    |linked| {
                        vec![
                            (inode, linked.item.clone()),
                            (directory, linked.parent.clone()),
                        ]
                    },
                    |_, reads, result_handling| {
                        volume.invalidate_attributes([directory]);
                        volume
                            .items
                            .reply(reads.iter().cloned(), result_handling, |locked| {
                                locked.hand(directory, &name, inode);
                                sent(&mut reply).entry_with_ttls(
                                    &CHANGE_CACHED_FOR,
                                    &cached_for(locked),
                                    &locked.attr(inode),
                                    GENERATION,
                                )
                            })
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    // remove removes name from directory, refusing with ESTALE when it names another item than the one the kernel was
    // handed it as. The kernel then looks the name up again and asks once more.
    pub(crate) fn remove(
        self: Arc<Self>,
        directory: Inode,
        name: Bytes,
        removal: Removal,
        reply: ReplyEmpty,
    ) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let expected = volume.items.named(directory, &name);
            let reads = |removed: &EntryChange| {
                vec![
                    (removed.inode, removed.item.clone()),
                    (directory, removed.parent.clone()),
                ]
            };
            let hand =
                |_: &EntryChange,
                 reads: &[(Inode, Attributes)],
                 result_handling: &(dyn Fn() -> ResultHandling + Sync)| {
                    volume.invalidate_attributes([directory]);
                    volume
                        .items
                        .reply(reads.iter().cloned(), result_handling, |locked| {
                            locked.unname(directory, &name);
                            sent(&mut reply).ok()
                        })
                };
            let served = match removal {
                Removal::Rmdir => volume
                    .engine
                    .transact_and_serve(
                        purges::NAME_CHANGES,
                        |mut transaction| {
                            let name = name.clone();
                            Box::pin(async move {
                                let result = transaction.rmdir(directory, &name, expected).await;
                                result.map(|removed| (removed, transaction))
                            })
                        },
                        reads,
                        hand,
                    )
                    .await
                    .map_err(failed),
                Removal::Unlink => volume
                    .engine
                    .transact_and_serve(
                        purges::NAME_CHANGES,
                        |mut transaction| {
                            let name = name.clone();
                            Box::pin(async move {
                                let result =
                                    transaction.hard_unlink(directory, &name, expected).await;
                                result.map(|removed| (removed, transaction))
                            })
                        },
                        reads,
                        hand,
                    )
                    .await
                    .map_err(failed),
            };
            if let Err(error) = served {
                sent(&mut reply).error(error);
            }
        });
    }

    // rename moves from_name in from to to_name in to, refusing with ESTALE as remove does when from_name names another
    // item than the one the kernel was handed it as.
    pub(crate) fn rename(
        self: Arc<Self>,
        (from, from_name): (Inode, Bytes),
        (to, to_name): (Inode, Bytes),
        mode: RenameMode,
        reply: ReplyEmpty,
    ) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let expected = volume.items.named(from, &from_name);
            let served = volume
                .engine
                .transact_and_serve(
                    purges::NAME_CHANGES,
                    |mut transaction| {
                        let (from_name, to_name) = (from_name.clone(), to_name.clone());
                        Box::pin(async move {
                            let result = transaction
                                .rename(from, &from_name, to, &to_name, mode, expected)
                                .await;
                            result.map(|renamed| (renamed, transaction))
                        })
                    },
                    |renamed| {
                        [
                            (renamed.inode, renamed.item.clone()),
                            (from, renamed.from.clone()),
                            (to, renamed.to.clone()),
                        ]
                        .into_iter()
                        .chain(renamed.replaced.clone())
                        .collect()
                    },
                    |renamed, reads, result_handling| {
                        let exchanged = renamed
                            .replaced
                            .as_ref()
                            .filter(|_| mode == RenameMode::Exchange)
                            .map(|(other, _)| *other);
                        volume.invalidate_attributes(
                            [from, to, renamed.inode].into_iter().chain(exchanged),
                        );
                        volume
                            .items
                            .reply(reads.iter().cloned(), result_handling, |locked| {
                                // An exchange leaves both names in place, each naming the other item.
                                match mode {
                                    RenameMode::Exchange => {
                                        let other = renamed
                                            .replaced
                                            .as_ref()
                                            .map_or(renamed.inode, |(other, _)| *other);
                                        locked.name(from, &from_name, other)
                                    }
                                    RenameMode::Replace | RenameMode::NoReplace => {
                                        locked.unname(from, &from_name)
                                    }
                                }
                                locked.name(to, &to_name, renamed.inode);
                                sent(&mut reply).ok()
                            })
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    // readdir lists directory from offset, without attributes. The kernel keeps nothing of such a listing, so it is
    // read with no result handling.
    pub(crate) fn readdir(
        self: Arc<Self>,
        directory: Inode,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        self.run(move |volume| async move {
            let mut after = offset;
            loop {
                let listed = volume
                    .engine
                    .read_transact(|transaction| {
                        Box::pin(async move {
                            let directory_attributes = transaction
                                .getattr(directory)
                                .await?
                                .ok_or(ReaddirError::StaleFileHandle)?;
                            let entries =
                                transaction.readdir(directory, after, LISTING_BATCH).await?;
                            Ok::<_, ReaddirError<_, _>>((directory_attributes, entries))
                        })
                    })
                    .await;
                let (directory_attributes, entries) = match listed {
                    Ok(listed) => listed,
                    Err(error) => return reply.error(failed(read_failed(error))),
                };
                let parent = parent_of(directory, &directory_attributes);
                for (inode, position, name) in dots(directory, parent, after) {
                    if reply.add(
                        node_id(inode),
                        position,
                        fuser::FileType::Directory,
                        file_name(name),
                    ) {
                        return reply.ok();
                    }
                }
                for entry in &entries {
                    if reply.add(
                        node_id(entry.inode),
                        entry.position,
                        file_type(entry.kind),
                        file_name(&entry.name),
                    ) {
                        return reply.ok();
                    }
                }
                match entries.last() {
                    Some(last) if entries.len() == LISTING_BATCH => after = last.position,
                    _ => return reply.ok(),
                }
            }
        });
    }

    // readdirplus lists directory from offset with each entry's attributes, and hands the kernel a lookup of each entry
    // it adds.
    pub(crate) fn readdirplus(
        self: Arc<Self>,
        directory: Inode,
        offset: u64,
        reply: ReplyDirectoryPlus,
    ) {
        self.run(move |volume| async move {
            let mut reply = Some(reply);
            let mut after = offset;
            // added says whether an earlier batch added entries, which the kernel must get, as it holds a lookup of
            // each.
            let mut added = false;
            loop {
                let mut left_out = Vec::new();
                let packed = volume
                    .engine
                    .serve(
                        purges::NAME_READS,
                        true,
                        |transaction| {
                            Box::pin(async move {
                                let directory_attributes = transaction
                                    .getattr(directory)
                                    .await?
                                    .ok_or(ReaddirError::StaleFileHandle)?;
                                let entries =
                                    transaction.readdir(directory, after, LISTING_BATCH).await?;
                                let attributes = try_join_all(entries.iter().map(|entry| async {
                                    transaction
                                        .getattr(entry.inode)
                                        .await?
                                        .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))
                                }))
                                .await?;
                                Ok::<_, ReaddirError<_, _>>((
                                    (directory_attributes, entries, attributes),
                                    transaction,
                                ))
                            })
                        },
                        // The kernel keeps the names it is handed, so the listing hands over the directory too.
                        |(directory_attributes, entries, attributes)| {
                            [(directory, directory_attributes.clone())]
                                .into_iter()
                                .chain(
                                    entries
                                        .iter()
                                        .map(|entry| entry.inode)
                                        .zip(attributes.iter().cloned()),
                                )
                                .collect()
                        },
                        |(directory_attributes, entries, _), reads, result_handling| {
                            volume
                                .items
                                .reply(reads.iter().cloned(), result_handling, |locked| {
                                    let listing = reply.as_mut().expect("a listing replies once");
                                    let packed = (|| {
                                        // The kernel takes neither a lookup nor the attributes of . and .., so they
                                        // carry the directory's, under their own numbers.
                                        let parent = parent_of(directory, directory_attributes);
                                        for (inode, position, name) in
                                            dots(directory, parent, after)
                                        {
                                            let attr = FileAttr {
                                                ino: node_id(inode),
                                                ..locked.attr(directory)
                                            };
                                            if listing.add(
                                                node_id(inode),
                                                position,
                                                file_name(name),
                                                &cached_for(locked),
                                                &attr,
                                                GENERATION,
                                            ) {
                                                left_out.extend(
                                                    entries.iter().map(|entry| entry.inode),
                                                );
                                                return Packed::Full;
                                            }
                                        }
                                        // fuser hands over a listed name for as long as its attributes.
                                        for (index, entry) in entries.iter().enumerate() {
                                            if listing.add(
                                                node_id(entry.inode),
                                                entry.position,
                                                file_name(&entry.name),
                                                &attributes_cached_for(locked, entry.inode),
                                                &locked.attr(entry.inode),
                                                GENERATION,
                                            ) {
                                                left_out.extend(
                                                    entries[index..]
                                                        .iter()
                                                        .map(|entry| entry.inode),
                                                );
                                                return Packed::Full;
                                            }
                                            locked.hand(directory, &entry.name, entry.inode);
                                        }
                                        Packed::Through {
                                            last: entries.last().map(|entry| entry.position),
                                            complete: entries.len() < LISTING_BATCH,
                                        }
                                    })();
                                    // An empty reply ends the listing, so a call replies only once full or complete,
                                    // under the locks of its last batch alone.
                                    match packed {
                                        Packed::Through {
                                            complete: false, ..
                                        } => packed,
                                        Packed::Full
                                        | Packed::Through { complete: true, .. }
                                        | Packed::Replied => {
                                            sent(&mut reply).ok();
                                            Packed::Replied
                                        }
                                    }
                                })
                        },
                    )
                    .await;
                // The engine took every entry read as handed to the kernel, so it lets go of those the reply left out,
                // unless the kernel holds them.
                for inode in left_out {
                    volume.items.release(inode, || volume.engine.forget(inode));
                }
                match packed {
                    Err(_) if added => return sent(&mut reply).ok(),
                    Err(error) => return sent(&mut reply).error(failed(error)),
                    Ok(Packed::Replied) => return,
                    Ok(Packed::Full | Packed::Through { complete: true, .. }) => {
                        unreachable!("a batch that ends the call replies itself")
                    }
                    Ok(Packed::Through { last, .. }) => {
                        if let Some(last) = last {
                            after = last;
                            added = true;
                        }
                    }
                }
            }
        });
    }

    // read may be a read ahead, which the kernel sends in the background, so it is counted as one.
    pub(crate) fn read(self: Arc<Self>, inode: Inode, offset: u64, size: u32, reply: ReplyData) {
        let background = self.kernel.background();
        self.run(move |volume| async move {
            let _background = background;
            let mut reply = Some(reply);
            let content = volume.engine.content();
            let served = volume
                .engine
                .serve_loaded(
                    purges::READ,
                    false,
                    |transaction| {
                        Box::pin(async move {
                            let (plan, attributes) = join(
                                transaction.read(inode, offset, u64::from(size)),
                                transaction.getattr(inode),
                            )
                            .await;
                            Ok::<_, ReadError<_, _>>((
                                (plan?, attributes?.ok_or(ReadError::StaleFileHandle)?),
                                transaction,
                            ))
                        })
                    },
                    // The content loads once the read's transaction ends, as one run of bytes, which the reply takes.
                    |(plan, attributes)| async move {
                        let pieces = try_join_all(plan.load(content))
                            .await
                            .map_err(|error| ReadError::from(IoFailure::ContentRead(error)))?;
                        let mut data = BytesMut::new();
                        for piece in pieces {
                            data.put(piece);
                        }
                        Ok((data.freeze(), attributes))
                    },
                    |(_, attributes)| vec![(inode, attributes.clone())],
                    // The kernel keeps a read's data whatever the reply, so a read never goes uncached and waits
                    // instead: only a write to the file purges its pages, and its delivery lets the read go. A read
                    // ahead holds a background request while it waits, which a purge may need, so one fails rather
                    // than wait while the mount drops a copy and too many are in flight.
                    |(data, _), reads, result_handling| {
                        let cached = || match result_handling() {
                            ResultHandling::NoCache => ResultHandling::Retry,
                            handling => handling,
                        };
                        volume.items.reply(reads.iter().cloned(), &cached, |_| {
                            sent(&mut reply).data(data)
                        })
                    },
                )
                .await;
            if let Err(error) = served {
                sent(&mut reply).error(failed(error));
            }
        });
    }

    // write writes contents at offset, or at the file's stored end when it appends: the kernel sends an append at the
    // end it caches, which another mount may have moved since. The kernel keeps the contents where it sent them, so a
    // write that lands elsewhere drops the kernel's copy of the file once it has replied. A write from the page cache
    // is sent in the background, so it is counted as one.
    pub(crate) fn write(
        self: Arc<Self>,
        inode: Inode,
        offset: u64,
        contents: Bytes,
        appends: bool,
        from_page_cache: bool,
        reply: ReplyWrite,
    ) {
        let (purge_waits, background) = match from_page_cache {
            true => (purges::WRITEBACK, Some(self.kernel.background())),
            false => (purges::WRITE, None),
        };
        self.run(move |volume| async move {
            let _background = background;
            let mut reply = Some(reply);
            let written = u32::try_from(contents.len())
                .expect("the kernel writes at most max_write bytes at once");
            let served = volume
                .engine
                .transact_and_serve(
                    purge_waits,
                    |mut transaction| {
                        let contents = contents.clone();
                        Box::pin(async move {
                            let at = match appends {
                                true => {
                                    let attributes = transaction.getattr(inode).await?;
                                    attributes.ok_or(WriteError::StaleFileHandle)?.settable.size
                                }
                                false => offset,
                            };
                            let result = transaction.write(inode, at, contents).await;
                            result.map(|attributes| ((attributes, at), transaction))
                        })
                    },
                    |(attributes, _)| vec![(inode, attributes.clone())],
                    |(_, at), reads, result_handling| {
                        volume
                            .items
                            .reply(reads.iter().cloned(), result_handling, |_| {
                                sent(&mut reply).written(written);
                                *at
                            })
                    },
                )
                .await;
            match served {
                Ok(at) if at != offset => {
                    if let Err(error) = volume.kernel.invalidate(inode).await {
                        tracing::warn!(
                            inode = inode.0,
                            %error,
                            "dropping the kernel's copy of a file after moving an append failed"
                        );
                    }
                }
                Ok(_) => {}
                // The kernel splits a long write into requests of max_write bytes, and returns what those before a
                // failed one wrote: a short write, which POSIX allows, such as when a later request gets EBUSY.
                Err(error) => sent(&mut reply).error(failed(error)),
            }
        });
    }

    // getxattr replies with the value of the extended attribute name, or its size when size is 0.
    pub(crate) fn getxattr(
        self: Arc<Self>,
        inode: Inode,
        name: Bytes,
        size: u32,
        reply: ReplyXattr,
    ) {
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
                Ok(Some(value)) => sized(reply, size, &value),
                Ok(None) => reply.error(Errno::NO_XATTR),
                Err(error) => reply.error(failed(read_failed(error))),
            }
        });
    }

    pub(crate) fn setxattr(
        self: Arc<Self>,
        inode: Inode,
        name: Bytes,
        value: Bytes,
        mode: SetAnnotationMode,
        reply: ReplyEmpty,
    ) {
        self.run(move |volume| async move {
            let changed = volume
                .engine
                .transact(purges::ITEM_CHANGES, |mut transaction| {
                    let (name, value) = (name.clone(), value.clone());
                    Box::pin(async move {
                        let result = transaction
                            .set_annotation(inode, annotation_key(&name), value, mode)
                            .await;
                        result.map(|attributes| (attributes, transaction))
                    })
                })
                .await;
            match changed {
                Ok(attributes) => {
                    volume.invalidate_attributes([inode]);
                    volume.engine.cached(inode, &attributes);
                    reply.ok()
                }
                Err(error) => reply.error(failed(error)),
            }
        });
    }

    pub(crate) fn removexattr(self: Arc<Self>, inode: Inode, name: Bytes, reply: ReplyEmpty) {
        self.run(move |volume| async move {
            let changed = volume
                .engine
                .transact(purges::ITEM_CHANGES, |mut transaction| {
                    let name = name.clone();
                    Box::pin(async move {
                        let result = transaction
                            .remove_annotation(inode, annotation_key(&name))
                            .await;
                        result.map(|attributes| (attributes, transaction))
                    })
                })
                .await;
            match changed {
                Ok(attributes) => {
                    volume.invalidate_attributes([inode]);
                    volume.engine.cached(inode, &attributes);
                    reply.ok()
                }
                Err(error) => reply.error(failed(error)),
            }
        });
    }

    // listxattr replies with the names of the item's extended attributes, each ended by a NUL, or their size when size
    // is 0.
    pub(crate) fn listxattr(self: Arc<Self>, inode: Inode, size: u32, reply: ReplyXattr) {
        self.run(move |volume| async move {
            let listed = volume
                .engine
                .read_transact(|transaction| {
                    Box::pin(async move {
                        let (portable, linux) = join(
                            transaction.list_annotations(inode, AnnotationScope::Portable),
                            transaction.list_annotations(inode, AnnotationScope::Platform(LINUX)),
                        )
                        .await;
                        Ok::<_, ListAnnotationsError<_, _>>((portable?, linux?))
                    })
                })
                .await;
            match listed {
                Ok((portable, linux)) => sized(reply, size, &xattr_names(&portable, &linux)),
                Err(error) => reply.error(failed(read_failed(error))),
            }
        });
    }

    // make_durable makes every committed operation durable against a power failure, up to the engine's durability
    // limit, as fsync asks. The store makes calls that arrive while it syncs share its next sync.
    pub(crate) fn make_durable(self: Arc<Self>, reply: ReplyEmpty) {
        self.run(move |volume| async move {
            match volume
                .engine
                .ensure_durable(DurableAgainst::PowerFailure)
                .await
            {
                Ok(()) => reply.ok(),
                Err(error) => {
                    tracing::warn!(%error, "making the volume durable failed");
                    reply.error(Errno::EIO)
                }
            }
        });
    }

    // forget lets go of lookups of inode the kernel forgot, tells the engine once the kernel holds none, and then
    // deletes the inode if no name refers to it.
    pub(crate) fn forget(self: Arc<Self>, inode: Inode, lookups: u64) {
        let unlinked = self
            .items
            .forget(inode, lookups, || self.engine.forget(inode));
        if unlinked == Some(true) {
            self.delete_if_unlinked(inode);
        }
    }

    // delete_unlinked deletes each item the kernel holds as the session ends that no name refers to: the kernel lets go
    // of every inode at unmount, and the forgets saying so may never come.
    pub(crate) fn delete_unlinked(self: &Arc<Self>) {
        for inode in self.items.unlinked() {
            Arc::clone(self).delete_if_unlinked(inode);
        }
    }

    // delete_if_unlinked deletes inode, which the kernel no longer holds, if no name refers to it. Nothing waits on it,
    // so it logs a failure.
    fn delete_if_unlinked(self: Arc<Self>, inode: Inode) {
        self.run(move |volume| async move {
            let deleted = volume
                .engine
                .transact(purges::NO_REQUEST, |mut transaction| {
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

    // invalidate_attributes moves the kernel's attribute version of inodes once a change to them commits, as its reply
    // only marks their attributes out of date, so a getattr reply read before the change is not cached after it.
    fn invalidate_attributes(&self, inodes: impl IntoIterator<Item = Inode>) {
        for inode in inodes {
            if let Err(error) = self.kernel.invalidate_attributes(inode) {
                tracing::warn!(inode = inode.0, %error, "dropping the kernel's attributes after a change failed");
            }
        }
    }

    // run runs an operation, handing it the volume, on FUSE's thread until it finishes or has to wait, such as while
    // the store's thread pool runs its transaction. An operation that has to wait finishes on the runtime, so FUSE can
    // hand over the next one meanwhile.
    fn run<O: Future<Output = ()> + Send + 'static>(
        self: Arc<Self>,
        operation: impl FnOnce(Arc<Self>) -> O,
    ) {
        let runtime = self.runtime.clone();
        let mut operation = Box::pin(operation(self));
        // The runtime polls the operation again with a waker of its own, so this first poll needs none.
        if operation
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
        {
            runtime.spawn(operation);
        }
    }
}

// sent is an operation's reply, taken to be sent, which happens once.
fn sent<R>(reply: &mut Option<R>) -> R {
    reply.take().expect("an operation replies once")
}

// parent_of is the parent a listing of directory names as .., which for the root is the root itself.
fn parent_of(directory: Inode, attributes: &Attributes) -> Inode {
    match directory {
        Inode::ROOT => Inode::ROOT,
        _ => attributes.parent,
    }
}

// dots are . and .. of a listing that carries on after the position after, each with its inode and its position.
// Engine positions start at 3, which leaves 1 for . and 2 for .., after 0 for the start.
fn dots(
    directory: Inode,
    parent: Inode,
    after: u64,
) -> impl Iterator<Item = (Inode, u64, &'static [u8])> {
    [
        (directory, 1, b".".as_slice()),
        (parent, 2, b"..".as_slice()),
    ]
    .into_iter()
    .filter(move |(_, position, _)| after < *position)
}

// annotation_key is the annotation an extended attribute's name refers to. A name in Linux's user namespace means the
// same on every platform, without the namespace, and every other name means something only to Linux.
pub(crate) fn annotation_key(name: &[u8]) -> AnnotationKey<'_> {
    match name.strip_prefix(PORTABLE_PREFIX) {
        Some(portable) => AnnotationKey::Portable(portable),
        None => AnnotationKey::Platform {
            platform: LINUX,
            name,
        },
    }
}

// xattr_names are the names of an item's extended attributes as listxattr gives them, from the names of its portable
// annotations and its Linux ones, each ended by a NUL.
pub(crate) fn xattr_names(portable: &[Bytes], linux: &[Bytes]) -> Vec<u8> {
    let mut names = Vec::new();
    for name in portable {
        names.extend_from_slice(PORTABLE_PREFIX);
        names.extend_from_slice(name);
        names.push(0);
    }
    for name in linux {
        names.extend_from_slice(name);
        names.push(0);
    }
    names
}

// sized replies to a getxattr or listxattr: with the value's size when the caller asks for it with size 0, the value
// when it fits in size, and ERANGE when it does not.
fn sized(reply: ReplyXattr, size: u32, value: &[u8]) {
    match u32::try_from(value.len()) {
        Ok(length) if size == 0 => reply.size(length),
        Ok(length) if length <= size => reply.data(value),
        _ => reply.error(Errno::ERANGE),
    }
}

// read_failed is a read transaction's error as an engine transaction's: no consistency takes part in a read, so only
// the store can fail it.
fn read_failed<R, F, E>(
    error: TransactError<R, F, E>,
) -> TransactError<R, EngineFailure<F, Infallible>, E> {
    match error {
        TransactError::Rejected(rejected) => TransactError::Rejected(rejected),
        TransactError::Failed(failure) => TransactError::Failed(EngineFailure::Store(failure)),
        TransactError::Aborted(work) => TransactError::Aborted(work),
    }
}

// failed is the error the kernel gets for a failed transaction: the work's own POSIX error when it aborted, the
// failure's own when the store or the consistency failed it, and EIO when the store refused it. It logs the error with
// each cause, a name or extended attribute that does not exist only for debugging, as that is ordinary.
fn failed<R, F, E>(error: TransactError<R, F, E>) -> Errno
where
    TransactError<R, F, E>: Error,
    for<'e> PosixError: From<&'e E> + From<&'e F>,
{
    let posix = match &error {
        TransactError::Aborted(work) => PosixError::from(work),
        TransactError::Failed(failure) => PosixError::from(failure),
        TransactError::Rejected(_) => PosixError::EIO,
    };
    let description = error_chain(&error);
    match posix {
        PosixError::ENOENT | PosixError::ENOATTR => {
            tracing::debug!(?posix, %description, "operation failed")
        }
        _ => tracing::warn!(?posix, %description, "operation failed"),
    }
    Errno::from_i32(errno(posix))
}

#[cfg(test)]
mod tests;
