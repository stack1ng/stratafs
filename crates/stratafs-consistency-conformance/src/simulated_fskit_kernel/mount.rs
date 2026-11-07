// The FSKit volume's operations, as the mount of a test volume calls the engine through them.

use std::sync::{Arc, Mutex, PoisonError};

use bytes::{Buf, Bytes};
use futures_channel::oneshot;
use futures_util::future::try_join_all;
use stratafs_fs_engine::attributes::{Attributes, Mode};
use stratafs_fs_engine::content::error::ReadError;
use stratafs_fs_engine::directories::error::{LookupError, ReaddirError, RenameError};
use stratafs_fs_engine::directories::{NewItem, RenameMode};
use stratafs_fs_engine::error::{CorruptValue, IoFailure, PosixError};
use stratafs_fs_engine::{Inode, Purge, Timestamp};

use crate::mount::{Consistency, Failure, LISTING_BATCH, Mount, Operations, READ_LENGTH, failed};
use crate::simulated_fskit_kernel::{Access, Kernel, Update, Work};

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

impl<C: Consistency> Mount<C, Kernel> {
    // returned is what an operation returns once its reply took effect, as the kernel returns it only then.
    async fn returned<T>(&self, replied: Result<(T, u64), Failure>) -> Result<T, Failure> {
        let (returned, reply) = replied?;
        self.kernel.took_effect(reply).await;
        Ok(returned)
    }
}

impl<C: Consistency> Operations for Mount<C, Kernel> {
    // resolve is the file name refers to in directory, or None for a free name, from the kernel's cache when it has
    // the name, or as the volume finds it, which the kernel caches with the file's attributes.
    async fn resolve(
        &self,
        directory: Inode,
        name: &'static str,
    ) -> Result<(Option<Inode>, bool), Failure> {
        if let Some(bound) = self.kernel.bound(directory, name).await {
            return Ok((bound, false));
        }
        let _looking_up = self.kernel.enter(Work::Lookup(directory)).await;
        let served = self
            .engine
            .serve(
                Purge::GoesAhead,
                false,
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
                |lookup, reads, result_handling| {
                    let found = lookup.found.as_ref().map(|(inode, _)| *inode);
                    let replied = self.kernel.reply(
                        reads,
                        &[(directory, Update::Name(name, found))],
                        None,
                        result_handling,
                    )?;
                    Some((found, replied.reply))
                },
            )
            .await;
        let found = self.returned(served.map_err(failed)).await?;
        Ok((found, false))
    }

    // list is every entry of directory, by name, with the attributes handed over for it.
    async fn list(&self, directory: Inode) -> Result<Vec<(Bytes, Inode, Attributes)>, Failure> {
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
                |listed, reads, result_handling| {
                    let cached =
                        |inode, attributes: &Attributes| self.engine.cached(inode, attributes);
                    let (handed, reply) =
                        self.kernel.reply_listed(reads, result_handling, cached)?;
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
    async fn stat(&self, inode: Inode) -> Result<Attributes, Failure> {
        if let Some(cached) = self.kernel.cached(inode).await {
            return Ok(cached);
        }
        let operation = self.kernel.begin(inode, Access::Stat).await;
        let served = self
            .engine
            .serve(
                Purge::Waits,
                false,
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
                    Some(_) => self
                        .kernel
                        .reply(reads, &[], Some(&operation), result_handling)
                        .map(|replied| Ok((replied.handed[&inode].clone(), replied.reply))),
                    None => Some(Err(Failure::Posix(PosixError::ESTALE))),
                },
            )
            .await;
        self.returned(served.map_err(failed)?).await
    }

    // read is a file's whole content, and the attributes handed over with it, from the kernel's cache when it has them.
    // UPSTREAM KERNEL BUG PIN: and whether it is a failed write's data the read is excused for.
    async fn read(&self, inode: Inode) -> Result<(Bytes, Attributes, bool), Failure> {
        if let Some(cached) = self.kernel.cached_content(inode).await {
            return Ok(cached);
        }
        let operation = self.kernel.begin(inode, Access::Read).await;
        let content = self.engine.content();
        let served = self
            .engine
            .serve_loaded(
                Purge::Waits,
                false,
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
                |(read, _), reads, result_handling| {
                    let content = [(inode, Update::Content(read.clone()))];
                    let replied =
                        self.kernel
                            .reply(reads, &content, Some(&operation), result_handling)?;
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
    async fn write(&self, inode: Inode, data: &'static [u8]) -> Result<Attributes, Failure> {
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
                |_, reads, result_handling| {
                    let replied =
                        self.kernel
                            .reply(reads, &[], Some(&operation), result_handling)?;
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
    async fn touch(&self, inode: Inode, at: i64) -> Result<Attributes, Failure> {
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
                |_, reads, result_handling| {
                    let replied =
                        self.kernel
                            .reply(reads, &[], Some(&operation), result_handling)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // chmod sets a file's mode.
    async fn chmod(&self, inode: Inode, mode: u16) -> Result<Attributes, Failure> {
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
                |_, reads, result_handling| {
                    let replied = self.kernel.reply(reads, &[], None, result_handling)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // create makes an item named name in directory, and is the inode its last attempt made, if it got as far.
    async fn create(
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
                |created, reads, result_handling| {
                    let inode = created.inode;
                    let binds = [(directory, Update::Name(name, Some(inode)))];
                    let replied = self.kernel.reply(reads, &binds, None, result_handling)?;
                    Some(((inode, replied.handed[&inode].clone()), replied.reply))
                },
            )
            .await;
        let created = self.returned(created.map_err(failed)).await;
        let made = *made.lock().unwrap_or_else(PoisonError::into_inner);
        (made, created)
    }

    // unlink removes name from directory, which refers to inode.
    async fn unlink(
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
                |_, reads, result_handling| {
                    let replied = self.kernel.reply(
                        reads,
                        &[(directory, Update::Name(name, None))],
                        None,
                        result_handling,
                    )?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // rename moves inode from one name in a directory to another, replacing the file the kernel found there.
    async fn rename(
        &self,
        (from_directory, from): (Inode, &'static str),
        (to_directory, to): (Inode, &'static str),
        inode: Inode,
        replaced: Option<Inode>,
    ) -> Result<(Attributes, Option<Inode>), Failure> {
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
                |_, reads, result_handling| {
                    let binds = [
                        (from_directory, Update::Name(from, None)),
                        (to_directory, Update::Name(to, Some(inode))),
                    ];
                    let replied = self.kernel.reply(reads, &binds, None, result_handling)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        let renamed = self.returned(served.map_err(failed)).await;
        renamed.map(|attributes| (attributes, replaced))
    }

    // link gives inode another name, name in directory.
    async fn link(
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
                |_, reads, result_handling| {
                    let binds = [(directory, Update::Name(name, Some(inode)))];
                    let replied = self.kernel.reply(reads, &binds, None, result_handling)?;
                    Some((replied.handed[&inode].clone(), replied.reply))
                },
            )
            .await;
        self.returned(served.map_err(failed)).await
    }

    // let_go lets go of a file as the kernel does once no operation refers to it, and says whether it did.
    fn let_go(&self, inode: Inode) -> bool {
        self.kernel.reclaim(inode, || self.engine.forget(inode))
    }
}
