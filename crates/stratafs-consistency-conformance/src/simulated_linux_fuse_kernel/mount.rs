// stratafs-fuse's operations, as the mount of a test volume calls the engine through them, each request holding the
// locks the Linux kernel holds for it until the kernel takes its reply in.

use std::sync::{Arc, Mutex, PoisonError};

use bytes::{BufMut, Bytes, BytesMut};
use futures_util::future::try_join_all;
use stratafs_fs_engine::attributes::{Attributes, Mode as Permissions, SettableAttributes};
use stratafs_fs_engine::content::error::ReadError;
use stratafs_fs_engine::directories::error::ReaddirError;
use stratafs_fs_engine::directories::{NewItem, RenameMode};
use stratafs_fs_engine::error::{CorruptValue, IoFailure, PosixError};
use stratafs_fs_engine::{Inode, ResultHandling, Timestamp};

use crate::activity::NAMES;
use crate::mount::{Consistency, Failure, LISTING_BATCH, Mount, Operations, READ_LENGTH, failed};
use crate::simulated_linux_fuse_kernel::{Kept, Kernel, Mode, Update, purges};

impl<C: Consistency> Mount<C, Kernel> {
    // find is the item name refers to in directory, from the kernel's cache, or as the volume finds it, while the
    // caller holds the directory's lock. A name the kernel keeps for no time it asks about again, as for a new one.
    async fn find(&self, directory: Inode, name: &'static str) -> Result<Option<Inode>, Failure> {
        match self.kernel.entry(directory, name) {
            Some((inode, true, _)) => Ok(Some(inode)),
            _ => self.look_up(directory, name, None).await,
        }
    }

    // look_up asks the volume what name refers to in directory, as a lookup request does. The reply hands over the
    // directory too, as the kernel keeps the name. A name not found is refused, which the kernel keeps nothing of.
    // Asked about a name the kernel keeps for no time, as the entry revalidated, the kernel goes on keeping that entry
    // only while it names the same inode, and forgets the lookup the reply handed over otherwise.
    async fn look_up(
        &self,
        directory: Inode,
        name: &'static str,
        revalidated: Option<(Inode, u64)>,
    ) -> Result<Option<Inode>, Failure> {
        let sent = self.kernel.sent();
        let served = self
            .engine
            .serve(
                purges::NAME_READS,
                true,
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
                    let found = lookup
                        .found
                        .as_ref()
                        .map(|(inode, attributes)| (*inode, attributes.change_version.number));
                    self.kernel.reply(reads, result_handling, |handing| {
                        let found = match found {
                            Some((inode, number))
                                if !handing.look_up(directory, name, inode, number) =>
                            {
                                return Err(Failure::Unchanged(PosixError::ESTALE));
                            }
                            found => found.map(|(inode, _)| (inode, handing.attr(inode))),
                        };
                        let kept = found
                            .as_ref()
                            .map_or(Kept::Nothing, |(inode, _)| handing.keeps(*inode));
                        let cached = handing.cached;
                        let update = match revalidated {
                            Some(revalidated) => Some(Update::Revalidated {
                                directory,
                                name,
                                revalidated,
                                found: found.clone(),
                                sent,
                                kept,
                                cached,
                            }),
                            None => found.clone().map(|(inode, attributes)| Update::Entries {
                                directory,
                                entries: vec![(name, inode, attributes, kept)],
                                sent: Some(sent),
                                cached,
                            }),
                        };
                        Ok((found.map(|(inode, _)| inode), update))
                    })
                },
            )
            .await;
        let (found, update) = served.map_err(failed)??;
        if let Some(update) = update {
            self.kernel.apply(update).await;
        }
        if let (Some((revalidated, _)), Some(found)) = (revalidated, found)
            && found != revalidated
        {
            self.kernel.forget(found, || self.engine.forget(found));
        }
        Ok(found)
    }

    // get_attributes asks the volume for inode's attributes, as a getattr request does, which holds no lock. The kernel
    // returns the reply's attributes, but keeps them only if cached and no change to them came since the request began.
    async fn get_attributes(&self, inode: Inode) -> Result<Attributes, Failure> {
        let sent = self.kernel.sent();
        let served = self
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
                    Some(_) => self.kernel.reply(reads, result_handling, |handing| {
                        handing.ask(inode);
                        Ok((handing.attr(inode), handing.cached))
                    }),
                    None => Some(Err(Failure::Posix(PosixError::ESTALE))),
                },
            )
            .await;
        let (attributes, cached) = served.map_err(failed)??;
        let update = Update::Attributes {
            inode,
            attributes: attributes.clone(),
            sent: Some(sent),
            cached,
        };
        self.kernel.apply(update).await;
        Ok(attributes)
    }

    // read_pages asks the volume for a file's data, as a read request does, holding its pages meanwhile. It is the data
    // another read had the kernel take in while this one waited for the pages, if one did, and whether it holds a
    // write's data the volume has not stored. The kernel keeps a read's data whatever the reply, so it never goes
    // uncached, and waits instead.
    async fn read_pages(&self, inode: Inode) -> Result<(Bytes, bool), Failure> {
        let _paging = self.kernel.page(inode, None).await;
        if let Some(cached) = self.kernel.pages(inode) {
            return Ok(cached);
        }
        let content = self.engine.content();
        let served = self
            .engine
            .serve_loaded(
                purges::READ,
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
                    let mut data = BytesMut::new();
                    for piece in pieces {
                        data.put(piece);
                    }
                    Ok((data.freeze(), attributes))
                },
                |(_, attributes)| vec![(inode, attributes.clone())],
                |(data, _), reads, result_handling| {
                    let cached = || match result_handling() {
                        ResultHandling::NoCache => ResultHandling::Retry,
                        handling => handling,
                    };
                    self.kernel.reply(reads, &cached, |_| data.clone())
                },
            )
            .await;
        let data = served.map_err(failed)?;
        let update = Update::Data {
            inode,
            data: data.clone(),
        };
        self.kernel.apply(update).await;
        Ok((data, false))
    }

    // set changes inode's settable attributes with change, as a setattr request does, holding the item's lock. The
    // kernel takes the reply's attributes in whatever changed them meanwhile, and asks again at the next stat.
    async fn set(
        &self,
        inode: Inode,
        change: impl Fn(&mut SettableAttributes) + Copy + Send + Sync,
    ) -> Result<Attributes, Failure> {
        let _setting = self.kernel.lock(vec![(inode, Mode::Exclusive)]).await;
        let served = self
            .engine
            .transact_and_serve(
                purges::ITEM_CHANGES,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction.setattr(inode, change).await;
                        result.map(|attributes| (attributes, transaction))
                    })
                },
                |attributes| vec![(inode, attributes.clone())],
                |_, reads, result_handling| {
                    self.kernel
                        .reply(reads, result_handling, |handing| handing.attr(inode))
                },
            )
            .await;
        let attributes = served.map_err(failed)?;
        let update = Update::Set {
            inode,
            attributes: attributes.clone(),
        };
        self.kernel.apply(update).await;
        Ok(attributes)
    }
}

impl<C: Consistency> Operations for Mount<C, Kernel> {
    // resolve is the file name refers to in directory, or None for a free name, from the kernel's cache when it has
    // the name, or as the volume finds it, sharing the directory's lock with other lookups.
    // UPSTREAM KERNEL BUG PIN: and whether it is a name this mount's change in flight drops.
    async fn resolve(
        &self,
        directory: Inode,
        name: &'static str,
    ) -> Result<(Option<Inode>, bool), Failure> {
        match self.kernel.entry(directory, name) {
            Some((inode, true, _)) => {
                return Ok((Some(inode), self.kernel.unnamed_at_reply(directory, name)));
            }
            // A name kept for no time is asked about again without the directory's lock, and looked up under it once
            // the kernel dropped it, or once the volume refused, which the kernel takes the name as gone for.
            Some((inode, false, number)) => {
                // The path walk holds the name's item while it asks again, so the kernel keeps the item meanwhile.
                let _revalidating = self.refer(inode);
                match self.look_up(directory, name, Some((inode, number))).await {
                    Ok(found) if found == Some(inode) => return Ok((Some(inode), false)),
                    Ok(_) | Err(Failure::Unchanged(PosixError::ESTALE)) => {}
                    Err(failure) => return Err(failure),
                }
            }
            None => {}
        }
        let _looking_up = self.kernel.lock(vec![(directory, Mode::Shared)]).await;
        Ok((self.find(directory, name).await?, false))
    }

    // list is every entry of directory, by name, with the attributes handed over for it, as readdirplus does: it hands
    // the kernel a lookup of each entry.
    async fn list(&self, directory: Inode) -> Result<Vec<(Bytes, Inode, Attributes)>, Failure> {
        let _listing = self.kernel.lock(vec![(directory, Mode::Shared)]).await;
        let sent = self.kernel.sent();
        let served = self
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
                        let entries = transaction.readdir(directory, 0, LISTING_BATCH).await?;
                        let mut listed = Vec::new();
                        for entry in entries {
                            let attributes = transaction
                                .getattr(entry.inode)
                                .await?
                                .ok_or(IoFailure::Corrupt(CorruptValue::DanglingEntry))?;
                            listed.push((entry.name, entry.inode, attributes));
                        }
                        Ok::<_, ReaddirError<_, _>>(((directory_attributes, listed), transaction))
                    })
                },
                |(directory_attributes, listed)| {
                    [(directory, directory_attributes.clone())]
                        .into_iter()
                        .chain(
                            listed
                                .iter()
                                .map(|(_, inode, attributes)| (*inode, attributes.clone())),
                        )
                        .collect()
                },
                |(_, listed), reads, result_handling| {
                    self.kernel.reply(reads, result_handling, |handing| {
                        // fuser hands over a listed name and its attributes for one time, the attributes' time.
                        let entries: Vec<_> = listed
                            .iter()
                            .map(|(name, inode, _)| {
                                let name = known(name);
                                handing.hand(directory, name, *inode);
                                let kept = match handing.keeps(*inode) {
                                    Kept::Name => Kept::Nothing,
                                    kept => kept,
                                };
                                (name, *inode, handing.attr(*inode), kept)
                            })
                            .collect();
                        let listed = entries
                            .iter()
                            .map(|(name, inode, attributes, _)| {
                                (
                                    Bytes::from_static(name.as_bytes()),
                                    *inode,
                                    attributes.clone(),
                                )
                            })
                            .collect();
                        let update = Update::Entries {
                            directory,
                            entries,
                            sent: Some(sent),
                            cached: handing.cached,
                        };
                        (listed, update)
                    })
                },
            )
            .await;
        let (listed, update) = served.map_err(failed)?;
        self.kernel.apply(update).await;
        Ok(listed)
    }

    // stat is a file's attributes, from the kernel's cache when they are not out of date.
    // UPSTREAM KERNEL BUG PIN: and whether they are stale ones a reply brought in after the item was let go of.
    async fn stat(&self, inode: Inode) -> Result<(Attributes, bool), Failure> {
        match self.kernel.attributes(inode) {
            Some(cached) => Ok(cached),
            None => Ok((self.get_attributes(inode).await?, false)),
        }
    }

    // read is one read past the size the kernel takes the file to have: it asks for the attributes unless cached, then
    // copies up to that size as it stands then from the cached pages, or those a read request brings in, padded with
    // zeros, and hands over those attributes as the stat had them.
    // UPSTREAM KERNEL BUG PIN: it says whether it copied a write's data the volume had not stored, copied up to a size
    // the kernel may have from before the data it copied, or its stat found stale attributes.
    async fn read(&self, inode: Inode) -> Result<(Bytes, Attributes, bool), Failure> {
        // UPSTREAM KERNEL BUG PIN: began numbers when the read began, so what left the size out of date after it shows.
        let began = self.kernel.sent();
        let (attributes, stale) = self.stat(inode).await?;
        let size = self.kernel.size(inode);
        let (pages, unstored) = match size {
            0 => (Bytes::new(), false),
            _ => match self.kernel.pages(inode) {
                Some(cached) => cached,
                None => self.read_pages(inode).await?,
            },
        };
        let copied = self.kernel.size(inode);
        // UPSTREAM KERNEL BUG PIN: the size it copied up to is out of date when the reply or the pages it got have
        // another size, and the kernel may have it from before them.
        let other_size = attributes.settable.size != copied || pages.len() as u64 != copied;
        let out_of_date = other_size && self.kernel.size_out_of_date(inode, began);
        let mut read = pages.to_vec();
        read.resize(copied as usize, 0);
        Ok((read.into(), attributes, unstored || out_of_date || stale))
    }

    // write writes data at the start of a file, holding its lock, so writes to it go one at a time, and its pages.
    async fn write(&self, inode: Inode, data: &'static [u8]) -> Result<Attributes, Failure> {
        let _writing = self.kernel.lock(vec![(inode, Mode::Exclusive)]).await;
        let _paging = self.kernel.page(inode, Some(data)).await;
        let served = self
            .engine
            .transact_and_serve(
                purges::WRITE,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction.write(inode, 0, Bytes::from_static(data)).await;
                        result.map(|attributes| (attributes, transaction))
                    })
                },
                |attributes| vec![(inode, attributes.clone())],
                |_, reads, result_handling| {
                    self.kernel
                        .reply(reads, result_handling, |handing| handing.attr(inode))
                },
            )
            .await;
        match served {
            Ok(attributes) => {
                let end = data.len() as u64;
                self.kernel.apply(Update::Written { inode, end }).await;
                Ok(attributes)
            }
            // Without the writeback cache, the kernel drops the pages of a write that failed.
            Err(error) => {
                self.kernel.write_failed(inode).await;
                Err(failed(error))
            }
        }
    }

    // touch sets a file's last access time.
    async fn touch(&self, inode: Inode, at: i64) -> Result<Attributes, Failure> {
        self.set(inode, move |settable| {
            settable.last_accessed_at = Timestamp(at)
        })
        .await
    }

    // chmod sets a file's mode.
    async fn chmod(&self, inode: Inode, mode: u16) -> Result<Attributes, Failure> {
        self.set(inode, move |settable| {
            settable.mode = Permissions::from_bits_retain(mode)
        })
        .await
    }

    // create makes an item named name in directory, holding the directory's lock, once the kernel finds the name free
    // as an open that creates does, and is the inode its last attempt made, if it got as far.
    async fn create(
        &self,
        directory: Inode,
        name: &'static str,
        item: NewItem,
    ) -> (Option<Inode>, Result<(Inode, Attributes), Failure>) {
        let _creating = self.kernel.lock(vec![(directory, Mode::Exclusive)]).await;
        match self.find(directory, name).await {
            Ok(None) => {}
            Ok(Some(_)) => return (None, Err(Failure::Posix(PosixError::EEXIST))),
            Err(failure) => return (None, Err(failure)),
        }
        let made = Arc::new(Mutex::new(None));
        let created = self
            .engine
            .transact_and_serve(
                purges::NAME_CHANGES,
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
                // The kernel takes a create's attributes in whatever purge came since, and fuser hands over the name
                // for the same time, so the kernel keeps neither.
                |created, reads, result_handling| {
                    let inode = created.inode;
                    self.kernel.reply(reads, result_handling, |handing| {
                        handing.invalidate_attributes([directory]);
                        handing.hand(directory, name, inode);
                        let attributes = handing.attr(inode);
                        let entries = vec![(name, inode, attributes.clone(), Kept::Nothing)];
                        let update = Update::Entries {
                            directory,
                            entries,
                            sent: None,
                            cached: handing.cached,
                        };
                        ((inode, attributes), update)
                    })
                },
            )
            .await;
        let created = match created {
            Ok((created, update)) => {
                self.kernel.apply(update).await;
                Ok(created)
            }
            Err(error) => Err(failed(error)),
        };
        let made = *made.lock().unwrap_or_else(PoisonError::into_inner);
        (made, created)
    }

    // unlink removes name from directory, holding the directory's lock and then the file's, once the kernel finds the
    // name refers to inode. The app unlinks only the file it looked up, and the volume only the one it handed over.
    async fn unlink(
        &self,
        directory: Inode,
        name: &'static str,
        inode: Inode,
    ) -> Result<Attributes, Failure> {
        let _unlinking = self.kernel.lock(vec![(directory, Mode::Exclusive)]).await;
        match self.find(directory, name).await? {
            None => return Err(Failure::Posix(PosixError::ENOENT)),
            Some(found) if found != inode => return Err(Failure::Posix(PosixError::ESTALE)),
            Some(_) => {}
        }
        let _removing = self.kernel.lock(vec![(inode, Mode::Exclusive)]).await;
        // UPSTREAM KERNEL BUG PIN: lookups on this mount still find the names until the reply.
        let _unnaming = self.kernel.unnaming(vec![(directory, name)]);
        let expected = self.kernel.named(directory, name);
        let served = self
            .engine
            .transact_and_serve(
                purges::NAME_CHANGES,
                |mut transaction| {
                    Box::pin(async move {
                        let result = transaction
                            .hard_unlink(directory, name.as_bytes(), expected)
                            .await;
                        result.map(|removed| (removed, transaction))
                    })
                },
                |removed| {
                    vec![
                        (removed.inode, removed.item.clone()),
                        (directory, removed.parent.clone()),
                    ]
                },
                |removed, reads, result_handling| {
                    self.kernel.reply(reads, result_handling, |handing| {
                        handing.invalidate_attributes([directory]);
                        handing.unname(directory, name);
                        let update = Update::Unlinked {
                            directory,
                            name,
                            inode,
                        };
                        (handing.attr(removed.inode), update)
                    })
                },
            )
            .await;
        let (attributes, update) = served.map_err(failed)?;
        self.kernel.apply(update).await;
        Ok(attributes)
    }

    // rename moves inode from one name in a directory to another, holding both directories' locks and then the locks of
    // the file and of the one the kernel finds at the destination, once it finds the file under its name. The volume
    // moves only the item it handed over under that name, and replaces whatever it finds at the destination, even when
    // the kernel found the name free, which the history records: the file itself leaves both names in place.
    async fn rename(
        &self,
        (from_directory, from): (Inode, &'static str),
        (to_directory, to): (Inode, &'static str),
        inode: Inode,
        _: Option<Inode>,
    ) -> Result<(Attributes, Option<Inode>), Failure> {
        let mut directories = vec![(from_directory, Mode::Exclusive)];
        if to_directory != from_directory {
            directories.push((to_directory, Mode::Exclusive));
        }
        let renaming = self.kernel.lock(directories).await;
        match self.find(from_directory, from).await? {
            None => return Err(Failure::Posix(PosixError::ENOENT)),
            Some(found) if found != inode => return Err(Failure::Posix(PosixError::ESTALE)),
            Some(_) => {}
        }
        let replaced = self.find(to_directory, to).await?;
        // The kernel renames nothing when it finds both names refer to one file, and the app sees the file as a stat
        // then finds it.
        if replaced == Some(inode) {
            drop(renaming);
            let (attributes, _) = self.stat(inode).await?;
            return Ok((attributes, replaced));
        }
        let items = [inode].into_iter().chain(replaced);
        let _moving = self
            .kernel
            .lock(items.map(|item| (item, Mode::Exclusive)).collect())
            .await;
        // UPSTREAM KERNEL BUG PIN: lookups on this mount still find the names until the reply.
        let _unnaming = self
            .kernel
            .unnaming(vec![(from_directory, from), (to_directory, to)]);
        let expected = self.kernel.named(from_directory, from);
        let found = Arc::new(Mutex::new(None));
        let served = self
            .engine
            .transact_and_serve(
                purges::NAME_CHANGES,
                |mut transaction| {
                    let found = Arc::clone(&found);
                    Box::pin(async move {
                        let at_destination = transaction.lookup(to_directory, to.as_bytes()).await;
                        *found.lock().unwrap_or_else(PoisonError::into_inner) = at_destination
                            .ok()
                            .and_then(|at| at.found)
                            .map(|(found, _)| found);
                        let result = transaction
                            .rename(
                                from_directory,
                                from.as_bytes(),
                                to_directory,
                                to.as_bytes(),
                                RenameMode::Replace,
                                expected,
                            )
                            .await;
                        result.map(|renamed| (renamed, transaction))
                    })
                },
                |renamed| {
                    [
                        (renamed.inode, renamed.item.clone()),
                        (from_directory, renamed.from.clone()),
                        (to_directory, renamed.to.clone()),
                    ]
                    .into_iter()
                    .chain(renamed.replaced.clone())
                    .collect()
                },
                |renamed, reads, result_handling| {
                    self.kernel.reply(reads, result_handling, |handing| {
                        handing.invalidate_attributes([from_directory, to_directory, inode]);
                        handing.unname(from_directory, from);
                        handing.name(to_directory, to, inode);
                        let update = Update::Renamed {
                            from: (from_directory, from),
                            to: (to_directory, to),
                            inode,
                            replaced,
                        };
                        (handing.attr(renamed.inode), update)
                    })
                },
            )
            .await;
        let (renamed, update) = served.map_err(failed)?;
        self.kernel.apply(update).await;
        let found = *found.lock().unwrap_or_else(PoisonError::into_inner);
        Ok((renamed, found))
    }

    // link gives inode another name, name in directory, holding the directory's lock and then the file's, once the
    // kernel finds the name free.
    async fn link(
        &self,
        inode: Inode,
        directory: Inode,
        name: &'static str,
    ) -> Result<Attributes, Failure> {
        let _linking = self.kernel.lock(vec![(directory, Mode::Exclusive)]).await;
        if self.find(directory, name).await?.is_some() {
            return Err(Failure::Posix(PosixError::EEXIST));
        }
        let _source = self.kernel.lock(vec![(inode, Mode::Exclusive)]).await;
        let served = self
            .engine
            .transact_and_serve(
                purges::NAME_CHANGES,
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
                // The kernel takes a link's attributes in whatever purge came since, so it keeps them for no time.
                |_, reads, result_handling| {
                    self.kernel.reply(reads, result_handling, |handing| {
                        handing.invalidate_attributes([directory]);
                        handing.hand(directory, name, inode);
                        let attributes = handing.attr(inode);
                        let kept = match handing.cached {
                            true => Kept::Name,
                            false => Kept::Nothing,
                        };
                        let entries = vec![(name, inode, attributes.clone(), kept)];
                        let update = Update::Entries {
                            directory,
                            entries,
                            sent: None,
                            cached: handing.cached,
                        };
                        (attributes, update)
                    })
                },
            )
            .await;
        let (attributes, update) = served.map_err(failed)?;
        self.kernel.apply(update).await;
        Ok(attributes)
    }

    // let_go lets go of a file once no operation refers to it, and says whether the volume forgot it.
    fn let_go(&self, inode: Inode) -> bool {
        self.kernel.reclaim(inode, || self.engine.forget(inode))
    }
}

// known is a name a listing read, as the activity names it.
fn known(name: &[u8]) -> &'static str {
    let known = NAMES.iter().find(|known| known.as_bytes() == name);
    known.expect("only known names are given")
}
