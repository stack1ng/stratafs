// Every write announces the version it will move each inode it changes to as the inode's pending change version,
// publishes it to the inode's holders, and commits once each dropped its copy. A write announces only while no other
// is in flight on the inode, so a number names it.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use futures_util::future::{join_all, select_all};
use stratafs_pubsub::{Key, PubSub};
use stratafs_store::error::StoreError;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};
use stratafs_store::store::Store;

use crate::attributes::{Attributes, ChangeVersion, read_attributes};
use crate::error::{IoFailure, refused_or_failed};
use crate::holds::{
    Connection, ConsistencyFailure, Failure, Inner, LATE_READS_WAIT, State, handled,
    handling_delivery, no_room,
};
use crate::kernel_cache::KernelCache;
use crate::key::FsKey;
use crate::{Cached, Change, Inode, MetadataFailure, Mount, PurgeWaits};

// SETTLE_MARGIN is how much longer than a publish's timeout a mount waits on another mount's write in flight before
// it takes the writer for gone and settles the write itself.
const SETTLE_MARGIN: Duration = Duration::from_secs(1);

// StoreWork is why announcing or settling a write failed in its transaction: reading an inode's attributes or pending
// change version, or writing one.
#[derive(Debug, thiserror::Error)]
pub enum StoreWork<R, M> {
    #[error(transparent)]
    Read(IoFailure<M, Infallible>),
    #[error(transparent)]
    Write(StoreError<R, M>),
}

// Pending is the pending change version of each inode that has one.
pub type Pending = HashMap<Inode, ChangeVersion>;

// Prepared is what a write readied: the version it announced and published for each inode it changes, and the holds
// of the inodes its reply hands the kernel, which wait for the reply.
#[derive(Clone)]
pub(crate) struct Prepared(Arc<Readied>);

struct Readied {
    // write names the write among this mount's writes, in claims.
    write: u64,
    announced: HashMap<Inode, ChangeVersion>,
    // new is each inode the write makes, held before it exists, and deleted each it deletes.
    new: Vec<Inode>,
    deleted: Vec<Inode>,
    // replying is each hold the write's reply holds up, by inode and hold.
    replying: Vec<(Inode, u64)>,
}

// Publishing counts a write a purge of any inode may wait for among those publishing until it is dropped.
struct Publishing<'a, P: PubSub>(&'a State<P>);

impl<P: PubSub> Drop for Publishing<'_, P> {
    fn drop(&mut self) {
        self.0.lock().publishing -= 1;
        self.0.wake();
    }
}

// Waiting is what a write waits for before it prepares again: another write of this mount to finish with inodes it
// claims, or other writes in flight on inodes it changes to end.
enum Waiting {
    Claimed,
    InFlight(Vec<(Inode, Attributes)>, Pending),
}

// claimed is each of inodes another write than write claims.
fn claimed<P: PubSub>(inner: &Inner<P>, write: u64, inodes: &[Inode]) -> Vec<Inode> {
    let other = |inode: &Inode| inner.claims.get(inode).is_some_and(|owner| *owner != write);
    inodes.iter().copied().filter(other).collect()
}

// contended says whether a write whose operation the kernel's purge waits for must fail rather than wait on inodes:
// a delivery of one is being handled here, or of any inode when a purge may wait on others, whose purge the kernel
// may hold back until the write ends.
fn contended<P: PubSub>(
    inner: &mut Inner<P>,
    inodes: &[Inode],
    purge_waits: PurgeWaits,
    kernel: &dyn KernelCache,
) -> bool {
    match purge_waits {
        PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode | PurgeWaits::Unknown
            if kernel.purge_waits_on_others() =>
        {
            handling_delivery(inner)
        }
        PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode | PurgeWaits::Unknown => inodes
            .iter()
            .any(|inode| inner.held(*inode).is_some_and(|hold| hold.pending > 0)),
        PurgeWaits::No => false,
    }
}

// live says whether a write is in flight on attributes, whose pending change version is pending.
pub(crate) fn live(attributes: &Attributes, pending: Option<&ChangeVersion>) -> bool {
    pending.is_some_and(|pending| pending.number > attributes.change_version.number)
}

// read_pending reads the pending change version of each inode of inodes that has one: the version a write in flight
// will move it to, named by the mount making it, kept apart from the attributes, which hold committed state only. The
// write's commit, or a settle, moves the change version up to it and clears it. It reads them in metadata, the
// transaction their attributes are read or written in, so both are of one moment.
pub(crate) async fn read_pending<T: ReadKeyValueTransaction>(
    metadata: &T,
    inodes: &[Inode],
) -> Result<Pending, IoFailure<T::Failure, Infallible>> {
    let mut pending = HashMap::new();
    for inode in inodes {
        if let Some(stored) = metadata
            .get(FsKey::new_pending_change_version(*inode).into())
            .await
            .map_err(IoFailure::Metadata)?
        {
            let version = ChangeVersion::view(&stored)
                .map_err(|corrupt| IoFailure::Corrupt(corrupt.into()))?;
            pending.insert(*inode, version);
        }
    }
    Ok(pending)
}

impl<St: Store, P: PubSub<Message = u64>> Mount<St, P> {
    // replied lets the holds a write's reply held up go on: it has replied, or will not.
    fn replied(&self, prepared: &Prepared) {
        if prepared.0.replying.is_empty() {
            return;
        }
        let mut inner = self.state.lock();
        for (inode, since) in &prepared.0.replying {
            if let Some(hold) = inner.held(*inode).filter(|hold| hold.since == *since) {
                hold.replying -= 1;
            }
        }
        drop(inner);
        self.state.wake();
    }

    // announce writes, for every inode of inodes, the version this write will move it to as its pending change
    // version, and returns the ones it wrote. An inode this write announced already keeps its announcement. When
    // another write is in flight on any of them, it writes nothing and returns their attributes and pending versions.
    #[allow(clippy::type_complexity)]
    async fn announce(
        &self,
        inodes: &[Inode],
        announced: &HashMap<Inode, ChangeVersion>,
    ) -> Result<
        Result<Vec<(Inode, ChangeVersion)>, (Vec<(Inode, Attributes)>, Pending)>,
        Failure<P, St>,
    > {
        self.store
            .metadata()
            .transact(|mut metadata| {
                let inodes = inodes.to_vec();
                let id = self.id;
                Box::pin(async move {
                    let mut read = Vec::new();
                    for inode in inodes {
                        // An inode that vanished fails the write when it runs again.
                        if let Some(attributes) = read_attributes(&*metadata, inode)
                            .await
                            .map_err(StoreWork::Read)?
                        {
                            read.push((inode, attributes));
                        }
                    }
                    let inodes: Vec<Inode> = read.iter().map(|(inode, _)| *inode).collect();
                    let pending = read_pending(&*metadata, &inodes)
                        .await
                        .map_err(StoreWork::Read)?;
                    let in_flight: Vec<(Inode, Attributes)> = read
                        .iter()
                        .filter(|(inode, attributes)| {
                            live(attributes, pending.get(inode))
                                && announced.get(inode) != pending.get(inode)
                        })
                        .cloned()
                        .collect();
                    if !in_flight.is_empty() {
                        return Ok((Err((in_flight, pending)), metadata));
                    }
                    let mut fresh = Vec::new();
                    for (inode, attributes) in read {
                        if live(&attributes, pending.get(&inode)) {
                            continue;
                        }
                        let version = attributes.change_version.after(id);
                        fresh.push((inode, version));
                        metadata
                            .set(
                                FsKey::new_pending_change_version(inode).into(),
                                Bytes::from(&version),
                            )
                            .map_err(|error| StoreWork::Write(refused_or_failed(error)))?;
                    }
                    Ok((Ok(fresh), metadata))
                })
            })
            .await
            .map_err(ConsistencyFailure::Store)
    }

    // ended waits until no write of in_flight is still in flight, settling any that outlasted its writer. A write whose
    // operation the kernel's purge waits for fails at once while a delivery it is contended by is handled here: that
    // delivery's write waits for this mount to drop a copy, which the kernel may hold back until this write ends.
    async fn ended(
        &self,
        mut in_flight: Vec<(Inode, Attributes)>,
        mut pending: Pending,
        purge_waits: PurgeWaits,
    ) -> Result<(), Failure<P, St>> {
        loop {
            let inodes: Vec<Inode> = in_flight.iter().map(|(inode, _)| *inode).collect();
            if contended(&mut self.state.lock(), &inodes, purge_waits, &*self.kernel) {
                return Err(ConsistencyFailure::Contended);
            }
            let ticket = self.state.clock.fetch_add(1, Ordering::SeqCst);
            self.settled(&in_flight, &pending, ticket, purge_waits)
                .await?;
            let (stored, stored_pending) = self.stored(&inodes).await?;
            in_flight = in_flight
                .into_iter()
                .zip(stored)
                .filter_map(|((inode, _), stored)| {
                    let version = stored_pending.get(&inode);
                    stored
                        .filter(|stored| live(stored, version) && version == pending.get(&inode))
                        .map(|stored| (inode, stored))
                })
                .collect();
            pending = stored_pending;
            if in_flight.is_empty() {
                return Ok(());
            }
        }
    }

    // stored reads the attributes of each inode of inodes, None for one that does not exist, and their pending change
    // versions.
    async fn stored(
        &self,
        inodes: &[Inode],
    ) -> Result<(Vec<Option<Attributes>>, Pending), Failure<P, St>> {
        self.store
            .metadata()
            .read_transact(|metadata| {
                Box::pin(async move {
                    let mut stored = Vec::new();
                    for inode in inodes {
                        stored.push(
                            read_attributes(&*metadata, *inode)
                                .await
                                .map_err(StoreWork::Read)?,
                        );
                    }
                    let pending = read_pending(&*metadata, inodes)
                        .await
                        .map_err(StoreWork::Read)?;
                    Ok((stored, pending))
                })
            })
            .await
            .map_err(ConsistencyFailure::Store)
    }

    // claim claims every inode of inodes for write, or none of them when another write of this mount claims one, and
    // says whether it did.
    fn claim(&self, write: u64, inodes: &[Inode]) -> bool {
        let mut inner = self.state.lock();
        if !claimed(&inner, write, inodes).is_empty() {
            return false;
        }
        for inode in inodes {
            inner.claims.insert(*inode, write);
        }
        true
    }

    // unclaimed waits until no other write of this mount claims any of inodes, and fails at once while the mount
    // purges, as client does. A write the kernel's purge waits for asks one running its own delivery to yield, since
    // that delivery's purge may wait on this write's operation.
    async fn unclaimed(
        &self,
        write: u64,
        inodes: &[Inode],
        purge_waits: PurgeWaits,
        kernel: &dyn KernelCache,
    ) -> Result<(), Failure<P, St>> {
        loop {
            let listener = {
                let mut inner = self.state.lock();
                let claimed = claimed(&inner, write, inodes);
                if claimed.is_empty() {
                    return Ok(());
                }
                if let Connection::Down { purging: true, .. } = inner.connection {
                    return Err(ConsistencyFailure::Purging);
                }
                if no_room(purge_waits, kernel) && handling_delivery(&inner) {
                    return Err(ConsistencyFailure::Contended);
                }
                if let PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode | PurgeWaits::Unknown =
                    purge_waits
                {
                    let Inner {
                        claims, delivering, ..
                    } = &mut *inner;
                    for owner in claimed.iter().filter_map(|inode| claims.get(inode)) {
                        if let Some(asked @ false) = delivering.get_mut(owner) {
                            *asked = true;
                            self.state.wake();
                        }
                    }
                }
                self.state.changed.listen()
            };
            listener.await;
        }
    }

    // publishing waits, for a write a purge of any inode may wait for, while one fewer than the kernel keeps in flight
    // of such writes publish, then counts it among them until it drops what it returns: a publish cannot stop, and a
    // purge may need one of them. It fails while the mount purges, and rather than wait while a delivery is handled
    // with no room.
    async fn publishing(
        &self,
        purge_waits: PurgeWaits,
        kernel: &dyn KernelCache,
    ) -> Result<Option<Publishing<'_, P>>, Failure<P, St>> {
        let Some(limit) = kernel
            .in_flight_limit()
            .filter(|_| purge_waits == PurgeWaits::OfAnyInode)
        else {
            return Ok(None);
        };
        // With a limit of one none may publish, and nothing would wake a wait.
        if limit < 2 {
            return Err(ConsistencyFailure::Contended);
        }
        loop {
            let listener = {
                let mut inner = self.state.lock();
                if inner.publishing + 1 < limit {
                    inner.publishing += 1;
                    return Ok(Some(Publishing(&self.state)));
                }
                if let Connection::Down { purging: true, .. } = inner.connection {
                    return Err(ConsistencyFailure::Purging);
                }
                if no_room(purge_waits, kernel) && handling_delivery(&inner) {
                    return Err(ConsistencyFailure::Contended);
                }
                self.state.changed.listen()
            };
            listener.await;
        }
    }

    // own_delivered waits until the own delivery of write ends, and fails once a write of this mount asks it to yield.
    async fn own_delivered(&self, write: u64) -> Result<(), Failure<P, St>> {
        loop {
            let listener = match self.state.lock().delivering.get(&write) {
                None => return Ok(()),
                Some(true) => return Err(ConsistencyFailure::Contended),
                Some(false) => self.state.changed.listen(),
            };
            listener.await;
        }
    }

    // unclaim lets go of every inode write claims.
    fn unclaim(&self, write: u64) {
        self.state.lock().claims.retain(|_, owner| *owner != write);
        self.state.wake();
    }

    // floor keeps this mount's reads of each inode of announced from going behind the write announcing it here, which
    // no delivery tells this mount of.
    fn floor(&self, announced: &HashMap<Inode, ChangeVersion>) {
        let mut inner = self.state.lock();
        for (inode, version) in announced {
            if let Some(hold) = inner.held(*inode) {
                hold.confirmed = hold.confirmed.max(version.number);
            }
        }
    }

    // settle ends writes in flight that no write will commit: every inode whose pending change version is still one of
    // announced moves to it with nothing else changed, which is what any mount told of the write waits for, and loses
    // its pending change version.
    async fn settle(
        &self,
        announced: &HashMap<Inode, ChangeVersion>,
    ) -> Result<(), Failure<P, St>> {
        if announced.is_empty() {
            return Ok(());
        }
        self.store
            .metadata()
            .transact(|mut metadata| {
                let announced = announced.clone();
                Box::pin(async move {
                    for (inode, version) in announced {
                        let Some(mut attributes) = read_attributes(&*metadata, inode)
                            .await
                            .map_err(StoreWork::Read)?
                        else {
                            continue;
                        };
                        let pending = read_pending(&*metadata, &[inode])
                            .await
                            .map_err(StoreWork::Read)?;
                        let pending = pending.get(&inode);
                        if live(&attributes, pending) && pending == Some(&version) {
                            attributes.change_version = version;
                            metadata
                                .set(
                                    FsKey::new_attributes(inode).into(),
                                    Bytes::from(&attributes),
                                )
                                .map_err(|error| StoreWork::Write(refused_or_failed(error)))?;
                            metadata
                                .clear(FsKey::new_pending_change_version(inode).into())
                                .map_err(|failure| StoreWork::Write(StoreError::Failed(failure)))?;
                        }
                    }
                    Ok(((), metadata))
                })
            })
            .await
            .map_err(ConsistencyFailure::Store)
    }

    // may_commit lets a write commit once it moves every inode it changes to the version it announced and published
    // and, if prepared by a mount that holds, still holds each through the hold it marked, so a delivery after the
    // commit waits for its reply. A new inode needs no announcement: no mount can hold it before the commit that names
    // it. The commit clears the pending change versions it takes up. It may change its own state of the inodes in
    // metadata, and must wait on nothing else.
    pub(crate) async fn may_commit<T: KeyValueTransaction<Failure = MetadataFailure<St>>>(
        &self,
        holds: bool,
        prepared: Option<&Prepared>,
        changes: &HashMap<Inode, Change>,
        metadata: &mut T,
    ) -> Result<bool, Failure<P, St>> {
        let new = |change: &Change| matches!(change, Change::Written(after) if after.change_version.number == 0);
        if prepared.is_none() && !changes.values().all(new) {
            return Ok(false);
        }
        let inodes: Vec<Inode> = changes.keys().copied().collect();
        let pending = read_pending(&*metadata, &inodes)
            .await
            .map_err(ConsistencyFailure::Pending)?;
        let published = |inode: &Inode, version: ChangeVersion| {
            prepared.is_some_and(|prepared| prepared.0.announced.get(inode) == Some(&version))
        };
        let versions = |(inode, change): (&Inode, &Change)| match change {
            Change::Written(after) => {
                after.change_version.number == 0
                    || (pending.get(inode) == Some(&after.change_version)
                        && published(inode, after.change_version))
            }
            Change::Deleted(before) => pending
                .get(inode)
                .is_some_and(|version| live(before, Some(version)) && published(inode, *version)),
        };
        let may_commit = match (holds, prepared) {
            (true, Some(prepared)) => {
                let mut inner = self.state.lock();
                let mut marked = |inode: Inode| {
                    inner
                        .held(inode)
                        .is_some_and(|hold| prepared.0.replying.contains(&(inode, hold.since)))
                };
                changes
                    .iter()
                    .all(|(inode, change)| marked(*inode) && versions((inode, change)))
            }
            (true, None) | (false, _) => changes.iter().all(versions),
        };
        if may_commit {
            for inode in pending.keys() {
                metadata
                    .clear(FsKey::new_pending_change_version(*inode).into())
                    .map_err(|failure| ConsistencyFailure::Pending(IoFailure::Metadata(failure)))?;
            }
        }
        Ok(may_commit)
    }

    // prepare claims every inode the write changes and, for a consistency that holds, holds it, new ones too so its
    // reply hands over only held inodes, then announces and publishes the version each existing one moves to. Before
    // waiting on another write in flight, it lets go of what it claimed and announced, so no two writes wait for each
    // other, and it does so and fails when a write its own delivery's purge may wait on asks it to yield. When it fails,
    // it first undoes what it did.
    pub(crate) async fn prepare<C: Cached<St, P>>(
        &self,
        consistency: &C,
        prepared: Option<Prepared>,
        changes: HashMap<Inode, Change>,
        purge_waits: PurgeWaits,
    ) -> Result<Prepared, Failure<P, St>> {
        let (write, mut announced) = match prepared {
            Some(prepared) => {
                self.replied(&prepared);
                (prepared.0.write, prepared.0.announced.clone())
            }
            None => (
                self.state.clock.fetch_add(1, Ordering::SeqCst),
                HashMap::new(),
            ),
        };
        // The inodes go in order, so a write holds, announces and publishes them the same way each time it runs.
        let mut changed: Vec<Inode> = changes.keys().copied().collect();
        changed.sort_by_key(|inode| inode.0);
        let of = |kind: fn(&Change) -> bool| -> Vec<Inode> {
            changes
                .iter()
                .filter(|(_, change)| kind(change))
                .map(|(inode, _)| *inode)
                .collect()
        };
        let new = of(
            |change| matches!(change, Change::Written(after) if after.change_version.number == 0),
        );
        let deleted = of(|change| matches!(change, Change::Deleted(_)));
        let existing: Vec<Inode> = changed
            .iter()
            .filter(|inode| !new.contains(inode))
            .copied()
            .collect();
        let holds = consistency.holds();
        loop {
            let readied = async {
                if !self.claim(write, &changed) {
                    return Ok(Some(Waiting::Claimed));
                }
                if holds {
                    consistency
                        .before_caching(self, &changed, consistency.reading(self))
                        .await?;
                }
                let (client, _) = self.client(None).await?;
                // It waits to publish before it announces, so other mounts never wait on its announcement for longer
                // than a publish. A mount that holds nothing gets no delivery, so it keeps no room for a purge.
                let publishing = match holds && !existing.is_empty() {
                    true => self.publishing(purge_waits, &*self.kernel).await?,
                    false => None,
                };
                let fresh = match self.announce(&existing, &announced).await? {
                    Ok(fresh) => fresh,
                    Err((in_flight, pending)) => {
                        return Ok(Some(Waiting::InFlight(in_flight, pending)));
                    }
                };
                announced.extend(fresh.iter().map(|(inode, version)| (*inode, *version)));
                // No delivery tells this mount of its own write. It drops this mount's copies as one would if the purge
                // goes ahead of it; one that would wait for it cannot, so the floor keeps reads behind it from being
                // cached, and only the kernel's copy of the attributes goes, which waits for nothing. A mount that holds
                // nothing has no copies of its own to drop here.
                let mut own = Vec::new();
                match purge_waits {
                    _ if !holds => {}
                    PurgeWaits::No => {
                        let mut inner = self.state.lock();
                        for (inode, version) in &fresh {
                            if let Some(hold) = inner.held(*inode) {
                                own.push((*inode, hold.since, version.number));
                            }
                        }
                    }
                    PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode | PurgeWaits::Unknown => {
                        self.floor(&announced);
                        for (inode, _) in &fresh {
                            self.kernel
                                .invalidate_attributes(*inode)
                                .map_err(ConsistencyFailure::Uninvalidated)?;
                        }
                    }
                }
                // The own delivery runs on its own, so a write that yields leaves its purge, which no one can cancel.
                let delivering = !own.is_empty();
                if delivering {
                    self.state.lock().delivering.insert(write, false);
                    self.state.wake();
                    let (state, kernel, runtime) = (
                        Arc::clone(&self.state),
                        Arc::clone(&self.kernel),
                        Arc::clone(&self.runtime),
                    );
                    self.runtime.spawn(Box::pin(async move {
                        join_all(own.into_iter().map(|(inode, id, version)| {
                            handled(&state, &*kernel, &runtime, inode, id, version)
                        }))
                        .await;
                        state.lock().delivering.remove(&write);
                        state.wake();
                    }));
                }
                let published = join_all(fresh.iter().map(|(inode, version)| {
                    let client = Arc::clone(&client);
                    let (inode, number) = (*inode, version.number);
                    async move { client.publish(Key(inode.0), &number, self.timeout).await }
                }))
                .await;
                drop(publishing);
                if let Some(failure) = published.into_iter().find_map(Result::err) {
                    return Err(ConsistencyFailure::Publish(failure));
                }
                if delivering {
                    self.own_delivered(write).await?;
                }
                Ok(None)
            }
            .await;
            let waiting = match readied {
                Ok(None) => break,
                Ok(Some(waiting)) => waiting,
                // Nothing of the write committed, so neither its new inodes nor the one it deletes get an item.
                Err(failure) => {
                    let settled = self.settle(&announced).await;
                    self.release(&new);
                    self.release(&deleted);
                    self.unclaim(write);
                    settled?;
                    return Err(failure);
                }
            };
            let settled = self.settle(&announced).await;
            announced.clear();
            self.unclaim(write);
            let waited = async {
                settled?;
                match waiting {
                    Waiting::Claimed => {
                        self.unclaimed(write, &changed, purge_waits, &*self.kernel)
                            .await?
                    }
                    Waiting::InFlight(in_flight, pending) => {
                        self.ended(in_flight, pending, purge_waits).await?;
                    }
                }
                Ok(())
            }
            .await;
            if let Err(failure) = waited {
                self.release(&new);
                self.release(&deleted);
                return Err(failure);
            }
        }
        let replying = match holds {
            true => {
                let mut inner = self.state.lock();
                changed
                    .iter()
                    .filter_map(|inode| {
                        let hold = inner.held(*inode)?;
                        hold.replying += 1;
                        Some((*inode, hold.since))
                    })
                    .collect()
            }
            false => Vec::new(),
        };
        Ok(Prepared(Arc::new(Readied {
            write,
            announced,
            new,
            deleted,
            replying,
        })))
    }

    // finish ends a write prepare readied: once it will not commit, with no changes, or once it committed the changes
    // it made and the kernel was handed what it committed. It settles every announcement the write left that no commit
    // of it took up: all of them when it did not commit, and those of inodes an earlier attempt changed but the
    // committed one did not.
    pub(crate) async fn finish(
        &self,
        prepared: Prepared,
        committed: Option<&HashMap<Inode, Change>>,
    ) {
        self.replied(&prepared);
        // The kernel never gets an item again for an inode a write deletes, which it let go of before, nor for a new
        // inode the write did not make, so nothing else lets go of their holds.
        let mut released = prepared.0.deleted.clone();
        match committed {
            Some(changes) => {
                let deleted = changes
                    .iter()
                    .filter(|(_, change)| matches!(change, Change::Deleted(_)))
                    .map(|(inode, _)| *inode);
                let unmade = prepared.0.new.iter().copied();
                let unmade = unmade.filter(|inode| !changes.contains_key(inode));
                released.extend(deleted.chain(unmade));
            }
            // A write that failed may have committed when only the store's answer was lost, so only the new inodes
            // the store does not have are let go of.
            None => match self.stored(&prepared.0.new).await {
                Ok((stored, _)) => released.extend(
                    prepared
                        .0
                        .new
                        .iter()
                        .zip(stored)
                        .filter(|(_, stored)| stored.is_none())
                        .map(|(inode, _)| *inode),
                ),
                Err(error) => {
                    tracing::warn!(%error, "reading the new inodes of a write that did not commit failed, so their holds stay");
                }
            },
        }
        self.release(&released);
        let left: HashMap<Inode, ChangeVersion> = prepared
            .0
            .announced
            .iter()
            .filter(|(inode, _)| committed.is_none_or(|changes| !changes.contains_key(inode)))
            .map(|(inode, version)| (*inode, *version))
            .collect();
        if let Err(error) = self.settle(&left).await {
            tracing::warn!(%error, "settling what a write announced and did not commit failed");
        }
        self.unclaim(prepared.0.write);
    }

    // settled waits for a change to this mount's state since ticket, the end of a write in flight a read shows or
    // is behind, or the deadline of a wait that ends by time alone: settling a write whose writer is gone, or a round
    // a late read waits for.
    pub(crate) async fn settled(
        &self,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
        purge_waits: PurgeWaits,
    ) -> Result<(), Failure<P, St>> {
        let listener = self.state.changed.listen();
        if no_room(purge_waits, &*self.kernel) && handling_delivery(&self.state.lock()) {
            return Err(ConsistencyFailure::Contended);
        }
        if self.state.woken.load(Ordering::SeqCst) > ticket {
            return Ok(());
        }
        let mut stale = HashMap::new();
        let mut deadline: Option<Instant> = None;
        let mut watched = Vec::new();
        {
            let mut inner = self.state.lock();
            // A write in flight not met again for twice the wait is long over, settled by some read or by its writer.
            let waits = self.timeout + SETTLE_MARGIN;
            let now = self.runtime.now();
            inner.seen.retain(|_, first| now - *first < waits * 2);
            for (inode, read) in reads {
                let version = pending
                    .get(inode)
                    .filter(|version| live(read, Some(version)));
                if let Some(version) = version {
                    let first = *inner
                        .seen
                        .entry((*inode, version.mount, version.number))
                        .or_insert(now);
                    if now - first >= waits {
                        stale.insert(*inode, *version);
                    }
                    deadline = Some(deadline.map_or(first + waits, |at| at.min(first + waits)));
                }
                let hold = inner.held(*inode);
                // A late read waits for the round at most LATE_READS_WAIT, which may have passed since it was handled.
                if matches!(purge_waits, PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode)
                    && let Some(asked) = hold.as_ref().and_then(|hold| hold.asked_at)
                {
                    let ended = asked + LATE_READS_WAIT;
                    deadline = Some(deadline.map_or(ended, |at| at.min(ended)));
                }
                let behind = hold.is_some_and(|hold| read.change_version.number < hold.confirmed);
                if version.is_some() || behind {
                    watched.push((*inode, read));
                }
            }
        }
        if !stale.is_empty() {
            return self.settle(&stale).await;
        }
        // Inodes the kernel may have an item for that are not held now, such as lost ones, are held again.
        let lost: Vec<Inode> = {
            let mut inner = self.state.lock();
            reads
                .iter()
                .map(|(inode, _)| *inode)
                .filter(|inode| inner.holds.contains_key(inode) && inner.held(*inode).is_none())
                .collect()
        };
        if !lost.is_empty() {
            return self.hold(&lost, 0).await.map(|_| ());
        }
        // A write ends by committing or being settled, which moves the inode's attributes and clears its pending
        // change version.
        let metadata = self.store.metadata();
        let mut ends: Vec<BoxFuture<'_, Result<(), MetadataFailure<St>>>> =
            vec![Box::pin(listener.map(Ok))];
        if let Some(deadline) = deadline {
            ends.push(Box::pin(self.runtime.sleep_until(deadline).map(Ok)));
        }
        for (inode, read) in watched {
            let key = FsKey::new_attributes(inode).into();
            ends.push(Box::pin(metadata.changed(key, Some(Bytes::from(read)))));
            let seen = pending.get(&inode).map(Bytes::from);
            let key = FsKey::new_pending_change_version(inode).into();
            ends.push(Box::pin(metadata.changed(key, seen)));
        }
        let (ended, _, _) = select_all(ends).await;
        ended.map_err(ConsistencyFailure::Watch)
    }

    // reread reads again the inodes a write committed for its reply, with their pending change versions, failing when
    // another write changed one since. A write's reply may bind names, so it cannot be read again; one not handed over
    // at once is handled as reread, which shows any write in flight since.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn reread(
        &self,
        committed: &[(Inode, Attributes)],
    ) -> Result<(Vec<(Inode, Attributes)>, Pending), Failure<P, St>> {
        let inodes: Vec<Inode> = committed.iter().map(|(inode, _)| *inode).collect();
        let (stored, pending) = self.stored(&inodes).await?;
        let reread = committed
            .iter()
            .zip(stored)
            .map(|((inode, written), stored)| match stored {
                Some(stored) if stored.change_version == written.change_version => {
                    Ok((*inode, stored))
                }
                _ => Err(ConsistencyFailure::Overtaken),
            })
            .collect::<Result<_, _>>()?;
        Ok((reread, pending))
    }
}

#[cfg(test)]
mod tests;
