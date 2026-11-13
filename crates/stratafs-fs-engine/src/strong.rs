// The strong until disconnect, cached consistency. A mount holds every inode the kernel caches, hands the kernel nothing
// older than it acknowledged, and waits on writes in flight it may have missed.

use std::sync::atomic::Ordering;

use stratafs_pubsub::PubSub;
use stratafs_store::store::Store;

use crate::attributes::Attributes;
use crate::holds::{Failure, LATE_READS_WAIT};
use crate::writes::{Pending, live};
use crate::{Cached, Consistency, Inode, Mount, PurgeWaits, ResultHandling};

// StrongUntilDisconnectCached serves reads from a cache, so every write, from a client of any consistency, waits until
// all other StrongUntilDisconnectCached clients have been invalidated before it can return. A client that disconnects
// may miss writes until it notices and fails.
pub struct StrongUntilDisconnectCached;

impl Consistency for StrongUntilDisconnectCached {}

impl StrongUntilDisconnectCached {
    // handling is the result handling of reads, as result_handling says, letting unheld inodes go when listed.
    #[allow(clippy::too_many_arguments)]
    fn handling<St, P: PubSub>(
        &self,
        mount: Mount<'_, St, P>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
        began: u64,
        purge_waits: PurgeWaits,
        no_cache_honoured: bool,
        listed: bool,
    ) -> ResultHandling {
        let mut inner = mount.state.lock();
        let mut replies = Vec::new();
        // behind says whether a read is older than a write this mount knows of, or shows a write in flight it may have
        // missed, which only that write's end lets it be cached past.
        let mut behind = false;
        for (inode, read) in reads {
            let Some(hold) = inner.held(*inode) else {
                // The kernel has an item for no inode this mount never held, nor let go of.
                if listed && !inner.holds.contains_key(inode) {
                    continue;
                }
                return ResultHandling::Retry;
            };
            let version = read.change_version.number;
            if hold.since >= ticket {
                return ResultHandling::Retry;
            }
            if version < hold.confirmed {
                behind = true;
                continue;
            }
            let pending_version = pending.get(inode);
            // A read showing a write in flight older than the delivery goes no further: that write may have finished.
            if hold.pending > 0
                && version < hold.pending_highest
                && (!live(read, pending_version)
                    || pending_version
                        .is_some_and(|pending| pending.number >= hold.pending_highest))
            {
                // The purge drops a read it waits for begun before the kernel was asked. Any other the kernel asks
                // again about goes uncached, as one behind does. One begun since, likely held back until the kernel
                // purged, waits for the round to end, at most LATE_READS_WAIT as the purge may wait for it. Any other,
                // such as a write's reply, which must go, makes the delivery invalidate again; then a read the purge
                // goes ahead of waits for the write, which waits on nothing here.
                match purge_waits {
                    PurgeWaits::No if hold.again > 0 => return ResultHandling::Retry,
                    PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode
                        if hold.asked == 0 || began < hold.round => {}
                    _ if no_cache_honoured => behind = true,
                    PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode
                        if hold.asked_at.is_some_and(|asked| {
                            mount.specification.runtime.now() - asked < LATE_READS_WAIT
                        }) =>
                    {
                        return ResultHandling::Retry;
                    }
                    PurgeWaits::No
                    | PurgeWaits::OfOwnInode
                    | PurgeWaits::OfAnyInode
                    | PurgeWaits::Unknown => replies.push(*inode),
                }
                continue;
            }
            behind |= live(read, pending_version);
        }
        // Waiting on a write while the kernel's purge may wait for this operation could hold up a purge that write
        // waits on, so the reply goes uncached instead: the read was taken while the operation ran, so it may be
        // returned, only not kept.
        if behind {
            return match purge_waits {
                PurgeWaits::OfOwnInode | PurgeWaits::OfAnyInode | PurgeWaits::Unknown => {
                    ResultHandling::NoCache
                }
                PurgeWaits::No => ResultHandling::Retry,
            };
        }
        for inode in replies {
            if let Some(hold) = inner.held(inode) {
                hold.replies += 1;
            }
        }
        ResultHandling::Cache
    }
}

impl<St: Store, P: PubSub<Message = u64>> Cached<St, P> for StrongUntilDisconnectCached {
    type Ticket = u64;

    fn reading(&self, mount: Mount<'_, St, P>) -> u64 {
        mount.state.clock.fetch_add(1, Ordering::SeqCst)
    }

    // before_caching holds every inode of inodes, and says whether each was held already when ticket was taken.
    async fn before_caching(
        &self,
        mount: Mount<'_, St, P>,
        inodes: &[Inode],
        ticket: u64,
    ) -> Result<bool, Failure<P, St>> {
        mount.hold(inodes, ticket).await
    }

    // result_handling has the kernel cache a read taken after its inodes were held, not older than any write this mount
    // acknowledged, and with no write in flight it may have missed. One failing only the last two is NoCache when the
    // purge may wait for its operation. A read older than a delivery being handled may go when the purge waits, or
    // NoCache when no_cache_honoured.
    fn result_handling(
        &self,
        mount: Mount<'_, St, P>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
        began: u64,
        purge_waits: PurgeWaits,
        no_cache_honoured: bool,
    ) -> ResultHandling {
        self.handling(
            mount,
            reads,
            pending,
            ticket,
            began,
            purge_waits,
            no_cache_honoured,
            false,
        )
    }

    // result_handling_listed handles the inodes of a listing held already as result_handling does, and lets the others
    // go, but for lost ones, which settled holds again.
    fn result_handling_listed(
        &self,
        mount: Mount<'_, St, P>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
    ) -> ResultHandling {
        self.handling(
            mount,
            reads,
            pending,
            ticket,
            ticket,
            PurgeWaits::Unknown,
            false,
            true,
        )
    }

    async fn settled(
        &self,
        mount: Mount<'_, St, P>,
        reads: &[(Inode, Attributes)],
        pending: &Pending,
        ticket: u64,
        purge_waits: PurgeWaits,
    ) -> Result<(), Failure<P, St>> {
        mount.settled(reads, pending, ticket, purge_waits).await
    }

    fn cached(&self, _: Mount<'_, St, P>, _: Inode, _: &Attributes) {}

    fn forget(&self, mount: Mount<'_, St, P>, inode: Inode) {
        mount.release(&[inode]);
    }

    fn write_back(&self) -> bool {
        false
    }

    fn holds(&self) -> bool {
        true
    }
}
