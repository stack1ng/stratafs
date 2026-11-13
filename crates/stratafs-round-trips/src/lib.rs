// stratafs-round-trips is a test helper that counts the round trips a store's reads take. CountedStore wraps a store,
// and reads sent together, such as the futures of one join, count as one round trip. It counts only while a test
// asks, and changes nothing about the store otherwise.

use std::future::Future;
use std::ops::RangeBounds;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::thread::{self, ThreadId};

use bytes::{Buf, Bytes};
use futures_core::future::BoxFuture;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::error::StoreError;
use stratafs_store::kv::{
    KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction, SetRejection, TransactError,
    TransactionHandle,
};
use stratafs_store::store::Store;

// RoundTrips counts round trips while during runs. A read joins the round trip that is gathering reads, or starts
// one. Each read waits one poll before it runs, so every read its task sends in the same pass joins before the first of
// them goes on, which ends the gathering: a read that needed another's answer starts the next round trip.
#[derive(Clone, Default)]
pub struct RoundTrips(Arc<Mutex<Counted>>);

#[derive(Default)]
struct Counted {
    // counting is the thread during runs on. Outside during a read runs at once, uncounted.
    counting: Option<ThreadId>,
    // elsewhere says whether another thread read while during ran, which the count cannot place.
    elsewhere: bool,
    count: usize,
    // gathering is the number of the round trip that still takes reads.
    gathering: Option<usize>,
}

impl RoundTrips {
    // during is how many round trips the reads operation makes take. operation must drive them on its own thread, as
    // block_on does: reads count as sent together when one task sends them in one pass, which threads have no like of.
    // It panics when another thread read meanwhile.
    pub fn during(&self, operation: impl FnOnce()) -> usize {
        *self.counted() = Counted {
            counting: Some(thread::current().id()),
            elsewhere: false,
            count: 0,
            gathering: None,
        };
        operation();
        let mut counted = self.counted();
        counted.counting = None;
        assert!(
            !counted.elsewhere,
            "another thread read from the store while its round trips were counted"
        );
        counted.count
    }

    // read resolves once a read may run, having counted it.
    fn read(&self) -> Read<'_> {
        Read {
            round_trips: self,
            joined: None,
        }
    }

    fn counted(&self) -> std::sync::MutexGuard<'_, Counted> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // end ends the gathering of round_trip, if it still gathers.
    fn end(&self, round_trip: usize) {
        let mut counted = self.counted();
        if counted.gathering == Some(round_trip) {
            counted.gathering = None;
        }
    }
}

// Read is one read waiting its poll, and the round trip it joined.
struct Read<'a> {
    round_trips: &'a RoundTrips,
    joined: Option<usize>,
}

impl Future for Read<'_> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        match self.joined {
            None => {
                let mut counted = self.round_trips.counted();
                match counted.counting {
                    None => return Poll::Ready(()),
                    Some(counting) if counting != thread::current().id() => {
                        counted.elsewhere = true;
                        return Poll::Ready(());
                    }
                    Some(_) => {}
                }
                let round_trip = match counted.gathering {
                    Some(gathering) => gathering,
                    None => {
                        counted.count += 1;
                        counted.gathering = Some(counted.count);
                        counted.count
                    }
                };
                drop(counted);
                self.joined = Some(round_trip);
                context.waker().wake_by_ref();
                Poll::Pending
            }
            Some(round_trip) => {
                self.round_trips.end(round_trip);
                Poll::Ready(())
            }
        }
    }
}

// A read dropped while it waits ends its round trip's gathering too, so no later read joins a round trip left open.
impl Drop for Read<'_> {
    fn drop(&mut self) {
        if let Some(round_trip) = self.joined {
            self.round_trips.end(round_trip);
        }
    }
}

// CountedStore is a store whose metadata reads are counted.
pub struct CountedStore<S> {
    inner: S,
    round_trips: RoundTrips,
}

impl<S> CountedStore<S> {
    pub fn new(inner: S) -> Self {
        CountedStore {
            inner,
            round_trips: RoundTrips::default(),
        }
    }

    pub fn round_trips(&self) -> &RoundTrips {
        &self.round_trips
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }
}

// CountedTransaction is a transaction whose reads are counted.
pub struct CountedTransaction<T> {
    inner: T,
    round_trips: RoundTrips,
}

impl<S: Store> CountedStore<S> {
    fn transaction<T>(&self, inner: T) -> CountedTransaction<T> {
        CountedTransaction {
            inner,
            round_trips: self.round_trips.clone(),
        }
    }
}

type Metadata<S> = <S as Store>::Metadata;

impl<S: Store> KeyValueStore for CountedStore<S> {
    type Rejection = <Metadata<S> as KeyValueStore>::Rejection;
    type Failure = <Metadata<S> as KeyValueStore>::Failure;
    type ReadTransaction<'txn> =
        CountedTransaction<<Metadata<S> as KeyValueStore>::ReadTransaction<'txn>>;
    type Transaction<'txn> = CountedTransaction<<Metadata<S> as KeyValueStore>::Transaction<'txn>>;

    const LARGEST_ACCEPTED_ENTRY: usize = <Metadata<S> as KeyValueStore>::LARGEST_ACCEPTED_ENTRY;

    async fn transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::Transaction<'txn>>,
        ) -> BoxFuture<
            'txn,
            Result<(T, TransactionHandle<'txn, 'store, Self::Transaction<'txn>>), E>,
        > + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        self.inner
            .metadata()
            .transact(|inner| {
                let working = work(TransactionHandle::new(self.transaction(inner.into_inner())));
                Box::pin(async move {
                    let (result, transaction) = working.await?;
                    Ok((
                        result,
                        TransactionHandle::new(transaction.into_inner().inner),
                    ))
                })
            })
            .await
    }

    async fn read_transact<'store, T: Send, E: Send>(
        &'store self,
        mut work: impl for<'txn> FnMut(
            TransactionHandle<'txn, 'store, Self::ReadTransaction<'txn>>,
        ) -> BoxFuture<'txn, Result<T, E>>
        + Send,
    ) -> Result<T, TransactError<Self::Rejection, Self::Failure, E>> {
        self.inner
            .metadata()
            .read_transact(|inner| {
                work(TransactionHandle::new(self.transaction(inner.into_inner())))
            })
            .await
    }

    fn changed(
        &self,
        key: Bytes,
        seen: Option<Bytes>,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.inner.metadata().changed(key, seen)
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.inner.metadata().ensure_durable(against)
    }
}

impl<S: Store> Store for CountedStore<S> {
    type Metadata = Self;
    type Content = S::Content;
    type Failure = S::Failure;

    fn metadata(&self) -> &Self {
        self
    }

    fn content(&self) -> &S::Content {
        self.inner.content()
    }

    fn ensure_durable(
        &self,
        against: DurableAgainst,
    ) -> impl Future<Output = Result<(), Self::Failure>> + Send {
        self.inner.ensure_durable(against)
    }
}

impl<T: ReadKeyValueTransaction> ReadKeyValueTransaction for CountedTransaction<T> {
    type Failure = T::Failure;

    async fn get(&self, key: Bytes) -> Result<Option<Bytes>, Self::Failure> {
        self.round_trips.read().await;
        self.inner.get(key).await
    }

    async fn get_range<R: RangeBounds<Bytes> + Send>(
        &self,
        range: R,
        limit: usize,
    ) -> Result<impl Iterator<Item = (Bytes, Bytes)> + Send + use<T, R>, Self::Failure> {
        self.round_trips.read().await;
        self.inner.get_range(range, limit).await
    }
}

impl<T: KeyValueTransaction> KeyValueTransaction for CountedTransaction<T> {
    type Rejection = T::Rejection;

    fn set(
        &mut self,
        key: Bytes,
        value: impl Buf,
    ) -> Result<(), StoreError<SetRejection<Self::Rejection>, Self::Failure>> {
        self.inner.set(key, value)
    }

    fn clear(&mut self, key: Bytes) -> Result<(), Self::Failure> {
        self.inner.clear(key)
    }

    fn clear_range(&mut self, range: impl RangeBounds<Bytes>) -> Result<(), Self::Failure> {
        self.inner.clear_range(range)
    }
}

#[cfg(test)]
mod tests;
