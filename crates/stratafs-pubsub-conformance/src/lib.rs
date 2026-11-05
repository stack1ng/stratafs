// stratafs-pubsub-conformance checks that a pub/sub keeps the PubSub contract. pubsub_conformance! makes a test of
// each check, each against a fresh volume of the pub/sub under test.

mod memory;
mod random;
mod runtime;

use std::future::pending;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use stratafs_pubsub::{Key, PubSub, PubSubError};
use tokio::sync::Notify;

pub use memory::{MemoryClient, MemoryFailure, MemorySubscription, MemoryVolume};
pub use random::no_holder_misses_a_publish;
pub use runtime::TokioRuntime;

// Volume is a fresh volume of the pub/sub under test.
pub trait Volume: Send + Sync + 'static {
    type Client: PubSub<Message = u64>;
    // Connection is a client's connection to the volume.
    type Connection: Send;

    fn connect(&self) -> impl Future<Output = (Self::Client, Self::Connection)> + Send;

    // cut ends connection the way a network failure does, without its client closing it.
    fn cut(&self, connection: &Self::Connection);

    // silence stops connection carrying anything, the way a network that goes dead does, without closing it.
    fn silence(&self, connection: &Self::Connection);
}

// pubsub_conformance makes a Tokio test of each check, with any attributes given after the volume, such as #[ignore].
// volume is an expression whose future is a fresh volume, evaluated once for each test.
#[macro_export]
macro_rules! pubsub_conformance {
    ($volume:expr $(, #[$attribute:meta])* $(,)?) => {
        $crate::pubsub_conformance!(@checks [$(#[$attribute])*] $volume;
            a_publish_with_no_other_holders_resolves,
            a_client_does_not_get_its_own_publishes,
            a_publish_resolves_once_every_other_holder_has_handled_it,
            a_client_holds_a_key_once,
            a_dropped_subscription_no_longer_gets_publishes,
            a_subscribe_dropped_before_it_returns_releases_its_key,
            a_holder_that_does_not_handle_a_publish_in_time_fails,
            a_dropped_publish_fails_the_holders_that_had_not_handled_it,
            a_holder_whose_connection_is_cut_no_longer_holds_up_publishes,
            a_client_whose_connection_is_cut_fails,
            a_silenced_holder_fails_and_no_longer_holds_up_publishes,
            a_silenced_writer_fails_and_its_publish_is_not_confirmed,
            no_holder_misses_a_publish,
        );
    };
    (@checks $attributes:tt $volume:expr; $($check:ident),* $(,)?) => {
        $($crate::pubsub_conformance!(@check $attributes $volume; $check);)*
    };
    (@check [$($attribute:tt)*] $volume:expr; $check:ident) => {
        #[::tokio::test(flavor = "multi_thread")]
        $($attribute)*
        async fn $check() {
            let volume = $volume.await;
            // A pub/sub that loses an answer fails the check, rather than hanging it.
            ::tokio::time::timeout(::std::time::Duration::from_secs(60), $crate::$check(volume))
                .await
                .expect("the check did not finish within a minute");
        }
    };
}

const KEY: Key = Key(7);
// PATIENT is long enough for any holder that is not stuck to handle a publish.
const PATIENT: Duration = Duration::from_secs(5);
// IMPATIENT is the timeout of a publish meant to time out.
const IMPATIENT: Duration = Duration::from_millis(200);

pub async fn a_publish_with_no_other_holders_resolves(volume: impl Volume) {
    let (writer, _) = volume.connect().await;
    writer.publish(KEY, &1, PATIENT).await.unwrap();
}

pub async fn a_client_does_not_get_its_own_publishes(volume: impl Volume) {
    let (writer, _) = volume.connect().await;
    let delivered = Delivered::default();
    // Its handler never finishes, so the publish resolves only if it does not wait on it.
    let _subscription = writer
        .subscribe(KEY, delivered.then(pending))
        .await
        .unwrap();
    writer.publish(KEY, &1, PATIENT).await.unwrap();
    assert_eq!(delivered.taken(), Vec::<u64>::new());
}

pub async fn a_publish_resolves_once_every_other_holder_has_handled_it(volume: impl Volume) {
    let mut holders = Vec::new();
    let handled = Arc::new(Mutex::new(Vec::new()));
    // The holders take different times, so a publish that waits only on the quick one resolves before the slow one
    // is done.
    for delay in [0, 100] {
        let (holder, _) = volume.connect().await;
        let subscription = holder
            .subscribe(KEY, records_after(Duration::from_millis(delay), &handled))
            .await
            .unwrap();
        holders.push((holder, subscription));
    }
    let (writer, _) = volume.connect().await;
    writer.publish(KEY, &2, PATIENT).await.unwrap();
    assert_eq!(*handled.lock().unwrap(), vec![2, 2]);
}

pub async fn a_client_holds_a_key_once(volume: impl Volume) {
    let (holder, _) = volume.connect().await;
    let subscription = holder.subscribe(KEY, |_| async {}).await.unwrap();
    let again = holder.subscribe(KEY, |_| async {}).await;
    assert!(
        matches!(again, Err(PubSubError::AlreadySubscribed)),
        "{:?}",
        again.err()
    );
    drop(subscription);
    holder.subscribe(KEY, |_| async {}).await.unwrap();
    // A subscribe of the key that is still running holds it too.
    let (first, second) = tokio::join!(
        holder.subscribe(Key(8), |_| async {}),
        holder.subscribe(Key(8), |_| async {}),
    );
    assert!(
        matches!(
            (&first, &second),
            (Ok(_), Err(PubSubError::AlreadySubscribed))
                | (Err(PubSubError::AlreadySubscribed), Ok(_))
        ),
        "{:?} {:?}",
        first.err(),
        second.err()
    );
}

pub async fn a_dropped_subscription_no_longer_gets_publishes(volume: impl Volume) {
    let (holder, _) = volume.connect().await;
    let delivered = Delivered::default();
    let reached = Arc::new(Notify::new());
    let reaching = Arc::clone(&reached);
    // Its handler never finishes, so the publish resolves only once dropping the subscription lets go of the key.
    let subscription = holder
        .subscribe(
            KEY,
            delivered.then(move || {
                reaching.notify_one();
                pending()
            }),
        )
        .await
        .unwrap();
    let (writer, _) = volume.connect().await;
    let publishing = writer.publish(KEY, &5, PATIENT);
    let dropping = async {
        reached.notified().await;
        drop(subscription);
    };
    let (published, ()) = within(PATIENT, async { tokio::join!(publishing, dropping) }).await;
    published.unwrap();
    writer.publish(KEY, &18, PATIENT).await.unwrap();
    assert_eq!(delivered.taken(), vec![5]);
    assert!(holder.failed().now_or_never().is_none());
}

pub async fn a_subscribe_dropped_before_it_returns_releases_its_key(volume: impl Volume) {
    // Its handler never finishes, so a publish resolves only if the dropped subscribe left nothing waiting on it.
    let (abandoning, _) = volume.connect().await;
    dropped_subscribe(&abandoning).await;
    let (writer, _) = volume.connect().await;
    writer.publish(KEY, &6, PATIENT).await.unwrap();
    // And the key can be subscribed again at once.
    let (holder, _) = volume.connect().await;
    dropped_subscribe(&holder).await;
    let delivered = Delivered::default();
    let _subscription = holder
        .subscribe(KEY, delivered.then(|| async {}))
        .await
        .unwrap();
    writer.publish(KEY, &7, PATIENT).await.unwrap();
    assert_eq!(delivered.taken(), vec![7]);
}

// dropped_subscribe starts a subscribe to KEY whose handler never finishes, polls it once, so that a subscribe which
// waits on an answer has sent for it, and drops it, with any subscription it already returned.
async fn dropped_subscribe(client: &impl PubSub<Message = u64>) {
    let mut subscribing = pin!(client.subscribe(KEY, |_| pending()));
    let _ = subscribing.as_mut().now_or_never();
}

pub async fn a_holder_that_does_not_handle_a_publish_in_time_fails(volume: impl Volume) {
    let (stuck, _) = volume.connect().await;
    let _stuck_subscription = stuck.subscribe(KEY, |_| pending()).await.unwrap();
    let (prompt, _) = volume.connect().await;
    let delivered = Delivered::default();
    let _prompt_subscription = prompt
        .subscribe(KEY, delivered.then(|| async {}))
        .await
        .unwrap();
    let (writer, _) = volume.connect().await;
    let published = writer.publish(KEY, &8, IMPATIENT).await;
    assert!(
        matches!(published, Err(PubSubError::TimedOut)),
        "{published:?}"
    );
    within(PATIENT, stuck.failed()).await;
    let after = stuck.publish(KEY, &9, PATIENT).await;
    assert!(matches!(after, Err(PubSubError::Failed(_))), "{after:?}");
    // The stuck holder no longer holds up publishes, and the one that handled it in time still gets them.
    writer.publish(KEY, &10, PATIENT).await.unwrap();
    assert_eq!(delivered.taken(), vec![8, 10]);
}

pub async fn a_dropped_publish_fails_the_holders_that_had_not_handled_it(volume: impl Volume) {
    let (stuck, _) = volume.connect().await;
    let _stuck_subscription = stuck.subscribe(KEY, |_| pending()).await.unwrap();
    let (prompt, _) = volume.connect().await;
    let delivered = Delivered::default();
    let _prompt_subscription = prompt
        .subscribe(KEY, delivered.then(|| async {}))
        .await
        .unwrap();
    let (writer, _) = volume.connect().await;
    // Its timeout is far off, so only the drop can fail the stuck holder in time.
    let dropped =
        tokio::time::timeout(IMPATIENT, writer.publish(KEY, &11, Duration::from_secs(60))).await;
    assert!(dropped.is_err(), "the publish resolved: {dropped:?}");
    within(PATIENT, stuck.failed()).await;
    writer.publish(KEY, &12, PATIENT).await.unwrap();
    assert_eq!(delivered.taken(), vec![11, 12]);
}

pub async fn a_holder_whose_connection_is_cut_no_longer_holds_up_publishes(volume: impl Volume) {
    let (holder, connection) = volume.connect().await;
    let reached = Arc::new(Notify::new());
    let reaching = Arc::clone(&reached);
    let _subscription = holder
        .subscribe(KEY, move |_| {
            reaching.notify_one();
            pending()
        })
        .await
        .unwrap();
    let (writer, _) = volume.connect().await;
    let publishing = writer.publish(KEY, &13, PATIENT);
    let cutting = async {
        reached.notified().await;
        volume.cut(&connection);
    };
    let (published, ()) = within(PATIENT, async { tokio::join!(publishing, cutting) }).await;
    published.unwrap();
    within(PATIENT, holder.failed()).await;
}

pub async fn a_client_whose_connection_is_cut_fails(volume: impl Volume) {
    let (slow, _) = volume.connect().await;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let _slow_subscription = slow
        .subscribe(KEY, records_after(2 * IMPATIENT, &handled))
        .await
        .unwrap();
    let (writer, connection) = volume.connect().await;
    let publishing = writer.publish(KEY, &14, Duration::from_secs(60));
    let cutting = async {
        tokio::time::sleep(IMPATIENT).await;
        volume.cut(&connection);
    };
    let (published, ()) = within(PATIENT, async { tokio::join!(publishing, cutting) }).await;
    assert!(
        matches!(published, Err(PubSubError::Failed(_))),
        "{published:?}"
    );
    within(PATIENT, writer.failed()).await;
    let subscribed = writer.subscribe(Key(8), |_| async {}).await;
    assert!(
        matches!(subscribed, Err(PubSubError::Failed(_))),
        "{:?}",
        subscribed.err()
    );
    // A publisher that fails disconnects none of the holders it was waiting on, which still handle its message.
    let (other_writer, _) = volume.connect().await;
    other_writer.publish(KEY, &15, PATIENT).await.unwrap();
    assert_eq!(*handled.lock().unwrap(), vec![14, 15]);
}

pub async fn a_silenced_holder_fails_and_no_longer_holds_up_publishes(volume: impl Volume) {
    let (holder, connection) = volume.connect().await;
    let _subscription = holder.subscribe(KEY, |_| pending()).await.unwrap();
    volume.silence(&connection);
    within(PATIENT, holder.failed()).await;
    let (writer, _) = volume.connect().await;
    writer.publish(KEY, &16, PATIENT).await.unwrap();
}

pub async fn a_silenced_writer_fails_and_its_publish_is_not_confirmed(volume: impl Volume) {
    let (writer, connection) = volume.connect().await;
    volume.silence(&connection);
    let published = writer.publish(KEY, &17, PATIENT).await;
    assert!(published.is_err(), "{published:?}");
    within(PATIENT, writer.failed()).await;
}

// Delivered records the messages handlers are given.
#[derive(Clone, Default)]
struct Delivered(Arc<Mutex<Vec<u64>>>);

impl Delivered {
    // then is a handler that records its message as soon as it is called, and then waits on what next makes.
    fn then<Next: Future<Output = ()> + Send + 'static>(
        &self,
        next: impl Fn() -> Next + Send + Sync + 'static,
    ) -> impl Fn(u64) -> Next + Send + Sync + 'static {
        let delivered = self.clone();
        move |message| {
            delivered.0.lock().unwrap().push(message);
            next()
        }
    }

    fn taken(&self) -> Vec<u64> {
        std::mem::take(&mut self.0.lock().unwrap())
    }
}

// records_after is a handler that takes delay to handle a message, and records it in handled once it has.
fn records_after(
    delay: Duration,
    handled: &Arc<Mutex<Vec<u64>>>,
) -> impl Fn(u64) -> BoxFuture<'static, ()> + Send + Sync + 'static {
    let handled = Arc::clone(handled);
    move |message| {
        let handled = Arc::clone(&handled);
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            handled.lock().unwrap().push(message);
        })
    }
}

// within is what future resolves to, which it must within timeout.
async fn within<T>(timeout: Duration, future: impl Future<Output = T>) -> T {
    tokio::time::timeout(timeout, future)
        .await
        .expect("it did not happen in time")
}
