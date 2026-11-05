// stratafs-pubsub is the contract of an acknowledged pub/sub: a publish to a key resolves once every other client
// holding that key has handled it. One pub/sub serves one volume, and its keys are the volume's inodes. It is strong
// until disconnect: a client that loses its connection may miss publishes until it notices and fails.

use std::time::Duration;

// Key is what a subscription and a publish are about: an inode within the volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key(pub u64);

#[derive(Debug, thiserror::Error)]
pub enum PubSubError<F> {
    // The publish did not resolve within its timeout.
    #[error("the publish did not resolve in time")]
    TimedOut,

    // The client holds the key through a Subscription, or a subscribe of the key is still running.
    #[error("the client already holds the key")]
    AlreadySubscribed,

    // The client failed, for the reason failed resolves to.
    #[error("the client failed")]
    Failed(#[source] F),
}

// PubSub is one client of a volume's pub/sub.
pub trait PubSub: Send + Sync + 'static {
    type Message: Send + Sync + 'static;
    // Subscription holds its key until it is dropped.
    type Subscription: Send + 'static;
    // Failure is why the client failed.
    type Failure: std::error::Error + Clone + Send + Sync + 'static;

    // subscribe makes the client a holder of key, and returns once it holds it. on_published then runs for each
    // message another client publishes to key, and that publish waits for the future it returns. A publish that lands
    // before the client holds key never reaches it, so the subscriber closes that race through its own store.
    fn subscribe<Handled>(
        &self,
        key: Key,
        on_published: impl Fn(Self::Message) -> Handled + Send + Sync + 'static,
    ) -> impl Future<Output = Result<Self::Subscription, PubSubError<Self::Failure>>> + Send
    where
        Handled: Future<Output = ()> + Send + 'static;

    // publish sends message to every other client holding key, and resolves once each has handled it. A publish that
    // does not resolve to Ok was not confirmed, so the change it announces must not be made. When timeout passes
    // first, or publish's future is dropped first, the pub/sub disconnects the holders it still waits on once it
    // hears of it, so they hold up no later publish, and their clients fail. When the publisher's own client fails
    // first, publish resolves to Failed and disconnects nobody.
    fn publish(
        &self,
        key: Key,
        message: &Self::Message,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), PubSubError<Self::Failure>>> + Send;

    // failed resolves to why the client failed, once it has. A client that failed stays failed.
    fn failed(&self) -> impl Future<Output = Self::Failure> + Send;
}
