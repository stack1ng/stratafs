// MemoryVolume is a volume's pub/sub kept in memory, which clients connect to directly. It is the reference the
// conformance suite checks itself against. Handlers run on the Tokio runtime of the publish that reaches them.

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use stratafs_pubsub::{Key, PubSub, PubSubError};
use tokio::sync::watch;

use crate::Volume;

// SILENT is how long a silenced client takes to notice and fail.
const SILENT: Duration = Duration::from_secs(2);

// ClientId names one client of a volume.
type ClientId = u64;

type OnPublished<M> = Arc<dyn Fn(M) -> BoxFuture<'static, ()> + Send + Sync>;

pub struct MemoryVolume<M> {
    state: Arc<Mutex<State<M>>>,
}

struct State<M> {
    next_client: ClientId,
    // failures is each connected client's failure, which it reports once it fails.
    failures: HashMap<ClientId, watch::Sender<Option<MemoryFailure>>>,
    // holders is each key's holders, by client.
    holders: HashMap<Key, HashMap<ClientId, Holder<M>>>,
    last: HashMap<Key, M>,
    // silenced is each client whose connection carries nothing. It holds nothing, and fails once SILENT passes.
    silenced: HashSet<ClientId>,
}

// Holder is a client's hold of a key.
struct Holder<M> {
    on_published: OnPublished<M>,
    // held is dropped once the hold ends, which stops the publishes waiting on it.
    held: watch::Sender<()>,
    // subscribed turns true once the hold's on_subscribed has run, which messages to it wait for.
    subscribed: watch::Receiver<bool>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum MemoryFailure {
    #[error("the client did not handle a publish in time")]
    Unhandled,
    #[error("the client's connection was cut")]
    Cut,
    #[error("the client's connection went silent")]
    Silent,
}

pub struct MemoryClient<M> {
    connection: Arc<Connection<M>>,
    failure: watch::Receiver<Option<MemoryFailure>>,
}

pub struct MemorySubscription<M> {
    connection: Arc<Connection<M>>,
    key: Key,
}

// Connection is a client's place in its volume, which it leaves once the client and its subscriptions are all gone.
struct Connection<M> {
    state: Arc<Mutex<State<M>>>,
    client: ClientId,
}

impl<M> Default for MemoryVolume<M> {
    fn default() -> Self {
        MemoryVolume {
            state: Arc::new(Mutex::new(State {
                next_client: 0,
                failures: HashMap::new(),
                holders: HashMap::new(),
                last: HashMap::new(),
                silenced: HashSet::new(),
            })),
        }
    }
}

impl<M> MemoryVolume<M> {
    pub fn connect(&self) -> MemoryClient<M> {
        let mut state = lock(&self.state);
        let client = state.next_client;
        state.next_client += 1;
        let (failed, failure) = watch::channel(None);
        state.failures.insert(client, failed);
        MemoryClient {
            connection: Arc::new(Connection {
                state: Arc::clone(&self.state),
                client,
            }),
            failure,
        }
    }
}

impl<M> State<M> {
    // fail fails client for failure, unless it already failed, and ends its holds.
    fn fail(&mut self, client: ClientId, failure: MemoryFailure) {
        if let Some(failed) = self.failures.get(&client) {
            failed.send_if_modified(|failed| {
                if failed.is_some() {
                    return false;
                }
                *failed = Some(failure);
                true
            });
        }
        self.release_all(client);
    }

    fn release_all(&mut self, client: ClientId) {
        self.holders.retain(|_, holders| {
            holders.remove(&client);
            !holders.is_empty()
        });
    }
}

impl<M: Clone + Send + Sync + 'static> PubSub for MemoryClient<M> {
    type Message = M;
    type Subscription = MemorySubscription<M>;
    type Failure = MemoryFailure;

    async fn subscribe<Handled>(
        &self,
        key: Key,
        on_subscribed: impl FnOnce(Option<M>) + Send + 'static,
        on_published: impl Fn(M) -> Handled + Send + Sync + 'static,
    ) -> Result<MemorySubscription<M>, PubSubError<MemoryFailure>>
    where
        Handled: Future<Output = ()> + Send + 'static,
    {
        // A silenced client's subscribe reaches no one, so it ends only once the client fails.
        if self.silenced() {
            return Err(PubSubError::Failed(self.failed().await));
        }
        let (subscribed, last) = {
            let mut state = lock(&self.connection.state);
            if let Some(failure) = self.failure.borrow().clone() {
                return Err(PubSubError::Failed(failure));
            }
            let holders = state.holders.entry(key).or_default();
            if holders.contains_key(&self.connection.client) {
                return Err(PubSubError::AlreadySubscribed);
            }
            let (subscribed, waiting) = watch::channel(false);
            holders.insert(
                self.connection.client,
                Holder {
                    on_published: Arc::new(move |message| Box::pin(on_published(message))),
                    held: watch::channel(()).0,
                    subscribed: waiting,
                },
            );
            (subscribed, state.last.get(&key).cloned())
        };
        // Made before on_subscribed runs, so a panic in it releases the key.
        let subscription = MemorySubscription {
            connection: Arc::clone(&self.connection),
            key,
        };
        on_subscribed(last);
        subscribed.send_replace(true);
        Ok(subscription)
    }

    async fn publish(
        &self,
        key: Key,
        message: &M,
        timeout: Duration,
    ) -> Result<(), PubSubError<MemoryFailure>> {
        // A silenced client's publish reaches no one, so it ends only by timing out or the client failing.
        if self.silenced() {
            return Err(match tokio::time::timeout(timeout, self.failed()).await {
                Ok(failure) => PubSubError::Failed(failure),
                Err(_) => PubSubError::TimedOut,
            });
        }
        let mut unhandled = Unhandled {
            state: Arc::clone(&self.connection.state),
            clients: HashSet::new(),
        };
        let mut handling = FuturesUnordered::new();
        {
            let mut state = lock(&self.connection.state);
            if let Some(failure) = self.failure.borrow().clone() {
                return Err(PubSubError::Failed(failure));
            }
            state.last.insert(key, message.clone());
            for (&client, holder) in state.holders.get(&key).into_iter().flatten() {
                if client == self.connection.client {
                    continue;
                }
                unhandled.clients.insert(client);
                let on_published = Arc::clone(&holder.on_published);
                let mut held = holder.held.subscribe();
                let mut subscribed = holder.subscribed.clone();
                // The handler runs on its own task, so a publish that ends first stops waiting on it but does not
                // stop it.
                let message = message.clone();
                let handler = tokio::spawn(async move {
                    if subscribed.wait_for(|subscribed| *subscribed).await.is_ok() {
                        on_published(message).await;
                    }
                });
                handling.push(async move {
                    // A handler that panics never handles the message, and a hold that ends no longer counts.
                    tokio::select! {
                        () = async {
                            if handler.await.is_err() {
                                pending::<()>().await;
                            }
                        } => {}
                        _ = held.changed() => {}
                    }
                    client
                });
            }
        }
        let all_handled = async {
            while let Some(client) = handling.next().await {
                unhandled.clients.remove(&client);
            }
        };
        let failure = tokio::select! {
            handled = tokio::time::timeout(timeout, all_handled) => {
                return handled.map_err(|_| PubSubError::TimedOut);
            }
            failure = self.failed() => failure,
        };
        // A publisher that failed no longer waits on anyone, so it disconnects nobody.
        unhandled.clients.clear();
        Err(PubSubError::Failed(failure))
    }

    async fn failed(&self) -> MemoryFailure {
        let mut failure = self.failure.clone();
        failure
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|failed| failed.clone())
            .expect("a client's failure is kept for as long as the client")
    }
}

impl<M> MemoryClient<M> {
    fn silenced(&self) -> bool {
        lock(&self.connection.state)
            .silenced
            .contains(&self.connection.client)
    }
}

// Unhandled fails the holders a publish still waits on when it ends without them, by timing out or being dropped.
struct Unhandled<M> {
    state: Arc<Mutex<State<M>>>,
    clients: HashSet<ClientId>,
}

impl<M> Drop for Unhandled<M> {
    fn drop(&mut self) {
        if self.clients.is_empty() {
            return;
        }
        let mut state = lock(&self.state);
        for client in self.clients.drain() {
            state.fail(client, MemoryFailure::Unhandled);
        }
    }
}

impl<M> Drop for MemorySubscription<M> {
    fn drop(&mut self) {
        let mut state = lock(&self.connection.state);
        if let Some(holders) = state.holders.get_mut(&self.key) {
            holders.remove(&self.connection.client);
            if holders.is_empty() {
                state.holders.remove(&self.key);
            }
        }
    }
}

impl<M> Drop for Connection<M> {
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        state.failures.remove(&self.client);
        state.silenced.remove(&self.client);
        state.release_all(self.client);
    }
}

impl Volume for MemoryVolume<u64> {
    type Client = MemoryClient<u64>;
    type Connection = ClientId;

    async fn connect(&self) -> (MemoryClient<u64>, ClientId) {
        let client = MemoryVolume::connect(self);
        let id = client.connection.client;
        (client, id)
    }

    fn cut(&self, connection: &ClientId) {
        lock(&self.state).fail(*connection, MemoryFailure::Cut);
    }

    fn silence(&self, connection: &ClientId) {
        let client = *connection;
        {
            let mut state = lock(&self.state);
            state.release_all(client);
            state.silenced.insert(client);
        }
        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            tokio::time::sleep(SILENT).await;
            lock(&state).fail(client, MemoryFailure::Silent);
        });
    }
}

fn lock<M>(state: &Mutex<State<M>>) -> MutexGuard<'_, State<M>> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}
