// MemoryVolume is a volume's pub/sub kept in memory, which clients connect to directly. It is the reference the
// conformance suite checks itself against. Handlers run on the volume's runtime.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::pending;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures_channel::oneshot;
use futures_util::future::{BoxFuture, Either, Shared, select};
use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use stratafs_pubsub::{Key, PubSub, PubSubError};
use stratafs_runtime::Runtime;

use crate::Volume;

// SILENT is how long a silenced client takes to notice and fail.
const SILENT: Duration = Duration::from_secs(2);

// ClientId names one client of a volume.
type ClientId = u64;

type OnPublished<M> = Arc<dyn Fn(M) -> BoxFuture<'static, ()> + Send + Sync>;

pub struct MemoryVolume<M> {
    state: Arc<Mutex<State<M>>>,
    runtime: Arc<dyn Runtime>,
}

struct State<M> {
    next_client: ClientId,
    // failures sends each connected client why it failed, once it does.
    failures: HashMap<ClientId, oneshot::Sender<MemoryFailure>>,
    // holders is each key's holders, by client, in order, so a publish reaches them the same way each time it runs.
    holders: HashMap<Key, BTreeMap<ClientId, Holder<M>>>,
    // silenced is each client whose connection carries nothing. It holds nothing, and fails once SILENT passes.
    silenced: HashSet<ClientId>,
}

// Holder is a client's hold of a key.
struct Holder<M> {
    on_published: OnPublished<M>,
    // held is dropped once the hold ends, which resolves ended for the publishes waiting on it.
    _held: oneshot::Sender<()>,
    ended: Shared<oneshot::Receiver<()>>,
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
    // failure resolves to why the client failed, once it has.
    failure: Shared<oneshot::Receiver<MemoryFailure>>,
    runtime: Arc<dyn Runtime>,
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

impl<M> MemoryVolume<M> {
    // new is an empty volume whose handlers and timers run on runtime.
    pub fn new(runtime: Arc<dyn Runtime>) -> Self {
        MemoryVolume {
            state: Arc::new(Mutex::new(State {
                next_client: 0,
                failures: HashMap::new(),
                holders: HashMap::new(),
                silenced: HashSet::new(),
            })),
            runtime,
        }
    }

    pub fn connect(&self) -> MemoryClient<M> {
        let mut state = lock(&self.state);
        let client = state.next_client;
        state.next_client += 1;
        let (failed, failure) = oneshot::channel();
        state.failures.insert(client, failed);
        MemoryClient {
            connection: Arc::new(Connection {
                state: Arc::clone(&self.state),
                client,
            }),
            failure: failure.shared(),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

impl<M> State<M> {
    // fail fails client for failure, unless it already failed, and ends its holds.
    fn fail(&mut self, client: ClientId, failure: MemoryFailure) {
        if let Some(failed) = self.failures.remove(&client) {
            let _ = failed.send(failure);
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
        on_published: impl Fn(M) -> Handled + Send + Sync + 'static,
    ) -> Result<MemorySubscription<M>, PubSubError<MemoryFailure>>
    where
        Handled: Future<Output = ()> + Send + 'static,
    {
        // A silenced client's subscribe reaches no one, so it ends only once the client fails.
        if self.silenced() {
            return Err(PubSubError::Failed(self.failed().await));
        }
        let mut state = lock(&self.connection.state);
        if let Some(failure) = self.failure_now() {
            return Err(PubSubError::Failed(failure));
        }
        let holders = state.holders.entry(key).or_default();
        if holders.contains_key(&self.connection.client) {
            return Err(PubSubError::AlreadySubscribed);
        }
        let (held, ended) = oneshot::channel();
        holders.insert(
            self.connection.client,
            Holder {
                on_published: Arc::new(move |message| Box::pin(on_published(message))),
                _held: held,
                ended: ended.shared(),
            },
        );
        Ok(MemorySubscription {
            connection: Arc::clone(&self.connection),
            key,
        })
    }

    async fn publish(
        &self,
        key: Key,
        message: &M,
        timeout: Duration,
    ) -> Result<(), PubSubError<MemoryFailure>> {
        // A silenced client's publish reaches no one, so it ends only by timing out or the client failing.
        if self.silenced() {
            return Err(match self.runtime.timeout(timeout, self.failed()).await {
                Some(failure) => PubSubError::Failed(failure),
                None => PubSubError::TimedOut,
            });
        }
        let mut unhandled = Unhandled {
            state: Arc::clone(&self.connection.state),
            clients: HashSet::new(),
        };
        let mut handling = FuturesUnordered::new();
        {
            let state = lock(&self.connection.state);
            if let Some(failure) = self.failure_now() {
                return Err(PubSubError::Failed(failure));
            }
            for (&client, holder) in state.holders.get(&key).into_iter().flatten() {
                if client == self.connection.client {
                    continue;
                }
                unhandled.clients.insert(client);
                let (on_published, ended) =
                    (Arc::clone(&holder.on_published), holder.ended.clone());
                // The handler runs on its own task, so a publish that ends first stops waiting on it but does not
                // stop it.
                let message = message.clone();
                let (done, handled) = oneshot::channel();
                self.runtime.spawn(Box::pin(async move {
                    on_published(message).await;
                    let _ = done.send(());
                }));
                handling.push(async move {
                    // A handler that panics never handles the message, and a hold that ends no longer counts.
                    let handled = async {
                        if handled.await.is_err() {
                            pending::<()>().await;
                        }
                    };
                    select(pin!(handled), ended).await;
                    client
                });
            }
        }
        let all_handled = async {
            while let Some(client) = handling.next().await {
                unhandled.clients.remove(&client);
            }
        };
        let handled = self.runtime.timeout(timeout, all_handled);
        let failure = match select(pin!(handled), pin!(self.failed())).await {
            Either::Left((handled, _)) => return handled.ok_or(PubSubError::TimedOut),
            Either::Right((failure, _)) => failure,
        };
        // A publisher that failed no longer waits on anyone, so it disconnects nobody.
        unhandled.clients.clear();
        Err(PubSubError::Failed(failure))
    }

    async fn failed(&self) -> MemoryFailure {
        self.failure
            .clone()
            .await
            .expect("a client's failure is kept for as long as the client")
    }
}

impl<M> MemoryClient<M> {
    // failure_now is why the client failed, if it has.
    fn failure_now(&self) -> Option<MemoryFailure> {
        self.failure.clone().now_or_never()?.ok()
    }

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
        let (state, runtime) = (Arc::clone(&self.state), Arc::clone(&self.runtime));
        self.runtime.spawn(Box::pin(async move {
            runtime.sleep(SILENT).await;
            lock(&state).fail(client, MemoryFailure::Silent);
        }));
    }
}

fn lock<M>(state: &Mutex<State<M>>) -> MutexGuard<'_, State<M>> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}
