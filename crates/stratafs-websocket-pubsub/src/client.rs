// The client a mount connects to its volume's coordinator with, typed by the message it publishes. One connection
// carries every subscription and publish, and runs on the Tokio runtime connect is called on. When it fails, every
// subscription and pending publish fails with it.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use stratafs_pubsub::{PubSub, PubSubError};
use stratafs_serialized::{LittleEndian, ViewError};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, interval, timeout_at};
use tokio_tungstenite::tungstenite::{self, Message as WebSocketMessage};
use zerocopy::{FromBytes, IntoBytes};

use crate::protocol::{
    ClientFrame, Key, MalformedFrame, PING, PING_EVERY, PONG, SILENT, ServerFrame,
};

#[derive(Debug, thiserror::Error)]
#[error("connecting to the coordinator failed")]
pub struct ConnectError(#[source] tungstenite::Error);

// Failure is why a connection failed.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    #[error("the connection broke")]
    Socket(#[source] tungstenite::Error),
    #[error("the coordinator closed the connection")]
    Closed,
    #[error("the coordinator stopped answering pings")]
    Unanswered,
    #[error(transparent)]
    Malformed(MalformedFrame),
    #[error("the coordinator sent a message that is not the client's message type")]
    Message(#[source] ViewError),
    #[error("the connection's task ended without saying why")]
    Ended,
}

pub struct Client<M> {
    outgoing: mpsc::UnboundedSender<WebSocketMessage>,
    shared: Arc<Mutex<Shared<M>>>,
}

// Subscription holds its key until it is dropped.
pub struct Subscription<M> {
    key: Key,
    outgoing: mpsc::UnboundedSender<WebSocketMessage>,
    shared: Arc<Mutex<Shared<M>>>,
}

type OnPublished<M> = Arc<dyn Fn(M) -> BoxFuture<'static, ()> + Send + Sync>;

// Subscribing is a subscribe waiting for its Subscribed.
struct Subscribing<M> {
    on_subscribed: Box<dyn FnOnce(Option<M>) + Send>,
    on_published: OnPublished<M>,
    waiter: oneshot::Sender<()>,
}

struct Shared<M> {
    // failure is why the connection failed, once it has.
    failure: Option<Arc<Failure>>,
    next_request: u64,
    // publishes is each publish waiting for its Done.
    publishes: HashMap<u64, oneshot::Sender<()>>,
    // subscribing is each key waiting for its Subscribed, or None once its Subscription was dropped first.
    subscribing: HashMap<Key, Option<Subscribing<M>>>,
    // published is what runs for each message published to a held key.
    published: HashMap<Key, OnPublished<M>>,
}

impl<M> Shared<M> {
    fn failure(&self) -> Arc<Failure> {
        self.failure
            .clone()
            .unwrap_or_else(|| Arc::new(Failure::Ended))
    }
}

impl<M: LittleEndian + Send + 'static> Client<M> {
    // connect connects to the coordinator at url. A connection whose pings go unanswered for SILENT fails. Connecting
    // has no time limit of its own, so a caller that needs one sets it.
    pub async fn connect(url: &str) -> Result<Self, ConnectError> {
        let (socket, _) = tokio_tungstenite::connect_async_with_config(url, None, true)
            .await
            .map_err(ConnectError)?;
        let (outgoing, sent) = mpsc::unbounded_channel();
        let shared = Arc::new(Mutex::new(Shared {
            failure: None,
            next_request: 0,
            publishes: HashMap::new(),
            subscribing: HashMap::new(),
            published: HashMap::new(),
        }));
        let failing = Failing {
            sent,
            shared: Arc::clone(&shared),
        };
        tokio::spawn(drive(socket, failing, outgoing.downgrade()));
        Ok(Client { outgoing, shared })
    }

    fn failure(&self) -> Arc<Failure> {
        self.shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .failure()
    }
}

impl<M: LittleEndian + Send + Sync + 'static> PubSub for Client<M> {
    type Message = M;
    type Subscription = Subscription<M>;
    type Failure = Arc<Failure>;

    async fn subscribe<Handled>(
        &self,
        key: Key,
        on_subscribed: impl FnOnce(Option<M>) + Send + 'static,
        on_published: impl Fn(M) -> Handled + Send + Sync + 'static,
    ) -> Result<Subscription<M>, PubSubError<Arc<Failure>>>
    where
        Handled: Future<Output = ()> + Send + 'static,
    {
        let (waiter, subscribed) = oneshot::channel();
        let answer_on_the_way = {
            let mut shared = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
            if shared.failure.is_some() {
                return Err(PubSubError::Failed(shared.failure()));
            }
            if shared.published.contains_key(&key)
                || matches!(shared.subscribing.get(&key), Some(Some(_)))
            {
                return Err(PubSubError::AlreadySubscribed);
            }
            let on_published: OnPublished<M> =
                Arc::new(move |message| Box::pin(on_published(message)));
            // A subscribe dropped before its answer leaves the key waiting for that answer, which answers this one.
            shared
                .subscribing
                .insert(
                    key,
                    Some(Subscribing {
                        on_subscribed: Box::new(on_subscribed),
                        on_published,
                        waiter,
                    }),
                )
                .is_some()
        };
        // Made before waiting, so dropping subscribe's future releases the key too.
        let subscription = Subscription {
            key,
            outgoing: self.outgoing.clone(),
            shared: Arc::clone(&self.shared),
        };
        if !answer_on_the_way {
            send(&self.outgoing, ClientFrame::Subscribe { key });
        }
        match subscribed.await {
            Ok(()) => Ok(subscription),
            Err(_) => Err(PubSubError::Failed(self.failure())),
        }
    }

    async fn publish(
        &self,
        key: Key,
        message: &M,
        timeout: Duration,
    ) -> Result<(), PubSubError<Arc<Failure>>> {
        let (waiter, done) = oneshot::channel();
        let request = {
            let mut shared = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
            if shared.failure.is_some() {
                return Err(PubSubError::Failed(shared.failure()));
            }
            let request = shared.next_request;
            shared.next_request += 1;
            shared.publishes.insert(request, waiter);
            request
        };
        let _waiting = Waiting {
            client: self,
            request,
        };
        send(
            &self.outgoing,
            ClientFrame::Publish {
                request,
                key,
                message: message.to_little_endian().as_bytes(),
            },
        );
        match tokio::time::timeout(timeout, done).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(PubSubError::Failed(self.failure())),
            Err(_) => Err(PubSubError::TimedOut),
        }
    }

    async fn failed(&self) -> Arc<Failure> {
        self.outgoing.closed().await;
        self.failure()
    }
}

// Waiting gives up on a publish that ends before its Done, by timing out or being dropped.
struct Waiting<'a, M> {
    client: &'a Client<M>,
    request: u64,
}

impl<M> Drop for Waiting<'_, M> {
    fn drop(&mut self) {
        let mut shared = self
            .client
            .shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if shared.publishes.remove(&self.request).is_some() {
            send(
                &self.client.outgoing,
                ClientFrame::TimedOut {
                    request: self.request,
                },
            );
        }
    }
}

impl<M> Drop for Subscription<M> {
    fn drop(&mut self) {
        let mut shared = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
        // A key still waiting for its Subscribed is released once that arrives.
        if let Some(subscribing) = shared.subscribing.get_mut(&self.key) {
            *subscribing = None;
        } else if shared.published.remove(&self.key).is_some() {
            send(&self.outgoing, ClientFrame::Unsubscribe { key: self.key });
        }
    }
}

fn send(outgoing: &mpsc::UnboundedSender<WebSocketMessage>, frame: ClientFrame<'_>) {
    // The connection has failed when the send fails, which its own waiters hear about.
    let _ = outgoing.send(WebSocketMessage::Binary(frame.encode().into()));
}

// Failing fails everything waiting on the connection once its task ends, however it ends, a panic included.
struct Failing<M> {
    sent: mpsc::UnboundedReceiver<WebSocketMessage>,
    shared: Arc<Mutex<Shared<M>>>,
}

impl<M> Drop for Failing<M> {
    fn drop(&mut self) {
        // Closing first means a subscribe or publish that starts meanwhile sees the failure.
        self.sent.close();
        let mut shared = self.shared.lock().unwrap_or_else(PoisonError::into_inner);
        shared
            .failure
            .get_or_insert_with(|| Arc::new(Failure::Ended));
        shared.publishes.clear();
        shared.subscribing.clear();
        shared.published.clear();
    }
}

// drive runs the connection until it fails or every handle to it is dropped: it sends what the handles queue, answers
// what the coordinator sends, and pings it. Every write has to finish before the ping deadline too.
async fn drive<M: LittleEndian + Send + 'static>(
    mut socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    mut failing: Failing<M>,
    outgoing: mpsc::WeakUnboundedSender<WebSocketMessage>,
) {
    let mut pings = interval(PING_EVERY);
    let mut answered = Instant::now();
    let failure = loop {
        let deadline = answered + SILENT;
        tokio::select! {
            received = socket.next() => match received {
                Some(Ok(WebSocketMessage::Binary(frame))) => {
                    if let Err(failure) = received_frame(&frame, &failing.shared, &outgoing) {
                        break failure;
                    }
                }
                Some(Ok(WebSocketMessage::Text(text))) if text.as_str() == PONG => {
                    answered = Instant::now();
                }
                // Tungstenite answers WebSocket pings itself.
                Some(Ok(WebSocketMessage::Ping(_) | WebSocketMessage::Pong(_))) => {}
                Some(Err(error)) => break Failure::Socket(error),
                Some(Ok(_)) | None => break Failure::Closed,
            },
            queued = failing.sent.recv() => match queued {
                Some(message) => match timeout_at(deadline, socket.send(message)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => break Failure::Socket(error),
                    Err(_) => break Failure::Unanswered,
                },
                // Every handle is gone, so nothing is waiting to hear why.
                None => break Failure::Ended,
            },
            _ = pings.tick() => {
                if Instant::now() > deadline {
                    break Failure::Unanswered;
                }
                match timeout_at(deadline, socket.send(WebSocketMessage::Text(PING.into()))).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => break Failure::Socket(error),
                    Err(_) => break Failure::Unanswered,
                }
            }
        }
    };
    failing
        .shared
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .failure = Some(Arc::new(failure));
    drop(failing);
    let _ = tokio::time::timeout(PING_EVERY, socket.close(None)).await;
}

// received_frame handles one frame from the coordinator. A frame that is not one, or a message that is not an M,
// fails the connection.
fn received_frame<M: LittleEndian + Send + 'static>(
    frame: &[u8],
    shared: &Mutex<Shared<M>>,
    outgoing: &mpsc::WeakUnboundedSender<WebSocketMessage>,
) -> Result<(), Failure> {
    let frame = ServerFrame::decode(frame).map_err(Failure::Malformed)?;
    let mut shared = shared.lock().unwrap_or_else(PoisonError::into_inner);
    match frame {
        ServerFrame::Subscribed { key, last } => {
            let last = last
                .map(decode::<M>)
                .transpose()
                .map_err(Failure::Message)?;
            match shared.subscribing.remove(&key) {
                Some(Some(subscribing)) => {
                    // Delivers read before this belong to an earlier hold of key, so the handler joins only now.
                    shared.published.insert(key, subscribing.on_published);
                    drop(shared);
                    (subscribing.on_subscribed)(last);
                    let _ = subscribing.waiter.send(());
                }
                Some(None) => {
                    if let Some(outgoing) = outgoing.upgrade() {
                        send(&outgoing, ClientFrame::Unsubscribe { key });
                    }
                }
                None => {}
            }
        }
        ServerFrame::Deliver {
            publisher,
            request,
            key,
            message,
        } => {
            let acknowledge = ClientFrame::Acknowledge { publisher, request };
            let Some(on_published) = shared.published.get(&key).cloned() else {
                // The key is not held now, so there is nothing to handle.
                if let Some(outgoing) = outgoing.upgrade() {
                    send(&outgoing, acknowledge);
                }
                return Ok(());
            };
            let message = decode::<M>(message).map_err(Failure::Message)?;
            // The handler holds the connection only weakly, so it does not keep the connection open once every
            // handle is gone.
            let outgoing = outgoing.clone();
            tokio::spawn(async move {
                on_published(message).await;
                if let Some(outgoing) = outgoing.upgrade() {
                    send(&outgoing, acknowledge);
                }
            });
        }
        ServerFrame::Done { request } => {
            if let Some(waiter) = shared.publishes.remove(&request) {
                let _ = waiter.send(());
            }
        }
    }
    Ok(())
}

fn decode<M: LittleEndian>(message: &[u8]) -> Result<M, ViewError> {
    let little_endian =
        M::LittleEndian::ref_from_bytes(message).map_err(|_| ViewError::WrongLength)?;
    M::from_little_endian(little_endian)
}
