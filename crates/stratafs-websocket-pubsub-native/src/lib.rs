// The native host: a WebSocket server that runs a coordinator per volume, locally or on any server.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use stratafs_websocket_pubsub::coordinator::{Coordinator, MemoryMembership, Sockets};
use stratafs_websocket_pubsub::protocol::{ConnectionId, PING, PONG};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;

// QUEUED is how many frames a connection may fall behind before it is closed.
const QUEUED: usize = 1024;

// Volumes is each volume's coordinator, by name, as the events it handles one at a time.
type Volumes = Arc<Mutex<HashMap<String, mpsc::UnboundedSender<Event>>>>;

enum Event {
    Opened {
        connection: ConnectionId,
        frames: mpsc::Sender<Message>,
        close: oneshot::Sender<()>,
    },
    Received {
        connection: ConnectionId,
        frame: Vec<u8>,
    },
    Closed {
        connection: ConnectionId,
    },
}

// serve accepts connections on listener for as long as it runs. A connection's path, /volume/<name>, picks its
// volume's coordinator, made the first time a connection names it.
pub async fn serve(listener: TcpListener) {
    let volumes = Volumes::default();
    let connections = Arc::new(AtomicU64::new(0));
    loop {
        // A failed accept, such as one out of file descriptors, fails that connection, not the server.
        let Ok((stream, _)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let connection = ConnectionId(connections.fetch_add(1, Ordering::Relaxed));
        tokio::spawn(connect(stream, connection, Arc::clone(&volumes)));
    }
}

// connect runs one connection: it hands the volume's coordinator each frame the connection sends and sends the
// connection each frame the coordinator gives it.
// Tungstenite's handshake callback returns its own ErrorResponse, however large.
#[allow(clippy::result_large_err)]
async fn connect(stream: TcpStream, connection: ConnectionId, volumes: Volumes) {
    if stream.set_nodelay(true).is_err() {
        return;
    }
    let mut volume = None;
    let accepted =
        tokio_tungstenite::accept_hdr_async(stream, |request: &Request, response: Response| {
            match request.uri().path().strip_prefix("/volume/") {
                Some(name) if !name.is_empty() && !name.contains('/') => {
                    volume = Some(name.to_owned());
                    Ok(response)
                }
                _ => {
                    let mut refused =
                        ErrorResponse::new(Some("the path is not /volume/<name>".to_owned()));
                    *refused.status_mut() = StatusCode::NOT_FOUND;
                    Err(refused)
                }
            }
        })
        .await;
    let (Ok(socket), Some(volume)) = (accepted, volume) else {
        return;
    };
    let events = coordinator_of(&volumes, volume);
    let (frames, mut queued) = mpsc::channel(QUEUED);
    let (close, mut closing) = oneshot::channel();
    let pongs = frames.clone();
    if events
        .send(Event::Opened {
            connection,
            frames,
            close,
        })
        .is_err()
    {
        return;
    }
    let (mut sink, mut stream) = socket.split();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                frame = queued.recv() => match frame {
                    Some(frame) => {
                        if sink.send(frame).await.is_err() {
                            break;
                        }
                    }
                    None => break,
                },
                _ = &mut closing => break,
            }
        }
        let _ = sink.close().await;
    });
    while let Some(Ok(message)) = stream.next().await {
        match message {
            Message::Binary(frame) => {
                let frame = frame.to_vec();
                if events.send(Event::Received { connection, frame }).is_err() {
                    break;
                }
            }
            Message::Text(text) if text.as_str() == PING => {
                if pongs.try_send(Message::Text(PONG.into())).is_err() {
                    break;
                }
            }
            // Tungstenite answers WebSocket pings itself.
            Message::Ping(_) | Message::Pong(_) => {}
            _ => break,
        }
    }
    drop(pongs);
    let _ = events.send(Event::Closed { connection });
    let _ = writer.await;
}

// coordinator_of is the events of volume's coordinator, which starts it the first time.
fn coordinator_of(volumes: &Volumes, volume: String) -> mpsc::UnboundedSender<Event> {
    let mut volumes = volumes.lock().unwrap_or_else(PoisonError::into_inner);
    volumes
        .entry(volume)
        .or_insert_with(|| {
            let (events, received) = mpsc::unbounded_channel();
            tokio::spawn(coordinate(received));
            events
        })
        .clone()
}

// coordinate runs one volume's coordinator, handling its events one at a time.
async fn coordinate(mut events: mpsc::UnboundedReceiver<Event>) {
    let mut coordinator = Coordinator::new(MemoryMembership::default());
    let mut connections = HashMap::new();
    while let Some(event) = events.recv().await {
        let mut closing = match event {
            Event::Opened {
                connection,
                frames,
                close,
            } => {
                connections.insert(connection, (frames, close));
                continue;
            }
            Event::Received { connection, frame } => {
                // A connection closed here can still have frames on the way, and they no longer count.
                if !connections.contains_key(&connection) {
                    continue;
                }
                let sockets = Connections {
                    open: &connections,
                    closing: RefCell::default(),
                };
                coordinator.received(connection, &frame, &sockets);
                sockets.closing.take()
            }
            Event::Closed { connection } => vec![connection],
        };
        // Closing a connection can leave publishes done, whose answers can close more.
        while let Some(connection) = closing.pop() {
            let Some((_, close)) = connections.remove(&connection) else {
                continue;
            };
            let _ = close.send(());
            let sockets = Connections {
                open: &connections,
                closing: RefCell::default(),
            };
            coordinator.closed(connection, &sockets);
            closing.extend(sockets.closing.take());
        }
    }
}

// Connections is a coordinator's view of its volume's open connections. A connection it closes, or that falls too
// far behind to be sent a frame, joins closing, and is closed once the coordinator returns.
struct Connections<'a> {
    open: &'a HashMap<ConnectionId, (mpsc::Sender<Message>, oneshot::Sender<()>)>,
    closing: RefCell<Vec<ConnectionId>>,
}

impl Sockets for Connections<'_> {
    fn send(&self, connection: ConnectionId, frame: &[u8]) {
        let Some((frames, _)) = self.open.get(&connection) else {
            return;
        };
        if frames
            .try_send(Message::Binary(frame.to_vec().into()))
            .is_err()
        {
            self.closing.borrow_mut().push(connection);
        }
    }

    fn close(&self, connection: ConnectionId) {
        self.closing.borrow_mut().push(connection);
    }
}
