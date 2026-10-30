// The Cloudflare host: a Worker that sends each connection to its volume's Durable Object, which runs that volume's
// coordinator. Connections stay open while the object hibernates, so its membership is kept in the object's SQLite.
#![cfg(target_arch = "wasm32")]

mod membership;

use std::cell::RefCell;
use std::rc::Rc;

use stratafs_websocket_pubsub::coordinator::{Coordinator, Sockets};
use stratafs_websocket_pubsub::protocol::{ConnectionId, PING, PONG};
use worker::{
    Context, DurableObject, Env, Request, Response, Result, State, WebSocket,
    WebSocketIncomingMessage, WebSocketPair, durable_object, event,
};

use membership::{StoredMembership, next_connection};

// fetch sends a connection to /volume/<name> to that volume's coordinator.
#[event(fetch)]
async fn fetch(request: Request, env: Env, _context: Context) -> Result<Response> {
    let path = request.path();
    let Some(volume) = path
        .strip_prefix("/volume/")
        .filter(|name| !name.is_empty() && !name.contains('/'))
    else {
        return Response::error("the path is not /volume/<name>", 404);
    };
    env.durable_object("COORDINATOR")?
        .id_from_name(volume)?
        .get_stub()?
        .fetch_with_request(request)
        .await
}

#[durable_object(websocket)]
pub struct VolumeCoordinator {
    state: Rc<State>,
    coordinator: RefCell<Coordinator<StoredMembership>>,
}

impl DurableObject for VolumeCoordinator {
    fn new(state: State, _env: Env) -> Self {
        let state = Rc::new(state);
        let mut membership = StoredMembership::open(Rc::clone(&state));
        // A connection the object no longer has, such as one a restart cut, holds nothing.
        let open: Vec<ConnectionId> = state
            .get_websockets()
            .iter()
            .filter_map(|socket| connection_of(&state, socket))
            .collect();
        membership.release_all_but(&open);
        // Cloudflare answers pings itself, without waking the object.
        if let Ok(pong) = worker::WebSocketRequestResponsePair::new(PING, PONG) {
            state.set_websocket_auto_response(&pong);
        }
        VolumeCoordinator {
            state,
            coordinator: RefCell::new(Coordinator::new(membership)),
        }
    }

    // fetch accepts a WebSocket, tagged with its connection's number so it is found again after hibernation.
    async fn fetch(&self, request: Request) -> Result<Response> {
        if request.headers().get("Upgrade")?.as_deref() != Some("websocket") {
            return Response::error("the request is not a WebSocket upgrade", 426);
        }
        let pair = WebSocketPair::new()?;
        let connection = next_connection(&self.state);
        self.state
            .accept_websocket_with_tags(&pair.server, &[&connection.0.to_string()]);
        Response::from_websocket(pair.client)
    }

    async fn websocket_message(
        &self,
        socket: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> Result<()> {
        let Some(connection) = connection_of(&self.state, &socket) else {
            return Ok(());
        };
        let sockets = Connections {
            state: &self.state,
            closing: RefCell::default(),
        };
        match message {
            WebSocketIncomingMessage::Binary(frame) => {
                self.coordinator
                    .borrow_mut()
                    .received(connection, &frame, &sockets);
            }
            // Pings are answered before they reach the object, so any other text is not a frame.
            WebSocketIncomingMessage::String(_) => sockets.close(connection),
        }
        self.close(sockets.closing.take());
        Ok(())
    }

    async fn websocket_close(
        &self,
        socket: WebSocket,
        _code: usize,
        _reason: String,
        _was_clean: bool,
    ) -> Result<()> {
        if let Some(connection) = connection_of(&self.state, &socket) {
            self.close(vec![connection]);
        }
        Ok(())
    }

    async fn websocket_error(&self, socket: WebSocket, _error: worker::Error) -> Result<()> {
        if let Some(connection) = connection_of(&self.state, &socket) {
            self.close(vec![connection]);
        }
        Ok(())
    }
}

impl VolumeCoordinator {
    // close closes each connection in closing and has the coordinator forget it. That can leave publishes done,
    // whose answers can close more.
    fn close(&self, mut closing: Vec<ConnectionId>) {
        while let Some(connection) = closing.pop() {
            for socket in self
                .state
                .get_websockets_with_tag(&connection.0.to_string())
            {
                let _ = socket.close(Some(1000), Some("closed by the coordinator"));
            }
            let sockets = Connections {
                state: &self.state,
                closing: RefCell::default(),
            };
            self.coordinator.borrow_mut().closed(connection, &sockets);
            closing.extend(sockets.closing.take());
        }
    }
}

// connection_of is the connection socket was accepted as.
fn connection_of(state: &State, socket: &WebSocket) -> Option<ConnectionId> {
    state
        .get_tags(socket)
        .first()
        .and_then(|tag| tag.parse().ok())
        .map(ConnectionId)
}

// Connections is a coordinator's view of the object's open connections. A connection it closes, or that can no
// longer be sent a frame, joins closing, and is closed once the coordinator returns.
struct Connections<'a> {
    state: &'a State,
    closing: RefCell<Vec<ConnectionId>>,
}

impl Sockets for Connections<'_> {
    fn send(&self, connection: ConnectionId, frame: &[u8]) {
        let sockets = self
            .state
            .get_websockets_with_tag(&connection.0.to_string());
        let sent = sockets
            .first()
            .is_some_and(|socket| socket.send_with_bytes(frame).is_ok());
        if !sent {
            self.closing.borrow_mut().push(connection);
        }
    }

    fn close(&self, connection: ConnectionId) {
        self.closing.borrow_mut().push(connection);
    }
}
