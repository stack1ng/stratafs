use std::cell::RefCell;

use proptest::collection::vec;
use proptest::prelude::*;

use super::*;

// Recorded is what was done with the connections, by a coordinator or by the model of one. silent is the ones a
// coordinator should take as gone.
#[derive(Default)]
struct Recorded {
    sent: RefCell<Vec<(ConnectionId, Vec<u8>)>>,
    closed: RefCell<Vec<ConnectionId>>,
    silent: Vec<ConnectionId>,
}

impl Sockets for Recorded {
    fn send(&self, connection: ConnectionId, frame: &[u8]) {
        self.sent.borrow_mut().push((connection, frame.to_vec()));
    }

    fn close(&self, connection: ConnectionId) {
        self.closed.borrow_mut().push(connection);
    }

    fn silent(&self, connection: ConnectionId) -> bool {
        self.silent.contains(&connection)
    }
}

impl Recorded {
    // took is every frame sent since the last call, decoded.
    fn took(&self) -> Vec<(ConnectionId, ServerFrame<'static>)> {
        self.sent
            .take()
            .into_iter()
            .map(|(connection, frame)| {
                let frame: &'static [u8] = frame.leak();
                (connection, ServerFrame::decode(frame).unwrap())
            })
            .collect()
    }
}

const WRITER: ConnectionId = ConnectionId(1);
const FIRST: ConnectionId = ConnectionId(2);
const SECOND: ConnectionId = ConnectionId(3);
const KEY: Key = Key(7);

fn received(
    coordinator: &mut Coordinator<MemoryMembership>,
    connection: ConnectionId,
    frame: ClientFrame<'_>,
    sockets: &Recorded,
) {
    coordinator.received(connection, &frame.encode(), sockets);
}

fn subscribed(holders: &[ConnectionId]) -> (Coordinator<MemoryMembership>, Recorded) {
    let mut coordinator = Coordinator::new(MemoryMembership::default());
    let sockets = Recorded::default();
    for holder in holders {
        received(
            &mut coordinator,
            *holder,
            ClientFrame::Subscribe { key: KEY },
            &sockets,
        );
    }
    sockets.took();
    (coordinator, sockets)
}

#[test]
fn a_publish_is_done_once_every_other_holder_acknowledges_it() {
    let (mut coordinator, sockets) = subscribed(&[WRITER, FIRST, SECOND]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 5,
            key: KEY,
            message: b"v2",
        },
        &sockets,
    );
    let mut delivered = sockets.took();
    delivered.sort_by_key(|(connection, _)| connection.0);
    let deliver = ServerFrame::Deliver {
        publisher: WRITER,
        request: 5,
        key: KEY,
        message: b"v2",
    };
    assert_eq!(delivered, vec![(FIRST, deliver), (SECOND, deliver)]);
    received(
        &mut coordinator,
        FIRST,
        ClientFrame::Acknowledge {
            publisher: WRITER,
            request: 5,
        },
        &sockets,
    );
    assert_eq!(sockets.took(), vec![]);
    received(
        &mut coordinator,
        SECOND,
        ClientFrame::Acknowledge {
            publisher: WRITER,
            request: 5,
        },
        &sockets,
    );
    assert_eq!(
        sockets.took(),
        vec![(WRITER, ServerFrame::Done { request: 5 })]
    );
}

#[test]
fn a_publish_with_no_other_holder_is_done_at_once() {
    let (mut coordinator, sockets) = subscribed(&[WRITER]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 1,
            key: KEY,
            message: b"v2",
        },
        &sockets,
    );
    assert_eq!(
        sockets.took(),
        vec![(WRITER, ServerFrame::Done { request: 1 })]
    );
}

#[test]
fn a_subscriber_gets_the_last_message_published_to_its_key() {
    let (mut coordinator, sockets) = subscribed(&[]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 1,
            key: KEY,
            message: b"v2",
        },
        &sockets,
    );
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 2,
            key: KEY,
            message: b"v3",
        },
        &sockets,
    );
    sockets.took();
    received(
        &mut coordinator,
        FIRST,
        ClientFrame::Subscribe { key: KEY },
        &sockets,
    );
    assert_eq!(
        sockets.took(),
        vec![(
            FIRST,
            ServerFrame::Subscribed {
                key: KEY,
                last: Some(b"v3")
            }
        )]
    );
    received(
        &mut coordinator,
        SECOND,
        ClientFrame::Subscribe { key: Key(8) },
        &sockets,
    );
    assert_eq!(
        sockets.took(),
        vec![(
            SECOND,
            ServerFrame::Subscribed {
                key: Key(8),
                last: None
            }
        )]
    );
}

#[test]
fn a_holder_that_closes_or_releases_the_key_no_longer_holds_up_a_publish() {
    let (mut coordinator, sockets) = subscribed(&[WRITER, FIRST, SECOND]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 5,
            key: KEY,
            message: b"v2",
        },
        &sockets,
    );
    sockets.took();
    coordinator.closed(FIRST, &sockets);
    assert_eq!(sockets.took(), vec![]);
    received(
        &mut coordinator,
        SECOND,
        ClientFrame::Unsubscribe { key: KEY },
        &sockets,
    );
    assert_eq!(
        sockets.took(),
        vec![(WRITER, ServerFrame::Done { request: 5 })]
    );
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 6,
            key: KEY,
            message: b"v3",
        },
        &sockets,
    );
    assert_eq!(
        sockets.took(),
        vec![(WRITER, ServerFrame::Done { request: 6 })]
    );
}

#[test]
fn a_timed_out_publish_closes_the_holders_that_did_not_acknowledge_it() {
    let (mut coordinator, sockets) = subscribed(&[WRITER, FIRST, SECOND]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 5,
            key: KEY,
            message: b"v2",
        },
        &sockets,
    );
    received(
        &mut coordinator,
        FIRST,
        ClientFrame::Acknowledge {
            publisher: WRITER,
            request: 5,
        },
        &sockets,
    );
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::TimedOut { request: 5 },
        &sockets,
    );
    assert_eq!(*sockets.closed.borrow(), vec![SECOND]);
    received(
        &mut coordinator,
        SECOND,
        ClientFrame::Acknowledge {
            publisher: WRITER,
            request: 5,
        },
        &sockets,
    );
    assert!(
        !sockets
            .took()
            .contains(&(WRITER, ServerFrame::Done { request: 5 }))
    );
}

#[test]
fn a_publisher_that_closes_takes_its_publishes_with_it() {
    let (mut coordinator, sockets) = subscribed(&[WRITER, FIRST]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 5,
            key: KEY,
            message: b"v2",
        },
        &sockets,
    );
    coordinator.closed(WRITER, &sockets);
    sockets.took();
    received(
        &mut coordinator,
        FIRST,
        ClientFrame::Acknowledge {
            publisher: WRITER,
            request: 5,
        },
        &sockets,
    );
    assert_eq!(sockets.took(), vec![]);
}

#[test]
fn a_publish_closes_a_silent_holder_rather_than_wait_on_it() {
    let (mut coordinator, mut sockets) = subscribed(&[WRITER, FIRST, SECOND]);
    sockets.silent = vec![SECOND];
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::Publish {
            request: 5,
            key: KEY,
            message: b"v2",
        },
        &sockets,
    );
    assert_eq!(*sockets.closed.borrow(), vec![SECOND]);
    assert_eq!(
        sockets.took(),
        vec![(
            FIRST,
            ServerFrame::Deliver {
                publisher: WRITER,
                request: 5,
                key: KEY,
                message: b"v2",
            }
        )]
    );
}

#[test]
fn a_malformed_frame_closes_its_connection() {
    let (mut coordinator, sockets) = subscribed(&[]);
    coordinator.received(FIRST, &[0xff], &sockets);
    assert_eq!(*sockets.closed.borrow(), vec![FIRST]);
}

#[test]
fn a_timed_out_publish_the_coordinator_no_longer_has_closes_no_one() {
    let (mut coordinator, sockets) = subscribed(&[WRITER, FIRST, SECOND]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::TimedOut { request: 5 },
        &sockets,
    );
    assert_eq!(*sockets.closed.borrow(), vec![]);
}

// The model test drives a coordinator the way a host does, with random steps by the connections in a few slots, and
// checks every frame it sends and every connection it closes against a model of what it should do.

// SLOTS is how many connections are open at once. A slot whose connection closes gets a new one, so no connection id
// is used twice, and a closed connection sends nothing more.
const SLOTS: usize = 4;
const KEYS: u64 = 2;

// Step is one thing the connection in a slot does.
#[derive(Debug, Clone)]
enum Step {
    Subscribe { key: u64 },
    Unsubscribe { key: u64 },
    Publish { key: u64, message: Vec<u8> },
    Acknowledge { publish: AcknowledgedPublish },
    // TimedOut gives up on one of the connection's own publishes, done or not, by its index. A connection with none
    // does nothing.
    TimedOut { publish: u64 },
    Close,
}

// AcknowledgedPublish is which publish an Acknowledge names. A connection with no publish of the kind named does
// nothing.
#[derive(Debug, Clone)]
enum AcknowledgedPublish {
    // Delivered is a publish delivered to the connection that it has not acknowledged, by its index.
    Delivered(usize),
    // Guessed is a request of the connection in slot publisher, which it may never have published.
    Guessed { publisher: usize, request: u64 },
}

fn step() -> impl Strategy<Value = Step> {
    let key = || 0..KEYS;
    prop_oneof![
        4 => key().prop_map(|key| Step::Subscribe { key }),
        2 => key().prop_map(|key| Step::Unsubscribe { key }),
        4 => (key(), vec(0u8..4, 0..3)).prop_map(|(key, message)| Step::Publish { key, message }),
        6 => acknowledged_publish().prop_map(|publish| Step::Acknowledge { publish }),
        1 => (0..8u64).prop_map(|publish| Step::TimedOut { publish }),
        1 => Just(Step::Close),
    ]
}

fn acknowledged_publish() -> impl Strategy<Value = AcknowledgedPublish> {
    prop_oneof![
        6 => (0..8usize).prop_map(AcknowledgedPublish::Delivered),
        1 => (0..SLOTS, 0..4u64)
            .prop_map(|(publisher, request)| AcknowledgedPublish::Guessed { publisher, request }),
    ]
}

// Model is what the coordinator should know: the connection in each slot, the last message published to each key,
// and each publish still waiting on holders, with its key and the holders it waits on.
struct Model {
    connections: [Connection; SLOTS],
    // next is the id of the next connection a slot gets.
    next: u64,
    last: HashMap<Key, Vec<u8>>,
    pending: HashMap<(ConnectionId, u64), (Key, HashSet<ConnectionId>)>,
}

// Connection is what the model knows of one open connection.
struct Connection {
    id: ConnectionId,
    held: HashSet<Key>,
    // publishes is how many publishes the connection made.
    publishes: u64,
    // delivered is each publish delivered to the connection that it has not acknowledged.
    delivered: Vec<(ConnectionId, u64)>,
}

impl Connection {
    fn new(id: ConnectionId) -> Self {
        Connection {
            id,
            held: HashSet::new(),
            publishes: 0,
            delivered: Vec::new(),
        }
    }
}

// Action is what a step has the host do: pass the coordinator a frame its connection sent, or close the connection.
enum Action {
    Receive(ConnectionId, Vec<u8>),
    Close(ConnectionId),
}

impl Model {
    fn new() -> Self {
        Model {
            connections: std::array::from_fn(|slot| Connection::new(ConnectionId(slot as u64))),
            next: SLOTS as u64,
            last: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    // slot_of is the slot of connection, if it is open.
    fn slot_of(&self, connection: ConnectionId) -> Option<usize> {
        self.connections
            .iter()
            .position(|open| open.id == connection)
    }

    // holders is every connection holding key.
    fn holders(&self, key: Key) -> HashSet<ConnectionId> {
        self.connections
            .iter()
            .filter(|connection| connection.held.contains(&key))
            .map(|connection| connection.id)
            .collect()
    }

    // action is what step, by the connection in slot, has the host do, if anything.
    fn action(&self, slot: usize, step: &Step) -> Option<Action> {
        let connection = &self.connections[slot];
        let receive = |frame: ClientFrame<'_>| Some(Action::Receive(connection.id, frame.encode()));
        match step {
            Step::Subscribe { key } => receive(ClientFrame::Subscribe { key: Key(*key) }),
            Step::Unsubscribe { key } => receive(ClientFrame::Unsubscribe { key: Key(*key) }),
            Step::Publish { key, message } => receive(ClientFrame::Publish {
                request: connection.publishes,
                key: Key(*key),
                message,
            }),
            Step::Acknowledge { publish } => {
                let (publisher, request) = match *publish {
                    AcknowledgedPublish::Delivered(index) => *connection
                        .delivered
                        .get(index.checked_rem(connection.delivered.len())?)?,
                    AcknowledgedPublish::Guessed { publisher, request } => {
                        (self.connections[publisher].id, request)
                    }
                };
                receive(ClientFrame::Acknowledge { publisher, request })
            }
            Step::TimedOut { publish } => receive(ClientFrame::TimedOut {
                request: publish.checked_rem(connection.publishes)?,
            }),
            Step::Close => Some(Action::Close(connection.id)),
        }
    }

    // received is what the coordinator should do with a frame connection sent.
    fn received(&mut self, connection: ConnectionId, frame: &[u8]) -> Recorded {
        let slot = self.slot_of(connection).unwrap();
        let expected = Recorded::default();
        match ClientFrame::decode(frame).unwrap() {
            ClientFrame::Subscribe { key } => {
                self.connections[slot].held.insert(key);
                let last = self.last.get(&key).map(Vec::as_slice);
                expected.send(connection, &ServerFrame::Subscribed { key, last }.encode());
            }
            ClientFrame::Unsubscribe { key } => {
                self.connections[slot].held.remove(&key);
                let publishes: Vec<(ConnectionId, u64)> = self
                    .pending
                    .iter()
                    .filter(|(_, (pending_key, _))| *pending_key == key)
                    .map(|(publish, _)| *publish)
                    .collect();
                for publish in publishes {
                    self.stop_waiting(publish, connection, &expected);
                }
            }
            ClientFrame::Publish {
                request,
                key,
                message,
            } => {
                self.connections[slot].publishes += 1;
                self.last.insert(key, message.to_vec());
                let mut others = HashSet::new();
                for other in &mut self.connections {
                    if other.id == connection || !other.held.contains(&key) {
                        continue;
                    }
                    let deliver = ServerFrame::Deliver {
                        publisher: connection,
                        request,
                        key,
                        message,
                    };
                    expected.send(other.id, &deliver.encode());
                    other.delivered.push((connection, request));
                    others.insert(other.id);
                }
                if others.is_empty() {
                    expected.send(connection, &ServerFrame::Done { request }.encode());
                } else {
                    self.pending.insert((connection, request), (key, others));
                }
            }
            ClientFrame::Acknowledge { publisher, request } => {
                let acknowledger = &mut self.connections[slot];
                acknowledger
                    .delivered
                    .retain(|publish| *publish != (publisher, request));
                self.stop_waiting((publisher, request), connection, &expected);
            }
            ClientFrame::TimedOut { request } => {
                if let Some((_, waiting)) = self.pending.remove(&(connection, request)) {
                    expected.closed.borrow_mut().extend(waiting);
                }
            }
        }
        expected
    }

    // closed is what the coordinator should do once connection closes. Its slot gets a new connection.
    fn closed(&mut self, connection: ConnectionId) -> Recorded {
        let slot = self.slot_of(connection).unwrap();
        self.connections[slot] = Connection::new(ConnectionId(self.next));
        self.next += 1;
        self.pending
            .retain(|(publisher, _), _| *publisher != connection);
        let expected = Recorded::default();
        let publishes: Vec<(ConnectionId, u64)> = self.pending.keys().copied().collect();
        for publish in publishes {
            self.stop_waiting(publish, connection, &expected);
        }
        expected
    }

    // stop_waiting stops publish waiting on holder, and answers it once it waits on nobody.
    fn stop_waiting(
        &mut self,
        publish: (ConnectionId, u64),
        holder: ConnectionId,
        expected: &Recorded,
    ) {
        let Some((_, waiting)) = self.pending.get_mut(&publish) else {
            return;
        };
        waiting.remove(&holder);
        if waiting.is_empty() {
            self.pending.remove(&publish);
            let (publisher, request) = publish;
            expected.send(publisher, &ServerFrame::Done { request }.encode());
        }
    }
}

// in_order is frames decoded and sorted, since a coordinator sends to the holders of a key, and answers the
// publishes a holder leaves done, in no defined order.
fn in_order(frames: &mut [(ConnectionId, Vec<u8>)]) -> Vec<(ConnectionId, ServerFrame<'_>)> {
    frames.sort_by(|left, right| (left.0.0, &left.1).cmp(&(right.0.0, &right.1)));
    frames
        .iter()
        .map(|(connection, frame)| (*connection, ServerFrame::decode(frame).unwrap()))
        .collect()
}

// checked compares what the coordinator did in one call with what the model expected, and is the connections it
// asked to close, in the order it asked.
fn checked(sockets: &Recorded, mut expected: Recorded) -> Result<Vec<ConnectionId>, TestCaseError> {
    let mut sent = sockets.sent.take();
    let closed = sockets.closed.take();
    prop_assert_eq!(
        in_order(&mut sent),
        in_order(expected.sent.get_mut()),
        "sent"
    );
    let mut sorted = closed.clone();
    sorted.sort_by_key(|connection| connection.0);
    let expected_closed = expected.closed.get_mut();
    expected_closed.sort_by_key(|connection| connection.0);
    prop_assert_eq!(&sorted, expected_closed, "closed");
    Ok(closed)
}

// matches_its_model runs steps against a coordinator and the model, as a host would: frames go to the coordinator
// one at a time, and each connection it asks to close is closed, and the coordinator told, until none are left.
fn matches_its_model(steps: &[(usize, Step)]) -> Result<(), TestCaseError> {
    let mut coordinator = Coordinator::new(MemoryMembership::default());
    let sockets = Recorded::default();
    let mut model = Model::new();
    for (number, (slot, step)) in steps.iter().enumerate() {
        let failed = |error: TestCaseError| {
            TestCaseError::fail(format!("step {number}, slot {slot}, {step:?}: {error}"))
        };
        let mut closing = match model.action(*slot, step) {
            None => continue,
            Some(Action::Close(connection)) => vec![connection],
            Some(Action::Receive(connection, frame)) => {
                let expected = model.received(connection, &frame);
                coordinator.received(connection, &frame, &sockets);
                checked(&sockets, expected).map_err(failed)?
            }
        };
        while let Some(connection) = closing.pop() {
            if model.slot_of(connection).is_none() {
                continue;
            }
            let expected = model.closed(connection);
            coordinator.closed(connection, &sockets);
            closing.extend(checked(&sockets, expected).map_err(failed)?);
        }
        for key in (0..KEYS).map(Key) {
            let holders: HashSet<ConnectionId> =
                coordinator.membership.holders(key).into_iter().collect();
            prop_assert_eq!(
                holders,
                model.holders(key),
                "step {}, slot {}, {:?}: holders of {:?}",
                number,
                slot,
                step,
                key
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn the_coordinator_matches_its_model(steps in vec((0..SLOTS, step()), 0..60)) {
        matches_its_model(&steps)?;
    }
}
