use std::cell::RefCell;

use super::*;

// Recorded is what a coordinator did with its connections.
#[derive(Default)]
struct Recorded {
    sent: RefCell<Vec<(ConnectionId, Vec<u8>)>>,
    closed: RefCell<Vec<ConnectionId>>,
}

impl Sockets for Recorded {
    fn send(&self, connection: ConnectionId, frame: &[u8]) {
        self.sent.borrow_mut().push((connection, frame.to_vec()));
    }

    fn close(&self, connection: ConnectionId) {
        self.closed.borrow_mut().push(connection);
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
        ClientFrame::TimedOut {
            request: 5,
            key: KEY,
        },
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
fn a_malformed_frame_closes_its_connection() {
    let (mut coordinator, sockets) = subscribed(&[]);
    coordinator.received(FIRST, &[0xff], &sockets);
    assert_eq!(*sockets.closed.borrow(), vec![FIRST]);
}

#[test]
fn a_timed_out_publish_the_coordinator_no_longer_has_closes_every_other_holder_of_its_key() {
    let (mut coordinator, sockets) = subscribed(&[WRITER, FIRST, SECOND]);
    received(
        &mut coordinator,
        WRITER,
        ClientFrame::TimedOut {
            request: 5,
            key: KEY,
        },
        &sockets,
    );
    let mut closed = sockets.closed.borrow().clone();
    closed.sort_by_key(|connection| connection.0);
    assert_eq!(closed, vec![FIRST, SECOND]);
}
