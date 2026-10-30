use super::*;

#[test]
fn every_client_frame_decodes_to_itself() {
    let message = [7u8, 8, 9];
    for frame in [
        ClientFrame::Subscribe { key: Key(1) },
        ClientFrame::Unsubscribe { key: Key(u64::MAX) },
        ClientFrame::Publish {
            request: 2,
            key: Key(3),
            message: &message,
        },
        ClientFrame::Publish {
            request: 2,
            key: Key(3),
            message: &[],
        },
        ClientFrame::Acknowledge {
            publisher: ConnectionId(4),
            request: 5,
        },
        ClientFrame::TimedOut {
            request: 6,
            key: Key(7),
        },
    ] {
        assert_eq!(ClientFrame::decode(&frame.encode()).unwrap(), frame);
    }
}

#[test]
fn every_server_frame_decodes_to_itself() {
    let message = [7u8, 8, 9];
    for frame in [
        ServerFrame::Subscribed {
            key: Key(1),
            last: None,
        },
        ServerFrame::Subscribed {
            key: Key(1),
            last: Some(&message),
        },
        ServerFrame::Subscribed {
            key: Key(1),
            last: Some(&[]),
        },
        ServerFrame::Deliver {
            publisher: ConnectionId(2),
            request: 3,
            key: Key(4),
            message: &message,
        },
        ServerFrame::Done { request: 5 },
    ] {
        assert_eq!(ServerFrame::decode(&frame.encode()).unwrap(), frame);
    }
}

#[test]
fn a_frame_with_an_unknown_tag_or_the_wrong_length_is_malformed() {
    assert!(ClientFrame::decode(&[]).is_err());
    assert!(ClientFrame::decode(&[0]).is_err());
    assert!(ClientFrame::decode(&[1, 0, 0]).is_err());
    let mut subscribe = ClientFrame::Subscribe { key: Key(1) }.encode();
    subscribe.push(0);
    assert!(ClientFrame::decode(&subscribe).is_err());
    assert!(ServerFrame::decode(&[1]).is_err());
    assert!(ServerFrame::decode(&[9, 1]).is_err());
}
