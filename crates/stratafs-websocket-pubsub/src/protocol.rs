// The frames a client and a coordinator exchange, each one binary WebSocket message: a tag byte, then little endian
// integers, then any message bytes. Pings are the text messages PING and PONG, which hosts answer themselves.

use std::time::Duration;

pub use stratafs_pubsub::Key;

// ConnectionId names one connection to a coordinator for as long as it is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

pub const PING: &str = "ping";
pub const PONG: &str = "pong";

// A client pings every PING_EVERY. A host counts a client gone once it has heard nothing from it for SILENT, eight
// pings, since a connection the network drops does not close.
pub const PING_EVERY: Duration = Duration::from_millis(250);
pub const SILENT: Duration = Duration::from_secs(2);
// A client counts its host gone once CLIENT_SILENT passes after its last answered ping. It is below SILENT, so a
// client fails before a host can drop it and let publishes past it. Pings come often, so a ping a busy machine sends
// late is still answered within CLIENT_SILENT.
pub const CLIENT_SILENT: Duration = Duration::from_millis(1500);

// ClientFrame is a frame a client sends. A publish is named by its publisher's connection and the request number
// the publisher gave it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientFrame<'a> {
    Subscribe {
        key: Key,
    },
    Unsubscribe {
        key: Key,
    },
    Publish {
        request: u64,
        key: Key,
        message: &'a [u8],
    },
    Acknowledge {
        publisher: ConnectionId,
        request: u64,
    },
    // TimedOut gives up on a publish, and the holders that have not acknowledged it are disconnected.
    TimedOut {
        request: u64,
    },
}

// ServerFrame is a frame a coordinator sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerFrame<'a> {
    // Subscribed answers Subscribe once the connection holds key.
    Subscribed {
        key: Key,
    },
    Deliver {
        publisher: ConnectionId,
        request: u64,
        key: Key,
        message: &'a [u8],
    },
    // Done answers Publish once every other holder of its key has acknowledged it.
    Done {
        request: u64,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("the frame is not a frame this protocol has")]
pub struct MalformedFrame;

impl ClientFrame<'_> {
    pub fn encode(&self) -> Vec<u8> {
        match *self {
            ClientFrame::Subscribe { key } => frame(1, &[key.0], &[]),
            ClientFrame::Unsubscribe { key } => frame(2, &[key.0], &[]),
            ClientFrame::Publish {
                request,
                key,
                message,
            } => frame(3, &[request, key.0], message),
            ClientFrame::Acknowledge { publisher, request } => {
                frame(4, &[publisher.0, request], &[])
            }
            ClientFrame::TimedOut { request } => frame(5, &[request], &[]),
        }
    }
}

impl<'a> ClientFrame<'a> {
    pub fn decode(frame: &'a [u8]) -> Result<Self, MalformedFrame> {
        let (&tag, rest) = frame.split_first().ok_or(MalformedFrame)?;
        let mut reader = Reader(rest);
        let decoded = match tag {
            1 => ClientFrame::Subscribe {
                key: Key(reader.integer()?),
            },
            2 => ClientFrame::Unsubscribe {
                key: Key(reader.integer()?),
            },
            3 => ClientFrame::Publish {
                request: reader.integer()?,
                key: Key(reader.integer()?),
                message: reader.rest(),
            },
            4 => ClientFrame::Acknowledge {
                publisher: ConnectionId(reader.integer()?),
                request: reader.integer()?,
            },
            5 => ClientFrame::TimedOut {
                request: reader.integer()?,
            },
            _ => return Err(MalformedFrame),
        };
        reader.finished(decoded)
    }
}

impl ServerFrame<'_> {
    pub fn encode(&self) -> Vec<u8> {
        match *self {
            ServerFrame::Subscribed { key } => frame(6, &[key.0], &[]),
            ServerFrame::Deliver {
                publisher,
                request,
                key,
                message,
            } => frame(8, &[publisher.0, request, key.0], message),
            ServerFrame::Done { request } => frame(9, &[request], &[]),
        }
    }
}

impl<'a> ServerFrame<'a> {
    pub fn decode(frame: &'a [u8]) -> Result<Self, MalformedFrame> {
        let (&tag, rest) = frame.split_first().ok_or(MalformedFrame)?;
        let mut reader = Reader(rest);
        let decoded = match tag {
            6 => ServerFrame::Subscribed {
                key: Key(reader.integer()?),
            },
            8 => ServerFrame::Deliver {
                publisher: ConnectionId(reader.integer()?),
                request: reader.integer()?,
                key: Key(reader.integer()?),
                message: reader.rest(),
            },
            9 => ServerFrame::Done {
                request: reader.integer()?,
            },
            _ => return Err(MalformedFrame),
        };
        reader.finished(decoded)
    }
}

fn frame(tag: u8, integers: &[u64], message: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(1 + 8 * integers.len() + message.len());
    frame.push(tag);
    for integer in integers {
        frame.extend_from_slice(&integer.to_le_bytes());
    }
    frame.extend_from_slice(message);
    frame
}

// Reader reads a frame's integers in order.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn integer(&mut self) -> Result<u64, MalformedFrame> {
        let (integer, rest) = self.0.split_first_chunk::<8>().ok_or(MalformedFrame)?;
        self.0 = rest;
        Ok(u64::from_le_bytes(*integer))
    }

    fn rest(&mut self) -> &'a [u8] {
        std::mem::take(&mut self.0)
    }

    // finished is decoded, unless bytes are left over.
    fn finished<T>(self, decoded: T) -> Result<T, MalformedFrame> {
        if self.0.is_empty() {
            Ok(decoded)
        } else {
            Err(MalformedFrame)
        }
    }
}

#[cfg(test)]
mod tests;
