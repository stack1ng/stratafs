// The frames a client and a coordinator exchange, each one binary WebSocket message: a tag byte, then little endian
// integers, then any message bytes. Pings are the text messages PING and PONG, which hosts answer themselves.

// Key is what a subscription and a publish are about: an inode within the volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key(pub u64);

// ConnectionId names one connection to a coordinator for as long as it is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

pub const PING: &str = "ping";
pub const PONG: &str = "pong";

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
        key: Key,
    },
}

// ServerFrame is a frame a coordinator sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerFrame<'a> {
    // Subscribed answers Subscribe once the connection holds key, with the last message published to key.
    Subscribed {
        key: Key,
        last: Option<&'a [u8]>,
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
            ClientFrame::TimedOut { request, key } => frame(5, &[request, key.0], &[]),
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
                key: Key(reader.integer()?),
            },
            _ => return Err(MalformedFrame),
        };
        reader.finished(decoded)
    }
}

impl ServerFrame<'_> {
    pub fn encode(&self) -> Vec<u8> {
        match *self {
            ServerFrame::Subscribed { key, last: None } => frame(6, &[key.0], &[]),
            ServerFrame::Subscribed {
                key,
                last: Some(last),
            } => frame(7, &[key.0], last),
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
                last: None,
            },
            7 => ServerFrame::Subscribed {
                key: Key(reader.integer()?),
                last: Some(reader.rest()),
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
