// The coordinator holds one volume's subscriptions and publishes. Its host owns the connections, and calls it for
// each frame a connection receives and each connection that closes, one call at a time.

use std::collections::{HashMap, HashSet};

use crate::protocol::{ClientFrame, ConnectionId, Key, ServerFrame};

// Sockets is what a host lets the coordinator do with its connections.
pub trait Sockets {
    fn send(&self, connection: ConnectionId, frame: &[u8]);

    // close closes connection, and the host then calls closed for it.
    fn close(&self, connection: ConnectionId);
}

// Membership is the coordinator's state that has to outlive the coordinator itself: which connections hold each key,
// and the last message published to each key. A host whose coordinator can be dropped while its connections stay
// open, as a hibernating Durable Object's is, keeps it in storage.
pub trait Membership {
    fn holders(&self, key: Key) -> Vec<ConnectionId>;

    fn hold(&mut self, key: Key, connection: ConnectionId);

    fn release(&mut self, key: Key, connection: ConnectionId);

    // release_all releases every key connection holds.
    fn release_all(&mut self, connection: ConnectionId);

    fn last(&self, key: Key) -> Option<Vec<u8>>;

    fn set_last(&mut self, key: Key, message: &[u8]);
}

pub struct Coordinator<M> {
    membership: M,
    // pending is each publish still waiting on holders, by its publisher and request, with its key and the holders
    // that have not acknowledged it.
    pending: HashMap<(ConnectionId, u64), Pending>,
}

struct Pending {
    key: Key,
    missing: HashSet<ConnectionId>,
}

impl<M: Membership> Coordinator<M> {
    pub fn new(membership: M) -> Self {
        Coordinator {
            membership,
            pending: HashMap::new(),
        }
    }

    // received handles a frame connection sent. A frame that is not one closes the connection.
    pub fn received(&mut self, connection: ConnectionId, frame: &[u8], sockets: &impl Sockets) {
        let Ok(frame) = ClientFrame::decode(frame) else {
            sockets.close(connection);
            return;
        };
        match frame {
            ClientFrame::Subscribe { key } => {
                self.membership.hold(key, connection);
                let last = self.membership.last(key);
                let last = last.as_deref();
                sockets.send(connection, &ServerFrame::Subscribed { key, last }.encode());
            }
            ClientFrame::Unsubscribe { key } => {
                self.membership.release(key, connection);
                self.forget_holder(connection, |pending| pending.key == key, sockets);
            }
            // A publish goes to every holder of its key but its publisher, which made the change itself.
            ClientFrame::Publish {
                request,
                key,
                message,
            } => {
                self.membership.set_last(key, message);
                let missing: HashSet<ConnectionId> = self
                    .membership
                    .holders(key)
                    .into_iter()
                    .filter(|holder| *holder != connection)
                    .collect();
                if missing.is_empty() {
                    sockets.send(connection, &ServerFrame::Done { request }.encode());
                    return;
                }
                let deliver = ServerFrame::Deliver {
                    publisher: connection,
                    request,
                    key,
                    message,
                }
                .encode();
                for holder in &missing {
                    sockets.send(*holder, &deliver);
                }
                self.pending
                    .insert((connection, request), Pending { key, missing });
            }
            ClientFrame::Acknowledge { publisher, request } => {
                let Some(pending) = self.pending.get_mut(&(publisher, request)) else {
                    return;
                };
                pending.missing.remove(&connection);
                if pending.missing.is_empty() {
                    self.pending.remove(&(publisher, request));
                    sockets.send(publisher, &ServerFrame::Done { request }.encode());
                }
            }
            // A publish the coordinator no longer has, such as one hibernation lost, closes every other holder of
            // its key, since any of them may not have handled it.
            ClientFrame::TimedOut { request, key } => {
                let unanswered: Vec<ConnectionId> =
                    match self.pending.remove(&(connection, request)) {
                        Some(pending) => pending.missing.into_iter().collect(),
                        None => self
                            .membership
                            .holders(key)
                            .into_iter()
                            .filter(|holder| *holder != connection)
                            .collect(),
                    };
                for holder in unanswered {
                    sockets.close(holder);
                }
            }
        }
    }

    // closed forgets connection: what it holds, and the publishes it made. Publishes it had not acknowledged are
    // done once nothing else is missing.
    pub fn closed(&mut self, connection: ConnectionId, sockets: &impl Sockets) {
        self.membership.release_all(connection);
        self.pending
            .retain(|(publisher, _), _| *publisher != connection);
        self.forget_holder(connection, |_| true, sockets);
    }

    // forget_holder stops the publishes that matches selects waiting on holder, and answers those it leaves done.
    fn forget_holder(
        &mut self,
        holder: ConnectionId,
        matches: impl Fn(&Pending) -> bool,
        sockets: &impl Sockets,
    ) {
        let mut done = Vec::new();
        for (publish, pending) in &mut self.pending {
            if matches(pending) && pending.missing.remove(&holder) && pending.missing.is_empty() {
                done.push(*publish);
            }
        }
        for (publisher, request) in done {
            self.pending.remove(&(publisher, request));
            sockets.send(publisher, &ServerFrame::Done { request }.encode());
        }
    }
}

// MemoryMembership keeps membership in memory, for a host whose coordinator lives as long as its connections.
#[derive(Default)]
pub struct MemoryMembership {
    holders: HashMap<Key, HashSet<ConnectionId>>,
    held: HashMap<ConnectionId, HashSet<Key>>,
    last: HashMap<Key, Vec<u8>>,
}

impl Membership for MemoryMembership {
    fn holders(&self, key: Key) -> Vec<ConnectionId> {
        self.holders
            .get(&key)
            .map(|holders| holders.iter().copied().collect())
            .unwrap_or_default()
    }

    fn hold(&mut self, key: Key, connection: ConnectionId) {
        self.holders.entry(key).or_default().insert(connection);
        self.held.entry(connection).or_default().insert(key);
    }

    fn release(&mut self, key: Key, connection: ConnectionId) {
        if let Some(holders) = self.holders.get_mut(&key) {
            holders.remove(&connection);
            if holders.is_empty() {
                self.holders.remove(&key);
            }
        }
        if let Some(held) = self.held.get_mut(&connection) {
            held.remove(&key);
            if held.is_empty() {
                self.held.remove(&connection);
            }
        }
    }

    fn release_all(&mut self, connection: ConnectionId) {
        for key in self.held.remove(&connection).unwrap_or_default() {
            if let Some(holders) = self.holders.get_mut(&key) {
                holders.remove(&connection);
                if holders.is_empty() {
                    self.holders.remove(&key);
                }
            }
        }
    }

    fn last(&self, key: Key) -> Option<Vec<u8>> {
        self.last.get(&key).cloned()
    }

    fn set_last(&mut self, key: Key, message: &[u8]) {
        self.last.insert(key, message.to_vec());
    }
}

#[cfg(test)]
mod tests;
