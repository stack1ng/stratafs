// StoredMembership keeps a coordinator's membership in its Durable Object's SQLite, where it outlives hibernation.
// Keys are written as hexadecimal text, since JavaScript numbers cannot hold every u64.

use std::rc::Rc;

use serde::Deserialize;
use stratafs_websocket_pubsub::coordinator::Membership;
use stratafs_websocket_pubsub::protocol::{ConnectionId, Key};
use worker::{SqlStorageValue, State};

pub(crate) struct StoredMembership(Rc<State>);

#[derive(Deserialize)]
struct Holder {
    connection: i64,
}

#[derive(Deserialize)]
struct Last {
    #[serde(with = "serde_bytes")]
    message: Vec<u8>,
}

impl StoredMembership {
    pub(crate) fn open(state: Rc<State>) -> Self {
        for table in [
            "CREATE TABLE IF NOT EXISTS holders (key TEXT NOT NULL, connection INTEGER NOT NULL, \
             PRIMARY KEY (key, connection)) WITHOUT ROWID",
            "CREATE INDEX IF NOT EXISTS holders_by_connection ON holders (connection)",
            "CREATE TABLE IF NOT EXISTS last (key TEXT PRIMARY KEY, message BLOB NOT NULL) WITHOUT ROWID",
            "CREATE TABLE IF NOT EXISTS connections (next INTEGER NOT NULL)",
            "INSERT INTO connections (next) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM connections)",
        ] {
            run(&state, table, vec![]);
        }
        StoredMembership(state)
    }

    // release_all_but releases everything held by connections not in open.
    pub(crate) fn release_all_but(&mut self, open: &[ConnectionId]) {
        let holders: Vec<Holder> = rows(&self.0, "SELECT DISTINCT connection FROM holders", vec![]);
        for holder in holders {
            let connection = ConnectionId(holder.connection as u64);
            if !open.contains(&connection) {
                self.release_all(connection);
            }
        }
    }
}

impl Membership for StoredMembership {
    fn holders(&self, key: Key) -> Vec<ConnectionId> {
        let holders: Vec<Holder> = rows(
            &self.0,
            "SELECT connection FROM holders WHERE key = ?",
            vec![text(key).into()],
        );
        holders
            .into_iter()
            .map(|holder| ConnectionId(holder.connection as u64))
            .collect()
    }

    fn hold(&mut self, key: Key, connection: ConnectionId) {
        run(
            &self.0,
            "INSERT OR IGNORE INTO holders (key, connection) VALUES (?, ?)",
            vec![text(key).into(), (connection.0 as i64).into()],
        );
    }

    fn release(&mut self, key: Key, connection: ConnectionId) {
        run(
            &self.0,
            "DELETE FROM holders WHERE key = ? AND connection = ?",
            vec![text(key).into(), (connection.0 as i64).into()],
        );
    }

    fn release_all(&mut self, connection: ConnectionId) {
        run(
            &self.0,
            "DELETE FROM holders WHERE connection = ?",
            vec![(connection.0 as i64).into()],
        );
    }

    fn last(&self, key: Key) -> Option<Vec<u8>> {
        let last: Vec<Last> = rows(
            &self.0,
            "SELECT message FROM last WHERE key = ?",
            vec![text(key).into()],
        );
        Some(last.into_iter().next()?.message)
    }

    fn set_last(&mut self, key: Key, message: &[u8]) {
        run(
            &self.0,
            "INSERT OR REPLACE INTO last (key, message) VALUES (?, ?)",
            vec![text(key).into(), message.to_vec().into()],
        );
    }
}

#[derive(Deserialize)]
struct Next {
    connection: i64,
}

// next_connection numbers a new connection, never reusing a number.
pub(crate) fn next_connection(state: &State) -> ConnectionId {
    let next: Vec<Next> = rows(
        state,
        "UPDATE connections SET next = next + 1 RETURNING next - 1 AS connection",
        vec![],
    );
    ConnectionId(next[0].connection as u64)
}

fn run(state: &State, query: &str, bindings: Vec<SqlStorageValue>) {
    if let Err(error) = state.storage().sql().exec(query, bindings) {
        failed(state, error);
    }
}

fn rows<T: for<'a> Deserialize<'a>>(
    state: &State,
    query: &str,
    bindings: Vec<SqlStorageValue>,
) -> Vec<T> {
    match state
        .storage()
        .sql()
        .exec(query, bindings)
        .and_then(|cursor| cursor.to_array())
    {
        Ok(rows) => rows,
        Err(error) => failed(state, error),
    }
}

// failed closes every connection, so every mount fails rather than trusting membership the object could not keep, and
// then ends the call. Cloudflare starts the object afresh for the next one.
fn failed(state: &State, error: worker::Error) -> ! {
    for socket in state.get_websockets() {
        let _ = socket.close(Some(1011), Some("the coordinator's storage failed"));
    }
    panic!("the Durable Object's storage failed: {error}");
}

fn text(key: Key) -> String {
    format!("{:016x}", key.0)
}
