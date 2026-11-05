// no_holder_misses_a_publish runs clients that subscribe, unsubscribe, publish, disconnect and are cut at random, all
// at once, and checks that each publish resolved only after every other client that held its key from before the
// publish began, and still held it when the publish resolved, had handled it. Each round has its own seed and keys.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::FutureExt;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use stratafs_pubsub::{Key, PubSub};

use crate::{PATIENT, Volume, within};

const ROUNDS: u64 = 32;
const CLIENTS: u64 = 4;
const KEYS: u64 = 2;
const STEPS: u64 = 24;

pub async fn no_holder_misses_a_publish(volume: impl Volume) {
    let volume = Arc::new(volume);
    let mut owed = 0;
    for round in 0..ROUNDS {
        let history = Arc::new(Mutex::new(History::default()));
        let clients: Vec<_> = (0..CLIENTS)
            .map(|client| {
                tokio::spawn(run_client(
                    Arc::clone(&volume),
                    Arc::clone(&history),
                    round,
                    client,
                ))
            })
            .collect();
        for client in clients {
            if let Err(error) = client.await {
                std::panic::resume_unwind(error.into_panic());
            }
        }
        owed += history.lock().unwrap().check(round);
    }
    assert!(
        owed > 0,
        "no publish was owed to a holder, so nothing was checked"
    );
}

// History is what a round's clients did, and when.
#[derive(Default)]
struct History {
    holds: Vec<Hold>,
    // handled is each message a hold's handler finished with, by hold, and when.
    handled: Vec<(usize, u64, Instant)>,
    publishes: Vec<Publish>,
    // gone is when each client disconnected or was cut.
    gone: HashMap<u64, Instant>,
}

struct Hold {
    client: u64,
    key: Key,
    // subscribed is when its subscribe returned.
    subscribed: Option<Instant>,
    // released is when its subscription was dropped.
    released: Option<Instant>,
}

struct Publish {
    client: u64,
    key: Key,
    message: u64,
    began: Instant,
    resolved: Instant,
}

// message is the message a client publishes at a step of a round, which says who published it.
fn message(round: u64, client: u64, step: u64) -> u64 {
    round * 1_000_000 + client * 1_000 + step
}

fn publisher(message: u64) -> u64 {
    message % 1_000_000 / 1_000
}

impl History {
    // check checks the round, and returns how many handlings its publishes were owed.
    fn check(&self, round: u64) -> usize {
        let mut owed_handlings = 0;
        for publish in &self.publishes {
            for (hold_index, hold) in self.holds.iter().enumerate() {
                let Some(subscribed) = hold.subscribed else {
                    continue;
                };
                if hold.key != publish.key {
                    continue;
                }
                let owed = hold.client != publish.client
                    && subscribed < publish.began
                    && self.held_through(hold, publish.resolved);
                if !owed {
                    continue;
                }
                owed_handlings += 1;
                let handled = self.handled.iter().any(|(handled_hold, message, at)| {
                    *handled_hold == hold_index
                        && *message == publish.message
                        && *at <= publish.resolved
                });
                assert!(
                    handled,
                    "round {round}: client {}'s publish of {} to {:?} resolved before client {}, which held the key \
                     throughout, had handled it",
                    publish.client, publish.message, publish.key, hold.client,
                );
            }
        }
        let mut seen = HashSet::new();
        for (hold, message, _) in &self.handled {
            let client = self.holds[*hold].client;
            assert_ne!(
                publisher(*message),
                client,
                "round {round}: client {client} got its own publish of {message}"
            );
            assert!(
                seen.insert((hold, message)),
                "round {round}: client {client} handled {message} twice"
            );
        }
        owed_handlings
    }

    // held_through says whether hold was still held at at: neither released nor its client gone.
    fn held_through(&self, hold: &Hold, at: Instant) -> bool {
        hold.released.is_none_or(|released| released > at)
            && self.gone.get(&hold.client).is_none_or(|gone| *gone > at)
    }
}

async fn run_client<V: Volume>(
    volume: Arc<V>,
    history: Arc<Mutex<History>>,
    round: u64,
    client: u64,
) {
    let mut random = StdRng::seed_from_u64(round * CLIENTS + client);
    let (pubsub, connection) = volume.connect().await;
    let mut holding: HashMap<Key, (usize, <V::Client as PubSub>::Subscription)> = HashMap::new();
    for step in 0..STEPS {
        let key = Key(round * KEYS + random.random_range(0..KEYS));
        match random.random_range(0..40) {
            0..10 => {
                if let Some((hold, subscription)) = holding.remove(&key) {
                    history.lock().unwrap().holds[hold].released = Some(Instant::now());
                    drop(subscription);
                    continue;
                }
                let hold = {
                    let mut history = history.lock().unwrap();
                    history.holds.push(Hold {
                        client,
                        key,
                        subscribed: None,
                        released: None,
                    });
                    history.holds.len() - 1
                };
                let handled = Arc::clone(&history);
                let delay = Duration::from_micros(random.random_range(0..1500));
                let subscription = pubsub
                    .subscribe(key, move |message| {
                        let handled = Arc::clone(&handled);
                        async move {
                            tokio::time::sleep(delay).await;
                            handled
                                .lock()
                                .unwrap()
                                .handled
                                .push((hold, message, Instant::now()));
                        }
                    })
                    .await
                    .unwrap_or_else(|error| {
                        panic!("round {round}: client {client} could not subscribe: {error:?}")
                    });
                history.lock().unwrap().holds[hold].subscribed = Some(Instant::now());
                holding.insert(key, (hold, subscription));
            }
            10..30 => {
                let message = message(round, client, step);
                let began = Instant::now();
                pubsub
                    .publish(key, &message, PATIENT)
                    .await
                    .unwrap_or_else(|error| {
                        panic!(
                            "round {round}: client {client}'s publish of {message} failed: {error:?}"
                        )
                    });
                history.lock().unwrap().publishes.push(Publish {
                    client,
                    key,
                    message,
                    began,
                    resolved: Instant::now(),
                });
            }
            30..38 => {
                tokio::time::sleep(Duration::from_millis(random.random_range(0..3))).await;
            }
            38 => break,
            _ => {
                history.lock().unwrap().gone.insert(client, Instant::now());
                volume.cut(&connection);
                within(PATIENT, pubsub.failed()).await;
                return;
            }
        }
    }
    // A client that was not cut has no reason to have failed.
    let failure = pubsub.failed().now_or_never();
    assert!(
        failure.is_none(),
        "round {round}: client {client} failed: {failure:?}"
    );
    history.lock().unwrap().gone.insert(client, Instant::now());
}
