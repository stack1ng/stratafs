// The conformance suite, run with the websocket client against a native host. The ignored tests run it against the
// host at STRATAFS_PUBSUB_URL instead, such as ws://127.0.0.1:18787 for a Durable Object in wrangler dev.

use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use stratafs_pubsub_conformance::Volume;
use stratafs_websocket_pubsub::client::Client;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::task::AbortHandle;

mod native {
    stratafs_pubsub_conformance::pubsub_conformance!(super::native_volume());
}

mod remote {
    stratafs_pubsub_conformance::pubsub_conformance!(
        super::remote_volume(),
        #[ignore = "needs a host at STRATAFS_PUBSUB_URL"]
    );
}

// native_volume is a volume of a native host of its own.
async fn native_volume() -> ProxiedVolume {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    tokio::spawn(stratafs_websocket_pubsub_native::serve(listener));
    ProxiedVolume::new(address, "a".to_owned()).await
}

// remote_volume is a volume of the host at STRATAFS_PUBSUB_URL, named so that no other test, in this run or an
// earlier one, has used it.
async fn remote_volume() -> ProxiedVolume {
    static MADE: AtomicU64 = AtomicU64::new(0);
    let url = std::env::var("STRATAFS_PUBSUB_URL").expect("STRATAFS_PUBSUB_URL is the host's URL");
    let address = url
        .strip_prefix("ws://")
        .expect("STRATAFS_PUBSUB_URL is a ws:// URL")
        .trim_end_matches('/')
        .to_owned();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let volume = format!("conformance-{now}-{}", MADE.fetch_add(1, Ordering::Relaxed));
    ProxiedVolume::new(address, volume).await
}

// ProxiedVolume is a volume its clients reach through a proxy, which cuts a client's connection by dropping it, and
// silences it by no longer copying it.
struct ProxiedVolume {
    url: String,
    // proxied is each connection the proxy accepts, in the order it accepts them.
    proxied: Mutex<mpsc::UnboundedReceiver<Proxied>>,
}

// Proxied is a connection through the proxy: the task carrying it, and what silences it.
struct Proxied {
    carrying: AbortHandle,
    silenced: Arc<Notify>,
}

impl ProxiedVolume {
    // new proxies volume on the host at address.
    async fn new(address: String, volume: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/volume/{volume}", listener.local_addr().unwrap());
        let (accepted, proxied) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let (mut inbound, _) = listener.accept().await.unwrap();
                let address = address.clone();
                let silenced = Arc::new(Notify::new());
                let silencing = Arc::clone(&silenced);
                let carrying = tokio::spawn(async move {
                    let mut outbound = TcpStream::connect(address).await.unwrap();
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                        // Silenced, it keeps both sockets open until cut.
                        () = silencing.notified() => pending().await,
                    }
                });
                let proxied = Proxied {
                    carrying: carrying.abort_handle(),
                    silenced,
                };
                if accepted.send(proxied).is_err() {
                    return;
                }
            }
        });
        ProxiedVolume {
            url,
            proxied: Mutex::new(proxied),
        }
    }
}

impl Volume for ProxiedVolume {
    type Client = Client<u64>;
    type Connection = Proxied;

    async fn connect(&self) -> (Client<u64>, Proxied) {
        // One client connects at a time, so the next connection the proxy accepts is this client's.
        let mut proxied = self.proxied.lock().await;
        let client = Client::connect(&self.url).await.unwrap();
        (client, proxied.recv().await.unwrap())
    }

    fn cut(&self, connection: &Proxied) {
        connection.carrying.abort();
    }

    fn silence(&self, connection: &Proxied) {
        connection.silenced.notify_one();
    }
}
