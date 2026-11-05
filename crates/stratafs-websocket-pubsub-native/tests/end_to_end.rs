// What the native host and client do beyond the conformance suite: route connections to their volumes, and serve
// futures polled outside Tokio.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use stratafs_pubsub::{PubSub, PubSubError};
use stratafs_websocket_pubsub::client::Client;
use stratafs_websocket_pubsub::protocol::Key;
use tokio::net::TcpListener;

const KEY: Key = Key(7);

// server serves on a free local port and returns the URL of volume's coordinator.
async fn server() -> impl Fn(&str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(stratafs_websocket_pubsub_native::serve(listener));
    move |volume| format!("ws://{address}/volume/{volume}")
}

async fn client(url: &str) -> Client<u64> {
    Client::connect(url).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn volumes_do_not_share_holders() {
    let url = server().await;
    let holder = client(&url("a")).await;
    let _subscription = holder
        .subscribe(KEY, |_| std::future::pending())
        .await
        .unwrap();
    let writer = client(&url("b")).await;
    writer
        .publish(KEY, &7, Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_path_that_names_no_volume_is_refused() {
    let url = server().await;
    let refused = Client::<u64>::connect(&url("a").replace("/volume/a", "/elsewhere")).await;
    assert!(refused.is_err());
}

// A client's futures can be polled outside Tokio, as FSKit polls them on a futures thread pool.
#[test]
fn a_client_works_outside_tokio() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let (holder, stuck, writer) = runtime.block_on(async {
        let url = server().await;
        (
            client(&url("a")).await,
            client(&url("a")).await,
            client(&url("a")).await,
        )
    });
    std::thread::spawn(move || {
        futures_executor::block_on(async {
            let handled = Arc::new(Mutex::new(Vec::new()));
            let handling = Arc::clone(&handled);
            let _subscription = holder
                .subscribe(KEY, move |message| {
                    handling.lock().unwrap().push(message);
                    async {}
                })
                .await
                .unwrap();
            writer
                .publish(KEY, &1, Duration::from_secs(5))
                .await
                .unwrap();
            assert_eq!(*handled.lock().unwrap(), vec![1]);
            // A publish that times out needs a timer, and its holder that did not handle it fails.
            let _stuck_subscription = stuck
                .subscribe(KEY, |_| std::future::pending())
                .await
                .unwrap();
            let published = writer.publish(KEY, &2, Duration::from_millis(200)).await;
            assert!(
                matches!(published, Err(PubSubError::TimedOut)),
                "{published:?}"
            );
            stuck.failed().await;
        });
    })
    .join()
    .unwrap();
}
