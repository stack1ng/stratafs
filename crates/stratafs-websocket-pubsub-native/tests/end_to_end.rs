// What the native host does beyond the conformance suite: route connections to their volumes.

use std::time::Duration;

use stratafs_pubsub::PubSub;
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
        .subscribe(KEY, |_| {}, |_| std::future::pending())
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
