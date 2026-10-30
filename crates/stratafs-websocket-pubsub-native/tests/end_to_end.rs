use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stratafs_websocket_pubsub::client::{Client, ClientError, Failure};
use stratafs_websocket_pubsub::protocol::Key;
use tokio::net::TcpListener;

const PINGS: Duration = Duration::from_millis(100);
const KEY: Key = Key(7);

// server serves on a free local port and returns the URL of volume's coordinator.
async fn server() -> impl Fn(&str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(stratafs_websocket_pubsub_native::serve(listener));
    move |volume| format!("ws://{address}/volume/{volume}")
}

async fn client(url: &str) -> Client<u64> {
    Client::connect(url, PINGS).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_resolves_once_every_other_holder_has_handled_it() {
    let url = server().await;
    let handled = Arc::new(Mutex::new(Vec::new()));
    let mut holders = Vec::new();
    let mut subscriptions = Vec::new();
    for holder in 0..2 {
        let holder_client = client(&url("a")).await;
        let handled = Arc::clone(&handled);
        subscriptions.push(
            holder_client
                .subscribe(
                    KEY,
                    |_| {},
                    move |message| {
                        let handled = Arc::clone(&handled);
                        async move {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            handled.lock().unwrap().push((holder, message));
                        }
                    },
                )
                .await
                .unwrap(),
        );
        holders.push(holder_client);
    }
    let writer = client(&url("a")).await;
    writer
        .publish(KEY, &2, Duration::from_secs(5))
        .await
        .unwrap();
    let mut handled = handled.lock().unwrap().clone();
    handled.sort();
    assert_eq!(handled, vec![(0, 2), (1, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_subscriber_gets_the_last_message_published() {
    let url = server().await;
    let writer = client(&url("a")).await;
    writer
        .publish(KEY, &3, Duration::from_secs(5))
        .await
        .unwrap();
    let late = client(&url("a")).await;
    let last = Arc::new(Mutex::new(None));
    let seen = Arc::clone(&last);
    let _subscription = late
        .subscribe(
            KEY,
            move |message| *seen.lock().unwrap() = message,
            |_| async {},
        )
        .await
        .unwrap();
    assert_eq!(*last.lock().unwrap(), Some(3));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_holder_that_does_not_answer_in_time_is_disconnected() {
    let url = server().await;
    let hung = client(&url("a")).await;
    let _subscription = hung
        .subscribe(KEY, |_| {}, |_| std::future::pending())
        .await
        .unwrap();
    let writer = client(&url("a")).await;
    let published = writer.publish(KEY, &4, Duration::from_millis(200)).await;
    assert!(matches!(published, Err(ClientError::TimedOut)));
    tokio::time::timeout(Duration::from_secs(5), hung.failed())
        .await
        .unwrap();
    writer
        .publish(KEY, &5, Duration::from_secs(5))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_holder_that_disconnects_no_longer_holds_up_publishes() {
    let url = server().await;
    let holder = client(&url("a")).await;
    let subscription = holder.subscribe(KEY, |_| {}, |_| async {}).await.unwrap();
    drop(subscription);
    drop(holder);
    let writer = client(&url("a")).await;
    let started = Instant::now();
    writer
        .publish(KEY, &6, Duration::from_secs(5))
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_secs(1));
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
    let refused = Client::<u64>::connect(&url("a").replace("/volume/a", "/elsewhere"), PINGS).await;
    assert!(matches!(refused, Err(ClientError::Connect(_))));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_whose_coordinator_stops_answering_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    // A coordinator that accepts the connection and then never reads from it, so no ping is answered.
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _silent = tokio_tungstenite::accept_async(stream).await.unwrap();
        std::future::pending::<()>().await;
    });
    let client = Client::<u64>::connect(&format!("ws://{address}/volume/a"), PINGS)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), client.failed())
        .await
        .unwrap();
    let published = client.publish(KEY, &1, Duration::from_secs(5)).await;
    assert!(
        matches!(&published, Err(ClientError::Failed(failure)) if matches!(**failure, Failure::Unanswered)),
        "{published:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_publish_dropped_before_it_is_done_disconnects_the_holders_that_did_not_answer() {
    let url = server().await;
    let hung = client(&url("a")).await;
    let _subscription = hung
        .subscribe(KEY, |_| {}, |_| std::future::pending())
        .await
        .unwrap();
    let writer = client(&url("a")).await;
    let dropped = tokio::time::timeout(
        Duration::from_millis(200),
        writer.publish(KEY, &8, Duration::from_secs(60)),
    )
    .await;
    assert!(dropped.is_err());
    tokio::time::timeout(Duration::from_secs(5), hung.failed())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscribe_dropped_before_it_is_answered_releases_its_key() {
    let url = server().await;
    let holder = client(&url("a")).await;
    {
        let subscribing = holder.subscribe(KEY, |_| {}, |_| std::future::pending());
        tokio::pin!(subscribing);
        // Polled once, so the subscribe is sent, then dropped before its answer.
        tokio::select! {
            biased;
            _ = &mut subscribing => panic!("the coordinator answered within one poll"),
            _ = async {} => {}
        }
    }
    let handled = Arc::new(Mutex::new(Vec::new()));
    let subscription = loop {
        let recorded = Arc::clone(&handled);
        match holder
            .subscribe(
                KEY,
                |_| {},
                move |message| {
                    let recorded = Arc::clone(&recorded);
                    async move { recorded.lock().unwrap().push(message) }
                },
            )
            .await
        {
            Ok(subscription) => break subscription,
            Err(ClientError::AlreadySubscribed) => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Err(error) => panic!("{error}"),
        }
    };
    let writer = client(&url("a")).await;
    writer
        .publish(KEY, &10, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(*handled.lock().unwrap(), vec![10]);
    drop(subscription);
}
