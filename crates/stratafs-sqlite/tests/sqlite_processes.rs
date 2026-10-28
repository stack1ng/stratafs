use std::convert::Infallible;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::{Child, Command, Stdio};
use std::sync::{Barrier, mpsc};
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_lite::future::{self, block_on};
use stratafs_sqlite::{SqliteContent, SqliteMetadata};
use stratafs_store::content::{ContentHash, ContentStore};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};
use stratafs_store::spawn::ThreadPool;

// A test runs its own process again as a child, which finds the store it works on in this variable.
const CHILD_STORE: &str = "STRATAFS_SQLITE_CHILD_STORE";

// A value as large as a tree node, so each commit adds a page or two to the write ahead log, and some thousands of
// commits fill it enough to checkpoint it.
const VALUE_SIZE: usize = 16 * 1024;
// Content a little over one chunk long, so each is two chunks, and a thousand fill the log enough to checkpoint it.
const CONTENT_SIZE: usize = 300 * 1024;

#[test]
fn a_killed_process_keeps_every_commit_it_made_durable() {
    if let Some(path) = child_store() {
        return commit_until_killed(&path);
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata.sqlite");
    let mut child = spawn_child("a_killed_process_keeps_every_commit_it_made_durable", &path);
    let mut durable = None;
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        if let Some(index) = line.unwrap().strip_prefix("durable ") {
            durable = Some(index.parse::<u64>().unwrap());
            if durable >= Some(12_000) {
                break;
            }
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let durable = durable.expect("the child made commits durable");
    // Only a checkpoint copies commits into the database file, so its size shows the child checkpointed.
    assert!(std::fs::metadata(&path).unwrap().len() > 16 * 1024 * 1024);

    let store = SqliteMetadata::local(&path, ThreadPool).unwrap();
    for index in 0..=durable {
        assert_eq!(
            get(&store, key(index)),
            Some(value(index)),
            "commit {index}"
        );
    }
}

fn commit_until_killed(path: &Path) {
    let store = SqliteMetadata::local(path, ThreadPool).unwrap();
    for index in 0..u64::MAX {
        set(&store, key(index), value(index));
        if index % 7 == 0 {
            let against = match index % 2 {
                0 => DurableAgainst::SoftwareFailure,
                _ => DurableAgainst::PowerFailure,
            };
            block_on(store.ensure_durable(against)).unwrap();
            println!("durable {index}");
        }
    }
}

#[test]
fn a_killed_process_keeps_every_commit_it_made_durable_with_its_content() {
    if let Some(path) = child_store() {
        return commit_content_until_killed(&path);
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.sqlite");
    let mut child = spawn_child(
        "a_killed_process_keeps_every_commit_it_made_durable_with_its_content",
        &path,
    );
    let mut durable = None;
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        if let Some(index) = line.unwrap().strip_prefix("durable ") {
            durable = Some(index.parse::<u64>().unwrap());
            if durable >= Some(1_000) {
                break;
            }
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let durable = durable.expect("the child made commits durable");
    // Only a checkpoint copies commits into the database file, so its size shows the child checkpointed.
    assert!(std::fs::metadata(&path).unwrap().len() > 16 * 1024 * 1024);

    let store = SqliteMetadata::local(&path, ThreadPool).unwrap();
    let stored = SqliteContent::of(&store);
    for index in 0..=durable {
        let hash = get(&store, key(index)).expect("the commit is durable");
        let hash = ContentHash(hash[..].try_into().unwrap());
        let mut read = block_on(stored.get(&hash, 0, CONTENT_SIZE as u64)).unwrap();
        assert_eq!(
            read.copy_to_bytes(read.remaining()),
            content(index),
            "commit {index}"
        );
    }
}

fn commit_content_until_killed(path: &Path) {
    let store = SqliteMetadata::local(path, ThreadPool).unwrap();
    for index in 0..u64::MAX {
        commit_content(&store, key(index), content(index));
        if index % 7 == 0 {
            let against = match index % 2 {
                0 => DurableAgainst::SoftwareFailure,
                _ => DurableAgainst::PowerFailure,
            };
            make_durable(&store, against);
            println!("durable {index}");
        }
    }
}

#[test]
fn two_processes_write_one_store_one_transaction_at_a_time() {
    const TRANSACTIONS: u64 = 500;
    if let Some(path) = child_store() {
        let store = SqliteMetadata::local(&path, ThreadPool).unwrap();
        // Both children start writing together, once the parent has heard both are open.
        println!("open");
        std::io::stdin().lines().next().unwrap().unwrap();
        for _ in 0..TRANSACTIONS {
            increment(&store, Bytes::from_static(b"counter"));
            // A pause between transactions lets the other child in, so both wait for each other often.
            std::thread::sleep(Duration::from_micros(50));
        }
        block_on(store.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
        return;
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata.sqlite");
    // The store exists before either child opens it, so the children only race to write.
    drop(SqliteMetadata::local(&path, ThreadPool).unwrap());
    let mut children: Vec<Child> = (0..2)
        .map(|_| {
            spawn_child(
                "two_processes_write_one_store_one_transaction_at_a_time",
                &path,
            )
        })
        .collect();
    for child in &mut children {
        // The test harness prints the test's name on the line the child prints to.
        let mut lines = BufReader::new(child.stdout.as_mut().unwrap()).lines();
        while !lines.next().unwrap().unwrap().ends_with("open") {}
    }
    for child in &mut children {
        writeln!(child.stdin.as_mut().unwrap(), "start").unwrap();
    }
    for child in children {
        assert!(child.wait_with_output().unwrap().status.success());
    }
    let store = SqliteMetadata::local(&path, ThreadPool).unwrap();
    assert_eq!(
        get(&store, Bytes::from_static(b"counter")),
        Some(Bytes::copy_from_slice(&(2 * TRANSACTIONS).to_be_bytes()))
    );
}

#[test]
fn two_processes_create_one_store_together() {
    for _ in 0..50 {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("metadata.sqlite");
        // Two stores in one process lock the file from each other as two processes would.
        let start = Barrier::new(2);
        std::thread::scope(|scope| {
            let opening = [(); 2].map(|()| {
                scope.spawn(|| {
                    start.wait();
                    SqliteMetadata::local(&path, ThreadPool).map(drop)
                })
            });
            for opened in opening {
                opened.join().unwrap().unwrap();
            }
        });
    }
}

#[test]
fn a_cancelled_wait_for_another_writer_leaves_its_transaction_rolled_back() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("metadata.sqlite");
    // Two stores on one file in one process lock it from each other as two processes would.
    let holder = SqliteMetadata::local(&path, ThreadPool).unwrap();
    let waiter = SqliteMetadata::local(&path, ThreadPool).unwrap();
    let (holding, released) = (Barrier::new(2), Barrier::new(2));
    std::thread::scope(|scope| {
        scope.spawn(|| {
            block_on(holder.transact(|mut transaction| {
                let (holding, released) = (&holding, &released);
                Box::pin(async move {
                    transaction
                        .set(Bytes::from_static(b"held"), &b"value"[..])
                        .unwrap();
                    holding.wait();
                    released.wait();
                    Ok::<_, Infallible>(((), transaction))
                })
            }))
            .unwrap();
        });
        holding.wait();
        // The waiter finds the file locked, starts waiting on its spawner, and is cancelled while it waits.
        {
            let mut waiting = pin!(waiter.transact(|mut transaction| {
                Box::pin(async move {
                    transaction
                        .set(Bytes::from_static(b"waited"), &b"value"[..])
                        .unwrap();
                    Ok::<_, Infallible>(((), transaction))
                })
            }));
            assert!(block_on(future::poll_once(waiting.as_mut())).is_none());
            std::thread::sleep(Duration::from_millis(50));
        }
        released.wait();
    });
    // The cancelled wait begins its transaction once the holder commits, and rolls it back, so both stores write on.
    let (wrote, written) = mpsc::channel();
    std::thread::spawn(move || {
        set(&holder, Bytes::from_static(b"after"), value(0));
        set(&waiter, Bytes::from_static(b"after"), value(1));
        wrote.send(holder).unwrap();
    });
    let holder = written
        .recv_timeout(Duration::from_secs(10))
        .expect("a write waits on a transaction the cancelled wait left open");
    assert_eq!(get(&holder, Bytes::from_static(b"waited")), None);
    assert_eq!(
        get(&holder, Bytes::from_static(b"held")),
        Some(Bytes::from_static(b"value"))
    );
}

fn child_store() -> Option<PathBuf> {
    std::env::var_os(CHILD_STORE).map(PathBuf::from)
}

fn spawn_child(test: &str, path: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture", "--test-threads=1"])
        .env(CHILD_STORE, path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap()
}

fn key(index: u64) -> Bytes {
    Bytes::copy_from_slice(&index.to_be_bytes())
}

fn value(index: u64) -> Bytes {
    let byte = u8::try_from(index % 251).unwrap();
    Bytes::from(vec![byte; VALUE_SIZE])
}

// content is content of its own for index, as long as two chunks of it.
fn content(index: u64) -> Bytes {
    let mut content = vec![u8::try_from(index % 251).unwrap(); CONTENT_SIZE];
    content[..8].copy_from_slice(&index.to_be_bytes());
    Bytes::from(content)
}

// commit_content commits key naming content, which the transaction's work puts, as a file system's write does.
fn commit_content(metadata: &SqliteMetadata<ThreadPool>, key: Bytes, content: Bytes) {
    let stored = SqliteContent::of(metadata);
    block_on(metadata.transact(|mut transaction| {
        let (stored, key, content) = (&stored, key.clone(), content.clone());
        Box::pin(async move {
            let hash = stored.put(content).await.unwrap();
            transaction.set(key, &hash.0[..]).unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
}

fn make_durable(metadata: &SqliteMetadata<ThreadPool>, against: DurableAgainst) {
    block_on(metadata.ensure_durable(against)).unwrap();
}

fn set(store: &SqliteMetadata<ThreadPool>, key: Bytes, value: Bytes) {
    block_on(store.transact(|mut transaction| {
        let (key, value) = (key.clone(), value.clone());
        Box::pin(async move {
            transaction.set(key, value).unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
}

fn increment(store: &SqliteMetadata<ThreadPool>, key: Bytes) {
    block_on(store.transact(|mut transaction| {
        let key = key.clone();
        Box::pin(async move {
            let count = match transaction.get(key.clone()).await.unwrap() {
                Some(count) => u64::from_be_bytes(count[..].try_into().unwrap()),
                None => 0,
            };
            transaction
                .set(key, &(count + 1).to_be_bytes()[..])
                .unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
}

fn get(store: &SqliteMetadata<ThreadPool>, key: Bytes) -> Option<Bytes> {
    block_on(store.read_transact(|transaction| {
        let key = key.clone();
        Box::pin(async move { Ok::<_, Infallible>(transaction.get(key).await.unwrap()) })
    }))
    .unwrap()
}
