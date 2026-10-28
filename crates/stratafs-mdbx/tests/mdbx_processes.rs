mod configuration;

use std::convert::Infallible;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{self, Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use futures_lite::future::block_on;
use stratafs_mdbx::{MdbxMetadata, MdbxOpenError};
use stratafs_store::durability::DurableAgainst;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction, ReadKeyValueTransaction};
use stratafs_store::spawn::{Inline, ThreadPool};

// A child process runs this test binary again, running only child, with its role and its store's path in
// these variables.
const ROLE: &str = "STRATAFS_MDBX_TEST_ROLE";
const STORE: &str = "STRATAFS_MDBX_TEST_STORE";

const NEXT: Bytes = Bytes::from_static(b"next");
const COUNT: Bytes = Bytes::from_static(b"count");
const GO: Bytes = Bytes::from_static(b"go");
const COUNTS_PER_PROCESS: u32 = 400;

#[test]
#[ignore = "runs only as a child process of the other tests in this file"]
fn child() {
    let store = configuration::open(env::var_os(STORE).unwrap(), ThreadPool).unwrap();
    match env::var(ROLE).as_deref() {
        Ok("commit until killed") => commit_until_killed(&store),
        Ok("count") => {
            println!("ready");
            while read(&store, GO).is_none() {
                thread::sleep(Duration::from_micros(100));
            }
            count(&store);
        }
        Ok("commit forever") => {
            for number in 0..=u64::MAX {
                block_on(store.transact(|mut transaction| {
                    Box::pin(async move {
                        transaction.set(NEXT, numbered(number)).unwrap();
                        Ok::<_, Infallible>(((), transaction))
                    })
                }))
                .unwrap();
                if number == 0 {
                    println!("ready");
                }
            }
        }
        Ok("read forever") => thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    loop {
                        read(&store, NEXT);
                    }
                });
            }
            println!("ready");
        }),
        role => panic!("no child role {role:?}"),
    }
}

#[test]
fn every_commit_ensure_durable_acknowledged_survives_a_kill() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    for kill in 0..4 {
        let mut child = Spawned::new("commit until killed", &path, Stdio::piped());
        let output = BufReader::new(child.0.stdout.take().unwrap());
        let mut acknowledged = output
            .lines()
            .map(Result::unwrap)
            .filter_map(|line| line.strip_prefix("durable ")?.parse::<u64>().ok());
        let durable = acknowledged.nth(10 + 5 * kill).unwrap();
        // The kill lands at a different point of the child's commits each time.
        thread::sleep(Duration::from_micros(300 * kill as u64));
        drop(child);

        let store = configuration::open(&path, Inline).unwrap();
        let next = read(&store, NEXT).map_or(0, |next| number(&next));
        assert!(
            next >= durable,
            "{next} commits survived, but {durable} were made durable"
        );
        let entries = block_on(store.read_transact(|transaction| {
            Box::pin(async move {
                let entries = transaction.get_range(..NEXT, usize::MAX).await.unwrap();
                Ok::<_, Infallible>(entries.collect::<Vec<_>>())
            })
        }))
        .unwrap();
        assert_eq!(entries.len() as u64, next);
        for (expected, (key, value)) in (0..next).zip(entries) {
            assert_eq!(key, numbered(expected));
            assert_eq!(value, value_of(expected), "the value of commit {expected}");
        }
    }
}

#[test]
fn processes_counting_in_one_store_at_once_lose_no_count() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let store = configuration::open(&path, ThreadPool).unwrap();
    assert!(matches!(
        configuration::open(&path, ThreadPool),
        Err(MdbxOpenError::AlreadyOpen)
    ));
    let mut children: Vec<Spawned> = (0..2)
        .map(|_| Spawned::new("count", &path, Stdio::piped()))
        .collect();
    // Every process starts counting at once. The outputs stay open until the children end.
    let outputs: Vec<_> = children
        .iter_mut()
        .map(|child| {
            let mut output = BufReader::new(child.0.stdout.take().unwrap()).lines();
            assert!(output.any(|line| line.unwrap() == "ready"));
            output
        })
        .collect();
    block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction.set(GO, &b""[..]).unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
    count(&store);
    let mut counters = vec![process::id()];
    for mut child in children {
        counters.push(child.0.id());
        assert!(child.0.wait().unwrap().success());
    }
    drop(outputs);

    let total = read(&store, COUNT).map_or(0, |count| number(&count));
    assert_eq!(total, u64::from(COUNTS_PER_PROCESS) * counters.len() as u64);
    let mut interleaved = false;
    for counter in counters {
        let seen = block_on(store.read_transact(|transaction| {
            Box::pin(async move {
                let start = counted_key(counter, 0);
                let end = counted_key(counter, COUNTS_PER_PROCESS);
                let entries = transaction.get_range(start..end, usize::MAX).await.unwrap();
                Ok::<_, Infallible>(entries.map(|(_, seen)| number(&seen)).collect::<Vec<_>>())
            })
        }))
        .unwrap();
        assert_eq!(seen.len(), COUNTS_PER_PROCESS as usize, "process {counter}");
        interleaved |= seen.windows(2).any(|pair| pair[1] != pair[0] + 1);
    }
    // A process saw another's count between two of its own, so their commits contended.
    assert!(interleaved);
}

#[test]
fn a_process_killed_while_it_holds_a_lock_of_the_store_stalls_no_other() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let store = Arc::new(configuration::open(&path, ThreadPool).unwrap());
    for (kill, role) in ["commit forever", "read forever"]
        .into_iter()
        .cycle()
        .take(80)
        .enumerate()
    {
        let mut child = Spawned::new(role, &path, Stdio::piped());
        let mut output = BufReader::new(child.0.stdout.take().unwrap()).lines();
        assert!(output.any(|line| line.unwrap() == "ready"));
        // The kill lands at a different point of the child's transactions each time.
        thread::sleep(Duration::from_micros(200 + kill as u64 * 7919 % 3000));
        drop(child);

        let (done, finished) = mpsc::channel();
        let probe = Arc::clone(&store);
        thread::spawn(move || {
            read(&*probe, NEXT);
            block_on(probe.transact(|mut transaction| {
                Box::pin(async move {
                    transaction.set(COUNT, numbered(0)).unwrap();
                    Ok::<_, Infallible>(((), transaction))
                })
            }))
            .unwrap();
            done.send(()).unwrap();
        });
        assert!(
            finished.recv_timeout(Duration::from_secs(10)).is_ok(),
            "a {role:?} process killed on kill {kill} stalled this one"
        );
    }
}

// commit_until_killed commits numbered entries one at a time, keeping the next number under NEXT. After every
// eighth commit it makes them durable against a software failure, then reports the next number.
fn commit_until_killed(store: &MdbxMetadata<ThreadPool>) {
    loop {
        let next = block_on(store.transact(|mut transaction| {
            Box::pin(async move {
                let next = transaction
                    .get(NEXT)
                    .await
                    .unwrap()
                    .map_or(0, |next| number(&next));
                transaction.set(numbered(next), value_of(next)).unwrap();
                transaction.set(NEXT, numbered(next + 1)).unwrap();
                Ok::<_, Infallible>((next + 1, transaction))
            })
        }))
        .unwrap();
        if next % 8 == 0 {
            block_on(store.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
            println!("durable {next}");
        }
    }
}

// count adds one to the shared count, and keeps the count it saw for each count this process made.
fn count(store: &MdbxMetadata<ThreadPool>) {
    for counted in 0..COUNTS_PER_PROCESS {
        block_on(store.transact(|mut transaction| {
            Box::pin(async move {
                let count = transaction
                    .get(COUNT)
                    .await
                    .unwrap()
                    .map_or(0, |count| number(&count));
                transaction.set(COUNT, numbered(count + 1)).unwrap();
                transaction
                    .set(counted_key(process::id(), counted), numbered(count))
                    .unwrap();
                Ok::<_, Infallible>(((), transaction))
            })
        }))
        .unwrap();
    }
}

// Spawned is a child process running this test binary, killed if it is still running when dropped.
struct Spawned(Child);

impl Spawned {
    fn new(role: &str, store: &Path, stdout: Stdio) -> Self {
        Self(
            Command::new(env::current_exe().unwrap())
                .args(["child", "--exact", "--ignored", "--nocapture", "--quiet"])
                .env(ROLE, role)
                .env(STORE, store)
                .stdout(stdout)
                .spawn()
                .unwrap(),
        )
    }
}

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn read<S: KeyValueStore>(store: &S, key: Bytes) -> Option<Bytes> {
    block_on(store.read_transact(|transaction| {
        let key = key.clone();
        Box::pin(async move { Ok::<_, Infallible>(transaction.get(key).await.unwrap()) })
    }))
    .unwrap()
}

fn numbered(number: u64) -> Bytes {
    Bytes::copy_from_slice(&number.to_be_bytes())
}

fn number(stored: &[u8]) -> u64 {
    u64::from_be_bytes(stored.try_into().unwrap())
}

// value_of is the value committed under a number, as large as a tree node.
fn value_of(number: u64) -> Bytes {
    number.to_be_bytes().repeat(2 * 1024).into()
}

fn counted_key(process: u32, counted: u32) -> Bytes {
    [
        b"by".as_slice(),
        &process.to_be_bytes(),
        &counted.to_be_bytes(),
    ]
    .concat()
    .into()
}
