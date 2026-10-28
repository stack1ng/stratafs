use std::convert::Infallible;
use std::env;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use futures_lite::future::block_on;
use mdbx_sys as ffi;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::kv::{KeyValueStore, KeyValueTransaction};
use stratafs_store::spawn::Inline;

use crate::{MdbxConfiguration, MdbxGeometry, MdbxMetadata, MdbxOpenError};

// CONFIGURATION opens a test store in 4 KiB pages, committing without syncs, with libmdbx making no durable point of
// its own, so only ensure_durable syncs.
const CONFIGURATION: MdbxConfiguration<'static> = MdbxConfiguration {
    // libmdbx sizes how many changed pages a write transaction keeps in memory from the machine's memory, which it
    // fails to read on macOS 27.
    options: &[(ffi::MDBX_opt_txn_dp_limit, (1 << 30) / 4096)],
    geometry: MdbxGeometry {
        size_lower: -1,
        size_now: -1,
        size_upper: 1 << 34,
        growth_step: 16 << 20,
        shrink_threshold: 0,
        page_size: 4096,
    },
    flags: ffi::MDBX_NOSUBDIR | ffi::MDBX_SAFE_NOSYNC,
    mode: 0o644,
    shared_options: &[
        (ffi::MDBX_opt_sync_bytes, 1 << 34),
        (ffi::MDBX_opt_sync_period, 0),
    ],
};

fn open(path: impl AsRef<std::path::Path>) -> MdbxMetadata<Inline> {
    MdbxMetadata::open(path, &CONFIGURATION, Inline).unwrap()
}

fn commit(store: &MdbxMetadata<Inline>, key: u64, value: &Bytes) {
    block_on(store.transact(|mut transaction| {
        Box::pin(async move {
            transaction
                .set(Bytes::copy_from_slice(&key.to_be_bytes()), value.clone())
                .unwrap();
            Ok::<_, Infallible>(((), transaction))
        })
    }))
    .unwrap();
}

#[test]
fn libmdbx_makes_no_durable_point_of_its_own_when_told_not_to() {
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path().join("store.mdbx"));
    let before = store.0.environment.syncs().unwrap();
    // Rewriting the same entries makes page reuse wait on the last durable point, and the file grow several
    // times, which are when libmdbx would make a durable point itself.
    for round in 0..300_u32 {
        let value = Bytes::from(vec![round as u8; 16 * 1024]);
        let value = &value;
        block_on(store.transact(|mut transaction| {
            Box::pin(async move {
                for entry in 0..20_u32 {
                    let key = (round * 20 + entry) % 400;
                    transaction
                        .set(Bytes::copy_from_slice(&key.to_be_bytes()), value.clone())
                        .unwrap();
                }
                Ok::<_, Infallible>(((), transaction))
            })
        }))
        .unwrap();
    }
    assert_eq!(store.0.environment.syncs().unwrap(), before);
    block_on(store.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
    assert!(store.0.environment.syncs().unwrap() > before);
}

#[test]
fn shared_options_reach_libmdbx_once_the_file_is_open() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = MdbxConfiguration {
        shared_options: &[(ffi::MDBX_opt_sync_bytes, 64 * 1024)],
        ..CONFIGURATION
    };
    let store =
        MdbxMetadata::open(directory.path().join("store.mdbx"), &configuration, Inline).unwrap();
    let before = store.0.environment.syncs().unwrap();
    for key in 0..8 {
        commit(&store, key, &Bytes::from(vec![0; 16 * 1024]));
    }
    assert!(store.0.environment.syncs().unwrap() > before);
}

#[test]
fn ensure_durable_leaves_the_newest_commit_durable() {
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path().join("store.mdbx"));
    for (key, against) in [
        DurableAgainst::SoftwareFailure,
        DurableAgainst::PowerFailure,
    ]
    .into_iter()
    .enumerate()
    {
        commit(&store, key as u64, &Bytes::from_static(b"value"));
        assert!(!store.0.environment.is_steady().unwrap());
        block_on(store.ensure_durable(against)).unwrap();
        assert!(store.0.environment.is_steady().unwrap(), "{against:?}");
    }
}

// A child process runs this test binary again, running only another_process_commits, with its store's path in
// this variable.
const STORE: &str = "STRATAFS_MDBX_TEST_STORE";

#[test]
#[ignore = "runs only as a child process of ensure_durable_makes_another_processs_commits_durable"]
fn another_process_commits() {
    let store = open(env::var_os(STORE).unwrap());
    commit(&store, 0, &Bytes::from_static(b"value"));
    println!("committed");
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn ensure_durable_makes_another_processs_commits_durable() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("store.mdbx");
    let store = open(&path);
    block_on(store.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
    let mut child = Spawned(
        Command::new(env::current_exe().unwrap())
            .args([
                "tests::another_process_commits",
                "--exact",
                "--ignored",
                "--nocapture",
                "--quiet",
            ])
            .env(STORE, &path)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let mut output = BufReader::new(child.0.stdout.take().unwrap()).lines();
    assert!(output.any(|line| line.unwrap() == "committed"));
    assert!(!store.0.environment.is_steady().unwrap());
    block_on(store.ensure_durable(DurableAgainst::SoftwareFailure)).unwrap();
    assert!(store.0.environment.is_steady().unwrap());
}

// Spawned is a child process, killed if it is still running when dropped.
struct Spawned(Child);

impl Drop for Spawned {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn a_file_with_pages_too_small_for_the_largest_entry_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    let configuration = MdbxConfiguration {
        geometry: MdbxGeometry {
            page_size: 2048,
            ..CONFIGURATION.geometry
        },
        ..CONFIGURATION
    };
    assert!(matches!(
        MdbxMetadata::open(directory.path().join("store.mdbx"), &configuration, Inline),
        Err(MdbxOpenError::PagesTooSmall)
    ));
    // libmdbx keeps the page size the file was made with, whatever a later opening asks for.
    assert!(matches!(
        MdbxMetadata::open(directory.path().join("store.mdbx"), &CONFIGURATION, Inline),
        Err(MdbxOpenError::PagesTooSmall)
    ));
}
