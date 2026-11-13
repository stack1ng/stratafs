use std::path::{Path, PathBuf};
use std::process::Command;

use crate::suite::config::TargetConfig;
use crate::suite::{SuiteError, execute, make_directory, quoted, shell};

const PURGE: &str = "sudo -n /usr/sbin/purge";

// Target is a file system ready to measure: the directory its workloads run in, and how it mounts.
pub struct Target {
    pub name: String,
    pub directory: PathBuf,
    mounting: Option<Mounting>,
}

// Mounting is how a file system mounts and unmounts, each as a shell command.
struct Mounting {
    mount: String,
    unmount: String,
}

impl Target {
    // clear_caches_command is the shell command that clears the target's caches: it unmounts the file system,
    // purges the operating system's caches, and mounts it again. It is a command because hyperfine runs it
    // before each timed run.
    pub fn clear_caches_command(&self) -> String {
        match &self.mounting {
            Some(Mounting { mount, unmount }) => format!("{unmount} && {PURGE} && {mount}"),
            None => PURGE.to_owned(),
        }
    }

    pub fn clear_caches(&self) -> Result<(), SuiteError> {
        shell(&self.clear_caches_command())
    }

    pub fn tear_down(self) -> Result<(), SuiteError> {
        match &self.mounting {
            Some(mounting) => shell(&mounting.unmount),
            None => Ok(()),
        }
    }
}

pub fn set_up(config: &TargetConfig) -> Result<Target, SuiteError> {
    let name = config.name().to_owned();
    match config {
        TargetConfig::Directory { path, .. } => {
            make_directory(path)?;
            Ok(Target {
                name,
                directory: path.clone(),
                mounting: None,
            })
        }
        TargetConfig::Stratafs {
            store,
            mount_point,
            database,
            consistency,
            signed,
            ..
        } => mounted(
            name,
            mount_point,
            stratafs(store, mount_point, database, consistency, *signed)?,
        ),
        TargetConfig::Juicefs {
            binary,
            store,
            mount_point,
            ..
        } => mounted(name, mount_point, juicefs(binary, store, mount_point)?),
    }
}

// mounted mounts a file system on mount_point, and is the target that measures it there.
fn mounted(name: String, mount_point: &Path, mounting: Mounting) -> Result<Target, SuiteError> {
    make_directory(mount_point)?;
    shell(&mounting.mount)?;
    Ok(Target {
        name,
        directory: mount_point.to_owned(),
        mounting: Some(mounting),
    })
}

// stratafs makes a fresh volume in the store of database in the store directory, mounted with the installed FSKit
// module and consistency. A signed volume gets a key of its own, which lasts as long as the run.
#[cfg(target_os = "macos")]
fn stratafs(
    store: &Path,
    mount_point: &Path,
    database: &str,
    consistency: &str,
    signed: bool,
) -> Result<Mounting, SuiteError> {
    use clap::ValueEnum;
    use futures_lite::future::block_on;
    use stratafs_ed25519_signer::Ed25519Signer;
    use stratafs_local::mount_options::MountOptions;
    use stratafs_local::{ConsistencyKind, StoreKind};

    let named = |what: &str, error: String| {
        SuiteError::Config(format!("a stratafs target's {what}: {error}"))
    };
    let database =
        StoreKind::from_str(database, false).map_err(|error| named("database", error))?;
    let consistency = ConsistencyKind::from_str(consistency, false)
        .map_err(|error| named("consistency", error))?;
    let signing_key = signed.then(Ed25519Signer::generate);
    let volume = block_on(stratafs_local::create(store, database, signing_key.clone()))
        .map_err(|error| SuiteError::Config(format!("creating a stratafs volume: {error}")))?;
    let coordinator = coordinator_url()?
        .parse()
        .map_err(|error: url::ParseError| {
            SuiteError::Config(format!("the coordinator's URL: {error}"))
        })?;
    let options = MountOptions {
        store: database,
        volume,
        consistency,
        coordinator,
        signing_key,
    };
    Ok(Mounting {
        mount: format!(
            "/sbin/mount -F -t stratafs -o {options} {} {}",
            quoted(store),
            quoted(mount_point)
        ),
        unmount: format!("/sbin/umount {}", quoted(mount_point)),
    })
}

// coordinator_url is the URL of the coordinator the run's stratafs mounts share. The first call starts it, in this
// process, where it serves until the run ends.
#[cfg(target_os = "macos")]
fn coordinator_url() -> Result<&'static str, SuiteError> {
    use std::sync::OnceLock;

    static URL: OnceLock<String> = OnceLock::new();
    if let Some(url) = URL.get() {
        return Ok(url);
    }
    let failed = |source| SuiteError::Io {
        action: "starting the coordinator".to_owned(),
        source,
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(failed)?;
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .map_err(failed)?;
    let url = format!("ws://{}", listener.local_addr().map_err(failed)?);
    runtime.spawn(stratafs_websocket_pubsub_native::serve(listener));
    // The runtime is never dropped, so the coordinator keeps serving.
    std::mem::forget(runtime);
    Ok(URL.get_or_init(|| url))
}

#[cfg(not(target_os = "macos"))]
fn stratafs(
    _store: &Path,
    _mount_point: &Path,
    _database: &str,
    _consistency: &str,
    _signed: bool,
) -> Result<Mounting, SuiteError> {
    Err(SuiteError::Config(
        "stratafs targets only mount on macOS so far".to_owned(),
    ))
}

// juicefs is a JuiceFS volume kept in the store directory, formatted the first time, with SQLite metadata and its
// blobs in the same directory. macFUSE's FSKit backend, which needs no kernel extension, mounts it.
fn juicefs(binary: &Path, store: &Path, mount_point: &Path) -> Result<Mounting, SuiteError> {
    make_directory(store)?;
    let metadata = format!("sqlite3://{}", store.join("metadata.db").display());
    if !store.join("metadata.db").exists() {
        execute(
            Command::new(binary)
                .args(["format", "--storage", "file", "--bucket"])
                .arg(store.join("blobs"))
                .arg(&metadata)
                .arg("stratafs-bench"),
        )?;
    }
    Ok(Mounting {
        mount: format!(
            "{} mount --background -o backend=fskit {metadata} {}",
            quoted(binary),
            quoted(mount_point)
        ),
        unmount: format!("{} umount {}", quoted(binary), quoted(mount_point)),
    })
}
