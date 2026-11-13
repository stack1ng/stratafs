// stratafs-local makes volumes in a store directory on this machine and, on Linux, mounts one with FUSE, serving it
// until it is unmounted.

use std::path::PathBuf;
use std::process::ExitCode;

use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use futures_lite::future::block_on;
use stratafs_ed25519_signer::Ed25519Signer;
use stratafs_local::{StoreKind, error_chain};

#[derive(Parser)]
#[command(about = "Make stratafs volumes in a store on this machine, and mount them")]
struct Command {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    #[command(
        about = "Make a new, empty volume in a store directory, making the store first if it has none, and print the volume's id. Nothing may have the store mounted."
    )]
    Create(CreateArguments),
    #[cfg(target_os = "linux")]
    #[command(
        about = "Mount a volume, and serve it until it is unmounted, or SIGINT or SIGTERM unmounts it"
    )]
    Mount(linux::MountArguments),
}

#[derive(Args)]
struct CreateArguments {
    #[arg(help = "The store directory")]
    store: PathBuf,
    #[arg(long = "store", value_enum, help = "The store's database")]
    database: StoreKind,
    #[arg(
        long,
        help = "The key the volume is signed with: an Ed25519 private key in PKCS#8 DER form, in base64. The volume is not signed when it is left out"
    )]
    signing_key: Option<Ed25519Signer>,
}

fn main() -> ExitCode {
    let failed = match Command::parse().action {
        Action::Create(arguments) => {
            let created =
                stratafs_local::create(&arguments.store, arguments.database, arguments.signing_key);
            match block_on(created) {
                Ok(volume) => {
                    println!("{}", hex::encode(Bytes::from(volume)));
                    return ExitCode::SUCCESS;
                }
                Err(error) => error_chain(&error),
            }
        }
        #[cfg(target_os = "linux")]
        Action::Mount(arguments) => match linux::mount(arguments) {
            Ok(()) => return ExitCode::SUCCESS,
            Err(error) => error_chain(&*error),
        },
    };
    eprintln!("{failed}");
    ExitCode::FAILURE
}

#[cfg(target_os = "linux")]
mod linux {
    use std::error::Error;
    use std::num::NonZeroU64;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;

    use clap::{Args, ValueEnum};
    use futures_executor::ThreadPool;
    use futures_lite::future::block_on;
    use nix::sys::signal::{SigSet, Signal};
    use stratafs_ed25519_signer::Ed25519Signer;
    use stratafs_fs_engine::{Cached, FsConfig};
    use stratafs_fuse::MountError;
    use stratafs_local::{
        ConsistencyKind, Integration, LocalSpecification, Mount, StoreKind, volume_id,
    };
    use stratafs_runtime::ThreadPoolRuntime;
    use stratafs_store::durability::DurableAgainst;
    use stratafs_store::store::Store;
    use stratafs_tree::header::TreeVolumeId;
    use stratafs_websocket_pubsub::client::Client;
    use url::Url;

    // DurabilityLimit is the most the volume makes commits durable against.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
    enum DurabilityLimit {
        None,
        Software,
        Power,
    }

    #[derive(Args)]
    pub struct MountArguments {
        #[arg(help = "The store directory")]
        store: PathBuf,
        #[arg(help = "The directory to mount the volume on")]
        mount_point: PathBuf,
        #[arg(long, value_parser = volume_id, help = "The volume's id, in hex, as create prints it")]
        volume: TreeVolumeId,
        #[arg(long = "store", value_enum, help = "The store's database")]
        database: StoreKind,
        #[arg(
            long,
            value_enum,
            help = "What the mount's reads promise about other mounts' writes"
        )]
        consistency: ConsistencyKind,
        #[arg(
            long,
            help = "The WebSocket URL of the coordinator the mounts of the volume share, such as ws://127.0.0.1:7600"
        )]
        coordinator: Url,
        #[arg(
            long,
            help = "The key the volume was created with, for a signed volume: an Ed25519 private key in PKCS#8 DER form, in base64"
        )]
        signing_key: Option<Ed25519Signer>,
        #[arg(
            long,
            value_enum,
            default_value = "power",
            help = "The most fsync, the durability cadence and unmounting make commits durable against"
        )]
        durability_limit: DurabilityLimit,
        #[arg(
            long,
            value_parser = seconds,
            default_value = "30",
            help = "How often every committed operation becomes durable without an fsync, in whole seconds"
        )]
        durability_cadence: Duration,
        #[arg(
            long,
            help = "Let every user reach the mount, not only the one who mounted it. A mount by a user other than root needs user_allow_other in /etc/fuse.conf"
        )]
        allow_other: bool,
    }

    // Fuse mounts a specification with FUSE.
    struct Fuse {
        mount_point: PathBuf,
        durability_cadence: Duration,
        allow_other: bool,
    }

    impl Integration for Fuse {
        type Mounted = Result<(), MountError>;

        fn mount<St, C, S>(self, specification: LocalSpecification<St, C, S>) -> Self::Mounted
        where
            St: Store,
            C: Cached<St, Client<u64>>,
            S: FsConfig,
        {
            stratafs_fuse::mount(
                specification,
                &self.mount_point,
                self.durability_cadence,
                self.allow_other,
            )
        }
    }

    // mount serves the volume arguments name until it is unmounted.
    pub fn mount(arguments: MountArguments) -> Result<(), Box<dyn Error>> {
        // SIGINT and SIGTERM unmount the volume. Every thread started from here on inherits them blocked, so only the
        // thread waiting for them takes them.
        SigSet::from_iter([Signal::SIGINT, Signal::SIGTERM]).thread_block()?;
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
        // Operations that have to wait, such as while commits are made durable, finish on the pool's threads.
        let pool = ThreadPool::new()?;
        let mount = Mount {
            directory: &arguments.store,
            database: arguments.database,
            volume: arguments.volume,
            consistency: arguments.consistency,
            coordinator: &arguments.coordinator,
            signer: arguments.signing_key,
            durability_limit: match arguments.durability_limit {
                DurabilityLimit::None => None,
                DurabilityLimit::Software => Some(DurableAgainst::SoftwareFailure),
                DurabilityLimit::Power => Some(DurableAgainst::PowerFailure),
            },
            runtime: Arc::new(ThreadPoolRuntime(pool)),
        };
        let fuse = Fuse {
            mount_point: arguments.mount_point,
            durability_cadence: arguments.durability_cadence,
            allow_other: arguments.allow_other,
        };
        Ok(block_on(stratafs_local::mount(mount, fuse))??)
    }

    // seconds reads a whole number of seconds above zero.
    fn seconds(seconds: &str) -> Result<Duration, String> {
        let seconds: NonZeroU64 = seconds
            .parse()
            .map_err(|error| format!("not a whole number of seconds above zero: {error}"))?;
        Ok(Duration::from_secs(seconds.get()))
    }
}
