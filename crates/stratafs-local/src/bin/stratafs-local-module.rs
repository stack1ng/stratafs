// The FSKit module the stratafs-local bundle installs. FSKit starts it once per mount.

// The module allocates with mimalloc, which keeps freed memory on each thread to reuse.
#[cfg(target_os = "macos")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(target_os = "macos")]
fn main() {
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    // The module logs from info up. The os_log layer formats every event it gets, whatever the system then keeps, and
    // every missing name or extended attribute is a debug event.
    tracing_subscriber::registry()
        .with(LevelFilter::INFO)
        .with(tracing_oslog::OsLogger::new(
            "dev.stratafs.bench",
            "extension",
        ))
        .init();
    // FSKit discards the module's standard error, so a panic is logged where it can be read. A panic is a bug
    // nothing recovers from, so the module then stops at once, wherever the panic happened.
    std::panic::set_hook(Box::new(|panic| {
        tracing::error!(%panic, "panicked");
        std::process::abort();
    }));
    stratafs_fskit::extension::run(macos::open)
}

#[cfg(target_os = "macos")]
mod macos {
    use std::io;
    use std::sync::Arc;

    use bytes::Bytes;
    use clap::Parser;
    use futures_executor::ThreadPool;
    use stratafs_fs_engine::error::PosixError;
    use stratafs_fs_engine::{Cached, FsConfig};
    use stratafs_fskit::extension::{Opened, Request};
    use stratafs_local::mount_options::{MountOptions, MountOptionsError};
    use stratafs_local::{DURABILITY_LIMIT, Integration, LocalSpecification, Mount, OpenError};
    use stratafs_runtime::ThreadPoolRuntime;
    use stratafs_store::store::Store;
    use stratafs_tree::header::TreeVolumeId;
    use stratafs_websocket_pubsub::client::Client;

    // ActivateArguments are the arguments FSKit hands activate: each -o mount was given.
    #[derive(Parser)]
    #[command(no_binary_name = true)]
    struct ActivateArguments {
        #[arg(short = 'o')]
        options: Vec<String>,
    }

    #[derive(Debug, thiserror::Error)]
    pub enum ModuleError {
        #[error("the mount options could not be read: {0}")]
        UnreadableOptions(#[source] clap::Error),
        #[error(transparent)]
        InvalidOptions(MountOptionsError),
        #[error("the pool the volume's work runs on could not start")]
        Pool(#[source] io::Error),
        #[error(transparent)]
        Open(OpenError),
    }

    impl From<&ModuleError> for PosixError {
        fn from(error: &ModuleError) -> Self {
            match error {
                ModuleError::UnreadableOptions(_) | ModuleError::InvalidOptions(_) => {
                    PosixError::EINVAL
                }
                ModuleError::Pool(_) => PosixError::EIO,
                ModuleError::Open(error) => PosixError::from(error),
            }
        }
    }

    // FsKit hands FSKit a specification, named after its volume.
    struct FsKit(TreeVolumeId);

    impl Integration for FsKit {
        type Mounted = Opened;

        fn mount<St, C, S>(self, specification: LocalSpecification<St, C, S>) -> Opened
        where
            St: Store,
            C: Cached<St, Client<u64>>,
            S: FsConfig,
        {
            Opened::new(Bytes::from(self.0), specification)
        }
    }

    // open reads the store, the volume, the consistency, the coordinator and the signing key from the request's -o
    // options, and opens the store in the directory mount was given.
    pub async fn open(request: Request) -> Result<Opened, ModuleError> {
        let arguments = ActivateArguments::try_parse_from(&request.arguments)
            .map_err(ModuleError::UnreadableOptions)?;
        let options: MountOptions = arguments
            .options
            .join(",")
            .parse()
            .map_err(ModuleError::InvalidOptions)?;
        // Operations that have to wait, such as while commits are made durable, finish on the pool's threads.
        let pool = ThreadPool::new().map_err(ModuleError::Pool)?;
        let mount = Mount {
            directory: &request.resource,
            database: options.store,
            volume: options.volume,
            consistency: options.consistency,
            coordinator: &options.coordinator,
            signer: options.signing_key,
            durability_limit: DURABILITY_LIMIT,
            runtime: Arc::new(ThreadPoolRuntime(pool)),
        };
        stratafs_local::mount(mount, FsKit(options.volume))
            .await
            .map_err(ModuleError::Open)
    }
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("the stratafs FSKit module only runs on macOS");
    std::process::exit(1);
}
