// The stratafs FUSE daemon: makes volumes in a store directory, and mounts one, serving it until it is unmounted.

#[cfg(target_os = "linux")]
fn main() -> std::process::ExitCode {
    use std::process::ExitCode;
    use std::sync::Arc;

    use bytes::Bytes;
    use clap::{Parser, Subcommand};
    use futures_executor::ThreadPool;
    use nix::sys::signal::{SigSet, Signal};
    use stratafs_fuse::error_chain;
    use stratafs_fuse::stores::{self, CreateArguments, MountArguments};
    use stratafs_runtime::ThreadPoolRuntime;

    #[derive(Parser)]
    #[command(about = "Make stratafs volumes, and mount them with FUSE")]
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
        #[command(
            about = "Mount a volume, and serve it until it is unmounted, or SIGINT or SIGTERM unmounts it"
        )]
        Mount(MountArguments),
    }

    // SIGINT and SIGTERM unmount the volume. Every thread started from here on inherits them blocked, so only the
    // thread waiting for them takes them.
    SigSet::from_iter([Signal::SIGINT, Signal::SIGTERM])
        .thread_block()
        .expect("blocking signals takes valid signals");
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let failed = match Command::parse().action {
        Action::Create(arguments) => match stores::create(&arguments) {
            Ok(volume) => {
                println!("{}", hex::encode(Bytes::from(volume)));
                return ExitCode::SUCCESS;
            }
            Err(error) => error_chain(&error),
        },
        Action::Mount(arguments) => {
            // Operations that have to wait, such as while commits are made durable, finish on the pool's threads.
            let pool = ThreadPool::new().expect("the operating system starts the pool's threads");
            match stores::mount(&arguments, Arc::new(ThreadPoolRuntime(pool))) {
                Ok(()) => return ExitCode::SUCCESS,
                Err(error) => error_chain(&error),
            }
        }
    };
    eprintln!("{failed}");
    ExitCode::FAILURE
}

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("the stratafs FUSE daemon only runs on Linux");
    std::process::ExitCode::FAILURE
}
