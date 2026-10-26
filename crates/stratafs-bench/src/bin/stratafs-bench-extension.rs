// The FSKit module the benchmark bundle installs. FSKit starts it once per mount.

#[cfg(target_os = "macos")]
fn main() {
    use futures_executor::ThreadPool;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    tracing_subscriber::registry()
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
    // Operations that have to wait, such as on a commit that flushes, finish on the pool's threads.
    let pool = ThreadPool::new().expect("the operating system starts the pool's threads");
    stratafs_fskit::extension::run(pool, stratafs_bench::open_volume)
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!("the stratafs FSKit module only runs on macOS");
    std::process::exit(1);
}
