use std::io;

use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};

// Coordinator is the coordinator the volume's mounts share, running on a Tokio runtime of its own until it is dropped.
pub struct Coordinator {
    _runtime: Runtime,
    // url is the WebSocket URL a mount reaches the coordinator at.
    pub url: String,
}

impl Coordinator {
    pub fn start() -> io::Result<Coordinator> {
        let runtime = Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()?;
        let listener = runtime.block_on(TcpListener::bind("127.0.0.1:0"))?;
        let url = format!("ws://{}", listener.local_addr()?);
        runtime.spawn(stratafs_websocket_pubsub_native::serve(listener));
        Ok(Coordinator {
            _runtime: runtime,
            url,
        })
    }
}
