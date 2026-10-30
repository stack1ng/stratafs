// The native coordinator server: stratafs-websocket-pubsub-native --listen 127.0.0.1:7600

use clap::Parser;
use tokio::net::TcpListener;

#[derive(Parser)]
struct Arguments {
    #[arg(long, help = "The address to accept WebSocket connections on")]
    listen: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let arguments = Arguments::parse();
    let listener = TcpListener::bind(&arguments.listen).await?;
    stratafs_websocket_pubsub_native::serve(listener).await;
    Ok(())
}
