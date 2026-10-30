// stratafs-websocket-pubsub is an acknowledged pub/sub over WebSockets: a publish to a key resolves once every other
// connection holding that key has handled it. One coordinator serves one volume, and its keys are the volume's
// inodes. Hosts run the coordinator (natively, or in a Cloudflare Durable Object) and feed it each connection's
// frames; mounts connect with the client.

#[cfg(feature = "client")]
pub mod client;
pub mod coordinator;
pub mod protocol;
