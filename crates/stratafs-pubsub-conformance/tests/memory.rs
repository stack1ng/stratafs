use std::sync::Arc;

use stratafs_pubsub_conformance::{MemoryVolume, TokioRuntime};

stratafs_pubsub_conformance::pubsub_conformance!(std::future::ready(MemoryVolume::<u64>::new(
    Arc::new(TokioRuntime::current())
)));
