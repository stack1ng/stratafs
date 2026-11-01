use stratafs_pubsub_conformance::MemoryVolume;

stratafs_pubsub_conformance::pubsub_conformance!(
    std::future::ready(MemoryVolume::<u64>::default())
);
