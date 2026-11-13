// volume_creation makes new volumes, apart from every mount.

use std::error::Error;
use std::sync::Arc;

use futures_util::FutureExt;
use stratafs_runtime::Runtime;
use stratafs_store::durability::DurableAgainst;
use stratafs_store::forkable::ForkableKeyValueStore;
use stratafs_store::store::Store;

use crate::holds::NoCoordinator;
use crate::kernel_cache::NoKernel;
use crate::{EngineSpecification, Eventual, FsConfig, FsEngine};

#[derive(Debug, thiserror::Error)]
pub enum CreateVolumeError {
    #[error("the volume could not be created")]
    Create(#[source] Box<dyn Error + Send + Sync>),
    #[error("the new volume could not be opened")]
    Open(#[source] Box<dyn Error + Send + Sync>),
    #[error("the volume's root directory could not be made")]
    Root(#[source] Box<dyn Error + Send + Sync>),
    #[error("the volume could not be made durable")]
    Durable(#[source] Box<dyn Error + Send + Sync>),
}

// create_volume makes a new, empty volume of configuration S in store and returns its id, once the volume is durable.
// It makes the volume's root directory, which signer signs, so a mount finds it ready. No mount holds what it writes,
// so it reaches no coordinator. The volume's tasks run on runtime.
pub async fn create_volume<F, S>(
    store: &F,
    signer: S::Signer,
    runtime: Arc<dyn Runtime>,
) -> Result<F::VolumeId, CreateVolumeError>
where
    F: ForkableKeyValueStore,
    F::Volume: Store,
    S: FsConfig,
{
    let id = store
        .create()
        .await
        .map_err(|error| CreateVolumeError::Create(Box::new(error)))?;
    let volume = store
        .open(id.clone())
        .await
        .map_err(|error| CreateVolumeError::Open(Box::new(error)))?;
    let specification = EngineSpecification::<_, NoCoordinator, _, _, S, _>::with_defaults(
        volume,
        || async { Err("making a volume reaches no coordinator".into()) }.boxed(),
        Eventual::default(),
        signer,
        Some(DurableAgainst::PowerFailure),
        runtime,
    );
    // Opening the engine makes the root directory.
    let engine = FsEngine::new(specification, NoKernel)
        .await
        .map_err(|error| CreateVolumeError::Root(Box::new(error)))?;
    engine
        .ensure_durable(DurableAgainst::PowerFailure)
        .await
        .map_err(|error| CreateVolumeError::Durable(Box::new(error)))
        .map(|()| id)
}
