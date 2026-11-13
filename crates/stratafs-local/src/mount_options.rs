// mount_options are the options the macOS module takes from mount's -o.

use std::fmt::{self, Display};
use std::str::FromStr;

use bytes::Bytes;
use clap::ValueEnum;
use stratafs_ed25519_signer::{Ed25519Signer, NotAKey};
use stratafs_tree::header::TreeVolumeId;
use url::Url;

use crate::{ConsistencyKind, StoreKind};

const STORE: &str = "store";
const VOLUME: &str = "volume";
const CONSISTENCY: &str = "consistency";
const COORDINATOR: &str = "coordinator";
const SIGNING_KEY: &str = "signing-key";

// MountOptions are what mount passes the module with -o, written as mount(8) takes options: name=value pairs,
// separated by commas. The volume id is written in hex, the coordinator as its WebSocket URL, and the signing key as
// the base64 of its PKCS#8 DER form. As with mount(8), a later option overrides an earlier one of the same name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountOptions {
    pub store: StoreKind,
    pub volume: TreeVolumeId,
    pub consistency: ConsistencyKind,
    // coordinator is the WebSocket URL of the coordinator the volume's mounts share.
    pub coordinator: Url,
    // signing_key is the key the volume is signed with, for a signed volume.
    pub signing_key: Option<Ed25519Signer>,
}

#[derive(Debug, thiserror::Error)]
pub enum MountOptionsError {
    #[error("{0} is not a mount option stratafs takes")]
    Unknown(String),
    #[error("no store was given: mount with -o {STORE}=mdbx, {STORE}=redb or {STORE}=sqlite")]
    NoStore,
    #[error("the {STORE} is not a store: {0}")]
    NotAStore(String),
    #[error("no volume was given: mount with -o {VOLUME}=<volume id in hex>")]
    NoVolume,
    #[error("the {VOLUME} is not a volume: {0}")]
    NotAVolume(String),
    #[error(
        "no consistency was given: mount with -o {CONSISTENCY}=strong or {CONSISTENCY}=eventual"
    )]
    NoConsistency,
    #[error("the {CONSISTENCY} is not a consistency: {0}")]
    NotAConsistency(String),
    #[error("no coordinator was given: mount with -o {COORDINATOR}=<its WebSocket URL>")]
    NoCoordinator,
    #[error("the {COORDINATOR} is not a URL")]
    NotAUrl(#[source] url::ParseError),
    #[error("the {SIGNING_KEY} is not a key")]
    NotAKey(#[source] NotAKey),
}

impl Display for MountOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{STORE}={}", name_of(&self.store))?;
        let volume = hex::encode(Bytes::from(self.volume));
        write!(formatter, ",{VOLUME}={volume}")?;
        write!(formatter, ",{CONSISTENCY}={}", name_of(&self.consistency))?;
        write!(formatter, ",{COORDINATOR}={}", self.coordinator)?;
        match &self.signing_key {
            Some(signer) => write!(formatter, ",{SIGNING_KEY}={}", signer.key()),
            None => Ok(()),
        }
    }
}

impl FromStr for MountOptions {
    type Err = MountOptionsError;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let (mut store, mut volume, mut consistency) = (None, None, None);
        let (mut coordinator, mut signing_key) = (None, None);
        for option in options.split(',').filter(|option| !option.is_empty()) {
            match option.split_once('=') {
                Some((STORE, kind)) => store = Some(kind),
                Some((VOLUME, id)) => volume = Some(id),
                Some((CONSISTENCY, kind)) => consistency = Some(kind),
                Some((COORDINATOR, url)) => coordinator = Some(url),
                Some((SIGNING_KEY, key)) => signing_key = Some(key),
                _ => return Err(MountOptionsError::Unknown(option.to_owned())),
            }
        }
        let store = StoreKind::from_str(store.ok_or(MountOptionsError::NoStore)?, false)
            .map_err(MountOptionsError::NotAStore)?;
        let volume = crate::volume_id(volume.ok_or(MountOptionsError::NoVolume)?)
            .map_err(MountOptionsError::NotAVolume)?;
        let consistency = consistency.ok_or(MountOptionsError::NoConsistency)?;
        let consistency = ConsistencyKind::from_str(consistency, false)
            .map_err(MountOptionsError::NotAConsistency)?;
        let coordinator = coordinator.ok_or(MountOptionsError::NoCoordinator)?;
        let coordinator = Url::parse(coordinator).map_err(MountOptionsError::NotAUrl)?;
        let signing_key = signing_key
            .map(Ed25519Signer::from_str)
            .transpose()
            .map_err(MountOptionsError::NotAKey)?;
        Ok(MountOptions {
            store,
            volume,
            consistency,
            coordinator,
            signing_key,
        })
    }
}

// name_of is a store's or a consistency's name as the command line takes it.
fn name_of(kind: &impl ValueEnum) -> String {
    let named = kind.to_possible_value().expect("no kind is skipped");
    named.get_name().to_owned()
}

#[cfg(test)]
mod tests;
