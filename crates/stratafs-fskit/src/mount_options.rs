use std::fmt::{self, Display};
use std::num::{NonZeroU64, ParseIntError};
use std::str::FromStr;
use std::time::Duration;

use bytes::Bytes;

const VOLUME: &str = "volume";
const DURABILITY_CADENCE: &str = "durability_cadence";

// MountOptions are what mount passes the extension with -o, written as mount(8) takes options: name=value
// pairs, separated by commas. The volume id is written in hex. As with mount(8), a later option overrides an
// earlier one of the same name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountOptions<V> {
    pub volume: V,
    // durability_cadence is how often the volume makes every committed operation durable, written in whole
    // seconds. FSKIT doesn't provide any precise signal for when applications need fsync, so either
    // we accept their shitty overly coarse signal and the significant performance hit that comes with it,
    // this is the best durability system we can possibly get.
    pub durability_cadence: Option<Duration>,
}

#[derive(Debug, thiserror::Error)]
pub enum MountOptionsError<E> {
    #[error("no volume was given: mount with -o {VOLUME}=<volume id in hex>")]
    NoVolume,
    #[error("{0} is not a mount option stratafs takes")]
    Unknown(String),
    #[error("the volume id is not hex")]
    NotHex(#[source] hex::FromHexError),
    #[error("the volume id does not name a volume")]
    NotAVolume(#[source] E),
    #[error("the {DURABILITY_CADENCE} is not a whole number of seconds above zero")]
    NotSeconds(#[source] ParseIntError),
}

impl<V: Clone + Into<Bytes>> Display for MountOptions<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id: Bytes = self.volume.clone().into();
        write!(formatter, "{VOLUME}={}", hex::encode(id))?;
        match self.durability_cadence {
            Some(cadence) => write!(formatter, ",{DURABILITY_CADENCE}={}", cadence.as_secs()),
            None => Ok(()),
        }
    }
}

impl<V: TryFrom<Bytes>> FromStr for MountOptions<V> {
    type Err = MountOptionsError<V::Error>;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let mut volume = None;
        let mut durability_cadence = None;
        for option in options.split(',').filter(|option| !option.is_empty()) {
            match option.split_once('=') {
                Some((VOLUME, id)) => volume = Some(id),
                Some((DURABILITY_CADENCE, seconds)) => durability_cadence = Some(seconds),
                _ => return Err(MountOptionsError::Unknown(option.to_owned())),
            }
        }
        let durability_cadence = durability_cadence
            .map(|seconds| seconds.parse::<NonZeroU64>())
            .transpose()
            .map_err(MountOptionsError::NotSeconds)?
            .map(|seconds| Duration::from_secs(seconds.get()));
        let id = hex::decode(volume.ok_or(MountOptionsError::NoVolume)?)
            .map_err(MountOptionsError::NotHex)?;
        let volume = V::try_from(Bytes::from(id)).map_err(MountOptionsError::NotAVolume)?;
        Ok(MountOptions {
            volume,
            durability_cadence,
        })
    }
}

#[cfg(test)]
mod tests;
