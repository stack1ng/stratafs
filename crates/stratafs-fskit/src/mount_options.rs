use std::fmt::{self, Display};
use std::str::FromStr;

use bytes::Bytes;

const VOLUME: &str = "volume";

// MountOptions are what mount passes the extension with -o, written as mount(8) takes options: name=value
// pairs, separated by commas. The volume id is written in hex. As with mount(8), a later option overrides an
// earlier one of the same name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountOptions<V> {
    pub volume: V,
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
}

impl<V: Clone + Into<Bytes>> Display for MountOptions<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id: Bytes = self.volume.clone().into();
        write!(formatter, "{VOLUME}={}", hex::encode(id))
    }
}

impl<V: TryFrom<Bytes>> FromStr for MountOptions<V> {
    type Err = MountOptionsError<V::Error>;

    fn from_str(options: &str) -> Result<Self, Self::Err> {
        let mut volume = None;
        for option in options.split(',').filter(|option| !option.is_empty()) {
            match option.split_once('=') {
                Some((VOLUME, id)) => volume = Some(id),
                _ => return Err(MountOptionsError::Unknown(option.to_owned())),
            }
        }
        let id = hex::decode(volume.ok_or(MountOptionsError::NoVolume)?)
            .map_err(MountOptionsError::NotHex)?;
        let volume = V::try_from(Bytes::from(id)).map_err(MountOptionsError::NotAVolume)?;
        Ok(MountOptions { volume })
    }
}

#[cfg(test)]
mod tests;
