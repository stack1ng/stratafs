use std::fmt::{self, Display};
use std::str::FromStr;

use bytes::Bytes;

const SCHEME_PREFIX: &str = "stratavolume://";

// VolumeUrl is the URL that names a volume to mount, which the mount command hands to the extension.
// It is written as stratavolume://<volume id in lowercase hex>, since the system may lowercase a URL's host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VolumeUrl<V> {
    pub volume: V,
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeUrlError<E> {
    #[error("the URL does not start with {SCHEME_PREFIX}")]
    WrongScheme,
    #[error("the volume id is not hex")]
    NotHex(#[source] hex::FromHexError),
    #[error("the volume id does not name a volume")]
    NotAVolume(#[source] E),
}

impl<V: Clone + Into<Bytes>> Display for VolumeUrl<V> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let id: Bytes = self.volume.clone().into();
        write!(formatter, "{SCHEME_PREFIX}{}", hex::encode(id))
    }
}

impl<V: TryFrom<Bytes>> FromStr for VolumeUrl<V> {
    type Err = VolumeUrlError<V::Error>;

    fn from_str(url: &str) -> Result<Self, Self::Err> {
        let id = url
            .strip_prefix(SCHEME_PREFIX)
            .ok_or(VolumeUrlError::WrongScheme)?;
        let id = hex::decode(id).map_err(VolumeUrlError::NotHex)?;
        let volume = V::try_from(Bytes::from(id)).map_err(VolumeUrlError::NotAVolume)?;
        Ok(VolumeUrl { volume })
    }
}

#[cfg(test)]
mod tests;
