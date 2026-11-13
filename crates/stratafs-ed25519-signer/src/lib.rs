// A signer for the mounts this repository makes itself, in its tests and benchmarks: it signs with one Ed25519 key
// and trusts only that key. The key is written as the base64 of its PKCS#8 DER form, to pass on a command line.

use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::Bytes;
use ed25519_dalek::pkcs8::{DecodePrivateKey, EncodePrivateKey};
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _};
use stratafs_fs_engine::verification::Signer;

// NotAKey is why a text is not a key.
#[derive(Debug, thiserror::Error)]
pub enum NotAKey {
    #[error("the key is not base64")]
    NotBase64(#[source] base64::DecodeError),
    #[error("the key is not an Ed25519 private key in PKCS#8 DER form")]
    NotPkcs8(#[source] ed25519_dalek::pkcs8::Error),
}

// Ed25519Signer signs with one key, and trusts only the signatures that key makes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ed25519Signer(SigningKey);

impl Ed25519Signer {
    // generate is a signer with a new key.
    pub fn generate() -> Self {
        Ed25519Signer(SigningKey::from_bytes(&rand::random()))
    }

    // key is the signer's key as from_str reads it.
    pub fn key(&self) -> String {
        let der = self
            .0
            .to_pkcs8_der()
            .expect("an Ed25519 key has a PKCS#8 form");
        STANDARD.encode(der.as_bytes())
    }
}

impl FromStr for Ed25519Signer {
    type Err = NotAKey;

    fn from_str(key: &str) -> Result<Self, NotAKey> {
        let der = STANDARD.decode(key).map_err(NotAKey::NotBase64)?;
        SigningKey::from_pkcs8_der(&der)
            .map(Ed25519Signer)
            .map_err(NotAKey::NotPkcs8)
    }
}

impl Signer for Ed25519Signer {
    const VERIFIES: bool = true;

    type Failure = std::convert::Infallible;

    async fn sign(&self, statement: &[u8]) -> Result<Bytes, Self::Failure> {
        Ok(Bytes::copy_from_slice(&self.0.sign(statement).to_bytes()))
    }

    async fn verify(&self, statement: &[u8], signature: &[u8]) -> Result<bool, Self::Failure> {
        let Ok(signature) = Signature::from_slice(signature) else {
            return Ok(false);
        };
        Ok(self.0.verifying_key().verify(statement, &signature).is_ok())
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::future::block_on;

    use super::*;

    #[test]
    fn a_signature_verifies_for_its_statement_and_key_only() {
        let (signer, other) = (Ed25519Signer::generate(), Ed25519Signer::generate());
        let signature = block_on(signer.sign(b"statement")).unwrap();
        assert_eq!(signature.len(), 64);
        assert_eq!(block_on(signer.verify(b"statement", &signature)), Ok(true));
        assert_eq!(block_on(signer.verify(b"another", &signature)), Ok(false));
        assert_eq!(block_on(other.verify(b"statement", &signature)), Ok(false));
        assert_eq!(
            block_on(signer.verify(b"statement", &signature[..63])),
            Ok(false)
        );
    }

    #[test]
    fn a_key_reads_back_from_its_text() {
        let signer = Ed25519Signer::generate();
        let read: Ed25519Signer = signer.key().parse().unwrap();
        let signature = block_on(read.sign(b"statement")).unwrap();
        assert_eq!(block_on(signer.verify(b"statement", &signature)), Ok(true));
        // The key openssl makes with `genpkey -algorithm ed25519 -outform DER`, in base64.
        let openssl = "MC4CAQAwBQYDK2VwBCIEIAcktqPBWQHYKorBw/AOr9PRsV0jpsBsNLi+ZNUYOzUj";
        assert!(openssl.parse::<Ed25519Signer>().is_ok());
        assert!(matches!(
            "not a key".parse::<Ed25519Signer>(),
            Err(NotAKey::NotBase64(_))
        ));
        assert!(matches!(
            "AAAA".parse::<Ed25519Signer>(),
            Err(NotAKey::NotPkcs8(_))
        ));
    }
}
