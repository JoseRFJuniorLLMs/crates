//! Institutional signature over consolidated snapshots.
//!
//! A timestamp proves *when* a commitment existed. An institutional signature
//! proves *who* vouches for it (the AGU, CGU, etc.). These are orthogonal and
//! both go into the legal package.
//!
//! Backends sit behind [`InstitutionalSigner`]:
//!
//! * [`SoftKeySigner`] — software P-256 key. **Dev only.** A software key file
//!   (A1 `.pfx`) sitting on the database host is exactly what an órgão security
//!   review tends to reject.
//! * [`Pkcs11Signer`] — the production path: the private key lives in an HSM /
//!   token and is used over PKCS#11. Stubbed here so the integration point is
//!   explicit; wiring a concrete PKCS#11 provider is deliberately deferred to
//!   the órgão's homologated module.

use crate::CompError;
use p256::ecdsa::signature::Signer as _;
use p256::ecdsa::{Signature, SigningKey};

/// A detached institutional signature plus the material to verify it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstitutionalSignature {
    pub subject: String,
    pub signature: Vec<u8>,
    /// SEC1 (uncompressed) public key of the signer (dev). In production the
    /// signer identity is the X.509 certificate chain instead.
    pub public_key_sec1: Vec<u8>,
}

/// Anything that can vouch for a snapshot on behalf of an institution.
pub trait InstitutionalSigner {
    /// Certificate subject / institution name.
    fn subject(&self) -> &str;
    /// Sign arbitrary snapshot bytes (e.g. the commitment DER).
    fn sign_snapshot(&self, data: &[u8]) -> Result<InstitutionalSignature, CompError>;
}

/// Dev-only software signer.
pub struct SoftKeySigner {
    signing: SigningKey,
    subject: String,
}

impl SoftKeySigner {
    pub fn generate(subject: impl Into<String>) -> Self {
        Self {
            signing: SigningKey::random(&mut rand::thread_rng()),
            subject: subject.into(),
        }
    }
}

impl InstitutionalSigner for SoftKeySigner {
    fn subject(&self) -> &str {
        &self.subject
    }

    fn sign_snapshot(&self, data: &[u8]) -> Result<InstitutionalSignature, CompError> {
        let sig: Signature = self.signing.sign(data);
        Ok(InstitutionalSignature {
            subject: self.subject.clone(),
            signature: sig.to_bytes().to_vec(),
            public_key_sec1: self
                .signing
                .verifying_key()
                .to_encoded_point(false)
                .as_bytes()
                .to_vec(),
        })
    }
}

/// Production signer whose key lives in an HSM / PKCS#11 token. Intentionally
/// not implemented in the open crate: the concrete provider is supplied by the
/// órgão's homologated build.
pub struct Pkcs11Signer {
    pub module_path: String,
    pub token_label: String,
    pub key_label: String,
    pub subject: String,
}

impl InstitutionalSigner for Pkcs11Signer {
    fn subject(&self) -> &str {
        &self.subject
    }

    fn sign_snapshot(&self, _data: &[u8]) -> Result<InstitutionalSignature, CompError> {
        Err(CompError::Unsupported(format!(
            "assinatura via HSM/PKCS#11 (módulo '{}', token '{}', chave '{}') exige o \
             provider homologado do órgão — não embutido no core aberto",
            self.module_path, self.token_label, self.key_label
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use p256::ecdsa::signature::Verifier;
    use p256::ecdsa::{Signature, VerifyingKey};

    #[test]
    fn soft_signer_produces_verifiable_signature() {
        let s = SoftKeySigner::generate("AGU");
        let data = b"commitment-snapshot";
        let sig = s.sign_snapshot(data).unwrap();
        assert_eq!(sig.subject, "AGU");
        let vk = VerifyingKey::from_sec1_bytes(&sig.public_key_sec1).unwrap();
        let parsed = Signature::from_slice(&sig.signature).unwrap();
        assert!(vk.verify(data, &parsed).is_ok());
    }

    #[test]
    fn pkcs11_signer_is_explicitly_unsupported() {
        let s = Pkcs11Signer {
            module_path: "/usr/lib/libcs.so".into(),
            token_label: "AGU-HSM".into(),
            key_label: "agu-sign".into(),
            subject: "AGU".into(),
        };
        assert!(matches!(
            s.sign_snapshot(b"x"),
            Err(CompError::Unsupported(_))
        ));
    }
}
