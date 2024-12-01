use crate::rules::encryption::azurekms::azure_aead::AzureAead;
use azure_core::auth::TokenCredential;
use azure_security_keyvault::prelude::EncryptionAlgorithm::RsaOaep256;
use azure_security_keyvault::prelude::{CryptographParamtersEncryption, RsaEncryptionParameters};
use std::sync::Arc;
use tink_core::TinkError;

/// Prefix for any AZURE-KMS key URIs.
pub const AZURE_PREFIX: &str = "azure-kms://";

pub const DEFAULT_ALGORITHM: CryptographParamtersEncryption =
    CryptographParamtersEncryption::Rsa(RsaEncryptionParameters {
        algorithm: RsaOaep256,
    });

/// `AzureClient` represents a client that connects to the Azure KMS backend.
pub struct AzureClient {
    key_uri_prefix: String,
    creds: Arc<dyn TokenCredential>,
    algorithm: CryptographParamtersEncryption,
}

impl std::fmt::Debug for AzureClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureClient")
            .field("key_uri_prefix", &self.key_uri_prefix)
            .finish()
    }
}

impl AzureClient {
    pub fn new(
        uri_prefix: &str,
        creds: Arc<dyn TokenCredential>,
        algorithm: CryptographParamtersEncryption,
    ) -> Result<AzureClient, TinkError> {
        Ok(AzureClient {
            key_uri_prefix: uri_prefix.to_string(),
            creds,
            algorithm,
        })
    }
}

impl tink_core::registry::KmsClient for AzureClient {
    fn supported(&self, key_uri: &str) -> bool {
        key_uri.starts_with(&self.key_uri_prefix)
    }

    fn get_aead(&self, key_uri: &str) -> Result<Box<dyn tink_core::Aead>, TinkError> {
        if !self.supported(key_uri) {
            return Err(format!(
                "key_uri must start with prefix {}, but got {}",
                self.key_uri_prefix, key_uri
            )
            .into());
        }

        let uri = if let Some(stripped) = key_uri.strip_prefix(AZURE_PREFIX) {
            stripped
        } else {
            key_uri
        };
        Ok(Box::new(AzureAead::new(
            uri,
            self.creds.clone(),
            self.algorithm.clone(),
        )?))
    }
}
