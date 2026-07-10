use crate::rules::encryption::azurekms::azure_aead;
use crate::rules::encryption::azurekms::azure_aead::AzureAead;
use crate::rules::encryption::azurekms::azure_driver::ENCRYPT_AZURE_KEY_VERSION_SAVE;
use azure_core::auth::TokenCredential;
use azure_security_keyvault::prelude::EncryptionAlgorithm::RsaOaep256;
use azure_security_keyvault::prelude::{CryptographParamtersEncryption, RsaEncryptionParameters};
use log::warn;
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
    save_version: bool,
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
        save_version: bool,
    ) -> Result<AzureClient, TinkError> {
        Ok(AzureClient {
            key_uri_prefix: uri_prefix.to_string(),
            creds,
            algorithm,
            save_version,
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
        if !self.save_version && azure_aead::is_versionless(uri).unwrap_or(false) {
            warn!(
                "Azure Key Vault key '{uri}' is versionless and {ENCRYPT_AZURE_KEY_VERSION_SAVE} \
                 is not enabled; DEKs wrapped with it may become undecryptable after the key is \
                 rotated."
            );
        }
        Ok(Box::new(AzureAead::new(
            uri,
            self.creds.clone(),
            self.algorithm.clone(),
            self.save_version,
        )?))
    }
}
