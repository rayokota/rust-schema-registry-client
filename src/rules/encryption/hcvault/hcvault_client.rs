use crate::rules::encryption::hcvault::hcvault_aead::HcVaultAead;
use std::sync::Arc;
use tink_core::{TinkError, utils::wrap_err};
use url::Url;
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};

/// Prefix for any HashiCorp Vault key URIs.
pub const HCVAULT_PREFIX: &str = "hcvault://";

/// `HcVaultClient` represents a client that connects to the HashiCorp Vault KMS backend.
pub struct HcVaultClient {
    key_uri_prefix: String,
    kms: Arc<VaultClient>,
}

impl std::fmt::Debug for HcVaultClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HcVaultClient")
            .field("key_uri_prefix", &self.key_uri_prefix)
            .finish()
    }
}

impl HcVaultClient {
    pub fn new(
        uri_prefix: &str,
        token: &str,
        namespace: Option<&str>,
    ) -> Result<HcVaultClient, TinkError> {
        if !uri_prefix.to_lowercase().starts_with(HCVAULT_PREFIX) {
            return Err(format!(
                "uri_prefix must start with {HCVAULT_PREFIX}, but got {uri_prefix}"
            )
            .into());
        }
        let uri = if let Some(stripped) = uri_prefix.strip_prefix(HCVAULT_PREFIX) {
            stripped
        } else {
            uri_prefix
        };
        let parsed = Url::parse(uri).map_err(|e| wrap_err("failed to parse URI", e))?;
        let mut vault_url =
            parsed.scheme().to_string() + "://" + parsed.host_str().unwrap_or("localhost");
        if let Some(port) = parsed.port() {
            vault_url.push_str(&format!(":{port}"));
        }

        let client_settings = VaultClientSettingsBuilder::default()
            .address(vault_url)
            .token(token)
            .namespace(namespace.map(str::to_string))
            .build()
            .map_err(|e| wrap_err("failed to build client settings", e))?;
        let client =
            VaultClient::new(client_settings).map_err(|e| wrap_err("failed to build client", e))?;

        Ok(HcVaultClient {
            key_uri_prefix: uri_prefix.to_string(),
            kms: Arc::new(client),
        })
    }
}

impl tink_core::registry::KmsClient for HcVaultClient {
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

        let uri = if let Some(stripped) = key_uri.strip_prefix(HCVAULT_PREFIX) {
            stripped
        } else {
            key_uri
        };
        let u = Url::parse(uri).map_err(|e| wrap_err("failed to parse URI", e))?;
        Ok(Box::new(HcVaultAead::new(u.path(), self.kms.clone())?))
    }
}
