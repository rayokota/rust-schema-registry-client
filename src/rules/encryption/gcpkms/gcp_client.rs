use crate::rules::encryption::gcpkms::gcp_aead::GcpAead;
use crate::rules::encryption::gcpkms::gcp_driver::GcpCredentials;
use google_cloud_auth::credentials::{create_access_token_credentials, Credentials};
use google_cloud_kms_v1::client::KeyManagementService;
use log::error;
use std::sync::mpsc;
use tink_core::{utils::wrap_err, TinkError};

/// Prefix for any GCP-KMS key URIs.
pub const GCP_PREFIX: &str = "gcp-kms://";

/// `GcpClient` represents a client that connects to the GCP KMS backend, providing appropriate
/// authorization credentials.
pub struct GcpClient {
    key_uri_prefix: String,
    kms: KeyManagementService,
}

impl GcpClient {
    /// Return a new GCP KMS client which will use default credentials to handle keys with
    /// `uri_prefix` prefix. `uri_prefix` must have the following format: `gcp-kms://[:path]`.
    pub fn new(uri_prefix: &str, creds: Credentials) -> Result<GcpClient, TinkError> {
        if !uri_prefix.to_lowercase().starts_with(GCP_PREFIX) {
            return Err(format!("uri_prefix must start with {GCP_PREFIX}").into());
        }

        let (sender, receiver) = mpsc::sync_channel(1);
        tokio::spawn(get_client(creds, sender));
        let kms = receiver
            .recv()
            .map_err(|e| wrap_err("failed to receive", e))??;

        Ok(GcpClient {
            key_uri_prefix: uri_prefix.to_string(),
            kms,
        })
    }
}

async fn get_client(
    creds: Credentials,
    sender: mpsc::SyncSender<Result<KeyManagementService, TinkError>>,
) {
    let result = KeyManagementService::builder()
        .with_credentials(creds)
        .build()
        .await
        .map_err(|e| wrap_err("failed to create GCP KMS client", e));
    if result.is_err() {
        error!("failed to decrypt: {:?}", result);
    }
    if sender.send(result).is_err() {
        error!("failed to send result");
    }
}

impl tink_core::registry::KmsClient for GcpClient {
    fn supported(&self, key_uri: &str) -> bool {
        key_uri.starts_with(&self.key_uri_prefix)
    }
    fn get_aead(&self, key_uri: &str) -> Result<Box<dyn tink_core::Aead>, TinkError> {
        if !self.supported(key_uri) {
            return Err("unsupported key_uri".into());
        }
        let uri = if let Some(rest) = key_uri.strip_prefix(GCP_PREFIX) {
            rest
        } else {
            key_uri
        };
        Ok(Box::new(GcpAead::new(uri, self.kms.clone())?))
    }
}
