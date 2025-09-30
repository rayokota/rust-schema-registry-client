use crate::rules::encryption::azurekms::azure_client::{AzureClient, DEFAULT_ALGORITHM};
use crate::rules::encryption::kms_driver::KmsDriver;
use crate::serdes::serde::SerdeError;
use azure_core::auth::TokenCredential;
use azure_identity::{ClientSecretCredential, DefaultAzureCredential, TokenCredentialOptions};
use std::collections::HashMap;
use std::sync::Arc;
use tink_core::registry::KmsClient;
use url::Url;

const PREFIX: &str = "azure-kms://";
const TENANT_ID: &str = "tenant.id";
const CLIENT_ID: &str = "client.id";
const CLIENT_SECRET: &str = "client.secret";

pub struct AzureKmsDriver {}

impl Default for AzureKmsDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl AzureKmsDriver {
    pub fn new() -> AzureKmsDriver {
        AzureKmsDriver {}
    }

    pub fn register() {
        crate::rules::encryption::register_kms_driver(AzureKmsDriver::new());
    }
}

impl KmsDriver for AzureKmsDriver {
    fn get_key_url_prefix(&self) -> &'static str {
        PREFIX
    }

    fn new_kms_client(
        &self,
        conf: &HashMap<String, String>,
        key_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError> {
        let tenant = conf.get(TENANT_ID).cloned();
        let client = conf.get(CLIENT_ID).cloned();
        let secret = conf.get(CLIENT_SECRET).cloned();

        let creds: Arc<dyn TokenCredential> = if let Some(tenant) = tenant
            && let Some(client) = client
            && let Some(secret) = secret
        {
            let http_client = azure_core::new_http_client();
            let token_url = "https://login.microsoftonline.com/";
            Arc::new(ClientSecretCredential::new(
                http_client,
                Url::parse(token_url).unwrap(),
                tenant,
                client,
                secret,
            ))
        } else {
            Arc::new(DefaultAzureCredential::create(
                TokenCredentialOptions::default(),
            )?)
        };
        Ok(Arc::new(AzureClient::new(
            key_url,
            creds,
            DEFAULT_ALGORITHM,
        )?))
    }
}

impl From<azure_core::Error> for SerdeError {
    fn from(value: azure_core::Error) -> Self {
        SerdeError::Rule(format!("Azure error: {value}"))
    }
}
