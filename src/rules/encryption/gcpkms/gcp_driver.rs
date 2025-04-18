use crate::rules::encryption::gcpkms::gcp_client::GcpClient;
use crate::rules::encryption::kms_driver::KmsDriver;
use crate::serdes::serde::SerdeError;
use google_cloud_auth::credentials::{Credentials, create_access_token_credentials};
use log::error;
use std::collections::HashMap;
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use tink_core::TinkError;
use tink_core::registry::KmsClient;

const PREFIX: &str = "gcp-kms://";
const ACCOUNT_TYPE: &str = "account.type";
const CLIENT_ID: &str = "client.id";
const CLIENT_EMAIL: &str = "client.email";
const PRIVATE_KEY_ID: &str = "private.key.id";
const PRIVATE_KEY: &str = "private.key";

pub struct GcpKmsDriver {}

impl Default for GcpKmsDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl GcpKmsDriver {
    pub fn new() -> GcpKmsDriver {
        GcpKmsDriver {}
    }

    pub fn register() {
        crate::rules::encryption::register_kms_driver(GcpKmsDriver::new());
    }
}

impl KmsDriver for GcpKmsDriver {
    fn get_key_url_prefix(&self) -> &'static str {
        PREFIX
    }

    fn new_kms_client(
        &self,
        conf: &HashMap<String, String>,
        key_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError> {
        /*
        let mut account_type = conf.get(ACCOUNT_TYPE).cloned();
        if account_type.is_none() {
            account_type = Some("service_account".to_string());
        }
        let client_id = conf.get(CLIENT_ID).cloned();
        let client_email = conf.get(CLIENT_EMAIL).cloned();
        let private_key_id = conf.get(PRIVATE_KEY_ID).cloned();
        let private_key = conf.get(PRIVATE_KEY).cloned();

        if client_id.is_none()
            || client_email.is_none()
            || private_key_id.is_none()
            || private_key.is_none()
        {
            Ok(Arc::new(GcpClient::new(key_url)?))
        } else {
            let creds = GcpCredentials {
                account_type: account_type.unwrap(),
                client_id: client_id.unwrap(),
                client_email: client_email.unwrap(),
                private_key_id: private_key_id.unwrap(),
                private_key: private_key.unwrap(),
            };
            Ok(Arc::new(GcpClient::new_with_credentials(key_url, &creds)?))
        }
        */
        let creds = get_creds(conf, key_url)?;
        Ok(Arc::new(GcpClient::new(key_url, creds)?))
    }
}

fn get_creds(conf: &HashMap<String, String>, key_url: &str) -> Result<Credentials, TinkError> {
    let (sender, receiver) = mpsc::sync_channel(1);
    tokio::spawn(get_creds_async(conf.clone(), key_url.to_string(), sender));
    receiver
        .recv()
        .map_err(|e| TinkError::new("failed to receive"))?
}

async fn get_creds_async(
    conf: HashMap<String, String>,
    key_url: String,
    sender: SyncSender<Result<Credentials, TinkError>>,
) {
    let creds = create_access_token_credentials()
        .await
        .map_err(|e| TinkError::new(format!("failed to get creds: {}", e).as_str()));
    if creds.is_err() {
        error!("failed to get creds: {:?}", creds);
    }
    if sender.send(creds).is_err() {
        error!("failed to send result");
    }
}

pub struct GcpCredentials {
    pub account_type: String,
    pub private_key_id: String,
    pub private_key: String,
    pub client_email: String,
    pub client_id: String,
}
