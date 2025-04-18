use crate::rules::encryption::hcvault::hcvault_client::HcVaultClient;
use crate::rules::encryption::kms_driver::KmsDriver;
use crate::serdes::serde::SerdeError;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tink_core::TinkError;
use tink_core::registry::KmsClient;

const PREFIX: &str = "hcvault://";
const TOKEN_ID: &str = "token.id";
const NAMESPACE: &str = "namespace";

pub struct HcVaultDriver {}

impl Default for HcVaultDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl HcVaultDriver {
    pub fn new() -> HcVaultDriver {
        HcVaultDriver {}
    }

    pub fn register() {
        crate::rules::encryption::register_kms_driver(HcVaultDriver::new());
    }
}

impl KmsDriver for HcVaultDriver {
    fn get_key_url_prefix(&self) -> &'static str {
        PREFIX
    }

    fn new_kms_client(
        &self,
        conf: &HashMap<String, String>,
        key_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError> {
        let mut token = conf.get(TOKEN_ID).cloned();
        let mut namespace = conf.get(NAMESPACE).cloned();
        if token.is_none() {
            token = env::var("VAULT_TOKEN").ok();
            namespace = env::var("VAULT_NAMESPACE").ok();
        }
        if token.is_none() {
            return Err(SerdeError::Tink(TinkError::new("cannot load token")));
        }

        Ok(Arc::new(HcVaultClient::new(
            key_url,
            token.unwrap().as_str(),
            namespace.as_deref(),
        )?))
    }
}
