use crate::rules::encryption::kms_driver::KmsDriver;
use crate::rules::encryption::localkms::local_client::LocalKmsClient;
use crate::serdes::serde::SerdeError;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tink_core::TinkError;
use tink_core::registry::KmsClient;

const PREFIX: &str = "local-kms://";
const SECRET: &str = "secret";

pub struct LocalKmsDriver {}

impl Default for LocalKmsDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalKmsDriver {
    pub fn new() -> LocalKmsDriver {
        LocalKmsDriver {}
    }

    pub fn register() {
        crate::rules::encryption::register_kms_driver(LocalKmsDriver::new());
    }
}

impl KmsDriver for LocalKmsDriver {
    fn get_key_url_prefix(&self) -> &'static str {
        PREFIX
    }

    fn new_kms_client(
        &self,
        conf: &HashMap<String, String>,
        key_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError> {
        let mut secret = conf.get(SECRET).cloned();
        if secret.is_none() {
            secret = env::var("LOCAL_SECRET").ok();
        }
        if secret.is_none() {
            return Err(SerdeError::Tink(TinkError::new("cannot load secret")));
        }
        Ok(Arc::new(LocalKmsClient::new(&secret.unwrap())))
    }
}
