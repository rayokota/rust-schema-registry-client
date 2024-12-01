use crate::serdes::serde::SerdeError;
use std::collections::HashMap;
use std::sync::Arc;
use tink_core::registry::KmsClient;

pub trait KmsDriver: Send + Sync {
    fn get_key_url_prefix(&self) -> &'static str;

    fn new_kms_client(
        &self,
        conf: &HashMap<String, String>,
        key_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError>;
}
