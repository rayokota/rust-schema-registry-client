use crate::rules::encryption::kms_driver::KmsDriver;
use crate::serdes::serde::SerdeError;
use lazy_static::lazy_static;
use std::sync::{Arc, RwLock};
use tink_core::registry::KmsClient;
use tink_core::TinkError;

#[cfg(feature = "rules-encryption-awskms")]
pub mod awskms;
#[cfg(feature = "rules-encryption-azurekms")]
pub mod azurekms;
#[cfg(feature = "rules-encryption-tink")]
pub mod encrypt_executor;
#[cfg(feature = "rules-encryption-gcpkms")]
pub mod gcpkms;
#[cfg(feature = "rules-encryption-hcvault")]
pub mod hcvault;
pub mod kms_driver;
#[cfg(feature = "rules-encryption-localkms")]
pub mod localkms;

lazy_static! {
    /// Global list of KMS driver objects.
    static ref KMS_DRIVERS: RwLock<Vec<Arc<dyn KmsDriver>>> = RwLock::new(Vec::new());

    /// Global list of KMS client objects.
    static ref KMS_CLIENTS: RwLock<Vec<Arc<dyn KmsClient>>> = RwLock::new(Vec::new());
}

/// Error message for global KMS driver list lock.
const DERR: &str = "global KMS_DRIVERS lock poisoned";

/// Error message for global KMS client list lock.
const CERR: &str = "global KMS_CLIENTS lock poisoned";

/// Register a new KMS driver
pub fn register_kms_driver<T>(k: T)
where
    T: 'static + KmsDriver,
{
    let mut kms_drivers = KMS_DRIVERS.write().expect(DERR); // safe: lock
    kms_drivers.push(Arc::new(k));
}

/// Remove all registered KMS drivers.
pub fn clear_kms_drivers() {
    let mut kms_drivers = KMS_DRIVERS.write().expect(DERR); // safe: lock
    kms_drivers.clear();
}

/// Fetches a [`KmsDriver`] by a given URI.
pub fn get_kms_driver(key_uri: &str) -> Result<Arc<dyn KmsDriver>, SerdeError> {
    let kms_drivers = KMS_DRIVERS.read().expect(CERR); // safe: lock
    for k in kms_drivers.iter() {
        if key_uri.starts_with(k.get_key_url_prefix()) {
            return Ok(k.clone());
        }
    }
    Err(SerdeError::Tink(TinkError::new(&format!(
        "kms driver supporting {key_uri} not found"
    ))))
}

/// Register a new KMS client
pub fn register_kms_client(k: Arc<dyn KmsClient>) {
    let mut kms_clients = KMS_CLIENTS.write().expect(CERR); // safe: lock
    kms_clients.push(k);
}

/// Remove all registered KMS clients.
pub fn clear_kms_clients() {
    let mut kms_clients = KMS_CLIENTS.write().expect(CERR); // safe: lock
    kms_clients.clear();
}

/// Fetches a [`KmsClient`] by a given URI.
pub fn get_kms_client(key_uri: &str) -> Result<Arc<dyn KmsClient>, TinkError> {
    let kms_clients = KMS_CLIENTS.read().expect(CERR); // safe: lock
    for k in kms_clients.iter() {
        if k.supported(key_uri) {
            return Ok(k.clone());
        }
    }
    Err(format!("KMS client supporting {key_uri} not found").into())
}
