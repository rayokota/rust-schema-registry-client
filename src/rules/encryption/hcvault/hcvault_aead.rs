use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use log::error;
use std::sync::mpsc::SyncSender;
use std::sync::{mpsc, Arc};
use std::{cell::RefCell, rc::Rc, str};
use tink_core::utils::wrap_err;
use tink_core::TinkError;
use vaultrs::client::VaultClient;
use vaultrs::transit::data;

/// `HcVaultAead` represents a HashiCorp Vault KMS service to a particular URI.
#[derive(Clone)]
pub struct HcVaultAead {
    kms: Arc<VaultClient>,
    mount_point: String,
    key_name: String,
}

impl HcVaultAead {
    pub(crate) fn new(key_path: &str, kms: Arc<VaultClient>) -> Result<HcVaultAead, TinkError> {
        let (mount_point, key_name) = get_endpoint_paths(key_path)?;
        Ok(HcVaultAead {
            kms,
            mount_point,
            key_name,
        })
    }

    async fn encrypt_async(
        self,
        plaintext: Vec<u8>,
        additional_data: Vec<u8>,
        sender: SyncSender<Result<Vec<u8>, TinkError>>,
    ) {
        // TODO additional data?
        let payload = BASE64_STANDARD.encode(plaintext);
        let result = data::encrypt(
            self.kms.as_ref(),
            &self.mount_point,
            &self.key_name,
            &payload,
            None,
        )
        .await
        .map(|r| r.ciphertext.into_bytes())
        .map_err(|e| wrap_err("request failed", e));

        if result.is_err() {
            error!("failed to encrypt: {:?}", result);
        }
        if sender.send(result).is_err() {
            error!("failed to send result");
        }
    }

    async fn decrypt_async(
        self,
        ciphertext: Vec<u8>,
        additional_data: Vec<u8>,
        sender: SyncSender<Result<Vec<u8>, TinkError>>,
    ) {
        // TODO additional data?
        let payload = str::from_utf8(&ciphertext)
            .map_err(|e| wrap_err("failed to convert ciphertext to string", e));
        if payload.is_err() {
            error!("failed to convert ciphertext to string: {:?}", payload);
            if sender.send(Err(payload.unwrap_err())).is_err() {
                error!("failed to send result");
            }
            return;
        }
        let rsp = data::decrypt(
            self.kms.as_ref(),
            &self.mount_point,
            &self.key_name,
            payload.unwrap(),
            None,
        )
        .await
        .map(|r| BASE64_STANDARD.decode(r.plaintext.into_bytes()).ok())
        .map_err(|e| wrap_err("request failed", e))
        .transpose();
        let result = match rsp {
            Some(Ok(ciphertext)) => Ok(ciphertext),
            Some(Err(e)) => Err(e),
            None => Err(TinkError::new("no ciphertext in response")),
        };
        if result.is_err() {
            error!("failed to encrypt: {:?}", result);
        }
        if sender.send(result).is_err() {
            error!("failed to send result");
        }
    }
}

impl tink_core::Aead for HcVaultAead {
    fn encrypt(&self, plaintext: &[u8], additional_data: &[u8]) -> Result<Vec<u8>, TinkError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let this = self.clone();
        let plaintext_vec = plaintext.to_vec();
        let ad_vec = additional_data.to_vec();
        tokio::spawn(async move { this.encrypt_async(plaintext_vec, ad_vec, sender).await });
        receiver
            .recv()
            .map_err(|e| wrap_err("failed to receive", e))?
    }

    fn decrypt(&self, ciphertext: &[u8], additional_data: &[u8]) -> Result<Vec<u8>, TinkError> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let this = self.clone();
        let cipher_vec = ciphertext.to_vec();
        let ad_vec = additional_data.to_vec();
        tokio::spawn(async move { this.decrypt_async(cipher_vec, ad_vec, sender).await });
        receiver
            .recv()
            .map_err(|e| wrap_err("failed to receive", e))?
    }
}

// get_endpoint_paths extracts the mount_path and key_name from the keyPath.
// The keyPath is expected to have the form "/{mount-path}/keys/{keyName}".
fn get_endpoint_paths(key_path: &str) -> Result<(String, String), TinkError> {
    let parts: Vec<&str> = key_path.split('/').collect();
    let len = parts.len();
    if len < 4 || !parts[0].is_empty() || parts[len - 2] != "keys" {
        return Err("invalid key path".into());
    }
    let mount_path = parts[1];
    let key_name = parts[3];
    Ok((mount_path.to_string(), key_name.to_string()))
}
