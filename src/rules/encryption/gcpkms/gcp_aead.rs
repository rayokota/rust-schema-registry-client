use google_cloud_kms_v1::client::KeyManagementService;
use google_cloud_kms_v1::model;
use log::error;
use serde::{Deserialize, Serialize};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, mpsc};
use tink_core::{TinkError, utils::wrap_err};

/// `GcpAead` represents a GCP KMS service to a particular URI.
#[derive(Clone)]
pub struct GcpAead {
    key_uri: String,
    kms: KeyManagementService,
}

impl GcpAead {
    /// Return a new AEAD primitive backed by the GCP KMS service.
    pub fn new(key_uri: &str, kms: KeyManagementService) -> Result<GcpAead, TinkError> {
        Ok(GcpAead {
            key_uri: key_uri.to_string(),
            kms,
        })
    }

    async fn encrypt_async(
        self,
        plaintext: Vec<u8>,
        additional_data: Vec<u8>,
        sender: SyncSender<Result<Vec<u8>, TinkError>>,
    ) {
        let result = self
            .kms
            .encrypt(self.key_uri)
            .set_plaintext(plaintext)
            .set_additional_authenticated_data(additional_data)
            .send()
            .await
            .map(|r| r.ciphertext.to_vec())
            .map_err(|e| wrap_err("failed to encrypt", e));
        if result.is_err() {
            error!("failed to encrypt: {result:?}");
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
        let result = self
            .kms
            .decrypt(self.key_uri)
            .set_ciphertext(ciphertext)
            .set_additional_authenticated_data(additional_data)
            .send()
            .await
            .map(|r| r.plaintext.to_vec())
            .map_err(|e| wrap_err("failed to decrypt", e));
        if result.is_err() {
            error!("failed to decrypt: {result:?}");
        }
        if sender.send(result).is_err() {
            error!("failed to send result");
        }
    }
}

impl tink_core::Aead for GcpAead {
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
