use azure_core::auth::TokenCredential;
use azure_security_keyvault::prelude::{
    CryptographParamtersEncryption, DecryptParameters, EncryptParameters,
};
use azure_security_keyvault::KeyClient;
use log::error;
use std::future::IntoFuture;
use std::sync::mpsc::SyncSender;
use std::sync::{mpsc, Arc};
use std::{cell::RefCell, rc::Rc};
use tink_core::utils::wrap_err;
use tink_core::TinkError;
use url::Url;

/// `AzureAead` represents a Azure KMS service to a particular URI.
#[derive(Clone)]
pub struct AzureAead {
    kms: KeyClient,
    key_name: String,
    key_version: String,
    algorithm: CryptographParamtersEncryption,
}

impl AzureAead {
    /// Return a new Azure KMS service.
    pub(crate) fn new(
        key_url: &str,
        creds: Arc<dyn TokenCredential>,
        algorithm: CryptographParamtersEncryption,
    ) -> Result<AzureAead, TinkError> {
        let (vault_url, key_name, key_version) = get_key_info(key_url)?;
        let kms = KeyClient::new(&vault_url, creds)
            .map_err(|e| wrap_err("failed to create KeyClient", e))?;
        Ok(AzureAead {
            kms,
            key_name: key_name.to_string(),
            key_version: key_version.to_string(),
            algorithm,
        })
    }

    async fn encrypt_async(
        self,
        plaintext: Vec<u8>,
        additional_data: Vec<u8>,
        sender: SyncSender<Result<Vec<u8>, TinkError>>,
    ) {
        let params = EncryptParameters {
            encrypt_parameters_encryption: self.algorithm.clone(),
            plaintext,
        };
        let req = self
            .kms
            .encrypt(self.key_name.clone(), params)
            .version(self.key_version.clone());
        // TODO additional data
        /*
        if !additional_data.is_empty() {
            req = req.context(additional_data);
        };
        */
        let result = req
            .into_future()
            .await
            .map(|r| r.result)
            .map_err(|e| wrap_err("failed to encrypt", e));

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
        let params = DecryptParameters {
            decrypt_parameters_encryption: self.algorithm.clone(),
            ciphertext,
        };
        let req = self
            .kms
            .decrypt(self.key_name.clone(), params)
            .version(self.key_version.clone());
        // TODO additional data
        /*
        if !additional_data.is_empty() {
            req = req.context(additional_data);
        };
        */
        let result = req
            .into_future()
            .await
            .map(|r| r.result)
            .map_err(|e| wrap_err("request failed", e));

        if result.is_err() {
            error!("failed to decrypt: {:?}", result);
        }
        if sender.send(result).is_err() {
            error!("failed to send result");
        }
    }
}

impl tink_core::Aead for AzureAead {
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

    /// Returns an error if the `key_id` field in the response does not match the `key_uri`
    /// provided when creating the client. If we don't do this, the possibility exists
    /// for the ciphertext to be replaced by one under a key we don't control/expect,
    /// but do have decrypt permissions on.
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

fn get_key_info(key_uri: &str) -> Result<(String, String, String), TinkError> {
    let parsed = Url::parse(key_uri).map_err(|e| wrap_err("failed to parse URI", e))?;
    let path = parsed.path();
    let parts: Vec<&str> = path.split('/').collect();
    let len = parts.len();
    if len != 4 || !parts[0].is_empty() || parts[1] != "keys" {
        return Err("invalid key uri".into());
    }
    let vault_url = parsed.scheme().to_string() + "://" + parsed.host_str().unwrap_or("localhost");
    Ok((vault_url, parts[2].to_string(), parts[3].to_string()))
}
