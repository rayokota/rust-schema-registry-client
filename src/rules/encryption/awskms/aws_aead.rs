use aws_sdk_kms::primitives::Blob;
use log::error;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use tink_core::TinkError;
use tink_core::utils::wrap_err;

/// `AwsAead` represents a AWS KMS service to a particular URI.
#[derive(Clone)]
pub struct AwsAead {
    key_uri: String,
    kms: aws_sdk_kms::Client,
}

impl AwsAead {
    /// Return a new AEAD primitive backed by the AWS KMS service.
    /// `key_uri` must have the following format: `arn:<partition>:kms:<region>:[:path]`.
    /// See <http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>.
    pub(crate) fn new(key_uri: &str, kms: aws_sdk_kms::Client) -> Result<AwsAead, TinkError> {
        Ok(AwsAead {
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
        let ad = if !additional_data.is_empty() {
            let mut map = HashMap::new();
            map.insert("additionalData".to_string(), hex::encode(additional_data));
            Some(map)
        } else {
            None
        };
        let req = self
            .kms
            .encrypt()
            .key_id(self.key_uri)
            .set_encryption_context(ad)
            .plaintext(Blob::new(plaintext));
        let rsp = req
            .send()
            .await
            .map(|r| r.ciphertext_blob.map(|b| b.into_inner()))
            .map_err(|e| wrap_err("failed to encrypt", e))
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

    async fn decrypt_async(
        self,
        ciphertext: Vec<u8>,
        additional_data: Vec<u8>,
        sender: SyncSender<Result<Vec<u8>, TinkError>>,
    ) {
        let ad = if !additional_data.is_empty() {
            let mut map = HashMap::new();
            map.insert("additionalData".to_string(), hex::encode(additional_data));
            Some(map)
        } else {
            None
        };
        let req = self
            .kms
            .decrypt()
            .key_id(self.key_uri)
            .set_encryption_context(ad)
            .ciphertext_blob(Blob::new(ciphertext));
        let rsp = req
            .send()
            .await
            .map(|r| r.plaintext.map(|b| b.into_inner()))
            .map_err(|e| wrap_err("failed to decrypt", e))
            .transpose();
        let result = match rsp {
            Some(Ok(plaintext)) => Ok(plaintext),
            Some(Err(e)) => Err(e),
            None => Err(TinkError::new("no plaintext in response")),
        };
        if result.is_err() {
            error!("failed to decrypt: {:?}", result);
        }
        if sender.send(result).is_err() {
            error!("failed to send result");
        }
    }
}

impl tink_core::Aead for AwsAead {
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
    ///
    /// This check is disabled if `AwsAead.key_uri` is not in key ARN format.
    ///
    /// See <https://docs.aws.amazon.com/kms/latest/developerguide/concepts.html#key-id>.
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
