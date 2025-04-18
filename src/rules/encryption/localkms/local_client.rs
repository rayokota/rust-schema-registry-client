use prost::Message;
use tink_aead::AES_GCM_TYPE_URL;
use tink_core::registry::{KmsClient, primitive_from_key_data};
use tink_core::subtle::compute_hkdf;
use tink_core::utils::wrap_err;
use tink_core::{Aead, TinkError};
use tink_proto::{AesGcmKey, HashType, KeyData};

const PREFIX: &str = "local-kms://";

pub struct LocalKmsClient {
    secret: String,
}

impl LocalKmsClient {
    pub fn new(secret: &str) -> LocalKmsClient {
        LocalKmsClient {
            secret: secret.to_string(),
        }
    }

    pub fn get_primitive(secret: &str) -> Result<Box<dyn Aead>, TinkError> {
        let key = LocalKmsClient::get_key(secret)?;
        let aes_gcm_key = AesGcmKey {
            key_value: key,
            version: 0,
        };
        let mut serialized_aes_gcm_key = Vec::new();
        aes_gcm_key
            .encode(&mut serialized_aes_gcm_key)
            .map_err(|e| wrap_err("failed to encode new key", e))?;
        let key_data = KeyData {
            type_url: AES_GCM_TYPE_URL.to_string(),
            value: serialized_aes_gcm_key,
            key_material_type: tink_proto::key_data::KeyMaterialType::Symmetric as i32,
        };
        let primitive = primitive_from_key_data(&key_data)?;
        if let tink_core::Primitive::Aead(primitive) = primitive {
            Ok(primitive)
        } else {
            Err("Aead primitive expected".into())
        }
    }

    pub fn get_key(secret: &str) -> Result<Vec<u8>, TinkError> {
        compute_hkdf(HashType::Sha256, secret.as_bytes(), &[], &[], 16)
    }
}

impl KmsClient for LocalKmsClient {
    fn supported(&self, key_uri: &str) -> bool {
        key_uri.starts_with(PREFIX)
    }

    fn get_aead(&self, key_uri: &str) -> Result<Box<dyn Aead>, TinkError> {
        LocalKmsClient::get_primitive(&self.secret)
    }
}
