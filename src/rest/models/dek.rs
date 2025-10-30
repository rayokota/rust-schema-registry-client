use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use serde::{Deserialize, Serialize};

/// Dek : Dek
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct Dek {
    /// Kek name of the dek
    #[serde(rename = "kekName")]
    pub kek_name: String,
    /// Subject of the dek
    #[serde(rename = "subject")]
    pub subject: String,
    /// Version of the dek
    #[serde(rename = "version")]
    pub version: i32,
    /// Algorithm of the dek
    #[serde(rename = "algorithm")]
    pub algorithm: Algorithm,
    /// Encrypted key material of the dek
    #[serde(
        rename = "encryptedKeyMaterial",
        skip_serializing_if = "Option::is_none"
    )]
    pub encrypted_key_material: Option<String>,
    /// Encrypted key material of the dek in bytes
    #[serde(
        rename = "encryptedKeyMaterialBytes",
        skip_serializing_if = "Option::is_none"
    )]
    pub encrypted_key_material_bytes: Option<Vec<u8>>,
    /// Raw key material of the dek
    #[serde(rename = "keyMaterial", skip_serializing_if = "Option::is_none")]
    pub key_material: Option<String>,
    /// Raw key material of the dek in bytes
    #[serde(rename = "keyMaterialBytes", skip_serializing_if = "Option::is_none")]
    pub key_material_bytes: Option<Vec<u8>>,
    /// Timestamp of the dek
    #[serde(rename = "ts")]
    pub ts: i64,
    /// Whether the dek is deleted
    #[serde(rename = "deleted", skip_serializing_if = "Option::is_none")]
    pub deleted: Option<bool>,
}

impl Dek {
    /// Dek
    pub fn new(
        kek_name: String,
        subject: String,
        version: i32,
        algorithm: Algorithm,
        encrypted_key_material: Option<String>,
        key_material: Option<String>,
        ts: i64,
        deleted: Option<bool>,
    ) -> Dek {
        Dek {
            kek_name,
            subject,
            version,
            algorithm,
            encrypted_key_material,
            encrypted_key_material_bytes: None,
            key_material,
            key_material_bytes: None,
            ts,
            deleted,
        }
    }

    pub fn populate_key_material_bytes(&mut self) {
        self.get_encrypted_key_material_bytes();
        self.get_key_material_bytes();
    }

    pub fn get_encrypted_key_material_bytes(&mut self) -> Option<&Vec<u8>> {
        self.encrypted_key_material.as_ref()?;
        if self.encrypted_key_material_bytes.is_none() {
            let bytes = BASE64_STANDARD
                .decode(self.encrypted_key_material.as_ref().unwrap())
                .unwrap();
            self.encrypted_key_material_bytes = Some(bytes);
        }
        self.encrypted_key_material_bytes.as_ref()
    }

    pub fn get_key_material_bytes(&mut self) -> Option<&Vec<u8>> {
        self.key_material.as_ref()?;
        if self.key_material_bytes.is_none() {
            let bytes = BASE64_STANDARD
                .decode(self.key_material.as_ref().unwrap())
                .unwrap();
            self.key_material_bytes = Some(bytes);
        }
        self.key_material_bytes.as_ref()
    }

    pub fn set_key_material(&mut self, key_material_bytes: &[u8]) {
        self.key_material = Some(BASE64_STANDARD.encode(key_material_bytes));
    }
}
/// Algorithm of the dek
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Default,
)]
pub enum Algorithm {
    #[serde(rename = "AES128_GCM")]
    Aes128Gcm,
    #[serde(rename = "AES256_GCM")]
    #[default]
    Aes256Gcm,
    #[serde(rename = "AES256_SIV")]
    Aes256Siv,
}
