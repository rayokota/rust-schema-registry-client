use crate::rest::models::dek::Algorithm;
use serde::{Deserialize, Serialize};

/// CreateDekRequest : Create dek request
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateDekRequest {
    /// Subject of the dek
    #[serde(rename = "subject")]
    pub subject: String,
    /// Version of the dek
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
    /// Algorithm of the dek
    #[serde(rename = "algorithm", skip_serializing_if = "Option::is_none")]
    pub algorithm: Option<Algorithm>,
    /// Encrypted key material of the dek
    #[serde(
        rename = "encryptedKeyMaterial",
        skip_serializing_if = "Option::is_none"
    )]
    pub encrypted_key_material: Option<String>,
}

impl CreateDekRequest {
    /// Create dek request
    pub fn new(
        subject: String,
        version: Option<i32>,
        algorithm: Option<Algorithm>,
        encrypted_key_material: Option<String>,
    ) -> CreateDekRequest {
        CreateDekRequest {
            subject,
            version,
            algorithm,
            encrypted_key_material,
        }
    }
}
