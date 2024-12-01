use serde::{Deserialize, Serialize};

/// Kek : Kek
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct Kek {
    /// Name of the kek
    #[serde(rename = "name")]
    pub name: String,
    /// KMS type of the kek
    #[serde(rename = "kmsType")]
    pub kms_type: String,
    /// KMS key ID of the kek
    #[serde(rename = "kmsKeyId")]
    pub kms_key_id: String,
    /// Properties of the kek
    #[serde(rename = "kmsProps", skip_serializing_if = "Option::is_none")]
    pub kms_props: Option<std::collections::HashMap<String, String>>,
    /// Description of the kek
    #[serde(rename = "doc", skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Whether the kek is shared
    #[serde(rename = "shared")]
    pub shared: bool,
    /// Timestamp of the kek
    #[serde(rename = "ts")]
    pub ts: i64,
    /// Whether the kek is deleted
    #[serde(rename = "deleted", skip_serializing_if = "Option::is_none")]
    pub deleted: Option<bool>,
}

impl Kek {
    /// Kek
    pub fn new(
        name: String,
        kms_type: String,
        kms_key_id: String,
        kms_props: Option<std::collections::HashMap<String, String>>,
        doc: Option<String>,
        shared: bool,
        ts: i64,
        deleted: Option<bool>,
    ) -> Kek {
        Kek {
            name,
            kms_type,
            kms_key_id,
            kms_props,
            doc,
            shared,
            ts,
            deleted,
        }
    }
}
