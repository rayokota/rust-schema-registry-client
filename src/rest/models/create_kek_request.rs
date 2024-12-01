use serde::{Deserialize, Serialize};

/// CreateKekRequest : Create kek request
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateKekRequest {
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
}

impl CreateKekRequest {
    /// Create kek request
    pub fn new(
        name: String,
        kms_type: String,
        kms_key_id: String,
        kms_props: Option<std::collections::HashMap<String, String>>,
        doc: Option<String>,
        shared: bool,
    ) -> CreateKekRequest {
        CreateKekRequest {
            name,
            kms_type,
            kms_key_id,
            kms_props,
            doc,
            shared,
        }
    }
}
