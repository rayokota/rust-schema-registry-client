use serde::{Deserialize, Serialize};

/// Metadata : User-defined metadata
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Metadata {
    #[serde(rename = "tags", skip_serializing_if = "Option::is_none")]
    pub tags: Option<std::collections::BTreeMap<String, Vec<String>>>,
    #[serde(rename = "properties", skip_serializing_if = "Option::is_none")]
    pub properties: Option<std::collections::BTreeMap<String, String>>,
    #[serde(rename = "sensitive", skip_serializing_if = "Option::is_none")]
    pub sensitive: Option<Vec<String>>,
}

impl Metadata {
    /// User-defined metadata
    pub fn new() -> Metadata {
        Metadata {
            tags: None,
            properties: None,
            sensitive: None,
        }
    }
}
