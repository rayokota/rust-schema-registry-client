use crate::rest::models;
use serde::{Deserialize, Serialize};

/// RuleSet : Schema rule set
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct RuleSet {
    #[serde(rename = "migrationRules", skip_serializing_if = "Option::is_none")]
    pub migration_rules: Option<Vec<models::Rule>>,
    #[serde(rename = "domainRules", skip_serializing_if = "Option::is_none")]
    pub domain_rules: Option<Vec<models::Rule>>,
    #[serde(rename = "encodingRules", skip_serializing_if = "Option::is_none")]
    pub encoding_rules: Option<Vec<models::Rule>>,
    #[serde(rename = "enableAt", skip_serializing_if = "Option::is_none")]
    pub enable_at: Option<String>,
}

impl RuleSet {
    /// Schema rule set
    pub fn new() -> RuleSet {
        RuleSet {
            migration_rules: None,
            domain_rules: None,
            encoding_rules: None,
            enable_at: None,
        }
    }
}
