use crate::rest::models;
use serde::{Deserialize, Serialize};

/// Config : Config
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(rename = "compatibility", skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<CompatibilityLevel>,
    #[serde(rename = "compatibilityLevel", skip_serializing_if = "Option::is_none")]
    pub compatibility_level: Option<CompatibilityLevel>,
    #[serde(rename = "alias", skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(rename = "normalize", skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
    #[serde(rename = "validateFields", skip_serializing_if = "Option::is_none")]
    pub validate_fields: Option<bool>,
    #[serde(rename = "validateRules", skip_serializing_if = "Option::is_none")]
    pub validate_rules: Option<bool>,
    #[serde(rename = "compatibilityGroup", skip_serializing_if = "Option::is_none")]
    pub compatibility_group: Option<String>,
    #[serde(rename = "defaultMetadata", skip_serializing_if = "Option::is_none")]
    pub default_metadata: Option<Box<models::Metadata>>,
    #[serde(rename = "overrideMetadata", skip_serializing_if = "Option::is_none")]
    pub override_metadata: Option<Box<models::Metadata>>,
    #[serde(rename = "defaultRuleSet", skip_serializing_if = "Option::is_none")]
    pub default_rule_set: Option<Box<models::RuleSet>>,
    #[serde(rename = "overrideRuleSet", skip_serializing_if = "Option::is_none")]
    pub override_rule_set: Option<Box<models::RuleSet>>,
}

impl ServerConfig {
    /// Config
    pub fn new() -> ServerConfig {
        ServerConfig {
            compatibility: None,
            compatibility_level: None,
            alias: None,
            normalize: None,
            validate_fields: None,
            validate_rules: None,
            compatibility_group: None,
            default_metadata: None,
            override_metadata: None,
            default_rule_set: None,
            override_rule_set: None,
        }
    }
}
/// Compatibility Level
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Default,
)]
pub enum CompatibilityLevel {
    #[serde(rename = "BACKWARD")]
    #[default]
    Backward,
    #[serde(rename = "BACKWARD_TRANSITIVE")]
    BackwardTransitive,
    #[serde(rename = "FORWARD")]
    Forward,
    #[serde(rename = "FORWARD_TRANSITIVE")]
    ForwardTransitive,
    #[serde(rename = "FULL")]
    Full,
    #[serde(rename = "FULL_TRANSITIVE")]
    FullTransitive,
    #[serde(rename = "NONE")]
    None,
}
