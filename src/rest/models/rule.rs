use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

/// Rule : Rule
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Rule {
    /// Rule name
    #[serde(rename = "name")]
    pub name: String,
    /// Rule doc
    #[serde(rename = "doc", skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
    /// Rule kind
    #[serde(rename = "kind", skip_serializing_if = "Option::is_none")]
    pub kind: Option<Kind>,
    /// Rule mode
    #[serde(rename = "mode", skip_serializing_if = "Option::is_none")]
    pub mode: Option<Mode>,
    /// Rule type
    #[serde(rename = "type")]
    pub r#type: String,
    /// The tags to which this rule applies
    #[serde(rename = "tags", skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    /// Optional params for the rule
    #[serde(rename = "params", skip_serializing_if = "Option::is_none")]
    pub params: Option<std::collections::BTreeMap<String, String>>,
    /// Rule expression
    #[serde(rename = "expr", skip_serializing_if = "Option::is_none")]
    pub expr: Option<String>,
    /// Rule action on success
    #[serde(rename = "onSuccess", skip_serializing_if = "Option::is_none")]
    pub on_success: Option<String>,
    /// Rule action on failure
    #[serde(rename = "onFailure", skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<String>,
    /// Whether the rule is disabled
    #[serde(rename = "disabled", skip_serializing_if = "Option::is_none")]
    pub disabled: Option<bool>,
}

impl Rule {
    /// Rule
    pub fn new(name: &str, r#type: &str) -> Rule {
        Rule {
            name: name.to_string(),
            doc: None,
            kind: None,
            mode: None,
            r#type: r#type.to_string(),
            tags: None,
            params: None,
            expr: None,
            on_success: None,
            on_failure: None,
            disabled: None,
        }
    }
}

impl Display for Rule {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Rule {}", self.name)
    }
}

/// Rule kind
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Kind {
    #[serde(rename = "TRANSFORM")]
    Transform,
    #[serde(rename = "CONDITION")]
    Condition,
}

impl Default for Kind {
    fn default() -> Kind {
        Self::Transform
    }
}
/// Rule phase
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Phase {
    #[serde(rename = "MIGRATION")]
    Migration,
    #[serde(rename = "DOMAIN")]
    Domain,
    #[serde(rename = "ENCODING")]
    Encoding,
}

/// Rule mode
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum Mode {
    #[serde(rename = "UPGRADE")]
    Upgrade,
    #[serde(rename = "DOWNGRADE")]
    Downgrade,
    #[serde(rename = "UPDOWN")]
    UpDown,
    #[serde(rename = "WRITE")]
    Write,
    #[serde(rename = "READ")]
    Read,
    #[serde(rename = "WRITEREAD")]
    WriteRead,
}

impl Default for Mode {
    fn default() -> Mode {
        Self::WriteRead
    }
}
