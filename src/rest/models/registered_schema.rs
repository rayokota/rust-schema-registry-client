use crate::rest::models;
use serde::{Deserialize, Serialize};

/// RegisteredSchema : Registered schema
#[derive(Clone, Default, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RegisteredSchema {
    /// Unique identifier of the schema
    #[serde(rename = "id")]
    pub id: Option<i32>,
    /// Globally unique identifier of the schema
    #[serde(rename = "guid")]
    pub guid: Option<String>,
    /// Subject
    #[serde(rename = "subject", skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    /// Version number
    #[serde(rename = "version", skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
    /// Schema type
    #[serde(rename = "schemaType", skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<String>,
    /// References to other schemas
    #[serde(rename = "references", skip_serializing_if = "Option::is_none")]
    pub references: Option<Vec<models::SchemaReference>>,
    #[serde(rename = "metadata", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Box<models::Metadata>>,
    #[serde(rename = "ruleSet", skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<Box<models::RuleSet>>,
    /// Schema definition string
    #[serde(rename = "schema", skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
}

impl RegisteredSchema {
    pub fn new(id: i32) -> RegisteredSchema {
        RegisteredSchema {
            id: Some(id),
            guid: None,
            subject: None,
            version: None,
            schema_type: None,
            references: None,
            metadata: None,
            rule_set: None,
            schema: None,
        }
    }

    pub fn to_schema(&self) -> models::Schema {
        models::Schema {
            schema_type: self.schema_type.clone(),
            references: self.references.clone(),
            metadata: self.metadata.clone(),
            rule_set: self.rule_set.clone(),
            schema: self.schema.clone().unwrap_or_default(),
        }
    }
}
