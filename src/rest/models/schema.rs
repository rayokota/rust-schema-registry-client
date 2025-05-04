use crate::rest::models;
use serde::{Deserialize, Serialize};

/// Schema : Schema
#[derive(Clone, Default, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Schema {
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
    #[serde(rename = "schema")]
    pub schema: String,
}

impl Schema {
    /// Schema
    pub fn new(schema_type: Option<String>, schema: String) -> Schema {
        Schema {
            schema_type,
            references: None,
            metadata: None,
            rule_set: None,
            schema,
        }
    }

    pub fn to_registered_schema(
        &self,
        id: Option<i32>,
        guid: Option<String>,
        subject: Option<String>,
        version: Option<i32>,
    ) -> models::RegisteredSchema {
        models::RegisteredSchema {
            id,
            guid,
            subject,
            version,
            schema_type: self.schema_type.clone(),
            references: self.references.clone(),
            metadata: self.metadata.clone(),
            rule_set: self.rule_set.clone(),
            schema: Some(self.schema.clone()),
        }
    }
}
