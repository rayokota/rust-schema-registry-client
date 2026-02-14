use crate::rest::models::Schema;
use serde::{Deserialize, Serialize};

/// LifecyclePolicy represents the lifecycle policy for an association.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LifecyclePolicy {
    #[serde(rename = "STRONG")]
    Strong,
    #[serde(rename = "WEAK")]
    Weak,
}

/// Association represents an association between a subject and a resource.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Association {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub association_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<LifecyclePolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frozen: Option<bool>,
}

/// AssociationInfo represents association info returned in a response.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssociationInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub association_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<LifecyclePolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frozen: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
}

/// AssociationCreateOrUpdateInfo represents an association to create or update.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssociationCreateOrUpdateInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub association_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<LifecyclePolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frozen: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalize: Option<bool>,
}

/// AssociationCreateOrUpdateRequest represents a request to create or update associations.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssociationCreateOrUpdateRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub associations: Option<Vec<AssociationCreateOrUpdateInfo>>,
}

/// AssociationResponse represents a response from creating/updating associations.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssociationResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_namespace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub associations: Option<Vec<AssociationInfo>>,
}
