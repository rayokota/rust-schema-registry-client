use crate::rest::apis::Error::ResponseError;
use crate::rest::apis::error_message::ErrorMessage;
use crate::rest::apis::{Error, ResponseContent};
use crate::rest::client_config;
use crate::rest::models::{
    Association, AssociationCreateOrUpdateRequest, AssociationInfo, AssociationResponse,
    RegisteredSchema, Schema, ServerConfig,
};
use crate::rest::schema_registry_client::Client;
use reqwest::StatusCode;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct MockSchemaRegistryClient {
    store: Arc<Mutex<SchemaStore>>,
    association_store: Arc<Mutex<AssociationStore>>,
    config: client_config::ClientConfig,
}

impl Client for MockSchemaRegistryClient {
    fn new(config: client_config::ClientConfig) -> Self {
        MockSchemaRegistryClient {
            store: Arc::new(Mutex::new(SchemaStore::new())),
            association_store: Arc::new(Mutex::new(AssociationStore::new())),
            config,
        }
    }

    fn config(&self) -> &client_config::ClientConfig {
        &self.config
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &Schema,
        _normalize: bool,
    ) -> Result<RegisteredSchema, Error> {
        let mut store = self.store.lock().unwrap();
        let registered_schema = store.get_registered_by_schema(subject, schema);
        if let Some(rs) = registered_schema {
            return Ok(rs.clone());
        }

        let latest_schema = store.get_latest_version(subject);
        let version = latest_schema.map_or(1, |rs| rs.version.unwrap_or_default() + 1);

        let registered_schema = RegisteredSchema {
            id: Some(1),
            guid: Some(Uuid::new_v4().to_string()),
            subject: Some(subject.to_string()),
            version: Some(version),
            schema_type: schema.schema_type.clone(),
            references: schema.references.clone(),
            metadata: schema.metadata.clone(),
            rule_set: schema.rule_set.clone(),
            schema: Some(schema.schema.clone()),
        };

        store.set_registered_schema(&registered_schema);

        Ok(registered_schema)
    }

    async fn get_by_subject_and_id(
        &self,
        _subject: Option<&str>,
        id: i32,
        _format: Option<&str>,
    ) -> Result<Schema, Error> {
        let store = self.store.lock().unwrap();
        let schema = store.get_schema_by_id(id);
        schema.ok_or(ResponseError(ResponseContent {
            status: StatusCode::NOT_FOUND,
            content: String::new(),
            entity: Some(ErrorMessage {
                error_code: Some(40400),
                message: Some("schema not found".to_string()),
            }),
        }))
    }

    async fn get_by_guid(&self, guid: &str, _format: Option<&str>) -> Result<Schema, Error> {
        let store = self.store.lock().unwrap();
        let schema = store.get_schema_by_guid(guid);
        schema.ok_or(ResponseError(ResponseContent {
            status: StatusCode::NOT_FOUND,
            content: String::new(),
            entity: Some(ErrorMessage {
                error_code: Some(40400),
                message: Some("schema not found".to_string()),
            }),
        }))
    }
    async fn get_by_schema(
        &self,
        subject: &str,
        schema: &Schema,
        _normalize: bool,
        _deleted: bool,
    ) -> Result<RegisteredSchema, Error> {
        let store = self.store.lock().unwrap();
        let rs = store.get_registered_by_schema(subject, schema);
        rs.ok_or(ResponseError(ResponseContent {
            status: StatusCode::NOT_FOUND,
            content: String::new(),
            entity: Some(ErrorMessage {
                error_code: Some(40400),
                message: Some("schema not found".to_string()),
            }),
        }))
        .cloned()
    }

    async fn get_version(
        &self,
        subject: &str,
        version: i32,
        _deleted: bool,
        _format: Option<&str>,
    ) -> Result<RegisteredSchema, Error> {
        let store = self.store.lock().unwrap();
        let rs = store.get_registered_by_version(subject, version);
        rs.ok_or(ResponseError(ResponseContent {
            status: StatusCode::NOT_FOUND,
            content: String::new(),
            entity: Some(ErrorMessage {
                error_code: Some(40400),
                message: Some("schema not found".to_string()),
            }),
        }))
        .cloned()
    }

    async fn get_latest_version(
        &self,
        subject: &str,
        _format: Option<&str>,
    ) -> Result<RegisteredSchema, Error> {
        let store = self.store.lock().unwrap();
        let rs = store.get_latest_version(subject);
        rs.ok_or(ResponseError(ResponseContent {
            status: StatusCode::NOT_FOUND,
            content: String::new(),
            entity: Some(ErrorMessage {
                error_code: Some(40400),
                message: Some("schema not found".to_string()),
            }),
        }))
        .cloned()
    }

    async fn get_latest_with_metadata(
        &self,
        subject: &str,
        metadata: &HashMap<String, String>,
        _deleted: bool,
        _format: Option<&str>,
    ) -> Result<RegisteredSchema, Error> {
        let store = self.store.lock().unwrap();
        let rs = store.get_latest_with_metadata(subject, metadata);
        rs.ok_or(ResponseError(ResponseContent {
            status: StatusCode::NOT_FOUND,
            content: String::new(),
            entity: Some(ErrorMessage {
                error_code: Some(40400),
                message: Some("schema not found".to_string()),
            }),
        }))
        .cloned()
    }

    async fn get_all_versions(&self, subject: &str) -> Result<Vec<i32>, Error> {
        let store = self.store.lock().unwrap();
        Ok(store.get_versions(subject))
    }

    async fn get_all_subjects(&self, _deleted: bool) -> Result<Vec<String>, Error> {
        let store = self.store.lock().unwrap();
        Ok(store.get_subjects())
    }

    async fn delete_subject(&self, subject: &str, _permanent: bool) -> Result<Vec<i32>, Error> {
        let mut store = self.store.lock().unwrap();
        Ok(store.remove_by_subject(subject))
    }

    async fn delete_subject_version(
        &self,
        subject: &str,
        version: i32,
        _permanent: bool,
    ) -> Result<i32, Error> {
        let mut store = self.store.lock().unwrap();
        if let Some(rs) = store.get_registered_by_version(subject, version).cloned() {
            store.remove_by_schema(subject, &rs);
            return Ok(rs.version.unwrap_or_default());
        }
        Err(ResponseError(ResponseContent {
            status: StatusCode::NOT_FOUND,
            content: String::new(),
            entity: Some(ErrorMessage {
                error_code: Some(40400),
                message: Some("schema not found".to_string()),
            }),
        }))
    }

    async fn test_subject_compatibility(
        &self,
        _subject: &str,
        _schema: &Schema,
    ) -> Result<bool, Error> {
        todo!()
    }

    async fn test_compatibility(
        &self,
        _subject: &str,
        _version: i32,
        _schema: &Schema,
    ) -> Result<bool, Error> {
        todo!()
    }

    async fn get_config(&self, subject: &str) -> Result<ServerConfig, Error> {
        Ok(ServerConfig::new())
    }
    async fn update_config(
        &self,
        subject: &str,
        config: &ServerConfig,
    ) -> Result<ServerConfig, Error> {
        Ok(ServerConfig::new())
    }
    async fn get_default_config(&self) -> Result<ServerConfig, Error> {
        Ok(ServerConfig::new())
    }
    async fn update_default_config(&self, config: &ServerConfig) -> Result<ServerConfig, Error> {
        Ok(ServerConfig::new())
    }

    async fn get_associations_by_resource_name(
        &self,
        resource_name: &str,
        resource_namespace: &str,
        resource_type: &str,
        association_types: &[&str],
        _lifecycle: &str,
        offset: i32,
        limit: i32,
    ) -> Result<Vec<Association>, Error> {
        let store = self.association_store.lock().unwrap();
        let associations = store.get_associations_by_resource_name(
            resource_name,
            resource_namespace,
            resource_type,
            association_types,
        );
        // Apply pagination
        let start = offset as usize;
        let end = if limit >= 1 {
            std::cmp::min(start + limit as usize, associations.len())
        } else {
            associations.len()
        };
        if start >= associations.len() {
            return Ok(Vec::new());
        }
        Ok(associations[start..end].to_vec())
    }

    async fn create_association(
        &self,
        request: &AssociationCreateOrUpdateRequest,
    ) -> Result<AssociationResponse, Error> {
        let mut store = self.association_store.lock().unwrap();
        Ok(store.create_association(request))
    }

    async fn delete_associations(
        &self,
        resource_id: &str,
        resource_type: Option<&str>,
        association_types: Option<&[&str]>,
        _cascade_lifecycle: bool,
    ) -> Result<(), Error> {
        let mut store = self.association_store.lock().unwrap();
        store.delete_associations(resource_id, resource_type, association_types);
        Ok(())
    }

    fn clear_latest_caches(&self) {}

    fn clear_caches(&self) {
        self.store.lock().unwrap().clear();
        self.association_store.lock().unwrap().clear();
    }

    fn close(&mut self) {}
}

struct SchemaStore {
    schemas: HashMap<String, Vec<RegisteredSchema>>,
    schema_id_index: HashMap<i32, RegisteredSchema>,
    schema_guid_index: HashMap<String, RegisteredSchema>,
    schema_index: HashMap<String, RegisteredSchema>,
}

impl SchemaStore {
    pub fn new() -> Self {
        SchemaStore {
            schemas: HashMap::new(),
            schema_id_index: HashMap::new(),
            schema_guid_index: HashMap::new(),
            schema_index: HashMap::new(),
        }
    }

    pub fn set_registered_schema(&mut self, schema: &RegisteredSchema) {
        let subject = schema.subject.clone().unwrap_or_default();
        if let Some(id) = schema.id {
            self.schema_id_index.insert(id, schema.clone());
        }
        if let Some(guid) = &schema.guid {
            self.schema_guid_index.insert(guid.clone(), schema.clone());
        }
        self.schema_index.insert(subject.clone(), schema.clone());
        match self.schemas.get_mut(&subject) {
            Some(schemas) => schemas.push(schema.clone()),
            None => {
                self.schemas.insert(subject, vec![schema.clone()]);
            }
        }
    }

    pub fn get_schema_by_id(&self, schema_id: i32) -> Option<Schema> {
        let rs = self.schema_id_index.get(&schema_id);
        rs.map(|rs| rs.to_schema())
    }

    pub fn get_schema_by_guid(&self, guid: &str) -> Option<Schema> {
        let rs = self.schema_guid_index.get(guid);
        rs.map(|rs| rs.to_schema())
    }

    pub fn get_registered_by_schema(
        &self,
        subject: &str,
        schema: &Schema,
    ) -> Option<&RegisteredSchema> {
        if let Some(schemas) = self.schemas.get(subject) {
            return schemas
                .iter()
                .find(|rs| rs.schema.clone().unwrap_or_default() == schema.schema);
        }
        None
    }

    pub fn get_registered_by_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Option<&RegisteredSchema> {
        if let Some(schemas) = self.schemas.get(subject) {
            return schemas
                .iter()
                .find(|rs| rs.version.unwrap_or_default() == version);
        }
        None
    }

    pub fn get_latest_version(&self, subject: &str) -> Option<&RegisteredSchema> {
        if let Some(schemas) = self.schemas.get(subject) {
            return schemas.iter().max_by_key(|rs| rs.version);
        }
        None
    }

    pub fn get_latest_with_metadata(
        &self,
        subject: &str,
        metadata: &HashMap<String, String>,
    ) -> Option<&RegisteredSchema> {
        if let Some(schemas) = self.schemas.get(subject) {
            return schemas.iter().find(|rs| self.has_metadata(metadata, rs));
        }
        None
    }

    fn has_metadata(&self, metadata: &HashMap<String, String>, rs: &RegisteredSchema) -> bool {
        let mut props = &BTreeMap::new();
        if let Some(rs_metadata) = &rs.metadata
            && let Some(rs_props) = &rs_metadata.properties
        {
            props = rs_props;
        }
        !metadata.iter().any(|(k, v)| props.get(k) != Some(v))
    }

    pub fn get_subjects(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    pub fn get_versions(&self, subject: &str) -> Vec<i32> {
        if let Some(schemas) = self.schemas.get(subject) {
            return schemas
                .iter()
                .map(|rs| rs.version.unwrap_or_default())
                .collect();
        }
        Vec::new()
    }

    pub fn remove_by_schema(&mut self, subject: &str, registered_schema: &RegisteredSchema) {
        if let Some(schemas) = self.schemas.get_mut(subject) {
            schemas.retain(|rs| rs.schema != registered_schema.schema)
        }
    }

    pub fn remove_by_subject(&mut self, subject: &str) -> Vec<i32> {
        let mut versions = Vec::new();
        if let Some(schemas) = self.schemas.remove(subject) {
            for rs in schemas {
                versions.push(rs.version.unwrap_or_default());
                if let Some(id) = rs.id {
                    self.schema_id_index.remove(&id);
                }
                self.schema_index.remove(&rs.schema.unwrap_or_default());
            }
        }
        versions
    }

    pub fn clear(&mut self) {
        self.schemas.clear();
        self.schema_id_index.clear();
        self.schema_guid_index.clear();
        self.schema_index.clear();
    }
}

/// AssociationCacheEntry stores associations for a resource.
struct AssociationCacheEntry {
    resource_name: String,
    resource_namespace: String,
    resource_type: String,
    associations: Vec<Association>,
}

/// AssociationStore stores associations keyed by resource_id.
struct AssociationStore {
    associations_by_resource_id: HashMap<String, AssociationCacheEntry>,
}

impl AssociationStore {
    pub fn new() -> Self {
        AssociationStore {
            associations_by_resource_id: HashMap::new(),
        }
    }

    pub fn create_association(
        &mut self,
        request: &AssociationCreateOrUpdateRequest,
    ) -> AssociationResponse {
        let resource_id = request
            .resource_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let resource_name = request.resource_name.clone().unwrap_or_default();
        let resource_namespace = request
            .resource_namespace
            .clone()
            .unwrap_or_else(|| "-".to_string());
        let resource_type = request
            .resource_type
            .clone()
            .unwrap_or_else(|| "topic".to_string());

        let entry = self
            .associations_by_resource_id
            .entry(resource_id.clone())
            .or_insert_with(|| AssociationCacheEntry {
                resource_name: resource_name.clone(),
                resource_namespace: resource_namespace.clone(),
                resource_type: resource_type.clone(),
                associations: Vec::new(),
            });

        let mut response_associations = Vec::new();

        if let Some(assoc_infos) = &request.associations {
            for assoc_info in assoc_infos {
                let association = Association {
                    subject: assoc_info.subject.clone(),
                    guid: None,
                    resource_name: Some(resource_name.clone()),
                    resource_namespace: Some(resource_namespace.clone()),
                    resource_id: Some(resource_id.clone()),
                    resource_type: Some(resource_type.clone()),
                    association_type: assoc_info.association_type.clone(),
                    lifecycle: assoc_info.lifecycle.clone(),
                    frozen: assoc_info.frozen,
                };

                // Check if association already exists
                let existing_idx = entry.associations.iter().position(|a| {
                    a.subject == association.subject
                        && a.association_type == association.association_type
                });

                if let Some(idx) = existing_idx {
                    entry.associations[idx] = association.clone();
                } else {
                    entry.associations.push(association.clone());
                }

                response_associations.push(AssociationInfo {
                    subject: association.subject,
                    association_type: association.association_type,
                    lifecycle: association.lifecycle,
                    frozen: association.frozen,
                    schema: None,
                });
            }
        }

        AssociationResponse {
            resource_name: Some(resource_name),
            resource_namespace: Some(resource_namespace),
            resource_id: Some(resource_id),
            resource_type: Some(resource_type),
            associations: Some(response_associations),
        }
    }

    pub fn get_associations_by_resource_name(
        &self,
        resource_name: &str,
        resource_namespace: &str,
        resource_type: &str,
        association_types: &[&str],
    ) -> Vec<Association> {
        let mut results = Vec::new();

        for entry in self.associations_by_resource_id.values() {
            if entry.resource_name == resource_name
                && entry.resource_namespace == resource_namespace
            {
                if !resource_type.is_empty() && entry.resource_type != resource_type {
                    continue;
                }
                for assoc in &entry.associations {
                    if association_types.is_empty()
                        || assoc
                            .association_type
                            .as_ref()
                            .map_or(false, |at| association_types.contains(&at.as_str()))
                    {
                        results.push(assoc.clone());
                    }
                }
            }
        }

        results
    }

    pub fn delete_associations(
        &mut self,
        resource_id: &str,
        resource_type: Option<&str>,
        association_types: Option<&[&str]>,
    ) {
        if let Some(entry) = self.associations_by_resource_id.get_mut(resource_id) {
            if let Some(rt) = resource_type {
                if entry.resource_type != rt {
                    return;
                }
            }

            if let Some(types) = association_types {
                if types.is_empty() {
                    self.associations_by_resource_id.remove(resource_id);
                } else {
                    entry.associations.retain(|a| {
                        !a.association_type
                            .as_ref()
                            .map_or(false, |at| types.contains(&at.as_str()))
                    });

                    if entry.associations.is_empty() {
                        self.associations_by_resource_id.remove(resource_id);
                    }
                }
            } else {
                self.associations_by_resource_id.remove(resource_id);
            }
        }
    }

    pub fn clear(&mut self) {
        self.associations_by_resource_id.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_subjects() {
        let config = client_config::ClientConfig::default();
        let client = MockSchemaRegistryClient::new(config);
        let subjects = client.get_all_subjects(false).await.ok();
        println!("{:?}", subjects);
    }
}
