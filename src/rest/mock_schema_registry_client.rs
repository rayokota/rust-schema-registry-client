use crate::rest::apis::Error::ResponseError;
use crate::rest::apis::error_message::ErrorMessage;
use crate::rest::apis::{Error, ResponseContent};
use crate::rest::client_config;
use crate::rest::models::{RegisteredSchema, Schema, ServerConfig};
use crate::rest::schema_registry_client::Client;
use reqwest::StatusCode;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Clone)]
pub struct MockSchemaRegistryClient {
    store: Arc<Mutex<SchemaStore>>,
    config: client_config::ClientConfig,
}

impl Client for MockSchemaRegistryClient {
    fn new(config: client_config::ClientConfig) -> Self {
        MockSchemaRegistryClient {
            store: Arc::new(Mutex::new(SchemaStore::new())),
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

    fn clear_latest_caches(&self) {}

    fn clear_caches(&self) {
        self.store.lock().unwrap().clear();
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
        if let Some(rs_metadata) = &rs.metadata {
            if let Some(rs_props) = &rs_metadata.properties {
                props = rs_props;
            }
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
