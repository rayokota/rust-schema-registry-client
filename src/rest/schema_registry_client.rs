use crate::rest::apis::{Error, ResponseContent, urlencode};
use crate::rest::models::{RegisteredSchema, Schema, ServerConfig};
use crate::rest::{client_config, rest_service};
use mini_moka::sync::Cache;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[trait_variant::make(Send)]
pub trait Client {
    fn new(config: client_config::ClientConfig) -> Self;
    fn config(&self) -> &client_config::ClientConfig;
    async fn register_schema(
        &self,
        subject: &str,
        schema: &Schema,
        normalize: bool,
    ) -> Result<RegisteredSchema, Error>;
    async fn get_by_subject_and_id(
        &self,
        subject: Option<&str>,
        id: i32,
        format: Option<&str>,
    ) -> Result<Schema, Error>;
    async fn get_by_guid(&self, guid: &str, format: Option<&str>) -> Result<Schema, Error>;
    async fn get_by_schema(
        &self,
        subject: &str,
        schema: &Schema,
        normalize: bool,
        deleted: bool,
    ) -> Result<RegisteredSchema, Error>;
    async fn get_version(
        &self,
        subject: &str,
        version: i32,
        deleted: bool,
        format: Option<&str>,
    ) -> Result<RegisteredSchema, Error>;
    async fn get_latest_version(
        &self,
        subject: &str,
        format: Option<&str>,
    ) -> Result<RegisteredSchema, Error>;
    async fn get_latest_with_metadata(
        &self,
        subject: &str,
        metadata: &HashMap<String, String>,
        deleted: bool,
        format: Option<&str>,
    ) -> Result<RegisteredSchema, Error>;
    async fn get_all_versions(&self, subject: &str) -> Result<Vec<i32>, Error>;
    async fn get_all_subjects(&self, deleted: bool) -> Result<Vec<String>, Error>;
    async fn delete_subject(&self, subject: &str, permanent: bool) -> Result<Vec<i32>, Error>;
    async fn delete_subject_version(
        &self,
        subject: &str,
        version: i32,
        permanent: bool,
    ) -> Result<i32, Error>;
    async fn test_subject_compatibility(
        &self,
        subject: &str,
        schema: &Schema,
    ) -> Result<bool, Error>;
    async fn test_compatibility(
        &self,
        subject: &str,
        version: i32,
        schema: &Schema,
    ) -> Result<bool, Error>;
    async fn get_config(&self, subject: &str) -> Result<ServerConfig, Error>;
    async fn update_config(
        &self,
        subject: &str,
        config: &ServerConfig,
    ) -> Result<ServerConfig, Error>;
    async fn get_default_config(&self) -> Result<ServerConfig, Error>;
    async fn update_default_config(&self, config: &ServerConfig) -> Result<ServerConfig, Error>;
    fn clear_latest_caches(&self);
    fn clear_caches(&self);
    fn close(&mut self);
}

#[derive(Clone)]
pub struct SchemaRegistryClient {
    store: Arc<Mutex<SchemaStore>>,
    latest_version_cache: Cache<String, RegisteredSchema>,
    latest_with_metadata_cache: Cache<(String, BTreeMap<String, String>), RegisteredSchema>,
    rest_service: rest_service::RestService,
}

impl SchemaRegistryClient {}

impl Client for SchemaRegistryClient {
    fn new(config: client_config::ClientConfig) -> Self {
        if config.base_urls.is_empty() {
            panic!("base URL is required");
        }
        SchemaRegistryClient {
            store: Arc::new(Mutex::new(SchemaStore::new())),
            latest_version_cache: Cache::builder()
                .max_capacity(config.cache_capacity)
                .time_to_live(std::time::Duration::from_secs(config.cache_latest_ttl_sec))
                .build(),
            latest_with_metadata_cache: Cache::builder()
                .max_capacity(config.cache_capacity)
                .time_to_live(std::time::Duration::from_secs(config.cache_latest_ttl_sec))
                .build(),
            rest_service: rest_service::RestService::new(config),
        }
    }

    fn config(&self) -> &client_config::ClientConfig {
        self.rest_service.config()
    }

    async fn register_schema(
        &self,
        subject: &str,
        schema: &Schema,
        normalize: bool,
    ) -> Result<RegisteredSchema, Error> {
        {
            let store = self.store.lock().unwrap();
            let registered_schema = store.get_registered_by_schema(subject, schema);
            if let Some(rs) = registered_schema {
                return Ok(rs.clone());
            }
        }

        let url = format!("/subjects/{}/versions", urlencode(subject));
        let query = vec![("normalize".to_string(), normalize.to_string())];
        let body = serde_json::to_string(schema)?;
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::POST, Some(&query), Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let rs: RegisteredSchema = serde_json::from_str(&content)?;
            // The registered schema may not be fully populated
            let s = if rs.schema.is_some() {
                &rs.to_schema()
            } else {
                schema
            };
            store.set_schema(Some(subject.to_string()), rs.id, rs.guid.clone(), &s);
            Ok(rs)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_by_subject_and_id(
        &self,
        subject: Option<&str>,
        id: i32,
        format: Option<&str>,
    ) -> Result<Schema, Error> {
        {
            let store = self.store.lock().unwrap();
            let result = store.get_schema_by_id(subject.unwrap_or_default(), id);
            if let Some((_, schema)) = result {
                return Ok(schema.clone());
            }
        }

        let url = format!("/schemas/ids/{}", id);
        let mut query = Vec::new();
        if let Some(subject) = subject {
            query.push(("subject".to_string(), subject.to_string()));
        }
        if let Some(format) = format {
            query.push(("format".to_string(), format.to_string()));
        }
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let rs: RegisteredSchema = serde_json::from_str(&content)?;
            let s: Schema = rs.to_schema();
            store.set_schema(subject.map(|s| s.to_string()), Some(id), rs.guid, &s);
            Ok(s)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_by_guid(&self, guid: &str, format: Option<&str>) -> Result<Schema, Error> {
        {
            let store = self.store.lock().unwrap();
            let schema = store.get_schema_by_guid(guid);
            if let Some(schema) = schema {
                return Ok(schema.clone());
            }
        }

        let url = format!("/schemas/guids/{}", guid);
        let mut query = Vec::new();
        if let Some(format) = format {
            query.push(("format".to_string(), format.to_string()));
        }
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let rs: RegisteredSchema = serde_json::from_str(&content)?;
            let s: Schema = rs.to_schema();
            store.set_schema(None, rs.id, rs.guid, &s);
            Ok(s)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_by_schema(
        &self,
        subject: &str,
        schema: &Schema,
        normalize: bool,
        deleted: bool,
    ) -> Result<RegisteredSchema, Error> {
        {
            let store = self.store.lock().unwrap();
            let rs = store.get_registered_by_schema(subject, schema);
            if let Some(rs) = rs {
                return Ok(rs.clone());
            }
        }

        let url = format!("/subjects/{}", urlencode(subject));
        let query = vec![
            ("normalize".to_string(), normalize.to_string()),
            ("deleted".to_string(), deleted.to_string()),
        ];
        let body = serde_json::to_string(schema)?;
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::POST, Some(&query), Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let result: RegisteredSchema = serde_json::from_str(&content)?;
            // Ensure the schema matches the input
            let rs =
                schema.to_registered_schema(result.id, result.guid, result.subject, result.version);
            store.set_registered_schema(schema, &rs);
            Ok(rs)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_version(
        &self,
        subject: &str,
        version: i32,
        deleted: bool,
        format: Option<&str>,
    ) -> Result<RegisteredSchema, Error> {
        {
            let store = self.store.lock().unwrap();
            let rs = store.get_registered_by_version(subject, version);
            if let Some(rs) = rs {
                return Ok(rs.clone());
            }
        }

        let url = format!("/subjects/{}/versions/{}", urlencode(subject), version);
        let mut query = vec![("deleted".to_string(), deleted.to_string())];
        if let Some(format) = format {
            query.push(("format".to_string(), format.to_string()));
        }
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let rs: RegisteredSchema = serde_json::from_str(&content)?;
            store.set_registered_schema(&rs.to_schema(), &rs);
            Ok(rs)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_latest_version(
        &self,
        subject: &str,
        format: Option<&str>,
    ) -> Result<RegisteredSchema, Error> {
        let cache_key = subject.to_string();
        if let Some(rs) = self.latest_version_cache.get(&cache_key) {
            return Ok(rs.clone());
        }
        let url = format!("/subjects/{}/versions/latest", urlencode(subject));
        let mut query = Vec::new();
        if let Some(format) = format {
            query.push(("format".to_string(), format.to_string()));
        }
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let rs: RegisteredSchema = serde_json::from_str(&content)?;
            self.latest_version_cache.insert(cache_key, rs.clone());
            Ok(rs)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_latest_with_metadata(
        &self,
        subject: &str,
        metadata: &HashMap<String, String>,
        deleted: bool,
        format: Option<&str>,
    ) -> Result<RegisteredSchema, Error> {
        let cache_key = (
            subject.to_string(),
            BTreeMap::from_iter(metadata.iter().map(|(k, v)| (k.clone(), v.clone()))),
        );
        if let Some(rs) = self.latest_with_metadata_cache.get(&cache_key) {
            return Ok(rs.clone());
        }
        let url = format!("/subjects/{}/metadata", urlencode(subject));
        let mut query = vec![("deleted".to_string(), deleted.to_string())];
        if let Some(format) = format {
            query.push(("format".to_string(), format.to_string()));
        }
        for (k, v) in metadata {
            query.push(("key".to_string(), k.to_string()));
            query.push(("value".to_string(), v.to_string()));
        }
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let rs: RegisteredSchema = serde_json::from_str(&content)?;
            self.latest_with_metadata_cache
                .insert(cache_key, rs.clone());
            Ok(rs)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_all_versions(&self, subject: &str) -> Result<Vec<i32>, Error> {
        let url = format!("/subjects/{}/versions", urlencode(subject));
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, None, None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            Ok(serde_json::from_str(&content)?)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_all_subjects(&self, deleted: bool) -> Result<Vec<String>, Error> {
        let url = "/subjects";
        let query = vec![("deleted".to_string(), deleted.to_string())];
        let resp = self
            .rest_service
            .send_request_urls(url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            Ok(serde_json::from_str(&content)?)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn delete_subject(&self, subject: &str, permanent: bool) -> Result<Vec<i32>, Error> {
        let url = format!("/subjects/{}", urlencode(subject));
        let query = vec![("permanent".to_string(), permanent.to_string())];
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::DELETE, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            Ok(serde_json::from_str(&content)?)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn delete_subject_version(
        &self,
        subject: &str,
        version: i32,
        permanent: bool,
    ) -> Result<i32, Error> {
        let url = format!("/subjects/{}/versions/{}", urlencode(subject), version);
        let query = vec![("permanent".to_string(), permanent.to_string())];
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::DELETE, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            Ok(serde_json::from_str(&content)?)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn test_subject_compatibility(
        &self,
        subject: &str,
        schema: &Schema,
    ) -> Result<bool, Error> {
        let url = format!(
            "/compatibility/subjects/{}/versions/latest",
            urlencode(subject)
        );
        let body = serde_json::to_string(schema)?;
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::POST, None, Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            Ok(serde_json::from_str(&content)?)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn test_compatibility(
        &self,
        subject: &str,
        version: i32,
        schema: &Schema,
    ) -> Result<bool, Error> {
        let url = format!(
            "/compatibility/subjects/{}/versions/{}",
            urlencode(subject),
            version
        );
        let body = serde_json::to_string(schema)?;
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::POST, None, Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            Ok(serde_json::from_str(&content)?)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }

    async fn get_config(&self, subject: &str) -> Result<ServerConfig, Error> {
        let url = format!("/config/{}", urlencode(subject));
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, None, None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let config: ServerConfig = serde_json::from_str(&content)?;
            Ok(config)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }
    async fn update_config(
        &self,
        subject: &str,
        config: &ServerConfig,
    ) -> Result<ServerConfig, Error> {
        let url = format!("/config/{}", urlencode(subject));
        let body = serde_json::to_string(config)?;
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::PUT, None, Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let config: ServerConfig = serde_json::from_str(&content)?;
            Ok(config)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }
    async fn get_default_config(&self) -> Result<ServerConfig, Error> {
        let url = "/config";
        let resp = self
            .rest_service
            .send_request_urls(url, reqwest::Method::GET, None, None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let config: ServerConfig = serde_json::from_str(&content)?;
            Ok(config)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }
    async fn update_default_config(&self, config: &ServerConfig) -> Result<ServerConfig, Error> {
        let url = "/config";
        let body = serde_json::to_string(config)?;
        let resp = self
            .rest_service
            .send_request_urls(url, reqwest::Method::PUT, None, Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let config: ServerConfig = serde_json::from_str(&content)?;
            Ok(config)
        } else {
            let entity = serde_json::from_str(&content).ok();
            let error = ResponseContent {
                status,
                content,
                entity,
            };
            Err(Error::ResponseError(error))
        }
    }
    fn clear_latest_caches(&self) {
        self.latest_version_cache.invalidate_all();
        self.latest_with_metadata_cache.invalidate_all();
    }

    fn clear_caches(&self) {
        self.latest_version_cache.invalidate_all();
        self.latest_with_metadata_cache.invalidate_all();
        self.store.lock().unwrap().clear();
    }

    fn close(&mut self) {}
}

pub(crate) struct SchemaStore {
    schema_id_index: HashMap<String, HashMap<i32, (Option<String>, Schema)>>,
    schema_guid_index: HashMap<String, Schema>,
    schema_index: HashMap<String, HashMap<Schema, i32>>,
    rs_id_index: HashMap<String, HashMap<i32, RegisteredSchema>>,
    rs_version_index: HashMap<String, HashMap<i32, RegisteredSchema>>,
    rs_schema_index: HashMap<String, HashMap<Schema, RegisteredSchema>>,
}

impl SchemaStore {
    pub fn new() -> Self {
        SchemaStore {
            schema_id_index: HashMap::new(),
            schema_guid_index: HashMap::new(),
            schema_index: HashMap::new(),
            rs_id_index: HashMap::new(),
            rs_version_index: HashMap::new(),
            rs_schema_index: HashMap::new(),
        }
    }

    pub fn set_schema(
        &mut self,
        subject: Option<String>,
        schema_id: Option<i32>,
        schema_guid: Option<String>,
        schema: &Schema,
    ) {
        let subject = subject.unwrap_or_default();
        if let Some(id) = schema_id {
            self.schema_id_index
                .entry(subject.clone())
                .and_modify(|m| {
                    m.insert(id, (schema_guid.clone(), schema.clone()));
                })
                .or_insert_with(|| {
                    let mut m = HashMap::new();
                    m.insert(id, (schema_guid.clone(), schema.clone()));
                    m
                });
            self.schema_index
                .entry(subject.clone())
                .and_modify(|m| {
                    m.insert(schema.clone(), id);
                })
                .or_insert_with(|| {
                    let mut m = HashMap::new();
                    m.insert(schema.clone(), id);
                    m
                });
        }
        if let Some(guid) = schema_guid {
            self.schema_guid_index.insert(guid, schema.clone());
        }
    }

    pub fn set_registered_schema(&mut self, schema: &Schema, rs: &RegisteredSchema) {
        let schema_id = rs.id;
        let schema_guid = rs.guid.clone();
        let subject = rs.subject.clone().unwrap_or_default();
        let version = rs.version.unwrap_or_default();
        if let Some(id) = schema_id {
            self.schema_id_index
                .entry(subject.clone())
                .and_modify(|m| {
                    m.insert(id, (schema_guid.clone(), schema.clone()));
                })
                .or_insert_with(|| {
                    let mut m = HashMap::new();
                    m.insert(id, (schema_guid.clone(), schema.clone()));
                    m
                });
            self.schema_index
                .entry(subject.clone())
                .and_modify(|m| {
                    m.insert(schema.clone(), id);
                })
                .or_insert_with(|| {
                    let mut m = HashMap::new();
                    m.insert(schema.clone(), id);
                    m
                });
            self.rs_id_index
                .entry(subject.clone())
                .and_modify(|m| {
                    m.insert(id, rs.clone());
                })
                .or_insert_with(|| {
                    let mut m = HashMap::new();
                    m.insert(id, rs.clone());
                    m
                });
        }
        if let Some(guid) = schema_guid {
            self.schema_guid_index.insert(guid, schema.clone());
        }
        self.rs_version_index
            .entry(subject.clone())
            .and_modify(|m| {
                m.insert(version, rs.clone());
            })
            .or_insert_with(|| {
                let mut m = HashMap::new();
                m.insert(version, rs.clone());
                m
            });
        self.rs_schema_index
            .entry(subject.clone())
            .and_modify(|m| {
                m.insert(schema.clone(), rs.clone());
            })
            .or_insert_with(|| {
                let mut m = HashMap::new();
                m.insert(schema.clone(), rs.clone());
                m
            });
    }

    pub fn get_schema_by_id(
        &self,
        subject: &str,
        schema_id: i32,
    ) -> Option<(Option<String>, Schema)> {
        let s = self.schema_id_index.get(subject);
        s.and_then(|m| m.get(&schema_id).cloned())
    }

    pub fn get_schema_by_guid(&self, guid: &str) -> Option<Schema> {
        self.schema_guid_index.get(guid).cloned()
    }

    pub fn get_id_by_schema(&self, subject: &str, schema: &Schema) -> Option<i32> {
        let s = self.schema_index.get(subject);
        s.and_then(|m| m.get(schema).copied())
    }

    pub fn get_registered_by_schema(
        &self,
        subject: &str,
        schema: &Schema,
    ) -> Option<RegisteredSchema> {
        let s = self.rs_schema_index.get(subject);
        s.and_then(|m| m.get(schema).cloned())
    }

    pub fn get_registered_by_version(
        &self,
        subject: &str,
        version: i32,
    ) -> Option<RegisteredSchema> {
        let s = self.rs_version_index.get(subject);
        s.and_then(|m| m.get(&version).cloned())
    }

    pub fn get_registered_by_id(&self, subject: &str, schema_id: i32) -> Option<RegisteredSchema> {
        let s = self.rs_id_index.get(subject);
        s.and_then(|m| m.get(&schema_id).cloned())
    }

    pub fn clear(&mut self) {
        self.schema_id_index.clear();
        self.schema_guid_index.clear();
        self.schema_index.clear();
        self.rs_id_index.clear();
        self.rs_version_index.clear();
        self.rs_schema_index.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_subjects() {
        let config = client_config::ClientConfig::default();
        let client = SchemaRegistryClient::new(config);
        let subjects = client.get_all_subjects(false).await.ok();
        println!("{:?}", subjects);
    }
}
