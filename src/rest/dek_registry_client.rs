use crate::rest::apis::{Error, ResponseContent, urlencode};
use crate::rest::models::dek::Algorithm;
use crate::rest::models::{CreateDekRequest, CreateKekRequest, Dek, Kek};
use crate::rest::{client_config, rest_service};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[trait_variant::make(Send)]
pub trait Client {
    fn new(config: client_config::ClientConfig) -> Self;
    fn config(&self) -> &client_config::ClientConfig;
    async fn register_kek(&self, request: CreateKekRequest) -> Result<Kek, Error>;
    async fn register_dek(&self, kek_name: &str, request: CreateDekRequest) -> Result<Dek, Error>;
    async fn get_kek(&self, name: &str, deleted: bool) -> Result<Kek, Error>;
    async fn get_dek(
        &self,
        kek_name: &str,
        subject: &str,
        algorithm: Option<Algorithm>,
        version: Option<i32>,
        deleted: bool,
    ) -> Result<Dek, Error>;
    async fn set_dek_key_material(
        &self,
        kek_name: &str,
        subject: &str,
        algorithm: Option<Algorithm>,
        version: Option<i32>,
        deleted: bool,
        key_material_bytes: &[u8],
    ) -> Result<Dek, Error>;
    fn clear_caches(&self);
    fn close(&mut self);
}

#[derive(Clone)]
pub struct DekRegistryClient {
    store: Arc<Mutex<DekStore>>,
    rest_service: rest_service::RestService,
}

impl Client for DekRegistryClient {
    fn new(config: client_config::ClientConfig) -> Self {
        if config.base_urls.is_empty() {
            panic!("base URL is required");
        }
        DekRegistryClient {
            store: Arc::new(Mutex::new(DekStore::new())),
            rest_service: rest_service::RestService::new(config),
        }
    }

    fn config(&self) -> &client_config::ClientConfig {
        self.rest_service.config()
    }

    async fn register_kek(&self, request: CreateKekRequest) -> Result<Kek, Error> {
        let cache_key = KekId {
            name: request.name.clone(),
            deleted: false,
        };
        {
            let store = self.store.lock().unwrap();
            if let Some(kek) = store.keks.get(&cache_key) {
                return Ok(kek.clone());
            }
        }
        let url = "/dek-registry/v1/keks";
        let body = serde_json::to_string(&request)?;
        let resp = self
            .rest_service
            .send_request_urls(url, reqwest::Method::POST, None, Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let kek: Kek = serde_json::from_str(&content)?;
            store.keks.insert(cache_key, kek.clone());
            Ok(kek)
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

    async fn register_dek(&self, kek_name: &str, request: CreateDekRequest) -> Result<Dek, Error> {
        let cache_key = DekId {
            kek_name: kek_name.to_string(),
            subject: request.subject.clone(),
            version: request.version.unwrap_or(1),
            algorithm: request.algorithm.unwrap_or_default(),
            deleted: false,
        };
        {
            let store = self.store.lock().unwrap();
            if let Some(dek) = store.deks.get(&cache_key) {
                return Ok(dek.clone());
            }
        }

        // Try newer API with subject in path first
        let url = format!(
            "/dek-registry/v1/keks/{}/deks/{}",
            urlencode(kek_name),
            urlencode(&request.subject)
        );
        let body = serde_json::to_string(&request)?;
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::POST, None, Some(&body))
            .await?;
        let status = resp.status();
        let content = resp.text().await?;

        // If we get a 405 (Method Not Allowed), fall back to older API
        if status == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            let url = format!("/dek-registry/v1/keks/{}/deks", urlencode(kek_name));
            let resp = self
                .rest_service
                .send_request_urls(&url, reqwest::Method::POST, None, Some(&body))
                .await?;
            let status = resp.status();
            let content = resp.text().await?;

            if !status.is_client_error() && !status.is_server_error() {
                let mut store = self.store.lock().unwrap();
                let dek: Dek = serde_json::from_str(&content)?;
                store.deks.insert(cache_key, dek.clone());
                Ok(dek)
            } else {
                let entity = serde_json::from_str(&content).ok();
                let error = ResponseContent {
                    status,
                    content,
                    entity,
                };
                Err(Error::ResponseError(error))
            }
        } else if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let dek: Dek = serde_json::from_str(&content)?;
            store.deks.insert(cache_key, dek.clone());
            Ok(dek)
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

    async fn get_kek(&self, name: &str, deleted: bool) -> Result<Kek, Error> {
        let kek_id = KekId {
            name: name.to_string(),
            deleted,
        };
        {
            let store = self.store.lock().unwrap();
            if let Some(kek) = store.get_kek(&kek_id) {
                return Ok(kek.clone());
            }
        }
        let url = format!("/dek-registry/v1/keks/{name}");
        let query = vec![("deleted".to_string(), deleted.to_string())];
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let kek: Kek = serde_json::from_str(&content)?;
            store.set_kek(&kek_id, &kek);
            Ok(kek)
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

    async fn get_dek(
        &self,
        kek_name: &str,
        subject: &str,
        algorithm: Option<Algorithm>,
        version: Option<i32>,
        deleted: bool,
    ) -> Result<Dek, Error> {
        let algorithm = algorithm.unwrap_or_default();
        let version = version.unwrap_or(1);
        let dek_id = DekId {
            kek_name: kek_name.to_string(),
            subject: subject.to_string(),
            version,
            algorithm,
            deleted,
        };
        {
            let store = self.store.lock().unwrap();
            // Use the dek stored during write
            if let Some(dek) = store.get_dek(&dek_id) {
                return Ok(dek.clone());
            }
        }
        let url = format!("/dek-registry/v1/keks/{kek_name}/deks/{subject}/versions/{version}");
        let query = vec![
            (
                "algorithm".to_string(),
                serde_json::to_string(&algorithm)?
                    .trim_matches('"')
                    .to_string(),
            ),
            ("deleted".to_string(), deleted.to_string()),
        ];
        let resp = self
            .rest_service
            .send_request_urls(&url, reqwest::Method::GET, Some(&query), None)
            .await?;
        let status = resp.status();
        let content = resp.text().await?;
        if !status.is_client_error() && !status.is_server_error() {
            let mut store = self.store.lock().unwrap();
            let mut dek: Dek = serde_json::from_str(&content)?;
            dek.populate_key_material_bytes();
            store.set_dek(&dek_id, &dek);
            Ok(dek)
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

    async fn set_dek_key_material(
        &self,
        kek_name: &str,
        subject: &str,
        algorithm: Option<Algorithm>,
        version: Option<i32>,
        deleted: bool,
        key_material_bytes: &[u8],
    ) -> Result<Dek, Error> {
        let algorithm = algorithm.unwrap_or_default();
        let version = version.unwrap_or(1);
        let dek_id = DekId {
            kek_name: kek_name.to_string(),
            subject: subject.to_string(),
            version,
            algorithm,
            deleted,
        };
        let mut store = self.store.lock().unwrap();
        if let Some(dek) = store.get_mut_dek(&dek_id) {
            dek.set_key_material(key_material_bytes);
            dek.populate_key_material_bytes();
            return Ok(dek.clone());
        }
        let error = ResponseContent {
            status: reqwest::StatusCode::NOT_FOUND,
            content: "not found".to_string(),
            entity: None,
        };
        Err(Error::ResponseError(error))
    }

    fn clear_caches(&self) {
        self.store.lock().unwrap().clear();
    }

    fn close(&mut self) {}
}

#[derive(Clone, Default, Debug, Eq, Hash, PartialEq)]
pub(crate) struct KekId {
    pub name: String,
    pub deleted: bool,
}

#[derive(Clone, Default, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DekId {
    pub kek_name: String,
    pub subject: String,
    pub version: i32,
    pub algorithm: Algorithm,
    pub deleted: bool,
}

pub(crate) struct DekStore {
    pub keks: HashMap<KekId, Kek>,
    pub deks: HashMap<DekId, Dek>,
}

impl DekStore {
    pub fn new() -> Self {
        DekStore {
            keks: HashMap::new(),
            deks: HashMap::new(),
        }
    }

    pub fn set_kek(&mut self, kek_id: &KekId, kek: &Kek) {
        self.keks.insert(kek_id.clone(), kek.clone());
    }

    pub fn set_dek(&mut self, dek_id: &DekId, dek: &Dek) {
        self.deks.insert(dek_id.clone(), dek.clone());
    }

    pub fn get_kek(&self, kek_id: &KekId) -> Option<&Kek> {
        self.keks.get(kek_id)
    }

    pub fn get_dek(&self, dek_id: &DekId) -> Option<&Dek> {
        self.deks.get(dek_id)
    }

    pub fn get_mut_dek(&mut self, dek_id: &DekId) -> Option<&mut Dek> {
        self.deks.get_mut(dek_id)
    }

    pub fn clear(&mut self) {
        self.keks.clear();
        self.deks.clear();
    }
}
