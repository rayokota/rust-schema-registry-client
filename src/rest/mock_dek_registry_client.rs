use crate::rest::apis::{Error, ResponseContent};
use crate::rest::client_config::ClientConfig;
use crate::rest::dek_registry_client::{Client, DekId, DekStore, KekId};
use crate::rest::models::dek::Algorithm;
use crate::rest::models::{CreateDekRequest, CreateKekRequest, Dek, Kek};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct MockDekRegistryClient {
    store: Arc<Mutex<DekStore>>,
    config: ClientConfig,
}

impl Client for MockDekRegistryClient {
    fn new(config: ClientConfig) -> Self {
        MockDekRegistryClient {
            store: Arc::new(Mutex::new(DekStore::new())),
            config,
        }
    }

    fn config(&self) -> &ClientConfig {
        &self.config
    }

    async fn register_kek(&self, request: CreateKekRequest) -> Result<Kek, Error> {
        let mut store = self.store.lock().unwrap();
        let cache_key = KekId {
            name: request.name.clone(),
            deleted: false,
        };
        if let Some(kek) = store.keks.get(&cache_key) {
            return Ok(kek.clone());
        }
        let kek = Kek {
            name: request.name,
            kms_type: request.kms_type,
            kms_key_id: request.kms_key_id,
            kms_props: request.kms_props,
            doc: request.doc,
            shared: request.shared,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            deleted: Some(false),
        };
        store.keks.insert(cache_key, kek.clone());
        Ok(kek)
    }

    async fn register_dek(&self, kek_name: &str, request: CreateDekRequest) -> Result<Dek, Error> {
        let mut store = self.store.lock().unwrap();
        let cache_key = DekId {
            kek_name: kek_name.to_string(),
            subject: request.subject.clone(),
            version: request.version.unwrap_or(1),
            algorithm: request.algorithm.unwrap_or_default(),
            deleted: false,
        };
        if let Some(dek) = store.deks.get(&cache_key) {
            return Ok(dek.clone());
        }
        let dek = Dek {
            kek_name: kek_name.to_string(),
            subject: request.subject,
            version: request.version.unwrap_or(1),
            algorithm: request.algorithm.unwrap_or_default(),
            encrypted_key_material: request.encrypted_key_material,
            encrypted_key_material_bytes: None,
            key_material: None,
            key_material_bytes: None,
            ts: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            deleted: Some(false),
        };
        store.deks.insert(cache_key, dek.clone());
        Ok(dek)
    }

    async fn get_kek(&self, name: &str, deleted: bool) -> Result<Kek, Error> {
        let kek_id = KekId {
            name: name.to_string(),
            deleted,
        };
        let store = self.store.lock().unwrap();
        if let Some(kek) = store.get_kek(&kek_id) {
            return Ok(kek.clone());
        }
        let error = ResponseContent {
            status: reqwest::StatusCode::NOT_FOUND,
            content: "not found".to_string(),
            entity: None,
        };
        Err(Error::ResponseError(error))
    }

    async fn get_dek(
        &self,
        kek_name: &str,
        subject: &str,
        algorithm: Option<Algorithm>,
        version: Option<i32>,
        _deleted: bool,
    ) -> Result<Dek, Error> {
        let algorithm = algorithm.unwrap_or_default();
        let version = version.unwrap_or(1);
        let dek_id = DekId {
            kek_name: kek_name.to_string(),
            subject: subject.to_string(),
            version,
            algorithm,
            deleted: false,
        };
        let store = self.store.lock().unwrap();
        // Use the dek stored during write
        let deleted = false;
        if let Some(dek) = store.get_dek(&dek_id) {
            return Ok(dek.clone());
        }
        let error = ResponseContent {
            status: reqwest::StatusCode::NOT_FOUND,
            content: "not found".to_string(),
            entity: None,
        };
        Err(Error::ResponseError(error))
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
        let deleted = false;
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
