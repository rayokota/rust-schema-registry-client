use crate::rest::apis::Error::ResponseError;
use crate::rest::client_config::ClientConfig;
use crate::rest::dek_registry_client::{Client, DekId, KekId};
use crate::rest::models::dek::Algorithm;
use crate::rest::models::{CreateDekRequest, CreateKekRequest, Dek, Kek, Mode};
use crate::rules::encryption::kms_driver::KmsDriver;
use crate::rules::encryption::{get_kms_client, get_kms_driver, register_kms_client};
use crate::serdes::serde::SerdeError::{Rest, Rule};
use crate::serdes::serde::{
    FieldRuleExecutor, FieldType, RuleBase, RuleContext, RuleExecutor, SerdeError, SerdeValue,
};
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use log::warn;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use tink_aead::{AES_GCM_KEY_VERSION, AES_GCM_TYPE_URL};
use tink_core::Aead;
use tink_core::registry::KmsClient;
use tink_daead::{AES_SIV_KEY_VERSION, AES_SIV_TYPE_URL};
use tink_proto::OutputPrefixType::Raw;
use tink_proto::{KeyTemplate, prost::Message};

const ENCRYPT_KEK_NAME: &str = "encrypt.kek.name";
const ENCRYPT_KMS_KEY_ID: &str = "encrypt.kms.key.id";
const ENCRYPT_KMS_TYPE: &str = "encrypt.kms.type";
const ENCRYPT_DEK_ALGORITHM: &str = "encrypt.dek.algorithm";
const ENCRYPT_DEK_EXPIRY_DAYS: &str = "encrypt.dek.expiry.days";
const MILLIS_IN_DAY: i64 = 24 * 60 * 60 * 1000;
const EMPTY_AAD: &[u8] = b"";

pub trait Clock: Send + Sync {
    fn now(&self) -> i64;
}

#[derive(Clone, Debug, Default)]
pub struct SystemClock {}

impl SystemClock {
    pub fn new() -> Self {
        SystemClock {}
    }
}

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        let now = std::time::SystemTime::now();
        now.duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }
}

#[derive(Clone, Debug)]
pub struct FakeClock {
    pub time: i64,
}

impl FakeClock {
    pub fn new(time: i64) -> Self {
        FakeClock { time }
    }

    pub fn set_time(&mut self, time: i64) {
        self.time = time;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> i64 {
        self.time
    }
}

pub struct EncryptionExecutor<T: Client> {
    pub(crate) client: OnceLock<T>,
    config: RwLock<HashMap<String, String>>,
    clock: Arc<dyn Clock>,
}

impl<T: Client + Sync + 'static> RuleBase for EncryptionExecutor<T> {
    fn configure(
        &self,
        client_config: &ClientConfig,
        rule_config: &HashMap<String, String>,
    ) -> Result<(), SerdeError> {
        let client = self.client.get();
        if let Some(config) = client.map(|c| c.config()) {
            if *config != *client_config {
                return Err(Rule("client config already set".to_string()));
            }
        } else if self.client.set(T::new(client_config.clone())).is_err() {
            return Err(Rule("client config already set".to_string()));
        }

        let mut config = self.config.write().unwrap();
        for (key, value) in rule_config {
            if let Some(existing_value) = config.get(key) {
                if existing_value != value {
                    return Err(Rule(format!("rule config key {key} already set")));
                }
            } else {
                config.insert(key.clone(), value.clone());
            }
        }
        Ok(())
    }

    fn get_type(&self) -> &'static str {
        "ENCRYPT_PAYLOAD"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl<T: Client + Clone + Sync + 'static> EncryptionExecutor<T> {
    pub fn new<C: Clock + 'static>(clock: C) -> Self {
        tink_aead::init();
        tink_daead::init();
        EncryptionExecutor {
            client: OnceLock::new(),
            config: RwLock::new(HashMap::new()),
            clock: Arc::new(clock),
        }
    }

    pub fn client(&self) -> Option<T> {
        let client = self.client.get();
        client.cloned()
    }

    fn get_cryptor(&self, ctx: &RuleContext) -> Result<Cryptor, SerdeError> {
        let mut dek_algorithm = Algorithm::Aes256Gcm;
        if let Some(algorithm) = ctx.get_parameter(ENCRYPT_DEK_ALGORITHM) {
            let algorithm = "\"".to_string() + algorithm + "\"";
            dek_algorithm = serde_json::from_str(&algorithm)?;
        };
        Ok(Cryptor::new(dek_algorithm))
    }

    fn get_kek_name(&self, ctx: &RuleContext) -> Result<String, SerdeError> {
        if let Some(kek_name) = ctx.get_parameter(ENCRYPT_KEK_NAME) {
            if kek_name.is_empty() {
                Err(Rule("empty kek name".to_string()))
            } else {
                Ok(kek_name.clone())
            }
        } else {
            Err(Rule("no kek name found".to_string()))
        }
    }

    fn get_dek_expiry_days(&self, ctx: &RuleContext) -> Result<i64, SerdeError> {
        if let Some(expiry_days) = ctx.get_parameter(ENCRYPT_DEK_EXPIRY_DAYS) {
            match expiry_days.parse::<i64>() {
                Ok(days) => {
                    if days >= 0 {
                        Ok(days)
                    } else {
                        Err(Rule("negative expiry days".to_string()))
                    }
                }
                Err(_) => Err(Rule("invalid expiry days".to_string())),
            }
        } else {
            Ok(0)
        }
    }

    fn new_transform(
        &self,
        ctx: &mut RuleContext,
    ) -> Result<EncryptionExecutorTransform<T>, SerdeError> {
        let cryptor = self.get_cryptor(ctx)?;
        let kek_name = self.get_kek_name(ctx)?;
        let dek_expiry_days = self.get_dek_expiry_days(ctx)?;
        Ok(EncryptionExecutorTransform {
            executor: self,
            cryptor,
            kek_name,
            kek: OnceLock::new(),
            dek_expiry_days,
        })
    }

    pub fn register() {
        crate::serdes::rule_registry::register_rule_executor(EncryptionExecutor::<T>::new(
            SystemClock::new(),
        ));
    }
}

#[async_trait]
impl<T: Client + Clone + Sync + 'static> RuleExecutor for EncryptionExecutor<T> {
    async fn transform(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        let transform = self.new_transform(ctx)?;
        transform.transform(ctx, FieldType::Bytes, msg).await
    }
}

pub(crate) struct Cryptor {
    dek_format: Algorithm,
    key_template: KeyTemplate,
}

impl Cryptor {
    fn new(dek_format: Algorithm) -> Self {
        let key_template = match dek_format {
            Algorithm::Aes128Gcm => {
                // Construct AES128 GCM RAW since helper method is missing in Tink
                let format = tink_proto::AesGcmKeyFormat {
                    version: AES_GCM_KEY_VERSION,
                    key_size: 16,
                };
                let mut serialized_format = Vec::new();
                format.encode(&mut serialized_format).unwrap(); // safe: proto-encode
                KeyTemplate {
                    type_url: AES_GCM_TYPE_URL.to_string(),
                    value: serialized_format,
                    output_prefix_type: Raw as i32,
                }
            }
            Algorithm::Aes256Gcm => tink_aead::aes256_gcm_no_prefix_key_template(),
            Algorithm::Aes256Siv => {
                // Construct AES256 SIV RAW since helper method is missing in Tink
                let format = tink_proto::AesSivKeyFormat {
                    version: AES_SIV_KEY_VERSION,
                    // Generate 2 256-bit keys
                    key_size: 64,
                };
                let mut serialized_format = Vec::new();
                format.encode(&mut serialized_format).unwrap(); // safe: proto-encode
                KeyTemplate {
                    type_url: AES_SIV_TYPE_URL.to_string(),
                    value: serialized_format,
                    output_prefix_type: Raw as i32,
                }
            }
        };
        Cryptor {
            dek_format,
            key_template,
        }
    }

    fn is_deterministic(&self) -> bool {
        self.dek_format == Algorithm::Aes256Siv
    }

    fn generate_key(&self) -> Result<Vec<u8>, SerdeError> {
        let key_data = tink_core::registry::new_key_data(&self.key_template)?;
        Ok(key_data.value)
    }

    fn encrypt(
        &self,
        dek: &[u8],
        plaintext: &[u8],
        associated_data: &[u8],
    ) -> Result<Vec<u8>, SerdeError> {
        let key_data = tink_proto::KeyData {
            type_url: self.key_template.type_url.clone(),
            value: dek.to_vec(),
            key_material_type: tink_proto::key_data::KeyMaterialType::Symmetric as i32,
        };
        let primitive = tink_core::registry::primitive_from_key_data(&key_data)?;
        if self.is_deterministic() {
            if let tink_core::Primitive::DeterministicAead(primitive) = primitive {
                Ok(primitive.encrypt_deterministically(plaintext, associated_data)?)
            } else {
                Err(Rule(
                    "could not get deterministic aead primitive".to_string(),
                ))
            }
        } else if let tink_core::Primitive::Aead(primitive) = primitive {
            Ok(primitive.encrypt(plaintext, associated_data)?)
        } else {
            Err(Rule("could not get aead primitive".to_string()))
        }
    }

    fn decrypt(
        &self,
        dek: &[u8],
        ciphertext: &[u8],
        associated_data: &[u8],
    ) -> Result<Vec<u8>, SerdeError> {
        let key_data = tink_proto::KeyData {
            type_url: self.key_template.type_url.clone(),
            value: dek.to_vec(),
            key_material_type: tink_proto::key_data::KeyMaterialType::Symmetric as i32,
        };
        let primitive = tink_core::registry::primitive_from_key_data(&key_data)?;
        if self.is_deterministic() {
            if let tink_core::Primitive::DeterministicAead(primitive) = primitive {
                Ok(primitive.decrypt_deterministically(ciphertext, associated_data)?)
            } else {
                Err(Rule(
                    "could not get deterministic aead primitive".to_string(),
                ))
            }
        } else if let tink_core::Primitive::Aead(primitive) = primitive {
            Ok(primitive.decrypt(ciphertext, associated_data)?)
        } else {
            Err(Rule("could not get aead primitive".to_string()))
        }
    }
}

pub(crate) struct EncryptionExecutorTransform<'a, T: Client> {
    pub executor: &'a EncryptionExecutor<T>,
    pub cryptor: Cryptor,
    pub kek_name: String,
    pub kek: OnceLock<Kek>,
    pub dek_expiry_days: i64,
}

impl<T: Client> EncryptionExecutorTransform<'_, T> {
    fn is_dek_rotated(&self) -> bool {
        self.dek_expiry_days > 0
    }

    async fn get_kek(&self, ctx: &RuleContext) -> Result<Kek, SerdeError> {
        let kek = self.kek.get();
        if let Some(kek) = kek {
            Ok(kek.clone())
        } else {
            let kek = self.get_or_create_kek(ctx).await?;
            if self.kek.set(kek.clone()).is_err() {
                return Err(Rule("kek already set".to_string()));
            }
            Ok(kek)
        }
    }

    async fn get_or_create_kek(&self, ctx: &RuleContext) -> Result<Kek, SerdeError> {
        let is_read = ctx.rule_mode == Mode::Read;
        let kms_type = ctx.get_parameter(ENCRYPT_KMS_TYPE);
        let kms_key_id = ctx.get_parameter(ENCRYPT_KMS_KEY_ID);
        let kek_id = KekId {
            name: self.kek_name.clone(),
            deleted: false,
        };
        let mut kek = self.retrieve_kek_from_registry(&kek_id).await?;
        if let Some(kek) = kek {
            if let Some(kms_type) = kms_type {
                if kek.kms_type != *kms_type {
                    return Err(Rule(format!(
                        "found {} with kms type {} which differs from rule kms type {}",
                        self.kek_name, kek.kms_type, kms_type
                    )));
                }
            }
            if let Some(kms_key_id) = kms_key_id {
                if kek.kms_key_id != *kms_key_id {
                    return Err(Rule(format!(
                        "found {} with kms key id {} which differs from rule kms key id {}",
                        self.kek_name, kek.kms_key_id, kms_key_id
                    )));
                }
            }
            Ok(kek)
        } else {
            if is_read {
                return Err(Rule(format!(
                    "no kek found for {} during consume",
                    kek_id.name
                )));
            }
            if kms_type.is_none() {
                return Err(Rule(format!(
                    "no kms type found for {} during produce",
                    kek_id.name
                )));
            }
            if kms_key_id.is_none() {
                return Err(Rule(format!(
                    "no kms key id found for {} during produce",
                    kek_id.name
                )));
            }
            kek = self
                .store_kek_to_registry(&kek_id, kms_type.unwrap(), kms_key_id.unwrap(), false)
                .await?;
            if kek.is_none() {
                // handle conflicts (409)
                kek = self.retrieve_kek_from_registry(&kek_id).await?;
            }
            kek.ok_or(Rule(format!(
                "no kek found for {} during produce",
                kek_id.name
            )))
        }
    }

    async fn retrieve_kek_from_registry(&self, kek_id: &KekId) -> Result<Option<Kek>, SerdeError> {
        let kek = self
            .executor
            .client
            .get()
            .unwrap()
            .get_kek(&kek_id.name, kek_id.deleted)
            .await;
        match kek {
            Ok(kek) => Ok(Some(kek)),
            Err(err) => match err {
                ResponseError(error) => {
                    if error.status == 404 {
                        Ok(None)
                    } else {
                        Err(Rule(format!("could not get kek {}", kek_id.name)))
                    }
                }
                _ => Err(Rest(err)),
            },
        }
    }

    async fn store_kek_to_registry(
        &self,
        kek_id: &KekId,
        kms_type: &str,
        kms_key_id: &str,
        shared: bool,
    ) -> Result<Option<Kek>, SerdeError> {
        let request = CreateKekRequest {
            name: kek_id.name.clone(),
            kms_type: kms_type.to_string(),
            kms_key_id: kms_key_id.to_string(),
            kms_props: None,
            doc: None,
            shared,
        };
        let kek = self
            .executor
            .client
            .get()
            .unwrap()
            .register_kek(request)
            .await;
        match kek {
            Ok(kek) => Ok(Some(kek)),
            Err(err) => match err {
                ResponseError(error) => {
                    if error.status == 409 {
                        Ok(None)
                    } else {
                        Err(Rule(format!("could not register kek {}", kek_id.name)))
                    }
                }
                _ => Err(Rest(err)),
            },
        }
    }

    async fn get_or_create_dek(
        &self,
        ctx: &RuleContext,
        version: Option<i32>,
    ) -> Result<Dek, SerdeError> {
        let kek = self.get_kek(ctx).await?;
        let is_read = ctx.rule_mode == Mode::Read;
        let version = match version {
            Some(v) => {
                if v == 0 {
                    1
                } else {
                    v
                }
            }
            None => 1,
        };
        let algorithm = self.cryptor.dek_format;
        let dek_id = DekId {
            kek_name: kek.name.clone(),
            subject: ctx.subject.clone(),
            version,
            algorithm,
            deleted: is_read,
        };
        let mut dek = self.retrieve_dek_from_registry(&dek_id).await?;
        let is_expired = self.is_expired(ctx, dek.as_ref());
        if dek.is_none() || is_expired {
            if is_read {
                return Err(Rule(format!(
                    "no dek found for {} during consume",
                    dek_id.kek_name
                )));
            }
            let mut encrypted_dek = None;
            if !kek.shared {
                let raw_dek = self.cryptor.generate_key()?;
                encrypted_dek = self.encrypt_dek(&kek, &raw_dek)?;
            }
            let new_version = if is_expired {
                dek.as_ref().unwrap().version + 1
            } else {
                1
            };
            let result = self
                .create_dek(&dek_id, new_version, encrypted_dek.as_deref())
                .await;
            if result.is_err() {
                if dek.is_none() {
                    return result;
                }
                warn!(
                    "Failed to create dek for {}, subject {}, version {}, using existing dek",
                    kek.name, ctx.subject, new_version
                );
            } else {
                dek = Some(result?);
            }
        }
        let key_bytes = dek.as_mut().unwrap().get_key_material_bytes();
        if key_bytes.is_none() {
            let encrypted_dek = dek.as_mut().unwrap().get_encrypted_key_material_bytes();
            let raw_dek = self.decrypt_dek(&kek, encrypted_dek.unwrap())?;
            dek = Some(
                self.update_cached_dek(
                    &dek_id.kek_name,
                    &dek_id.subject,
                    Some(dek_id.algorithm),
                    Some(dek_id.version),
                    dek_id.deleted,
                    &raw_dek,
                )
                .await?,
            );
        }
        Ok(dek.unwrap())
    }

    async fn create_dek(
        &self,
        dek_id: &DekId,
        new_version: i32,
        encrypted_dek: Option<&[u8]>,
    ) -> Result<Dek, SerdeError> {
        let new_dek_id = DekId {
            kek_name: dek_id.kek_name.clone(),
            subject: dek_id.subject.clone(),
            version: new_version,
            algorithm: dek_id.algorithm,
            deleted: dek_id.deleted,
        };
        let mut dek = self
            .store_dek_to_registry(&new_dek_id, encrypted_dek)
            .await?;
        if dek.is_none() {
            // handle conflicts (409)
            dek = self.retrieve_dek_from_registry(dek_id).await?;
        }
        if dek.is_none() {
            return Err(Rule(format!(
                "no dek found for {} during produce",
                dek_id.kek_name
            )));
        }
        Ok(dek.unwrap())
    }

    fn encrypt_dek(&self, kek: &Kek, raw_dek: &[u8]) -> Result<Option<Vec<u8>>, SerdeError> {
        let config = self.executor.config.read().unwrap();
        let primitive = self.get_aead(&config, kek)?;
        Ok(Some(primitive.encrypt(raw_dek, EMPTY_AAD)?))
    }

    fn decrypt_dek(&self, kek: &Kek, encrypted_dek: &[u8]) -> Result<Vec<u8>, SerdeError> {
        let config = self.executor.config.read().unwrap();
        let primitive = self.get_aead(&config, kek)?;
        Ok(primitive.decrypt(encrypted_dek, EMPTY_AAD)?)
    }

    async fn update_cached_dek(
        &self,
        kek_name: &str,
        subject: &str,
        algorithm: Option<Algorithm>,
        version: Option<i32>,
        deleted: bool,
        key_material_bytes: &[u8],
    ) -> Result<Dek, SerdeError> {
        let dek = self
            .executor
            .client
            .get()
            .as_ref()
            .unwrap()
            .set_dek_key_material(
                kek_name,
                subject,
                algorithm,
                version,
                deleted,
                key_material_bytes,
            )
            .await?;
        Ok(dek)
    }

    async fn retrieve_dek_from_registry(&self, dek_id: &DekId) -> Result<Option<Dek>, SerdeError> {
        let version = dek_id.version;
        let dek = self
            .executor
            .client
            .get()
            .as_ref()
            .unwrap()
            .get_dek(
                &dek_id.kek_name,
                &dek_id.subject,
                Some(dek_id.algorithm),
                Some(version),
                dek_id.deleted,
            )
            .await;
        match dek {
            Ok(dek) => Ok(Some(dek)),
            Err(err) => match err {
                ResponseError(error) => {
                    if error.status == 404 {
                        Ok(None)
                    } else {
                        Err(Rule(format!(
                            "could not get dek for kek {}, subject {}",
                            dek_id.kek_name, dek_id.subject
                        )))
                    }
                }
                _ => Err(Rest(err)),
            },
        }
    }

    async fn store_dek_to_registry(
        &self,
        dek_id: &DekId,
        encrypted_dek: Option<&[u8]>,
    ) -> Result<Option<Dek>, SerdeError> {
        let encrypted_dek_str = encrypted_dek.map(|dek| BASE64_STANDARD.encode(dek));
        let request = CreateDekRequest {
            subject: dek_id.subject.clone(),
            version: Some(dek_id.version),
            algorithm: Some(dek_id.algorithm),
            encrypted_key_material: encrypted_dek_str,
        };
        let dek = self
            .executor
            .client
            .get()
            .as_ref()
            .unwrap()
            .register_dek(&dek_id.kek_name, request)
            .await;
        match dek {
            Ok(dek) => Ok(Some(dek)),
            Err(err) => match err {
                ResponseError(error) => {
                    if error.status == 409 {
                        Ok(None)
                    } else {
                        Err(Rule(format!(
                            "could not register dek for kek {}, subject {}",
                            dek_id.kek_name, dek_id.subject
                        )))
                    }
                }
                _ => Err(Rest(err)),
            },
        }
    }

    fn is_expired(&self, ctx: &RuleContext, dek: Option<&Dek>) -> bool {
        let now = self.executor.clock.now();
        ctx.rule_mode != Mode::Read
            && self.dek_expiry_days > 0
            && dek.is_some()
            && (now - dek.unwrap().ts / MILLIS_IN_DAY) > self.dek_expiry_days
    }

    async fn transform(
        &self,
        ctx: &RuleContext,
        field_type: FieldType,
        field_value: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        match ctx.rule_mode {
            Mode::Write => {
                let plaintext = self.to_bytes(field_type, field_value);
                if plaintext.is_none() {
                    return Err(Rule(format!("unsupported field type {field_type}")));
                }
                let version = if self.is_dek_rotated() {
                    Some(-1)
                } else {
                    None
                };
                let mut dek = self.get_or_create_dek(ctx, version).await?;
                let key_material_bytes = dek.get_key_material_bytes();
                if key_material_bytes.is_none() {
                    return Err(Rule("no key material found".to_string()));
                }
                let mut ciphertext = self.cryptor.encrypt(
                    key_material_bytes.unwrap(),
                    &plaintext.unwrap(),
                    EMPTY_AAD,
                )?;
                if self.is_dek_rotated() {
                    ciphertext = self.prefix_version(dek.version, &ciphertext);
                }
                if let FieldType::String = field_type {
                    let encrypted_value_str = BASE64_STANDARD.encode(&ciphertext);
                    Ok(SerdeValue::new_string(
                        ctx.ser_ctx.serde_format,
                        &encrypted_value_str,
                    ))
                } else {
                    Ok(SerdeValue::new_bytes(ctx.ser_ctx.serde_format, &ciphertext))
                }
            }
            Mode::Read => {
                let mut ciphertext;
                if let FieldType::String = field_type {
                    ciphertext = Some(
                        BASE64_STANDARD
                            .decode(field_value.as_string())
                            .map_err(|e| Rule("could not decode base64 ciphertext".to_string()))?,
                    );
                } else {
                    ciphertext = self.to_bytes(field_type, field_value);
                }
                if ciphertext.is_none() {
                    return Ok(field_value.clone());
                }

                let mut version = None;
                if self.is_dek_rotated() {
                    let (v, c) = self.extract_version(&ciphertext.unwrap())?;
                    if v.is_none() {
                        return Err(Rule("no version found".to_string()));
                    }
                    version = v;
                    ciphertext = Some(c);
                }
                let mut dek = self.get_or_create_dek(ctx, version).await?;
                let key_material_bytes = dek.get_key_material_bytes();
                let plaintext = self.cryptor.decrypt(
                    key_material_bytes.unwrap(),
                    &ciphertext.unwrap(),
                    EMPTY_AAD,
                )?;
                Ok(self
                    .to_object(ctx, field_type, &plaintext)
                    .unwrap_or(field_value.clone()))
            }
            _ => Err(Rule("unsupported rule mode".to_string())),
        }
    }

    fn prefix_version(&self, version: i32, ciphertext: &[u8]) -> Vec<u8> {
        let mut payload = vec![0u8];
        let mut buf = [0u8; 4];
        BigEndian::write_u32(&mut buf, version as u32);
        payload.extend_from_slice(&buf);
        payload.extend_from_slice(ciphertext);
        payload
    }

    fn extract_version(&self, ciphertext: &[u8]) -> Result<(Option<i32>, Vec<u8>), SerdeError> {
        if ciphertext.len() < 5 {
            return Ok((None, ciphertext.to_vec()));
        }
        let mut buf = &ciphertext[1..5];
        let version = buf
            .read_u32::<BigEndian>()
            .map_err(|e| Rule("could not read version".to_string()))?;
        Ok((Some(version as i32), ciphertext[5..].to_vec()))
    }

    fn to_bytes(&self, field_type: FieldType, value: &SerdeValue) -> Option<Vec<u8>> {
        match field_type {
            FieldType::String => Some(value.as_string().as_bytes().to_vec()),
            FieldType::Bytes => Some(value.as_bytes()),
            _ => None,
        }
    }

    fn to_object(
        &self,
        ctx: &RuleContext,
        field_type: FieldType,
        value: &[u8],
    ) -> Option<SerdeValue> {
        match field_type {
            FieldType::String => Some(SerdeValue::new_string(
                ctx.ser_ctx.serde_format,
                &String::from_utf8_lossy(value),
            )),
            FieldType::Bytes => Some(SerdeValue::new_bytes(ctx.ser_ctx.serde_format, value)),
            _ => None,
        }
    }

    fn get_aead(
        &self,
        config: &HashMap<String, String>,
        kek: &Kek,
    ) -> Result<Box<dyn Aead>, SerdeError> {
        let kek_url = kek.kms_type.clone() + "://" + &kek.kms_key_id;
        let kms_client = self.get_kms_client(config, &kek_url)?;
        let aead = kms_client.get_aead(&kek_url)?;
        Ok(aead)
    }

    fn get_kms_client(
        &self,
        config: &HashMap<String, String>,
        kek_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError> {
        let driver = get_kms_driver(kek_url)?;
        match get_kms_client(kek_url) {
            Ok(kms_client) => Ok(kms_client),
            _ => Ok(self.register_kms_client(driver, config, kek_url)?),
        }
    }

    fn register_kms_client(
        &self,
        kms_driver: Arc<dyn KmsDriver>,
        config: &HashMap<String, String>,
        kek_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError> {
        let kms_client = kms_driver.new_kms_client(config, kek_url)?;
        register_kms_client(kms_client);
        Ok(get_kms_client(kek_url)?)
    }
}

pub struct FieldEncryptionExecutor<T: Client> {
    pub(crate) executor: EncryptionExecutor<T>,
}

impl<T: Client + Sync + 'static> RuleBase for FieldEncryptionExecutor<T> {
    fn configure(
        &self,
        client_config: &ClientConfig,
        rule_config: &HashMap<String, String>,
    ) -> Result<(), SerdeError> {
        self.executor.configure(client_config, rule_config)
    }

    fn get_type(&self) -> &'static str {
        "ENCRYPT"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl<T: Client + Clone + Sync + 'static> FieldEncryptionExecutor<T> {
    pub fn new<C: Clock + 'static>(clock: C) -> Self {
        tink_aead::init();
        tink_daead::init();
        FieldEncryptionExecutor {
            executor: EncryptionExecutor::new(clock),
        }
    }

    pub fn register() {
        crate::serdes::rule_registry::register_rule_executor(FieldEncryptionExecutor::<T>::new(
            SystemClock::new(),
        ));
    }
}

#[async_trait]
impl<T: Client + Clone + Sync + 'static> FieldRuleExecutor for FieldEncryptionExecutor<T> {
    async fn transform_field(
        &self,
        ctx: &mut RuleContext,
        field_value: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        let transform = self.executor.new_transform(ctx)?;
        let field_ctx = ctx.current_field().expect("no field context");
        transform
            .transform(ctx, field_ctx.get_field_type(), field_value)
            .await
    }
}
