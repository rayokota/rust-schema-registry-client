use crate::rest::apis::Error as RestError;
use crate::rest::client_config::ClientConfig;
use crate::rest::models::Mode::{Downgrade, Upgrade};
use crate::rest::models::{Kind, Mode, RegisteredSchema, Rule, RuleSet, Schema};
use crate::rest::schema_registry_client::Client;
use crate::serdes::config::{DeserializerConfig, SchemaSelector, SerializerConfig};
use crate::serdes::rule_registry::{
    RuleOverride, RuleRegistry, get_rule_action, get_rule_actions, get_rule_executor,
    get_rule_executors, get_rule_override, get_rule_overrides,
};
use crate::serdes::serde::SerdeError::Serialization;
use crate::serdes::wildcard_matcher::wildcard_match;
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use dashmap::DashMap;
use futures::future::BoxFuture;
use prost::Message;
use prost::bytes::Bytes;
use referencing::Registry;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, Mutex};
use std::{fmt, io};
use tink_core::TinkError;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SerdeFormat {
    Avro,
    Json,
    Protobuf,
}

#[derive(Clone, Debug)]
pub enum SerdeValue {
    Avro(apache_avro::types::Value),
    Json(serde_json::Value),
    Protobuf(prost_reflect::Value),
}

impl SerdeValue {
    pub fn new_string(format: &SerdeFormat, value: &str) -> SerdeValue {
        match format {
            SerdeFormat::Avro => {
                SerdeValue::Avro(apache_avro::types::Value::String(value.to_string()))
            }
            SerdeFormat::Json => SerdeValue::Json(serde_json::Value::String(value.to_string())),
            SerdeFormat::Protobuf => {
                SerdeValue::Protobuf(prost_reflect::Value::String(value.to_string()))
            }
        }
    }

    pub fn new_bytes(format: &SerdeFormat, value: &[u8]) -> SerdeValue {
        match format {
            SerdeFormat::Avro => SerdeValue::Avro(apache_avro::types::Value::Bytes(value.to_vec())),
            SerdeFormat::Json => {
                SerdeValue::Json(serde_json::Value::String(BASE64_STANDARD.encode(value)))
            }
            SerdeFormat::Protobuf => {
                SerdeValue::Protobuf(prost_reflect::Value::Bytes(Bytes::from(value.to_vec())))
            }
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            SerdeValue::Avro(value) => match value {
                apache_avro::types::Value::Boolean(value) => *value,
                _ => true,
            },
            SerdeValue::Json(value) => value.as_bool().unwrap_or(true),
            SerdeValue::Protobuf(value) => match value {
                prost_reflect::Value::Bool(value) => *value,
                _ => true,
            },
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            SerdeValue::Avro(value) => match value {
                apache_avro::types::Value::String(value) => value.clone(),
                _ => String::new(),
            },
            SerdeValue::Json(value) => value.as_str().unwrap_or("").to_string(),
            SerdeValue::Protobuf(value) => match value {
                prost_reflect::Value::String(value) => value.clone(),
                _ => String::new(),
            },
        }
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            SerdeValue::Avro(value) => match value {
                apache_avro::types::Value::Bytes(value) => value.clone(),
                _ => Vec::new(),
            },
            SerdeValue::Json(value) => BASE64_STANDARD
                .decode(value.as_str().unwrap_or_default())
                .unwrap_or_default(),
            SerdeValue::Protobuf(value) => match value {
                prost_reflect::Value::Bytes(value) => value.as_ref().to_vec(),
                _ => Vec::new(),
            },
        }
    }
}

#[derive(Clone, Debug)]
pub enum SerdeSchema {
    Avro((apache_avro::Schema, Vec<apache_avro::Schema>)),
    Json((serde_json::Value, Registry)),
    Protobuf(prost_reflect::FileDescriptor),
}

#[derive(thiserror::Error, Debug)]
pub enum SerdeError {
    #[error("avro error: {0}")]
    Avro(#[from] apache_avro::Error),
    #[error("json referencing error")]
    JsonReferencing(#[from] referencing::Error),
    #[error("json serde error")]
    Json(#[from] serde_json::Error),
    #[error("json validation error: {0}")]
    JsonValidation(String),
    #[error("protobuf decode error")]
    ProtobufDecode(#[from] prost::DecodeError),
    #[error("protobuf encode error")]
    ProtobufEncode(#[from] prost::EncodeError),
    #[error("protobuf reflect error")]
    ProtobufReflect(#[from] prost_reflect::DescriptorError),
    #[error("rule failed: {0}")]
    Rule(String),
    #[error("rule condition failed: {0}")]
    RuleCondition(Rule),
    #[error("rest error")]
    Rest(#[from] RestError),
    #[error("serde error: {0}")]
    Serialization(String),
    #[error("tink error")]
    Tink(#[from] TinkError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SerdeType {
    Key,
    Value,
}

#[derive(Clone, Debug)]
pub struct SerializationContext {
    pub topic: String,
    pub serde_type: SerdeType,
    pub serde_format: SerdeFormat,
    pub headers: Option<HashMap<String, Vec<u8>>>,
}

pub type SubjectNameStrategy =
    fn(topic: &str, serde_type: &SerdeType, schema: Option<&Schema>) -> Option<String>;

pub fn topic_name_strategy(
    topic: &str,
    serde_type: &SerdeType,
    _schema: Option<&Schema>,
) -> Option<String> {
    match serde_type {
        SerdeType::Key => Some(format!("{}-key", topic)),
        SerdeType::Value => Some(format!("{}-value", topic)),
    }
}

#[derive(Clone)]
pub(crate) struct Serde<'a, T: Client> {
    pub client: &'a T,
    pub subject_name_strategy: SubjectNameStrategy,
    pub rule_registry: Option<RuleRegistry>,
}

impl<'a, T: Client> Serde<'a, T> {
    pub fn new(
        client: &'a T,
        subject_name_strategy: SubjectNameStrategy,
        rule_registry: Option<RuleRegistry>,
    ) -> Serde<'a, T> {
        Serde {
            client,
            subject_name_strategy,
            rule_registry,
        }
    }

    pub(crate) async fn get_reader_schema(
        &self,
        subject: &str,
        fmt: Option<&str>,
        use_schema: &Option<SchemaSelector>,
    ) -> Result<Option<RegisteredSchema>, SerdeError> {
        match use_schema {
            Some(SchemaSelector::SchemaId(id)) => {
                let schema = self
                    .client
                    .get_by_subject_and_id(Some(subject), *id, fmt)
                    .await?;
                let rs = self
                    .client
                    .get_by_schema(subject, &schema, false, true)
                    .await?;
                Ok(Some(rs))
            }
            Some(SchemaSelector::LatestVersion) => {
                let latest_schema = self.client.get_latest_version(subject, fmt).await?;
                Ok(Some(latest_schema))
            }
            Some(SchemaSelector::LatestWithMetadata(metadata)) => {
                let latest_schema = self
                    .client
                    .get_latest_with_metadata(subject, metadata, true, fmt)
                    .await?;
                Ok(Some(latest_schema))
            }
            None => Ok(None),
        }
    }

    pub(crate) async fn execute_rules(
        &self,
        ser_ctx: &SerializationContext,
        subject: &str,
        rule_mode: Mode,
        source: Option<&Schema>,
        target: Option<&Schema>,
        parsed_target: Option<&SerdeSchema>,
        msg: &SerdeValue,
        field_transformer: Option<Arc<FieldTransformer>>,
    ) -> Result<SerdeValue, SerdeError> {
        let mut rules: Vec<Rule>;
        match rule_mode {
            Upgrade => rules = self.get_migration_rules(target),
            Downgrade => {
                rules = self.get_migration_rules(source);
                rules.reverse()
            }
            _ => {
                rules = self.get_domain_rules(target);
                if rule_mode == Mode::Read {
                    rules.reverse()
                }
            }
        }

        if rules.is_empty() {
            return Ok(msg.clone());
        }

        let mut msg = msg.clone();
        for (index, rule) in rules.iter().enumerate() {
            if self.is_disabled(rule) {
                continue;
            }
            let mode = rule.mode.unwrap_or_default();
            match mode {
                Mode::WriteRead => {
                    if rule_mode != Mode::Read && rule_mode != Mode::Write {
                        continue;
                    }
                }
                Mode::UpDown => {
                    if rule_mode != Upgrade && rule_mode != Downgrade {
                        continue;
                    }
                }
                _ => {
                    if mode != rule_mode {
                        continue;
                    }
                }
            }
            let mut ctx = RuleContext::new(
                ser_ctx.clone(),
                source.cloned(),
                target.cloned(),
                parsed_target.cloned(),
                subject.to_string(),
                rule_mode,
                rule.clone(),
                index,
                rules.clone(),
                field_transformer.clone(),
                self.rule_registry.clone(),
            );
            let executor = get_executor(self.rule_registry.as_ref(), &rule.r#type);
            if executor.is_none() {
                self.run_action(
                    &ctx,
                    rule_mode,
                    rule,
                    self.get_on_failure(rule).as_deref(),
                    &msg,
                    Some(SerdeError::Rule(format!(
                        "rule executor {} not found",
                        rule.r#type
                    ))),
                    "ERROR",
                )
                .await?;
                return Ok(msg.clone());
            }
            let executor = executor.unwrap().clone();
            let result = executor.transform(&mut ctx, &msg).await;
            if result.is_err() {
                self.run_action(
                    &ctx,
                    rule_mode,
                    rule,
                    self.get_on_failure(rule).as_deref(),
                    &msg,
                    result.err(),
                    "ERROR",
                )
                .await?;
                return Ok(msg.clone());
            }
            let result = result?;
            let kind = rule.kind.unwrap_or_default();
            if kind == Kind::Condition {
                if !result.as_bool() {
                    self.run_action(
                        &ctx,
                        rule_mode,
                        rule,
                        self.get_on_failure(rule).as_deref(),
                        &msg,
                        Some(SerdeError::RuleCondition(rule.clone())),
                        "ERROR",
                    )
                    .await?;
                }
            } else {
                msg = result;
            }
            self.run_action(
                &ctx,
                rule_mode,
                rule,
                self.get_on_success(rule).as_deref(),
                &msg,
                None,
                "NONE",
            )
            .await?;
        }
        Ok(msg.clone())
    }

    fn get_migration_rules(&self, schema: Option<&Schema>) -> Vec<Rule> {
        schema
            .and_then(|schema| schema.rule_set.clone())
            .and_then(|rule_set| rule_set.migration_rules)
            .unwrap_or_default()
    }

    fn get_domain_rules(&self, schema: Option<&Schema>) -> Vec<Rule> {
        schema
            .and_then(|schema| schema.rule_set.clone())
            .and_then(|rule_set| rule_set.domain_rules)
            .unwrap_or_default()
    }

    fn get_on_success(&self, rule: &Rule) -> Option<String> {
        get_override(self.rule_registry.as_ref(), &rule.r#type)
            .and_then(|rule_override| rule_override.on_success.clone())
            .or(rule.on_success.clone())
    }

    fn get_on_failure(&self, rule: &Rule) -> Option<String> {
        get_override(self.rule_registry.as_ref(), &rule.r#type)
            .and_then(|rule_override| rule_override.on_failure.clone())
            .or(rule.on_failure.clone())
    }

    fn is_disabled(&self, rule: &Rule) -> bool {
        get_override(self.rule_registry.as_ref(), &rule.r#type)
            .and_then(|rule_override| rule_override.disabled)
            .unwrap_or(false)
    }

    async fn run_action(
        &self,
        ctx: &RuleContext,
        rule_mode: Mode,
        rule: &Rule,
        action: Option<&str>,
        msg: &SerdeValue,
        ex: Option<SerdeError>,
        default_action: &str,
    ) -> Result<(), SerdeError> {
        let action_name = self.get_rule_action_name(rule, rule_mode, action);
        let action_name = action_name.unwrap_or(default_action.to_string());
        let action = self.get_rule_action(ctx, &action_name);
        let action = action.ok_or(SerdeError::Rule(format!(
            "rule action {} not found",
            action_name
        )))?;
        action.run(ctx, msg, ex).await
    }

    fn get_rule_action_name(
        &self,
        rule: &Rule,
        mode: Mode,
        action_name: Option<&str>,
    ) -> Option<String> {
        let action_name = action_name?;
        if rule.mode.is_none() {
            return Some(action_name.to_string());
        }
        let rule_mode = rule.mode.unwrap();
        if (rule_mode == Mode::WriteRead || rule_mode == Mode::UpDown) && action_name.contains(",")
        {
            let parts: Vec<_> = action_name.split(",").collect();
            if mode == Mode::Write || mode == Upgrade {
                return Some(parts[0].to_string());
            } else if mode == Mode::Read || mode == Downgrade {
                return Some(parts[1].to_string());
            }
        }
        Some(action_name.to_string())
    }

    fn get_rule_action(&self, ctx: &RuleContext, action_name: &str) -> Option<Arc<dyn RuleAction>> {
        if action_name == "ERROR" {
            return Some(Arc::new(ErrorAction {}));
        } else if action_name == "NONE" {
            return Some(Arc::new(NoneAction {}));
        }
        get_action(self.rule_registry.as_ref(), action_name)
    }

    fn has_rules(&self, rule_set: Option<&RuleSet>, mode: Mode) -> bool {
        if rule_set.is_none() {
            return false;
        }
        let migration_rules = rule_set.unwrap().migration_rules.as_ref();
        if migration_rules.is_none() {
            return false;
        }
        let mut migration_rules = migration_rules.unwrap().iter();
        match mode {
            Upgrade | Downgrade => migration_rules.any(|rule| {
                rule.mode
                    .map(|m| m == mode || m == Mode::UpDown)
                    .unwrap_or(false)
            }),
            Mode::Write | Mode::Read => migration_rules.any(|rule| {
                rule.mode
                    .map(|m| m == mode || m == Mode::WriteRead)
                    .unwrap_or(false)
            }),
            _ => migration_rules.any(|rule| rule.mode.map(|m| m == mode).unwrap_or(false)),
        }
    }

    pub(crate) async fn get_migrations(
        &self,
        subject: &str,
        source_info: &Schema,
        target: &RegisteredSchema,
        format: Option<&str>,
    ) -> Result<Vec<Migration>, SerdeError> {
        let source = self
            .client
            .get_by_schema(subject, source_info, false, true)
            .await?;
        let mut migrations = Vec::new();
        let migration_mode: Mode;
        let first: &RegisteredSchema;
        let last: &RegisteredSchema;
        match source.version.cmp(&target.version) {
            Ordering::Less => {
                migration_mode = Upgrade;
                first = &source;
                last = target;
            }
            Ordering::Greater => {
                migration_mode = Downgrade;
                first = target;
                last = &source;
            }
            Ordering::Equal => {
                return Ok(migrations);
            }
        }
        let mut previous = None;
        let versions = self
            .get_schemas_between(subject, first, last, format)
            .await?;
        for (i, version) in versions.iter().enumerate() {
            if i == 0 {
                previous = Some(version);
                continue;
            }
            if self.has_rules(version.rule_set.as_deref(), migration_mode) {
                let migration: Migration = if migration_mode == Upgrade {
                    Migration {
                        rule_mode: migration_mode,
                        source: previous.cloned(),
                        target: Some(version.clone()),
                    }
                } else {
                    Migration {
                        rule_mode: migration_mode,
                        source: Some(version.clone()),
                        target: previous.cloned(),
                    }
                };
                migrations.push(migration);
            }
            previous = Some(version);
        }
        if migration_mode == Downgrade {
            migrations.reverse();
        }
        Ok(migrations)
    }

    pub(crate) async fn get_schemas_between(
        &self,
        subject: &str,
        first: &RegisteredSchema,
        last: &RegisteredSchema,
        format: Option<&str>,
    ) -> Result<Vec<RegisteredSchema>, SerdeError> {
        if last.version.unwrap_or_default() - first.version.unwrap_or_default() < 2 {
            return Ok(vec![first.clone(), last.clone()]);
        }
        let version1 = first.version.unwrap_or_default();
        let version2 = last.version.unwrap_or_default();
        let mut result = vec![first.clone()];
        for i in (version1 + 1)..version2 {
            let schema = self.client.get_version(subject, i, true, format).await?;
            result.push(schema);
        }
        result.push(last.clone());
        Ok(result)
    }

    pub(crate) async fn execute_migrations(
        &self,
        ser_ctx: &SerializationContext,
        subject: &str,
        migrations: &[Migration],
        msg: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        let mut msg = msg.clone();
        for migration in migrations {
            let source = migration.source.as_ref().map(|s| s.to_schema());
            let target = migration.target.as_ref().map(|s| s.to_schema());
            let result = self
                .execute_rules(
                    ser_ctx,
                    subject,
                    migration.rule_mode,
                    source.as_ref(),
                    target.as_ref(),
                    None,
                    &msg,
                    None,
                )
                .await?;
            msg = result;
        }
        Ok(msg)
    }
}

impl<T: Client> Debug for Serde<'_, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Serde")
            .field("subject_name_strategy", &"SubjectNameStrategy")
            .field("field_transformer", &"FieldTransformer")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BaseSerializer<'a, T: Client> {
    pub serde: Serde<'a, T>,
    pub config: SerializerConfig,
}

impl<'a, T: Client> BaseSerializer<'a, T> {
    pub fn new(serde: Serde<'a, T>, config: SerializerConfig) -> BaseSerializer<'a, T> {
        BaseSerializer { serde, config }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BaseDeserializer<'a, T: Client> {
    pub serde: Serde<'a, T>,
    pub config: DeserializerConfig,
}

impl<'a, T: Client> BaseDeserializer<'a, T> {
    pub fn new(serde: Serde<'a, T>, config: DeserializerConfig) -> BaseDeserializer<'a, T> {
        BaseDeserializer { serde, config }
    }
}

pub trait RuleBase: Send + Sync {
    fn configure(
        &self,
        client_config: &ClientConfig,
        rule_config: &HashMap<String, String>,
    ) -> Result<(), SerdeError> {
        Ok(())
    }
    fn get_type(&self) -> &'static str;
    fn as_any(&self) -> &dyn std::any::Any;
    fn close(&mut self) {}
}

#[async_trait]
pub trait RuleExecutor: RuleBase {
    async fn transform(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError>;

    fn as_field_rule_executor(&self) -> Option<&dyn FieldRuleExecutor> {
        None
    }
}

#[async_trait]
pub trait FieldRuleExecutor: RuleExecutor {
    async fn transform_field(
        &self,
        ctx: &mut RuleContext,
        field_value: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError>;
}

#[async_trait]
impl<FRE: FieldRuleExecutor> RuleExecutor for FRE {
    async fn transform(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        if ctx.rule_mode == Mode::Write || ctx.rule_mode == Upgrade {
            for i in 0..ctx.index {
                let other_rule = &ctx.rules[i];
                if are_transforms_with_same_tag(&ctx.rule, other_rule) {
                    return Ok(msg.clone());
                }
            }
        } else if ctx.rule_mode == Mode::Read || ctx.rule_mode == Downgrade {
            for i in (ctx.index + 1)..ctx.rules.len() {
                let other_rule = &ctx.rules[i];
                if are_transforms_with_same_tag(&ctx.rule, other_rule) {
                    return Ok(msg.clone());
                }
            }
        }
        if let Some(field_transformer) = ctx.field_transformer.clone() {
            let result = field_transformer(ctx, self.get_type(), msg).await?;
            return Ok(result);
        }
        Ok(msg.clone())
    }

    fn as_field_rule_executor(&self) -> Option<&dyn FieldRuleExecutor> {
        Some(self)
    }
}

fn are_transforms_with_same_tag(rule1: &Rule, rule2: &Rule) -> bool {
    rule1.tags.is_some()
        && rule2.tags.is_some()
        && rule1.tags == rule2.tags
        && rule1.kind == Some(Kind::Transform)
        && rule1.kind == rule2.kind
        && rule1.mode == rule2.mode
        && rule1.r#type == rule2.r#type
}

#[async_trait]
pub trait RuleAction: RuleBase {
    async fn run(
        &self,
        ctx: &RuleContext,
        msg: &SerdeValue,
        ex: Option<SerdeError>,
    ) -> Result<(), SerdeError>;
}

#[derive(Debug)]
pub struct ErrorAction {}

impl RuleBase for ErrorAction {
    fn get_type(&self) -> &'static str {
        "ERROR"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl RuleAction for ErrorAction {
    async fn run(
        &self,
        ctx: &RuleContext,
        msg: &SerdeValue,
        ex: Option<SerdeError>,
    ) -> Result<(), SerdeError> {
        let err_msg = format!("rule {} failed", ctx.rule.name);
        if let Some(e) = ex {
            Err(Serialization(format!("{}: {}", err_msg, e)))
        } else {
            Err(Serialization(err_msg))
        }
    }
}

#[derive(Debug)]
pub struct NoneAction {}

impl RuleBase for NoneAction {
    fn get_type(&self) -> &'static str {
        "NONE"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl RuleAction for NoneAction {
    async fn run(
        &self,
        ctx: &RuleContext,
        msg: &SerdeValue,
        ex: Option<SerdeError>,
    ) -> Result<(), SerdeError> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum FieldType {
    Record,
    Enum,
    Array,
    Map,
    Combined,
    Fixed,
    String,
    Bytes,
    Int,
    Long,
    Float,
    Double,
    Boolean,
    Null,
}

impl Display for FieldType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FieldType::Record => write!(f, "RECORD"),
            FieldType::Enum => write!(f, "ENUM"),
            FieldType::Array => write!(f, "ARRAY"),
            FieldType::Map => write!(f, "MAP"),
            FieldType::Combined => write!(f, "COMBINED"),
            FieldType::Fixed => write!(f, "FIXED"),
            FieldType::String => write!(f, "STRING"),
            FieldType::Bytes => write!(f, "BYTES"),
            FieldType::Int => write!(f, "INT"),
            FieldType::Long => write!(f, "LONG"),
            FieldType::Float => write!(f, "FLOAT"),
            FieldType::Double => write!(f, "DOUBLE"),
            FieldType::Boolean => write!(f, "BOOLEAN"),
            FieldType::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug)]
pub struct FieldContext {
    pub containing_message: SerdeValue,
    pub full_name: String,
    pub name: String,
    pub field_type: Mutex<FieldType>,
    pub tags: HashSet<String>,
}

impl FieldContext {
    pub fn new(
        containing_message: SerdeValue,
        full_name: String,
        name: String,
        field_type: FieldType,
        tags: HashSet<String>,
    ) -> FieldContext {
        FieldContext {
            containing_message,
            full_name,
            name,
            field_type: Mutex::new(field_type),
            tags,
        }
    }

    pub fn get_field_type(&self) -> FieldType {
        *self.field_type.lock().unwrap()
    }

    pub fn set_field_type(&self, field_type: FieldType) {
        let mut ft = self.field_type.lock().unwrap();
        *ft = field_type;
    }

    pub fn is_primitive(&self) -> bool {
        let field_type = self.get_field_type();
        field_type == FieldType::String
            || field_type == FieldType::Bytes
            || field_type == FieldType::Int
            || field_type == FieldType::Long
            || field_type == FieldType::Float
            || field_type == FieldType::Double
            || field_type == FieldType::Boolean
            || field_type == FieldType::Null
    }

    pub fn type_name(&self) -> String {
        self.get_field_type().to_string().to_uppercase()
    }
}

pub struct RuleContext {
    pub ser_ctx: SerializationContext,
    pub source: Option<Schema>,
    pub target: Option<Schema>,
    pub parsed_target: Option<SerdeSchema>,
    pub subject: String,
    pub rule_mode: Mode,
    pub rule: Rule,
    pub index: usize,
    pub rules: Vec<Rule>,
    pub field_transformer: Option<Arc<FieldTransformer>>,
    pub rule_registry: Option<RuleRegistry>,
    pub field_contexts: Vec<FieldContext>,
}

impl RuleContext {
    pub fn new(
        ser_ctx: SerializationContext,
        source: Option<Schema>,
        target: Option<Schema>,
        parsed_target: Option<SerdeSchema>,
        subject: String,
        rule_mode: Mode,
        rule: Rule,
        index: usize,
        rules: Vec<Rule>,
        field_transformer: Option<Arc<FieldTransformer>>,
        rule_registry: Option<RuleRegistry>,
    ) -> RuleContext {
        RuleContext {
            ser_ctx,
            source,
            target,
            parsed_target,
            subject,
            rule_mode,
            rule,
            index,
            rules,
            field_transformer,
            field_contexts: Vec::new(),
            rule_registry,
        }
    }

    pub fn get_parameter(&self, name: &str) -> Option<&String> {
        let param_value = self
            .rule
            .params
            .as_ref()
            .and_then(|params| params.get(name));
        if param_value.is_some() {
            return param_value;
        }
        self.target
            .as_ref()
            .and_then(|target| target.metadata.as_ref())
            .and_then(|metadata| metadata.properties.as_ref())
            .and_then(|properties| properties.get(name))
    }

    pub fn current_field(&self) -> Option<&FieldContext> {
        self.field_contexts.last()
    }

    pub fn enter_field(
        &mut self,
        containing_message: SerdeValue,
        full_name: String,
        name: String,
        field_type: FieldType,
        tags: HashSet<String>,
    ) {
        let mut all_tags = HashSet::new();
        all_tags.extend(tags);
        all_tags.extend(self.get_tags(&full_name));
        let field_context =
            FieldContext::new(containing_message, full_name, name, field_type, all_tags);
        self.field_contexts.push(field_context);
    }

    pub fn get_tags(&self, full_name: &str) -> HashSet<String> {
        let tags = self
            .target
            .as_ref()
            .and_then(|target| target.metadata.as_ref())
            .and_then(|metadata| metadata.tags.as_ref());
        tags.map(|m| {
            m.iter()
                .filter(|(k, _)| wildcard_match(full_name, k))
                .map(|(k, _)| k.clone())
                .collect()
        })
        .unwrap_or_default()
    }

    pub fn exit_field(&mut self) {
        self.field_contexts.pop();
    }
}

// See https://stackoverflow.com/questions/65696254/is-it-possible-to-create-a-type-alias-for-async-functions
pub type FieldTransformer = Box<
    dyn Send
        + Sync
        + for<'a> Fn(
            &'a mut RuleContext,
            &'a str,
            &'a SerdeValue,
        ) -> BoxFuture<'a, Result<SerdeValue, SerdeError>>,
>;

#[derive(Clone, Debug)]
pub struct Migration {
    pub rule_mode: Mode,
    pub source: Option<RegisteredSchema>,
    pub target: Option<RegisteredSchema>,
}

pub struct ParsedSchemaCache<T: Clone> {
    parsed_schemas: DashMap<Schema, T>,
}

impl<T: Clone> Default for ParsedSchemaCache<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone> ParsedSchemaCache<T> {
    pub fn new() -> ParsedSchemaCache<T> {
        ParsedSchemaCache {
            parsed_schemas: DashMap::new(),
        }
    }

    pub fn set(&self, schema: &Schema, parsed_schema: T) {
        self.parsed_schemas.insert(schema.clone(), parsed_schema);
    }

    pub fn get(&self, schema: &Schema) -> Option<T> {
        self.parsed_schemas.get(schema).map(|v| v.clone())
    }

    pub fn clear(&self) {
        self.parsed_schemas.clear();
    }
}

pub(crate) fn get_executor(
    rule_registry: Option<&RuleRegistry>,
    r#type: &str,
) -> Option<Arc<dyn RuleExecutor>> {
    if let Some(rule_registry) = rule_registry {
        rule_registry.get_executor(r#type)
    } else {
        get_rule_executor(r#type)
    }
}

pub(crate) fn get_executors(rule_registry: Option<&RuleRegistry>) -> Vec<Arc<dyn RuleExecutor>> {
    if let Some(rule_registry) = rule_registry {
        rule_registry.get_executors()
    } else {
        get_rule_executors()
    }
}

pub(crate) fn get_action(
    rule_registry: Option<&RuleRegistry>,
    r#type: &str,
) -> Option<Arc<dyn RuleAction>> {
    if let Some(rule_registry) = rule_registry {
        rule_registry.get_action(r#type)
    } else {
        get_rule_action(r#type)
    }
}

pub(crate) fn get_actions(rule_registry: Option<&RuleRegistry>) -> Vec<Arc<dyn RuleAction>> {
    if let Some(rule_registry) = rule_registry {
        rule_registry.get_actions()
    } else {
        get_rule_actions()
    }
}

pub(crate) fn get_override(
    rule_registry: Option<&RuleRegistry>,
    r#type: &str,
) -> Option<RuleOverride> {
    if let Some(rule_registry) = rule_registry {
        rule_registry.get_override(r#type)
    } else {
        get_rule_override(r#type)
    }
}

pub(crate) fn get_overrides(rule_registry: Option<&RuleRegistry>) -> Vec<RuleOverride> {
    if let Some(rule_registry) = rule_registry {
        rule_registry.get_overrides()
    } else {
        get_rule_overrides()
    }
}
