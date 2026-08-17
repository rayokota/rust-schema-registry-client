use crate::rest::models::{Kind, Mode};
use crate::rest::models::{Phase, Schema};
use crate::rest::schema_registry_client::Client;
use crate::serdes::config::{DeserializerConfig, SerializerConfig};
use crate::serdes::rule_registry::RuleRegistry;
use crate::serdes::serde::SerdeError::Serialization;
use crate::serdes::serde::{
    BaseDeserializer, BaseSerializer, FALLBACK_TYPE_CONFIG, FieldTransformer, FieldType,
    KAFKA_CLUSTER_ID_CONFIG, RuleContext, SchemaId, Serde, SerdeError, SerdeFormat, SerdeSchema,
    SerdeType, SerdeValue, SerializationContext, SubjectCacheKey, SubjectNameStrategyType,
    get_executor, get_executors, load_associated_subject, parse_subject_name_strategy_type,
    topic_name_strategy,
};
use crate::serdes::validation_rule::{
    VALIDATION_RULES_PROP, ValidationRule, ValidationRuleError, ValidationRuleExecutor,
    ValidationRulesExecution, append_validation_path, evaluate_validation_rule,
    parse_validation_rules, raise_validation_violations,
};
use apache_avro::schema::{Name, RecordField, RecordSchema, UnionSchema};
use apache_avro::types::Value;
use async_recursion::async_recursion;
use dashmap::DashMap;
use futures::StreamExt;
use futures::future::FutureExt;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub(crate) struct AvroSerde {
    parsed_schemas: DashMap<Schema, (apache_avro::Schema, Vec<apache_avro::Schema>)>,
    subject_cache: DashMap<SubjectCacheKey, Option<String>>,
}

#[derive(Clone)]
pub struct AvroSerializer<'a, T: Client> {
    schema: Option<&'a Schema>,
    base: BaseSerializer<'a, T>,
    serde: AvroSerde,
    subject_name_strategy_type: SubjectNameStrategyType,
}

impl<'a, T: Client> std::fmt::Debug for AvroSerializer<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSerializer")
            .field("schema", &self.schema)
            .field("serde", &self.serde)
            .field(
                "subject_name_strategy_type",
                &self.subject_name_strategy_type,
            )
            .finish_non_exhaustive()
    }
}

impl<'a, T: Client + Sync> AvroSerializer<'a, T> {
    pub fn new(
        client: &'a T,
        schema: Option<&'a Schema>,
        rule_registry: Option<RuleRegistry>,
        serializer_config: SerializerConfig,
    ) -> Result<AvroSerializer<'a, T>, SerdeError> {
        for executor in get_executors(rule_registry.as_ref()) {
            executor.configure(client.config(), &serializer_config.rule_config)?;
        }
        Ok(AvroSerializer {
            schema,
            base: BaseSerializer::new(Serde::new(client, rule_registry), serializer_config.clone()),
            serde: AvroSerde {
                parsed_schemas: DashMap::new(),
                subject_cache: DashMap::new(),
            },
            subject_name_strategy_type: serializer_config.subject_name_strategy_type,
        })
    }

    pub async fn serialize_ser(
        &self,
        ctx: &SerializationContext,
        value: impl Serialize,
    ) -> Result<Vec<u8>, SerdeError> {
        let v = apache_avro::to_value(value)?;
        self.serialize(ctx, v).await
    }

    pub async fn serialize(
        &self,
        ctx: &SerializationContext,
        value: Value,
    ) -> Result<Vec<u8>, SerdeError> {
        let mut value = value;
        let subject = self
            .get_subject(&ctx.topic, &ctx.serde_type, self.schema)
            .await?;
        let latest_schema = if let Some(ref subj) = subject {
            self.base
                .serde
                .get_reader_schema(subj, None, &self.base.config.use_schema)
                .await?
        } else {
            None
        };
        let subject = subject.ok_or_else(|| {
            Serialization("Could not determine subject for serialization".to_string())
        })?;

        let schema_id;
        if let Some(ref schema) = latest_schema {
            schema_id = SchemaId::new(SerdeFormat::Avro, schema.id, schema.guid.clone(), None)?;
        } else {
            let schema = self
                .schema
                .ok_or(Serialization("schema needs to be set".to_string()))?;
            if self.base.config.auto_register_schemas {
                let rs = self
                    .base
                    .serde
                    .client
                    .register_schema(&subject, schema, self.base.config.normalize_schemas)
                    .await?;
                schema_id = SchemaId::new(SerdeFormat::Avro, rs.id, rs.guid.clone(), None)?;
            } else {
                let rs = self
                    .base
                    .serde
                    .client
                    .get_by_schema(&subject, schema, self.base.config.normalize_schemas, false)
                    .await?;
                schema_id = SchemaId::new(SerdeFormat::Avro, rs.id, rs.guid.clone(), None)?;
            }
        }

        let schema_tuple;
        if let Some(ref latest_schema) = latest_schema {
            let schema = latest_schema.to_schema();
            schema_tuple = self.get_parsed_schema(&schema).await?;
            if self
                .base
                .validation_enabled(Some(ValidationRulesExecution::BeforeDomainRules))
            {
                self.validate_inline_rules(&schema_tuple, &value)?;
            }
            let field_transformer: FieldTransformer =
                Box::new(|ctx, value| transform_fields(ctx, value).boxed());
            let serde_value = self
                .base
                .serde
                .execute_rules(
                    ctx,
                    &subject,
                    Mode::Write,
                    None,
                    Some(&schema),
                    Some(&SerdeSchema::Avro(schema_tuple.clone())),
                    &SerdeValue::Avro(value),
                    Some(Arc::new(field_transformer)),
                )
                .await?;
            value = match serde_value {
                SerdeValue::Avro(value) => value,
                _ => return Err(Serialization("unexpected serde value".to_string())),
            };
            if self
                .base
                .validation_enabled(Some(ValidationRulesExecution::AfterDomainRules))
            {
                self.validate_inline_rules(&schema_tuple, &value)?;
            }
        } else {
            let schema = self
                .schema
                .ok_or(Serialization("schema needs to be set".to_string()))?;
            schema_tuple = self.get_parsed_schema(schema).await?;
            // No domain rules run on this path, so there is a single validation point
            // regardless of the configured phase.
            if self.base.validation_enabled(None) {
                self.validate_inline_rules(&schema_tuple, &value)?;
            }
        }

        let mut encoded_bytes = if matches!(schema_tuple.0, apache_avro::Schema::Bytes) {
            // If the writer schema is bytes, just pass the bytes along
            match value {
                Value::Bytes(bytes) => bytes.clone(),
                _ => {
                    return Err(Serialization(
                        "expected bytes value for bytes schema".to_string(),
                    ));
                }
            }
        } else {
            apache_avro::to_avro_datum_schemata(
                &schema_tuple.0,
                schema_tuple.1.iter().collect(),
                value,
            )?
        };
        if let Some(ref latest_schema) = latest_schema {
            let schema = latest_schema.to_schema();
            if let Some(ref rule_set) = schema.rule_set
                && rule_set.encoding_rules.is_some()
            {
                encoded_bytes = self
                    .base
                    .serde
                    .execute_rules_with_phase(
                        ctx,
                        &subject,
                        Phase::Encoding,
                        Mode::Write,
                        None,
                        Some(&schema),
                        None,
                        &SerdeValue::new_bytes(SerdeFormat::Avro, &encoded_bytes),
                        None,
                    )
                    .await?
                    .as_bytes();
            }
        }

        let id_ser = self.base.config.schema_id_serializer;
        id_ser(&encoded_bytes, ctx, &schema_id)
    }

    /// Evaluates the schema's inline validation rules against `value`, returning a single
    /// error listing every violation found.
    fn validate_inline_rules(
        &self,
        schema_tuple: &(apache_avro::Schema, Vec<apache_avro::Schema>),
        value: &Value,
    ) -> Result<(), SerdeError> {
        let executor = self.base.validation_executor()?;
        raise_validation_violations(validate_message(
            executor.as_ref(),
            &schema_tuple.0,
            &schema_tuple.1,
            value,
            self.base.config.validation_rules_fail_fast,
        ))
    }

    async fn get_parsed_schema(
        &self,
        schema: &Schema,
    ) -> Result<(apache_avro::Schema, Vec<apache_avro::Schema>), SerdeError> {
        let parsed_schema = self.serde.parsed_schemas.get(schema);
        if let Some(parsed_schema) = parsed_schema {
            return Ok(parsed_schema.clone());
        }
        let mut schemas = Vec::new();
        resolve_named_schema(
            schema,
            self.base.serde.client,
            &mut schemas,
            &mut HashSet::new(),
        )
        .await?;
        let parsed_schema = apache_avro::Schema::parse_str_with_list(
            &schema.schema,
            schemas.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )?;
        self.serde
            .parsed_schemas
            .insert(schema.clone(), parsed_schema.clone());
        Ok(parsed_schema)
    }

    pub async fn get_record_name(&self, schema: &Schema) -> Result<String, SerdeError> {
        let (parsed_schema, _) = self.get_parsed_schema(schema).await?;
        match parsed_schema {
            apache_avro::Schema::Record(r) => Ok(match &r.name.namespace {
                Some(ns) => format!("{ns}.{}", r.name.name),
                None => r.name.name.clone(),
            }),
            _ => Err(Serialization(
                "Schema is not an Avro record type".to_string(),
            )),
        }
    }

    async fn get_subject(
        &self,
        topic: &str,
        serde_type: &SerdeType,
        schema: Option<&Schema>,
    ) -> Result<Option<String>, SerdeError> {
        self.get_subject_for_type(self.subject_name_strategy_type, topic, serde_type, schema)
            .await
    }

    #[async_recursion]
    async fn get_subject_for_type(
        &self,
        strategy_type: SubjectNameStrategyType,
        topic: &str,
        serde_type: &SerdeType,
        schema: Option<&Schema>,
    ) -> Result<Option<String>, SerdeError> {
        match strategy_type {
            SubjectNameStrategyType::Record => {
                if let Some(schema) = schema {
                    Ok(Some(self.get_record_name(schema).await?))
                } else {
                    Ok(None)
                }
            }
            SubjectNameStrategyType::TopicRecord => {
                if let Some(schema) = schema {
                    let name = self.get_record_name(schema).await?;
                    Ok(Some(format!("{topic}-{name}")))
                } else {
                    Ok(None)
                }
            }
            SubjectNameStrategyType::Associated => {
                match load_associated_subject(
                    self.base.serde.client,
                    &self.serde.subject_cache,
                    &self.base.config.strategy_config,
                    topic,
                    serde_type,
                    schema,
                )
                .await?
                {
                    Some(s) => Ok(Some(s)),
                    None => {
                        let fallback_type = self
                            .base
                            .config
                            .strategy_config
                            .get(FALLBACK_TYPE_CONFIG)
                            .map(|s| parse_subject_name_strategy_type(s))
                            .transpose()?
                            .unwrap_or(SubjectNameStrategyType::Topic);
                        match fallback_type {
                            SubjectNameStrategyType::None | SubjectNameStrategyType::Associated => {
                                Ok(None)
                            }
                            other => {
                                self.get_subject_for_type(other, topic, serde_type, schema)
                                    .await
                            }
                        }
                    }
                }
            }
            _ => Ok(Some(
                topic_name_strategy(topic, serde_type, schema).unwrap(),
            )),
        }
    }

    fn close(&mut self) {}
}

async fn transform_fields(
    ctx: &mut RuleContext,
    value: &SerdeValue,
) -> Result<SerdeValue, SerdeError> {
    if let Some(SerdeSchema::Avro((s, named))) = ctx.parsed_target.clone()
        && let SerdeValue::Avro(v) = value
    {
        let value = transform(ctx, &s, &named, v).await?;
        return Ok(SerdeValue::Avro(value));
    }
    Ok(value.clone())
}

#[derive(Clone, Debug, PartialEq)]
pub struct NamedValue {
    pub name: Option<Name>,
    pub value: Value,
}

#[derive(Clone)]
pub struct AvroDeserializer<'a, T: Client> {
    base: BaseDeserializer<'a, T>,
    serde: AvroSerde,
    subject_name_strategy_type: SubjectNameStrategyType,
}

impl<'a, T: Client> std::fmt::Debug for AvroDeserializer<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroDeserializer")
            .field("serde", &self.serde)
            .field(
                "subject_name_strategy_type",
                &self.subject_name_strategy_type,
            )
            .finish_non_exhaustive()
    }
}

impl<'a, T: Client + Sync> AvroDeserializer<'a, T> {
    pub fn new(
        client: &'a T,
        rule_registry: Option<RuleRegistry>,
        deserializer_config: DeserializerConfig,
    ) -> Result<AvroDeserializer<'a, T>, SerdeError> {
        for executor in get_executors(rule_registry.as_ref()) {
            executor.configure(client.config(), &deserializer_config.rule_config)?;
        }
        Ok(AvroDeserializer {
            base: BaseDeserializer::new(
                Serde::new(client, rule_registry),
                deserializer_config.clone(),
            ),
            serde: AvroSerde {
                parsed_schemas: DashMap::new(),
                subject_cache: DashMap::new(),
            },
            subject_name_strategy_type: deserializer_config.subject_name_strategy_type,
        })
    }

    pub async fn deserialize(
        &self,
        ctx: &SerializationContext,
        data: &[u8],
    ) -> Result<NamedValue, SerdeError> {
        // Get initial subject using configured subject name strategy (without schema)
        let initial_subject = self
            .get_subject(&ctx.topic, &ctx.serde_type, None)
            .await
            .ok()
            .flatten();
        let mut latest_schema = None;

        // Try to get reader schema with initial subject
        if let Some(ref init_subj) = initial_subject {
            latest_schema = self
                .base
                .serde
                .get_reader_schema(init_subj, None, &self.base.config.use_schema)
                .await
                .ok()
                .flatten();
        }

        let mut schema_id = SchemaId::new(SerdeFormat::Avro, None, None, None)?;
        let id_deser = self.base.config.schema_id_deserializer;
        let bytes_read = id_deser(data, ctx, &mut schema_id)?;
        let mut data = &data[bytes_read..];

        let writer_schema_raw = self
            .base
            .get_writer_schema(&schema_id, initial_subject.as_deref(), None)
            .await?;
        let (writer_schema, writer_named) = self.get_parsed_schema(&writer_schema_raw).await?;

        // Recompute subject with writer schema (needed for Record/TopicRecord strategies)
        let subject = self
            .get_subject(&ctx.topic, &ctx.serde_type, Some(&writer_schema_raw))
            .await?;

        // If subject changed, try to get reader schema again
        if subject != initial_subject {
            if let Some(ref subj) = subject {
                if let Ok(Some(schema)) = self
                    .base
                    .serde
                    .get_reader_schema(subj, None, &self.base.config.use_schema)
                    .await
                {
                    latest_schema = Some(schema);
                }
            }
        }

        let subject = subject.ok_or_else(|| {
            Serialization("Could not determine subject for deserialization".to_string())
        })?;
        let serde_value;
        if let Some(ref rule_set) = writer_schema_raw.rule_set
            && rule_set.encoding_rules.is_some()
        {
            serde_value = self
                .base
                .serde
                .execute_rules_with_phase(
                    ctx,
                    &subject,
                    Phase::Encoding,
                    Mode::Read,
                    None,
                    Some(&writer_schema_raw),
                    None,
                    &SerdeValue::new_bytes(SerdeFormat::Avro, data),
                    None,
                )
                .await?
                .as_bytes();
            data = &serde_value;
        }

        let migrations;
        let reader_schema_raw;
        let reader_schema;
        let reader_named;
        if let Some(ref latest_schema) = latest_schema {
            migrations = self
                .base
                .serde
                .get_migrations(&subject, &writer_schema_raw, latest_schema, None)
                .await?;
            reader_schema_raw = latest_schema.to_schema();
            (reader_schema, reader_named) = self.get_parsed_schema(&reader_schema_raw).await?;
        } else {
            migrations = Vec::new();
            reader_schema_raw = writer_schema_raw.clone();
            reader_schema = writer_schema.clone();
            reader_named = writer_named.clone();
        }

        let mut reader = Cursor::new(data);
        let mut value;
        if let Some(ref latest_schema) = latest_schema {
            value = if matches!(writer_schema, apache_avro::Schema::Bytes) {
                // If the writer schema is bytes, just pass the bytes along
                Value::Bytes(data.to_vec())
            } else {
                apache_avro::from_avro_datum_schemata(
                    &writer_schema,
                    writer_named.iter().collect(),
                    &mut reader,
                    None,
                )?
            };
            let json = from_avro_value(value.clone())?;
            let mut serde_value = SerdeValue::Json(json);
            serde_value = self
                .base
                .serde
                .execute_migrations(ctx, &subject, &migrations, &serde_value)
                .await?;
            value = match serde_value {
                SerdeValue::Json(v) => to_avro_value(&value, &v)?,
                _ => return Err(Serialization("unexpected serde value".to_string())),
            }
        } else {
            value = if matches!(writer_schema, apache_avro::Schema::Bytes) {
                Value::Bytes(data.to_vec())
            } else {
                apache_avro::from_avro_datum_reader_schemata(
                    &writer_schema,
                    writer_named.iter().collect(),
                    &mut reader,
                    Some(&reader_schema),
                    reader_named.iter().collect(),
                )?
            };
        }

        let field_transformer: FieldTransformer =
            Box::new(|ctx, value| transform_fields(ctx, value).boxed());
        let serde_value = self
            .base
            .serde
            .execute_rules(
                ctx,
                &subject,
                Mode::Read,
                None,
                Some(&reader_schema_raw),
                Some(&SerdeSchema::Avro((
                    reader_schema.clone(),
                    reader_named.clone(),
                ))),
                &SerdeValue::Avro(value),
                Some(Arc::new(field_transformer)),
            )
            .await?;
        value = match serde_value {
            SerdeValue::Avro(value) => value,
            _ => return Err(Serialization("unexpected serde value".to_string())),
        };

        Ok(NamedValue {
            name: self.get_name(&reader_schema),
            value,
        })
    }

    fn get_name(&self, schema: &apache_avro::Schema) -> Option<Name> {
        match schema {
            apache_avro::Schema::Record(schema) => Some(schema.name.clone()),
            _ => None,
        }
    }

    async fn get_parsed_schema(
        &self,
        schema: &Schema,
    ) -> Result<(apache_avro::Schema, Vec<apache_avro::Schema>), SerdeError> {
        let parsed_schema = self.serde.parsed_schemas.get(schema);
        if let Some(parsed_schema) = parsed_schema {
            return Ok(parsed_schema.clone());
        }
        let mut schemas = Vec::new();
        resolve_named_schema(
            schema,
            self.base.serde.client,
            &mut schemas,
            &mut HashSet::new(),
        )
        .await?;
        let parsed_schema = apache_avro::Schema::parse_str_with_list(
            &schema.schema,
            schemas.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )?;
        self.serde
            .parsed_schemas
            .insert(schema.clone(), parsed_schema.clone());
        Ok(parsed_schema)
    }

    pub async fn get_record_name(&self, schema: &Schema) -> Result<String, SerdeError> {
        let (parsed_schema, _) = self.get_parsed_schema(schema).await?;
        match parsed_schema {
            apache_avro::Schema::Record(r) => Ok(match &r.name.namespace {
                Some(ns) => format!("{ns}.{}", r.name.name),
                None => r.name.name.clone(),
            }),
            _ => Err(Serialization(
                "Schema is not an Avro record type".to_string(),
            )),
        }
    }

    async fn get_subject(
        &self,
        topic: &str,
        serde_type: &SerdeType,
        schema: Option<&Schema>,
    ) -> Result<Option<String>, SerdeError> {
        self.get_subject_for_type(self.subject_name_strategy_type, topic, serde_type, schema)
            .await
    }

    #[async_recursion]
    async fn get_subject_for_type(
        &self,
        strategy_type: SubjectNameStrategyType,
        topic: &str,
        serde_type: &SerdeType,
        schema: Option<&Schema>,
    ) -> Result<Option<String>, SerdeError> {
        match strategy_type {
            SubjectNameStrategyType::Record => {
                if let Some(schema) = schema {
                    Ok(Some(self.get_record_name(schema).await?))
                } else {
                    Ok(None)
                }
            }
            SubjectNameStrategyType::TopicRecord => {
                if let Some(schema) = schema {
                    let name = self.get_record_name(schema).await?;
                    Ok(Some(format!("{topic}-{name}")))
                } else {
                    Ok(None)
                }
            }
            SubjectNameStrategyType::Associated => {
                match load_associated_subject(
                    self.base.serde.client,
                    &self.serde.subject_cache,
                    &self.base.config.strategy_config,
                    topic,
                    serde_type,
                    schema,
                )
                .await?
                {
                    Some(s) => Ok(Some(s)),
                    None => {
                        let fallback_type = self
                            .base
                            .config
                            .strategy_config
                            .get(FALLBACK_TYPE_CONFIG)
                            .map(|s| parse_subject_name_strategy_type(s))
                            .transpose()?
                            .unwrap_or(SubjectNameStrategyType::Topic);
                        match fallback_type {
                            SubjectNameStrategyType::None | SubjectNameStrategyType::Associated => {
                                Ok(None)
                            }
                            other => {
                                self.get_subject_for_type(other, topic, serde_type, schema)
                                    .await
                            }
                        }
                    }
                }
            }
            _ => Ok(Some(
                topic_name_strategy(topic, serde_type, schema).unwrap(),
            )),
        }
    }
}

#[async_recursion]
async fn resolve_named_schema<'a, T>(
    schema: &Schema,
    client: &'a T,
    schemas: &mut Vec<String>,
    visited: &mut HashSet<String>,
) -> Result<(), SerdeError>
where
    T: Client + Sync,
{
    if let Some(refs) = schema.references.as_ref() {
        for r in refs {
            let name = r.name.clone().unwrap_or_default();
            if visited.contains(&name) {
                continue;
            }
            visited.insert(name);
            let ref_schema = client
                .get_version(
                    &r.subject.clone().unwrap_or_default(),
                    r.version.unwrap_or(-1),
                    true,
                    None,
                )
                .await?;
            resolve_named_schema(&ref_schema.to_schema(), client, schemas, visited).await?;
            schemas.push(ref_schema.schema.clone().unwrap_or_default());
        }
    }
    Ok(())
}

#[async_recursion]
async fn transform(
    ctx: &mut RuleContext,
    schema: &apache_avro::Schema,
    named_schemas: &[apache_avro::Schema],
    message: &Value,
) -> Result<Value, SerdeError> {
    match schema {
        apache_avro::Schema::Union(union) => {
            // A `Value::Union` carries its branch index, and that index is authoritative:
            // `resolve_union` matches structurally, and a `Value::Record` does not record
            // which schema it came from, so it cannot tell two records of the same shape
            // apart. Descend with the unwrapped value - the branch schema describes the
            // inner value, not the wrapper - then re-wrap so the result is still a valid
            // union value.
            if let Value::Union(index, inner) = message {
                let Some(subschema) = union.variants().get(*index as usize) else {
                    return Ok(message.clone());
                };
                let result = transform(ctx, subschema, named_schemas, inner).await?;
                return Ok(Value::Union(*index, Box::new(result)));
            }
            // Not wrapped in a union: fall back to matching structurally.
            let subschema = resolve_union(union, message);
            if subschema.is_none() {
                return Ok(message.clone());
            }
            let result = transform(ctx, subschema.unwrap().1, named_schemas, message).await?;
            return Ok(result);
        }
        apache_avro::Schema::Array(array) => {
            if let Value::Array(items) = message {
                let mut result = Vec::with_capacity(items.len());
                for item in items {
                    let item = transform(ctx, &array.items, named_schemas, item).await?;
                    result.push(item);
                }
                return Ok(Value::Array(result));
            }
        }
        apache_avro::Schema::Map(map) => {
            if let Value::Map(values) = message {
                let mut result: HashMap<String, Value> = HashMap::with_capacity(values.len());
                for (key, value) in values {
                    let value = transform(ctx, &map.types, named_schemas, value).await?;
                    result.insert(key.clone(), value);
                }
                return Ok(Value::Map(result));
            }
        }
        apache_avro::Schema::Record(record) => {
            if let Value::Record(fields) = message {
                let mut result = Vec::with_capacity(fields.len());
                for field in fields {
                    let field =
                        transform_field_with_ctx(ctx, record, named_schemas, field, fields).await?;
                    result.push(field);
                }
                return Ok(Value::Record(result));
            }
        }
        _ => {}
    }
    if let Some(field_ctx) = ctx.current_field() {
        field_ctx.set_field_type(get_type(schema));
        let rule_tags = ctx
            .rule
            .tags
            .clone()
            .map(|v| HashSet::from_iter(v.into_iter()));
        if rule_tags.is_none_or(|tags| !tags.is_disjoint(&field_ctx.tags)) {
            let message_value = SerdeValue::Avro(message.clone());
            let field_executor_type = ctx.rule.r#type.clone();
            let executor = get_executor(ctx.rule_registry.as_ref(), &field_executor_type);
            if let Some(executor) = executor {
                let field_executor =
                    executor
                        .as_field_rule_executor()
                        .ok_or(SerdeError::Rule(format!(
                            "executor {field_executor_type} is not a field rule executor"
                        )))?;
                let new_value = field_executor.transform_field(ctx, &message_value).await?;
                if let SerdeValue::Avro(v) = new_value {
                    return Ok(v);
                }
            }
        }
    }
    Ok(message.clone())
}

async fn transform_field_with_ctx(
    ctx: &mut RuleContext,
    schema: &RecordSchema,
    named_schemas: &[apache_avro::Schema],
    field: &(String, Value),
    message: &[(String, Value)],
) -> Result<(String, Value), SerdeError> {
    let field_schema = schema
        .fields
        .iter()
        .find(|f| f.name == field.0)
        .ok_or(SerdeError::Rule(format!(
            "field {} not found in schema {}",
            field.0, schema.name
        )))?;
    let field_type = get_type(&field_schema.schema);
    let name = field.0.to_string();
    let full_name = schema.name.to_string() + "." + &name;
    let message_value = SerdeValue::Avro(Value::Record(message.to_vec()));
    ctx.enter_field(
        message_value,
        full_name,
        name,
        field_type,
        get_inline_tags(field_schema),
        // A field rule only runs on a primitive value, which never references a named type, so
        // the field's leaf schema is enough (no `named` list needed) to resolve a decimal's scale
        // or a timestamp's unit for the `value` binding and the result write-back.
        Some(SerdeSchema::Avro((
            avro_leaf_schema(&field_schema.schema).clone(),
            Vec::new(),
        ))),
    );
    let new_value = transform(ctx, &field_schema.schema, named_schemas, &field.1).await?;
    if let Some(Kind::Condition) = ctx.rule.kind
        && let Value::Boolean(b) = new_value
        && !b
    {
        return Err(SerdeError::RuleCondition(Box::new(ctx.rule.clone())));
    }
    ctx.exit_field();
    Ok((field.0.clone(), new_value))
}

/// The leaf schema a primitive field value carries: the walk descends unions/arrays/maps and
/// hands a field rule the already-unwrapped element (then re-wraps its result), so the field
/// context stores this leaf - not the container - to reconstruct a decimal's scale or timestamp's
/// unit without the conversion re-wrapping what the walk will wrap again.
fn avro_leaf_schema(schema: &apache_avro::Schema) -> &apache_avro::Schema {
    match schema {
        apache_avro::Schema::Union(u) => {
            for variant in u.variants() {
                if !matches!(variant, apache_avro::Schema::Null) {
                    return avro_leaf_schema(variant);
                }
            }
            schema
        }
        apache_avro::Schema::Array(a) => avro_leaf_schema(&a.items),
        apache_avro::Schema::Map(m) => avro_leaf_schema(&m.types),
        _ => schema,
    }
}

fn get_type(schema: &apache_avro::Schema) -> FieldType {
    match schema {
        apache_avro::Schema::Null => FieldType::Null,
        apache_avro::Schema::Boolean => FieldType::Boolean,
        apache_avro::Schema::Int => FieldType::Int,
        apache_avro::Schema::Long => FieldType::Long,
        apache_avro::Schema::Float => FieldType::Float,
        apache_avro::Schema::Double => FieldType::Double,
        apache_avro::Schema::Bytes => FieldType::Bytes,
        apache_avro::Schema::String => FieldType::String,
        apache_avro::Schema::Fixed(_) => FieldType::Fixed,
        apache_avro::Schema::Enum(_) => FieldType::Enum,
        apache_avro::Schema::Array(_) => FieldType::Array,
        apache_avro::Schema::Map(_) => FieldType::Map,
        apache_avro::Schema::Union(_) => FieldType::Combined,
        apache_avro::Schema::Record(_) => FieldType::Record,
        apache_avro::Schema::Decimal(_) => FieldType::Bytes,
        apache_avro::Schema::BigDecimal => FieldType::Bytes,
        apache_avro::Schema::Uuid => FieldType::String,
        apache_avro::Schema::Date => FieldType::Int,
        apache_avro::Schema::TimeMillis => FieldType::Int,
        apache_avro::Schema::TimeMicros => FieldType::Long,
        apache_avro::Schema::TimestampMillis => FieldType::Long,
        apache_avro::Schema::TimestampMicros => FieldType::Long,
        apache_avro::Schema::TimestampNanos => FieldType::Long,
        apache_avro::Schema::LocalTimestampMillis => FieldType::Long,
        apache_avro::Schema::LocalTimestampMicros => FieldType::Long,
        apache_avro::Schema::LocalTimestampNanos => FieldType::Long,
        apache_avro::Schema::Duration => FieldType::Fixed,
        // TODO assume Ref is a record, is this correct?
        apache_avro::Schema::Ref { name: _ } => FieldType::Record,
    }
}

fn get_inline_tags(field: &RecordField) -> HashSet<String> {
    let tags = field.custom_attributes.get("confluent:tags");
    if let Some(serde_json::Value::Array(tags)) = tags {
        return tags
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
    }
    HashSet::new()
}

/// Walks `message` against `schema`, evaluating every inline `confluent:rules` CHECK
/// constraint encountered and collecting all failures. Read-only — the message is not
/// modified.
///
/// Two kinds of rules are evaluated:
///   - Record-level (`confluent:rules` on a record schema) — `this` is the record.
///   - Field-level (`confluent:rules` on a record's field) — `this` is the field value.
///     Honors the skip-on-null contract: a null field value does not have its rules
///     invoked.
///
/// Failures carry their dotted-path location (e.g. `addr.zip`, `tags[3]`,
/// `scores["foo"]`). The walk continues after each failure so callers see the full set
/// rather than only the first, unless `fail_fast` is set.
fn validate_message(
    executor: &dyn ValidationRuleExecutor,
    schema: &apache_avro::Schema,
    named_schemas: &[apache_avro::Schema],
    message: &Value,
    fail_fast: bool,
) -> Vec<ValidationRuleError> {
    let mut definitions = HashMap::new();
    collect_named_schemas(schema, &mut definitions);
    for named in named_schemas {
        collect_named_schemas(named, &mut definitions);
    }
    let mut violations = Vec::new();
    validate(
        executor,
        schema,
        &definitions,
        "",
        message,
        fail_fast,
        &mut violations,
    );
    violations
}

/// Indexes every named definition reachable from `schema` by name, so that a
/// [`apache_avro::Schema::Ref`] can be resolved back to the definition carrying the inline
/// rules. Recursion terminates because a recursive type reaches itself through a `Ref`,
/// which has no children.
pub(crate) fn collect_named_schemas<'a>(
    schema: &'a apache_avro::Schema,
    out: &mut HashMap<Name, &'a apache_avro::Schema>,
) {
    match schema {
        apache_avro::Schema::Record(record) => {
            out.insert(record.name.clone(), schema);
            for field in &record.fields {
                collect_named_schemas(&field.schema, out);
            }
        }
        apache_avro::Schema::Enum(enum_schema) => {
            out.insert(enum_schema.name.clone(), schema);
        }
        apache_avro::Schema::Fixed(fixed) => {
            out.insert(fixed.name.clone(), schema);
        }
        apache_avro::Schema::Array(array) => collect_named_schemas(&array.items, out),
        apache_avro::Schema::Map(map) => collect_named_schemas(&map.types, out),
        apache_avro::Schema::Union(union) => {
            for variant in union.variants() {
                collect_named_schemas(variant, out);
            }
        }
        _ => {}
    }
}

/// Mirrors [`transform`]'s switch-on-schema-type dispatch shape.
fn validate(
    executor: &dyn ValidationRuleExecutor,
    schema: &apache_avro::Schema,
    named_schemas: &HashMap<Name, &apache_avro::Schema>,
    path: &str,
    message: &Value,
    fail_fast: bool,
    violations: &mut Vec<ValidationRuleError>,
) {
    if fail_fast && !violations.is_empty() {
        return;
    }
    match schema {
        apache_avro::Schema::Union(union) => {
            // Descend into the branch the value actually holds, carrying the unwrapped
            // value: the branch schema describes the inner value, not the union wrapper.
            //
            // A `Value::Union` carries its branch index, and that index is authoritative.
            // `resolve_union` matches structurally, and a `Value::Record` does not record
            // which schema it came from, so it cannot tell two records of the same shape
            // apart and would pick whichever resolves first. Only fall back to matching
            // when the value is not wrapped in a union.
            let (subschema, inner) = match message {
                Value::Union(index, inner) => (
                    union.variants().get(*index as usize),
                    unwrap_union(inner.as_ref()),
                ),
                other => (resolve_union(union, other).map(|(_, schema)| schema), other),
            };
            if let Some(subschema) = subschema {
                validate(
                    executor,
                    subschema,
                    named_schemas,
                    path,
                    inner,
                    fail_fast,
                    violations,
                );
            }
        }
        apache_avro::Schema::Array(array) => {
            if let Value::Array(items) = message {
                for (i, item) in items.iter().enumerate() {
                    validate(
                        executor,
                        &array.items,
                        named_schemas,
                        &format!("{path}[{i}]"),
                        item,
                        fail_fast,
                        violations,
                    );
                    if fail_fast && !violations.is_empty() {
                        return;
                    }
                }
            }
        }
        apache_avro::Schema::Map(map) => {
            if let Value::Map(values) = message {
                for (key, value) in values {
                    validate(
                        executor,
                        &map.types,
                        named_schemas,
                        &format!("{path}[\"{key}\"]"),
                        value,
                        fail_fast,
                        violations,
                    );
                    if fail_fast && !violations.is_empty() {
                        return;
                    }
                }
            }
        }
        apache_avro::Schema::Record(record) => {
            let Value::Record(fields) = message else {
                return;
            };
            // Record-level rules: `this` is the record itself.
            if evaluate_rules(
                executor,
                parse_validation_rules(record.attributes.get(VALIDATION_RULES_PROP)),
                message,
                path,
                fail_fast,
                violations,
            ) {
                return;
            }
            for (name, value) in fields {
                let Some(field) = record.fields.iter().find(|f| &f.name == name) else {
                    continue;
                };
                let field_path = append_validation_path(path, name);
                // Field-level rules: `this` is the field value.
                if evaluate_rules(
                    executor,
                    parse_validation_rules(field.custom_attributes.get(VALIDATION_RULES_PROP)),
                    value,
                    &field_path,
                    fail_fast,
                    violations,
                ) {
                    return;
                }
                validate(
                    executor,
                    &field.schema,
                    named_schemas,
                    &field_path,
                    value,
                    fail_fast,
                    violations,
                );
                if fail_fast && !violations.is_empty() {
                    return;
                }
            }
        }
        apache_avro::Schema::Ref { name } => {
            // A reference to a named schema; the rules live on the definition, so resolve
            // it before descending. Definitions may be inline in the schema being walked
            // (including recursive references to it) as well as in the referenced schemas.
            if let Some(resolved) = named_schemas.get(name) {
                validate(
                    executor,
                    resolved,
                    named_schemas,
                    path,
                    message,
                    fail_fast,
                    violations,
                );
            }
        }
        // Primitives, enums and fixed have no children, and their rules were evaluated by
        // the declaring record.
        _ => {}
    }
}

/// Resolves a value through any enclosing unions, so that a rule on a nullable field sees
/// the underlying value rather than the union wrapper.
fn unwrap_union(value: &Value) -> &Value {
    match value {
        Value::Union(_, inner) => unwrap_union(inner),
        _ => value,
    }
}

/// Evaluates the given rules against `value`, skipping null values to honor the
/// skip-on-null contract. Returns whether the walk should stop.
fn evaluate_rules(
    executor: &dyn ValidationRuleExecutor,
    rules: Vec<ValidationRule>,
    value: &Value,
    path: &str,
    fail_fast: bool,
    violations: &mut Vec<ValidationRuleError>,
) -> bool {
    let value = unwrap_union(value);
    if rules.is_empty() || matches!(value, Value::Null) {
        return false;
    }
    let serde_value = SerdeValue::Avro(value.clone());
    for rule in &rules {
        evaluate_validation_rule(executor, rule, &serde_value, path, violations);
        if fail_fast && !violations.is_empty() {
            return true;
        }
    }
    false
}

fn resolve_union<'a>(
    union: &'a UnionSchema,
    message: &Value,
) -> Option<(usize, &'a apache_avro::Schema)> {
    union.find_schema_with_known_schemata::<apache_avro::Schema>(message, None, &None)
}

fn from_avro_value(value: Value) -> Result<serde_json::Value, SerdeError> {
    Ok(serde_json::Value::try_from(value)?)
}

fn to_avro_value(input: &Value, value: &serde_json::Value) -> Result<Value, SerdeError> {
    let result = match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => (*b).into(),
        serde_json::Value::Number(n) => match input {
            Value::Long(_l) => Value::Long(n.as_i64().unwrap()),
            Value::Float(_f) => Value::Float(n.as_f64().unwrap() as f32),
            Value::Double(_d) => Value::Double(n.as_f64().unwrap()),
            Value::Date(_d) => Value::Date(n.as_i64().unwrap() as i32),
            Value::TimeMillis(_t) => Value::TimeMillis(n.as_i64().unwrap() as i32),
            Value::TimeMicros(_t) => Value::TimeMicros(n.as_i64().unwrap()),
            Value::TimestampMillis(_t) => Value::TimestampMillis(n.as_i64().unwrap()),
            Value::TimestampMicros(_t) => Value::TimestampMicros(n.as_i64().unwrap()),
            Value::TimestampNanos(_t) => Value::TimestampNanos(n.as_i64().unwrap()),
            Value::LocalTimestampMillis(_t) => Value::LocalTimestampMillis(n.as_i64().unwrap()),
            Value::LocalTimestampMicros(_t) => Value::LocalTimestampMicros(n.as_i64().unwrap()),
            Value::LocalTimestampNanos(_t) => Value::LocalTimestampNanos(n.as_i64().unwrap()),
            _ => Value::Int(n.as_i64().unwrap() as i32),
        },
        serde_json::Value::String(s) => match input {
            Value::Enum(i, _s) => Value::Enum(*i, s.to_string()),
            Value::Uuid(_uuid) => Value::Uuid(Uuid::parse_str(s)?),
            _ => s.as_str().into(),
        },
        serde_json::Value::Array(items) => match input {
            Value::Bytes(_bytes) => {
                Value::Bytes(items.iter().map(|v| v.as_u64().unwrap() as u8).collect())
            }
            Value::Fixed(size, _items) => Value::Fixed(
                *size,
                items.iter().map(|v| v.as_u64().unwrap() as u8).collect(),
            ),
            Value::Decimal(_d) => {
                let items: Vec<u8> = items.iter().map(|v| v.as_u64().unwrap() as u8).collect();
                Value::Decimal(items.into())
            }
            // TODO BigDecimal
            _ => Value::Array(
                items
                    .iter()
                    .map(|v| to_avro_value(input, v))
                    .collect::<Result<Vec<Value>, SerdeError>>()?,
            ),
        },
        serde_json::Value::Object(props) => match input {
            Value::Record(fields) => {
                let mut result = Vec::new();
                // use the order of the input fields
                for (k, _v) in fields {
                    let v = props
                        .get(k)
                        .ok_or(Serialization(format!("missing field {k}")))?;
                    result.push((k.to_string(), to_avro_value(input, v)?));
                }
                Value::Record(result)
            }
            _ => {
                let mut result = HashMap::new();
                for (k, v) in props {
                    result.insert(k.to_string(), to_avro_value(input, v)?);
                }
                Value::Map(result)
            }
        },
    };
    Ok(result)
}

impl From<uuid::Error> for SerdeError {
    fn from(value: uuid::Error) -> Self {
        Serialization(format!("UUID error: {value}"))
    }
}

#[cfg(test)]
#[cfg(feature = "rules")]
mod tests {
    use super::*;
    use crate::rest::client_config::ClientConfig;
    use crate::rest::dek_registry_client::Client as DekClient;
    use crate::rest::mock_dek_registry_client::MockDekRegistryClient;
    use crate::rest::mock_schema_registry_client::MockSchemaRegistryClient;
    use crate::rest::models::dek::Algorithm;
    use crate::rest::models::{
        CreateDekRequest, CreateKekRequest, Metadata, Rule, RuleSet, SchemaReference, ServerConfig,
    };
    use crate::rest::schema_registry_client::Client;
    use crate::rules::cel::cel_executor::CelExecutor;
    use crate::rules::cel::cel_field_executor::CelFieldExecutor;
    use crate::rules::cel::cel_validator::CelValidator;
    use crate::rules::encryption::encrypt_executor::{
        EncryptionExecutor, FakeClock, FieldEncryptionExecutor,
    };
    use crate::rules::encryption::localkms::local_driver::LocalKmsDriver;
    use crate::rules::jsonata::jsonata_executor::JsonataExecutor;
    use crate::serdes::config::SchemaSelector;
    use crate::serdes::serde::{SerdeFormat, SerdeHeaders, header_schema_id_serializer};
    use apache_avro::types::Value::{Record, Union};
    use std::collections::BTreeMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_basic_serialization() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::default();
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string"},
                {"name": "booleanField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes"}
            ]
        }
        "#;
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        let ser = AvroSerializer::new(
            &client,
            Some(&schema),
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_bytes_serialization() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::default();
        let schema_str = "\"bytes\"";
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        let obj = Value::Bytes(vec![2, 3, 4]);
        let rule_registry = RuleRegistry::new();
        let ser = AvroSerializer::new(
            &client,
            Some(&schema),
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();
        assert_eq!(bytes, vec![0, 0, 0, 0, 1, 2, 3, 4]);

        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Value::Bytes(v) = obj2.value {
            assert_eq!(v, vec![2, 3, 4]);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_guid_in_header() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let mut ser_conf = SerializerConfig::default();
        ser_conf.schema_id_serializer = header_schema_id_serializer;
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string"},
                {"name": "booleanField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes"}
            ]
        }
        "#;
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        let ser = AvroSerializer::new(
            &client,
            Some(&schema),
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: Some(SerdeHeaders::default()),
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_union_with_references() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let ref_schema_str = r#"
        {
            "type": "record",
            "name": "ref",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string", "confluent:tags": ["PII"]},
                {"name": "booleanField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes", "confluent:tags": ["PII"]}
            ]
        }
        "#;
        let ref_schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: ref_schema_str.to_string(),
        };
        client
            .register_schema("ref", &ref_schema, false)
            .await
            .unwrap();
        let ref2_schema_str = r#"
        {
            "type": "record",
            "name": "ref2",
            "fields": [
                {"name": "otherField", "type": "string"}
            ]
        }
        "#;
        let ref2_schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: ref2_schema_str.to_string(),
        };
        client
            .register_schema("ref2", &ref2_schema, false)
            .await
            .unwrap();
        let schema_str = r#"["ref", "ref2"]"#;
        let refs = vec![
            SchemaReference {
                name: Some("ref".to_string()),
                subject: Some("ref".to_string()),
                version: Some(1),
            },
            SchemaReference {
                name: Some("ref2".to_string()),
                subject: Some("ref2".to_string()),
                version: Some(1),
            },
        ];
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: Some(refs),
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();

        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();
        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let fields2 = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Union(_, v) = obj2.value {
            assert_eq!(*v, Record(fields2));
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_cel_condition() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string"},
                {"name": "booleanField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes"}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Condition),
            mode: Some(Mode::Write),
            r#type: "CEL".to_string(),
            tags: None,
            params: None,
            expr: Some("message.stringField == 'hi'".to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelExecutor::new());
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let fields2 = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields2);
        } else {
            unreachable!();
        }
    }

    /// Registers `schema_str` with a single message-level CEL condition and serializes `fields`,
    /// returning the serialize result (a failed condition surfaces as `SerdeError::RuleCondition`).
    async fn serialize_with_cel_condition(
        schema_str: &str,
        expr: &str,
        fields: Vec<(String, Value)>,
    ) -> Result<Vec<u8>, SerdeError> {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Condition),
            mode: Some(Mode::Write),
            r#type: "CEL".to_string(),
            tags: None,
            params: None,
            expr: Some(expr.to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelExecutor::new());
        let ser = AvroSerializer::new(&client, None, Some(rule_registry), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        ser.serialize(&ser_ctx, Record(fields)).await
    }

    const DECIMAL_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "decField", "type": {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}}
        ]
    }
    "#;

    // Unscaled 1234 with scale 2 == 12.34.
    fn decimal_field_12_34() -> Vec<(String, Value)> {
        vec![(
            "decField".to_string(),
            Value::Decimal(apache_avro::Decimal::from(vec![0x04u8, 0xd2])),
        )]
    }

    /// As [`serialize_with_cel_condition`], but a `CEL_FIELD` rule (its `value` binding is the
    /// field itself). `expr` is a `guard ; body` selecting the field by name.
    async fn serialize_with_cel_field_condition(
        schema_str: &str,
        expr: &str,
        fields: Vec<(String, Value)>,
    ) -> Result<Vec<u8>, SerdeError> {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let rule = Rule {
            name: "test-cel-field".to_string(),
            doc: None,
            kind: Some(Kind::Condition),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: None,
            params: None,
            expr: Some(expr.to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser = AvroSerializer::new(&client, None, Some(rule_registry), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        ser.serialize(&ser_ctx, Record(fields)).await
    }

    #[tokio::test]
    async fn test_cel_field_decimal_transform_requantizes() {
        // 12.34 * 2.0 = 24.680 (scale 3); the schema is scale 2, so the field rule's result must
        // be re-quantized and written back as 24.68 (unscaled 2468 = 0x09A4), not 246.80.
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let rule = Rule {
            name: "test-cel-field".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: None,
            params: None,
            expr: Some(
                "name == 'decField' ; decimals.mul(decimal(value), decimal(\"2.0\"))".to_string(),
            ),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: DECIMAL_SCHEMA.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser
            .serialize(&ser_ctx, Record(decimal_field_12_34()))
            .await
            .unwrap();
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry), DeserializerConfig::default())
                .unwrap();
        let out = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(fields) = out.value {
            let (_, v) = fields.iter().find(|(n, _)| n == "decField").unwrap();
            match v {
                Value::Decimal(d) => {
                    let unscaled = Vec::<u8>::try_from(d.clone()).unwrap();
                    assert_eq!(unscaled, vec![0x09u8, 0xa4]);
                }
                other => panic!("expected a decimal, got {other:?}"),
            }
        } else {
            unreachable!();
        }
    }

    const TS_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "tsField", "type": {"type": "long", "logicalType": "timestamp-millis"}}
        ]
    }
    "#;

    #[tokio::test]
    async fn test_cel_field_timestamp_value() {
        // The field rule's `value` binding must be a self-describing timestamp, so the 1-arg
        // `timestamp.of(value)` works (no unit literal).
        let r = serialize_with_cel_field_condition(
            TS_SCHEMA,
            "name == 'tsField' ; timestamp.of(value) < now",
            vec![("tsField".to_string(), Value::TimestampMillis(1000))],
        )
        .await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn test_cel_field_timestamp_transform() {
        // A field rule returning a Timestamp must re-encode to the field's epoch unit (millis).
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let rule = Rule {
            name: "test-cel-field".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: None,
            params: None,
            expr: Some("name == 'tsField' ; timestamp.of(value)".to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: TS_SCHEMA.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser
            .serialize(
                &ser_ctx,
                Record(vec![(
                    "tsField".to_string(),
                    Value::TimestampMillis(1_577_836_800_000),
                )]),
            )
            .await
            .unwrap();
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry), DeserializerConfig::default())
                .unwrap();
        let out = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(fields) = out.value {
            let (_, v) = fields.iter().find(|(n, _)| n == "tsField").unwrap();
            assert_eq!(*v, Value::TimestampMillis(1_577_836_800_000));
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_cel_field_decimal_value_is_scaled() {
        // The field rule's `value` binding must be the decimal at its schema scale (12.34), not
        // the unscaled integer (1234); `string(decimal(value))` distinguishes them.
        let r = serialize_with_cel_field_condition(
            DECIMAL_SCHEMA,
            "name == 'decField' ; string(decimal(value)) == \"12.34\"",
            decimal_field_12_34(),
        )
        .await;
        assert!(r.is_ok());
    }

    /// A message-level CEL transform that changes a decimal's scale must be re-quantized to the
    /// schema scale on write-back: `12.34 * 2.0 = 24.680` (scale 3) has to round-trip as `24.68`
    /// under a scale-2 schema, not as `246.80` (what encoding the scale-3 unscaled integer under
    /// the scale-2 schema would produce).
    #[tokio::test]
    async fn test_cel_decimal_transform_requantizes_to_schema_scale() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL".to_string(),
            tags: None,
            params: None,
            expr: Some(
                "{'decField': decimals.mul(message.decField, decimal(\"2.0\"))}".to_string(),
            ),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: DECIMAL_SCHEMA.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelExecutor::new());
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser
            .serialize(&ser_ctx, Record(decimal_field_12_34()))
            .await
            .unwrap();

        let deser =
            AvroDeserializer::new(&client, Some(rule_registry), DeserializerConfig::default())
                .unwrap();
        let out = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        // The unscaled integer under the scale-2 schema must be 2468 (24.68), not 24680.
        if let Record(fields) = out.value {
            let (_, v) = fields.iter().find(|(n, _)| n == "decField").unwrap();
            match v {
                Value::Decimal(d) => {
                    // Unscaled 2468 (24.68) is 0x09A4 big-endian, not 24680 (0x6068).
                    let unscaled = Vec::<u8>::try_from(d.clone()).unwrap();
                    assert_eq!(unscaled, vec![0x09u8, 0xa4]);
                }
                other => panic!("expected a decimal, got {other:?}"),
            }
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_cel_decimal_condition_passes() {
        let r = serialize_with_cel_condition(
            DECIMAL_SCHEMA,
            "decimals.gt(message.decField, decimal(\"10.00\"))",
            decimal_field_12_34(),
        )
        .await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn test_cel_decimal_condition_fails() {
        let r = serialize_with_cel_condition(
            DECIMAL_SCHEMA,
            "decimals.lt(message.decField, decimal(\"10.00\"))",
            decimal_field_12_34(),
        )
        .await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_cel_decimal_arithmetic() {
        // Only holds if the schema scale is applied (12.34 + 1.66 == 14.00).
        let r = serialize_with_cel_condition(
            DECIMAL_SCHEMA,
            "decimals.eq(decimals.add(message.decField, decimal(\"1.66\")), decimal(\"14.00\"))",
            decimal_field_12_34(),
        )
        .await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn test_cel_decimal_nullable() {
        // A nullable decimal ([null, decimal]) resolves through the union branch and still gets
        // its scale.
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "decField", "type": ["null", {"type": "bytes", "logicalType": "decimal", "precision": 4, "scale": 2}]}
            ]
        }
        "#;
        let r = serialize_with_cel_condition(
            schema_str,
            "decimals.eq(message.decField, decimal(\"12.34\"))",
            vec![(
                "decField".to_string(),
                Value::Union(
                    1,
                    Box::new(Value::Decimal(apache_avro::Decimal::from(vec![
                        0x04u8, 0xd2,
                    ]))),
                ),
            )],
        )
        .await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn test_cel_decimal_string() {
        // "12.34" (scaled), not "1234" (unscaled).
        let r = serialize_with_cel_condition(
            DECIMAL_SCHEMA,
            "string(message.decField) == \"12.34\"",
            decimal_field_12_34(),
        )
        .await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn test_cel_timestamp_millis_passes() {
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "tsField", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        "#;
        // A 1970 timestamp is before now.
        let r = serialize_with_cel_condition(
            schema_str,
            "timestamp.of(message.tsField) < now",
            vec![("tsField".to_string(), Value::TimestampMillis(1000))],
        )
        .await;
        assert!(r.is_ok());
    }

    #[tokio::test]
    async fn test_cel_timestamp_millis_fails() {
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "tsField", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        "#;
        let r = serialize_with_cel_condition(
            schema_str,
            "timestamp.of(message.tsField) > now",
            vec![("tsField".to_string(), Value::TimestampMillis(1000))],
        )
        .await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn test_cel_field() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string"},
                {"name": "booleanField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes"}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: None,
            params: None,
            expr: Some("name == 'stringField' ; value + '-suffix'".to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let fields2 = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            (
                "stringField".to_string(),
                Value::String("hi-suffix".to_string()),
            ),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields2);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_jsonata_with_cel_field() {
        let rule1_to_2 =
            "$merge([$sift($, function($v, $k) {$k != 'size'}), {'height': $.'size'}])";

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let server_config = ServerConfig {
            compatibility_group: Some("application.version".to_string()),
            ..Default::default()
        };
        client
            .update_config("test-value", &server_config)
            .await
            .unwrap();

        let schema_str = r#"
        {
            "type": "record",
            "name": "old",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "size", "type": "int"},
                {"name": "version", "type": "int"}
            ]
        }
        "#;
        let metadata = Metadata {
            tags: None,
            properties: Some(BTreeMap::from([(
                "application.version".to_string(),
                "v1".to_string(),
            )])),
            sensitive: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: Some(Box::new(metadata)),
            rule_set: None,
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let schema_str = r#"
        {
            "type": "record",
            "name": "new",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "height", "type": "int"},
                {"name": "version", "type": "int"}
            ]
        }
        "#;
        let rule1 = Rule {
            name: "test-jsonata".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Upgrade),
            r#type: "JSONATA".to_string(),
            tags: None,
            params: None,
            expr: Some(rule1_to_2.to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule2 = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Read),
            r#type: "CEL_FIELD".to_string(),
            tags: None,
            params: None,
            expr: Some("name == 'name' ; value + '-suffix'".to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: Some(vec![rule1]),
            domain_rules: Some(vec![rule2]),
            encoding_rules: None,
            enable_at: None,
        };
        let metadata = Metadata {
            tags: None,
            properties: Some(BTreeMap::from([(
                "application.version".to_string(),
                "v2".to_string(),
            )])),
            sensitive: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: Some(Box::new(metadata)),
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![
            ("name".to_string(), Value::String("alice".to_string())),
            ("size".to_string(), Value::Int(123)),
            ("version".to_string(), Value::Int(1)),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        rule_registry.register_executor(JsonataExecutor::new());
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestWithMetadata(HashMap::from([(
                "application.version".to_string(),
                "v1".to_string(),
            )]))),
            false,
            false,
            HashMap::new(),
        );
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser_conf = DeserializerConfig::new(
            Some(SchemaSelector::LatestWithMetadata(HashMap::from([(
                "application.version".to_string(),
                "v2".to_string(),
            )]))),
            false,
            HashMap::new(),
        );
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        let fields2 = vec![
            (
                "name".to_string(),
                Value::String("alice-suffix".to_string()),
            ),
            ("height".to_string(), Value::Int(123)),
            ("version".to_string(), Value::Int(1)),
        ];
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields2);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_encryption() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            rule_conf,
        );
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string", "confluent:tags": ["PII"]},
                {"name": "booleanField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes", "confluent:tags": ["PII"]}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-encrypt".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::WriteRead),
            r#type: "ENCRYPT".to_string(),
            tags: Some(vec!["PII".to_string()]),
            params: Some(BTreeMap::from([
                ("encrypt.kek.name".to_string(), "kek1".to_string()),
                ("encrypt.kms.type".to_string(), "local-kms".to_string()),
                ("encrypt.kms.key.id".to_string(), "mykey".to_string()),
            ])),
            expr: None,
            on_success: None,
            on_failure: Some("ERROR,NONE".to_string()),
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();
        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let fields2 = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields2);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_payload_encryption() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            rule_conf,
        );
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string", "confluent:tags": ["PII"]},
                {"name": "booleanField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes", "confluent:tags": ["PII"]}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-encrypt".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::WriteRead),
            r#type: "ENCRYPT_PAYLOAD".to_string(),
            tags: None,
            params: Some(BTreeMap::from([
                ("encrypt.kek.name".to_string(), "kek1".to_string()),
                ("encrypt.kms.type".to_string(), "local-kms".to_string()),
                ("encrypt.kms.key.id".to_string(), "mykey".to_string()),
            ])),
            expr: None,
            on_success: None,
            on_failure: Some("ERROR,NONE".to_string()),
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: None,
            encoding_rules: Some(vec![rule]),
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(EncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();
        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let fields2 = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("booleanField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2, 3])),
        ];
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields2);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_encryption_f1_preserialized() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let schema_str = r#"
        {
            "type": "record",
            "name": "f1Schema",
            "fields": [
                {"name": "f1", "type": "string", "confluent:tags": ["PII"]}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-encrypt".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::WriteRead),
            r#type: "ENCRYPT".to_string(),
            tags: Some(vec!["PII".to_string()]),
            params: Some(BTreeMap::from([
                ("encrypt.kek.name".to_string(), "kek1-f1".to_string()),
                ("encrypt.kms.type".to_string(), "local-kms".to_string()),
                ("encrypt.kms.key.id".to_string(), "mykey".to_string()),
            ])),
            expr: None,
            on_success: None,
            on_failure: Some("ERROR,ERROR".to_string()),
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![("f1".to_string(), Value::String("hello world".to_string()))];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));

        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let deser_conf = DeserializerConfig::new(None, false, rule_conf);
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        let executor = rule_registry.get_executor("ENCRYPT").unwrap();
        let field_executor = executor
            .as_any()
            .downcast_ref::<FieldEncryptionExecutor<MockDekRegistryClient>>()
            .unwrap();
        let dek_client = field_executor.executor.client().unwrap();
        let kek_req = CreateKekRequest {
            name: "kek1-f1".to_string(),
            kms_type: "local-kms".to_string(),
            kms_key_id: "mykey".to_string(),
            kms_props: None,
            doc: None,
            shared: false,
        };
        dek_client.register_kek(kek_req, None).await.unwrap();

        let encrypted_dek =
            "07V2ndh02DA73p+dTybwZFm7DKQSZN1tEwQh+FoX1DZLk4Yj2LLu4omYjp/84tAg3BYlkfGSz+zZacJHIE4=";
        let dek_req = CreateDekRequest {
            subject: "test-value".to_string(),
            version: None,
            algorithm: None,
            encrypted_key_material: Some(encrypted_dek.to_string()),
        };
        dek_client.register_dek("kek1-f1", dek_req).await.unwrap();

        let bytes = [
            0, 0, 0, 0, 1, 104, 122, 103, 121, 47, 106, 70, 78, 77, 86, 47, 101, 70, 105, 108, 97,
            72, 114, 77, 121, 101, 66, 103, 100, 97, 86, 122, 114, 82, 48, 117, 100, 71, 101, 111,
            116, 87, 56, 99, 65, 47, 74, 97, 108, 55, 117, 107, 114, 43, 77, 47, 121, 122,
        ];

        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_encryption_deterministic_f1_preserialized() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let schema_str = r#"
        {
            "type": "record",
            "name": "f1Schema",
            "fields": [
                {"name": "f1", "type": "string", "confluent:tags": ["PII"]}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-encrypt".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::WriteRead),
            r#type: "ENCRYPT".to_string(),
            tags: Some(vec!["PII".to_string()]),
            params: Some(BTreeMap::from([
                ("encrypt.kek.name".to_string(), "kek1-det-f1".to_string()),
                ("encrypt.kms.type".to_string(), "local-kms".to_string()),
                ("encrypt.kms.key.id".to_string(), "mykey".to_string()),
                (
                    "encrypt.dek.algorithm".to_string(),
                    "AES256_SIV".to_string(),
                ),
            ])),
            expr: None,
            on_success: None,
            on_failure: Some("ERROR,ERROR".to_string()),
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![("f1".to_string(), Value::String("hello world".to_string()))];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));

        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let deser_conf = DeserializerConfig::new(None, false, rule_conf);
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        let executor = rule_registry.get_executor("ENCRYPT").unwrap();
        let field_executor = executor
            .as_any()
            .downcast_ref::<FieldEncryptionExecutor<MockDekRegistryClient>>()
            .unwrap();
        let dek_client = field_executor.executor.client().unwrap();
        let kek_req = CreateKekRequest {
            name: "kek1-det-f1".to_string(),
            kms_type: "local-kms".to_string(),
            kms_key_id: "mykey".to_string(),
            kms_props: None,
            doc: None,
            shared: false,
        };
        dek_client.register_kek(kek_req, None).await.unwrap();

        let encrypted_dek = "YSx3DTlAHrmpoDChquJMifmPntBzxgRVdMzgYL82rgWBKn7aUSnG+WIu9ozBNS3y2vXd++mBtK07w4/W/G6w0da39X9hfOVZsGnkSvry/QRht84V8yz3dqKxGMOK5A==";
        let dek_req = CreateDekRequest {
            subject: "test-value".to_string(),
            version: None,
            algorithm: Some(Algorithm::Aes256Siv),
            encrypted_key_material: Some(encrypted_dek.to_string()),
        };
        dek_client
            .register_dek("kek1-det-f1", dek_req)
            .await
            .unwrap();

        let bytes = [
            0, 0, 0, 0, 1, 72, 68, 54, 89, 116, 120, 114, 108, 66, 110, 107, 84, 87, 87, 57, 78,
            54, 86, 98, 107, 51, 73, 73, 110, 106, 87, 72, 56, 49, 120, 109, 89, 104, 51, 107, 52,
            100,
        ];

        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_encryption_dek_rotation_f1_preserialized() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let schema_str = r#"
        {
            "type": "record",
            "name": "f1Schema",
            "fields": [
                {"name": "f1", "type": "string", "confluent:tags": ["PII"]}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-encrypt".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::WriteRead),
            r#type: "ENCRYPT".to_string(),
            tags: Some(vec!["PII".to_string()]),
            params: Some(BTreeMap::from([
                ("encrypt.kek.name".to_string(), "kek1-rot-f1".to_string()),
                ("encrypt.kms.type".to_string(), "local-kms".to_string()),
                ("encrypt.kms.key.id".to_string(), "mykey".to_string()),
                ("encrypt.dek.expiry.days".to_string(), "1".to_string()),
            ])),
            expr: None,
            on_success: None,
            on_failure: Some("ERROR,ERROR".to_string()),
            disabled: None,
        };
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
            enable_at: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let fields = vec![("f1".to_string(), Value::String("hello world".to_string()))];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));

        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let deser_conf = DeserializerConfig::new(None, false, rule_conf);
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        let executor = rule_registry.get_executor("ENCRYPT").unwrap();
        let field_executor = executor
            .as_any()
            .downcast_ref::<FieldEncryptionExecutor<MockDekRegistryClient>>()
            .unwrap();
        let dek_client = field_executor.executor.client().unwrap();
        let kek_req = CreateKekRequest {
            name: "kek1-rot-f1".to_string(),
            kms_type: "local-kms".to_string(),
            kms_key_id: "mykey".to_string(),
            kms_props: None,
            doc: None,
            shared: false,
        };
        dek_client.register_kek(kek_req, None).await.unwrap();

        let encrypted_dek =
            "W/v6hOQYq1idVAcs1pPWz9UUONMVZW4IrglTnG88TsWjeCjxmtRQ4VaNe/I5dCfm2zyY9Cu0nqdvqImtUk4=";
        let dek_req = CreateDekRequest {
            subject: "test-value".to_string(),
            version: None,
            algorithm: Some(Algorithm::Aes256Gcm),
            encrypted_key_material: Some(encrypted_dek.to_string()),
        };
        dek_client
            .register_dek("kek1-rot-f1", dek_req)
            .await
            .unwrap();

        let bytes = [
            0, 0, 0, 0, 1, 120, 65, 65, 65, 65, 65, 65, 71, 52, 72, 73, 54, 98, 49, 110, 88, 80,
            88, 113, 76, 121, 71, 56, 99, 73, 73, 51, 53, 78, 72, 81, 115, 101, 113, 113, 85, 67,
            100, 43, 73, 101, 76, 101, 70, 86, 65, 101, 78, 112, 83, 83, 51, 102, 120, 80, 110, 74,
            51, 50, 65, 61,
        ];

        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_avro_serde_with_record_name_strategy() {
        use crate::serdes::serde::SubjectNameStrategyType;

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema_str = r#"
        {
            "type": "record",
            "name": "DemoSchema",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes"}
            ]
        }
        "#;
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        // RecordNameStrategy uses "{recordName}" as subject
        client
            .register_schema("DemoSchema", &schema, false)
            .await
            .unwrap();

        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::Record;

        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("boolField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        // Pass schema to serializer for record name strategy to extract the record name
        let ser = AvroSerializer::new(
            &client,
            Some(&schema),
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::Record;
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_avro_serde_with_topic_record_name_strategy() {
        use crate::serdes::serde::SubjectNameStrategyType;

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema_str = r#"
        {
            "type": "record",
            "name": "DemoSchema",
            "fields": [
                {"name": "intField", "type": "int"},
                {"name": "doubleField", "type": "double"},
                {"name": "stringField", "type": "string"},
                {"name": "boolField", "type": "boolean"},
                {"name": "bytesField", "type": "bytes"}
            ]
        }
        "#;
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        // TopicRecordNameStrategy uses "{topic}-{recordName}" as subject
        client
            .register_schema("topic1-DemoSchema", &schema, false)
            .await
            .unwrap();

        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::TopicRecord;

        let fields = vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("boolField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2])),
        ];
        let obj = Record(fields.clone());
        let rule_registry = RuleRegistry::new();
        // Pass schema to serializer for topic record name strategy to extract the record name
        let ser = AvroSerializer::new(
            &client,
            Some(&schema),
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::TopicRecord;
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    const DEMO_SCHEMA_STR: &str = r#"
    {
        "type": "record",
        "name": "DemoSchema",
        "fields": [
            {"name": "intField", "type": "int"},
            {"name": "doubleField", "type": "double"},
            {"name": "stringField", "type": "string"},
            {"name": "boolField", "type": "boolean"},
            {"name": "bytesField", "type": "bytes"}
        ]
    }"#;

    fn demo_schema() -> Schema {
        Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: DEMO_SCHEMA_STR.to_string(),
        }
    }

    fn demo_fields() -> Vec<(String, Value)> {
        vec![
            ("intField".to_string(), Value::Int(123)),
            ("doubleField".to_string(), Value::Double(45.67)),
            ("stringField".to_string(), Value::String("hi".to_string())),
            ("boolField".to_string(), Value::Boolean(true)),
            ("bytesField".to_string(), Value::Bytes(vec![1, 2])),
        ]
    }

    fn make_association(
        resource_id: &str,
        subject: &str,
        association_type: &str,
    ) -> crate::rest::models::AssociationCreateOrUpdateRequest {
        use crate::rest::models::{
            AssociationCreateOrUpdateInfo, AssociationCreateOrUpdateRequest,
        };
        AssociationCreateOrUpdateRequest {
            resource_name: Some("topic1".to_string()),
            resource_namespace: Some("-".to_string()),
            resource_id: Some(resource_id.to_string()),
            resource_type: Some("topic".to_string()),
            associations: Some(vec![AssociationCreateOrUpdateInfo {
                subject: Some(subject.to_string()),
                association_type: Some(association_type.to_string()),
                lifecycle: None,
                frozen: None,
                schema: None,
                normalize: None,
            }]),
        }
    }

    #[tokio::test]
    async fn test_avro_serde_with_associated_name_strategy() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = demo_schema();
        client
            .register_schema("my-custom-subject", &schema, false)
            .await
            .unwrap();
        client
            .create_association(&make_association(
                "lkc-123:topic1",
                "my-custom-subject",
                "value",
            ))
            .await
            .unwrap();

        let rule_registry = RuleRegistry::new();
        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let fields = demo_fields();
        let bytes = ser
            .serialize(&ser_ctx, Record(fields.clone()))
            .await
            .unwrap();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_avro_serde_with_associated_name_strategy_fallback_to_topic() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = demo_schema();
        // Register schema under topic name strategy subject (no association created)
        client
            .register_schema("topic1-value", &schema, false)
            .await
            .unwrap();

        let rule_registry = RuleRegistry::new();
        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        // Default fallback is Topic strategy
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let fields = demo_fields();
        let bytes = ser
            .serialize(&ser_ctx, Record(fields.clone()))
            .await
            .unwrap();

        // Deserializer uses default (Topic) strategy
        let deser = AvroDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_avro_serde_with_associated_name_strategy_fallback_none() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        // No association created; fallback is NONE
        let rule_registry = RuleRegistry::new();
        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        ser_conf.strategy_config =
            HashMap::from([(FALLBACK_TYPE_CONFIG.to_string(), "NONE".to_string())]);
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let result = ser.serialize(&ser_ctx, Record(demo_fields())).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Could not determine subject")
        );
    }

    #[tokio::test]
    async fn test_avro_serde_with_associated_name_strategy_multiple_associations() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = demo_schema();
        client
            .register_schema("subject1", &schema, false)
            .await
            .unwrap();
        client
            .register_schema("subject2", &schema, false)
            .await
            .unwrap();
        client
            .create_association(&make_association("lkc-123:topic1", "subject1", "value"))
            .await
            .unwrap();
        client
            .create_association(&make_association("lkc-456:topic1", "subject2", "value"))
            .await
            .unwrap();

        let rule_registry = RuleRegistry::new();
        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let result = ser.serialize(&ser_ctx, Record(demo_fields())).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("multiple associated subjects found")
        );
    }

    #[tokio::test]
    async fn test_avro_serde_with_associated_name_strategy_with_kafka_cluster_id() {
        use crate::rest::models::{
            AssociationCreateOrUpdateInfo, AssociationCreateOrUpdateRequest,
        };

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = demo_schema();
        client
            .register_schema("my-custom-subject", &schema, false)
            .await
            .unwrap();

        // Create association with specific cluster namespace
        let request = AssociationCreateOrUpdateRequest {
            resource_name: Some("topic1".to_string()),
            resource_namespace: Some("lkc-my-cluster".to_string()),
            resource_id: Some("lkc-my-cluster:topic1".to_string()),
            resource_type: Some("topic".to_string()),
            associations: Some(vec![AssociationCreateOrUpdateInfo {
                subject: Some("my-custom-subject".to_string()),
                association_type: Some("value".to_string()),
                lifecycle: None,
                frozen: None,
                schema: None,
                normalize: None,
            }]),
        };
        client.create_association(&request).await.unwrap();

        let rule_registry = RuleRegistry::new();
        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        ser_conf.strategy_config = HashMap::from([(
            KAFKA_CLUSTER_ID_CONFIG.to_string(),
            "lkc-my-cluster".to_string(),
        )]);
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let fields = demo_fields();
        let bytes = ser
            .serialize(&ser_ctx, Record(fields.clone()))
            .await
            .unwrap();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        deser_conf.strategy_config = HashMap::from([(
            KAFKA_CLUSTER_ID_CONFIG.to_string(),
            "lkc-my-cluster".to_string(),
        )]);
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            unreachable!();
        }
    }

    #[tokio::test]
    async fn test_avro_serde_with_associated_name_strategy_caching() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = demo_schema();
        client
            .register_schema("my-cached-subject", &schema, false)
            .await
            .unwrap();
        client
            .create_association(&make_association(
                "lkc-123:topic1",
                "my-cached-subject",
                "value",
            ))
            .await
            .unwrap();

        let rule_registry = RuleRegistry::new();
        let mut ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            false,
            HashMap::new(),
        );
        ser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let fields = demo_fields();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        for _ in 0..5 {
            let bytes = ser
                .serialize(&ser_ctx, Record(fields.clone()))
                .await
                .unwrap();
            let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
            if let Record(v) = obj2.value {
                assert_eq!(v, fields);
            } else {
                unreachable!();
            }
        }
    }

    const VALIDATION_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "Order",
        "namespace": "test",
        "confluent:rules": [
            {"name": "quantity_matches_items",
             "expr": "this.quantity == size(this.items)"}
        ],
        "fields": [
            {"name": "id", "type": "string",
             "confluent:rules": [
                {"name": "id_prefix", "expr": "this.startsWith('ord-')"},
                {"name": "id_length", "expr": "size(this) > 4 ? '' : 'id is too short'"}
             ]},
            {"name": "quantity", "type": "int",
             "confluent:rules": [
                {"name": "positive_quantity", "doc": "quantity must be positive",
                 "expr": "this > 0"}
             ]},
            {"name": "items", "type": {"type": "array", "items": "string"}},
            {"name": "note", "type": ["null", "string"],
             "confluent:rules": [
                {"name": "note_not_empty", "expr": "size(this) > 0"}
             ]},
            {"name": "address", "type": {
                "type": "record",
                "name": "Address",
                "fields": [
                    {"name": "zip", "type": "string",
                     "confluent:rules": [
                        {"name": "zip_digits",
                         "expr": "this.matches('^[0-9]{5}$') ? '' : 'zip must be 5 digits'"}
                     ]}
                ]
            }}
        ]
    }
    "#;

    fn validation_order(
        id: &str,
        quantity: i32,
        items: &[&str],
        zip: &str,
        note: Option<&str>,
    ) -> Value {
        Record(vec![
            ("id".to_string(), Value::String(id.to_string())),
            ("quantity".to_string(), Value::Int(quantity)),
            (
                "items".to_string(),
                Value::Array(items.iter().map(|i| Value::String(i.to_string())).collect()),
            ),
            (
                "note".to_string(),
                match note {
                    None => Union(0, Box::new(Value::Null)),
                    Some(note) => Union(1, Box::new(Value::String(note.to_string()))),
                },
            ),
            (
                "address".to_string(),
                Record(vec![("zip".to_string(), Value::String(zip.to_string()))]),
            ),
        ])
    }

    fn validate_avro(message: &Value, fail_fast: bool) -> Vec<ValidationRuleError> {
        let parsed = apache_avro::Schema::parse_str(VALIDATION_SCHEMA).unwrap();
        let validator = CelValidator::new();
        validate_message(&validator, &parsed, &[], message, fail_fast)
    }

    fn validation_schema() -> Schema {
        Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: VALIDATION_SCHEMA.to_string(),
        }
    }

    fn validating_registry() -> RuleRegistry {
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelExecutor::new());
        rule_registry.register_validation_executor(CelValidator::new());
        rule_registry
    }

    #[test]
    fn test_validation_valid_record_has_no_violations() {
        let message = validation_order("ord-1234", 2, &["a", "b"], "12345", None);
        assert_eq!(validate_avro(&message, false), vec![]);
    }

    #[test]
    fn test_validation_collects_every_violation() {
        // Fails: record-level count, id prefix, id length, quantity, nested zip.
        let message = validation_order("x", 0, &["a"], "abc", None);
        let violations = validate_avro(&message, false);

        assert_eq!(violations.len(), 5, "{violations:?}");
        assert_eq!(violations[0].rule.name, "quantity_matches_items");
        assert_eq!(violations[0].field_path, "");
        assert_eq!(violations[1].rule.name, "id_prefix");
        assert_eq!(violations[1].field_path, "id");
        assert_eq!(violations[2].rule.name, "id_length");
        assert_eq!(violations[2].message, "id is too short");
        assert_eq!(violations[3].rule.name, "positive_quantity");
        assert_eq!(violations[3].field_path, "quantity");
        assert_eq!(violations[4].rule.name, "zip_digits");
        assert_eq!(violations[4].field_path, "address.zip");
        assert_eq!(violations[4].message, "zip must be 5 digits");
    }

    #[test]
    fn test_validation_fail_fast_stops_at_first_violation() {
        let message = validation_order("x", 0, &["a"], "abc", None);
        assert_eq!(validate_avro(&message, true).len(), 1);
    }

    #[test]
    fn test_validation_skips_rules_on_null_fields() {
        // note is null, so note_not_empty must not be invoked.
        let message = validation_order("ord-1234", 2, &["a", "b"], "12345", None);
        assert!(validate_avro(&message, false).is_empty());

        let message = validation_order("ord-1234", 2, &["a", "b"], "12345", Some(""));
        let violations = validate_avro(&message, false);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert_eq!(violations[0].rule.name, "note_not_empty");
        assert_eq!(violations[0].field_path, "note");
    }

    #[tokio::test]
    async fn test_validation_serializer_rejects_invalid_message() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let mut ser_conf = SerializerConfig::default();
        ser_conf.validation_rules_execution = ValidationRulesExecution::AfterDomainRules;
        let schema = validation_schema();
        let ser = AvroSerializer::new(
            &client,
            Some(&schema),
            Some(validating_registry()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };

        let valid = validation_order("ord-1234", 2, &["a", "b"], "12345", None);
        assert!(ser.serialize(&ser_ctx, valid).await.is_ok());

        let invalid = validation_order("bad", 2, &["a", "b"], "12345", None);
        let err = ser.serialize(&ser_ctx, invalid).await.unwrap_err();
        assert!(
            matches!(err, SerdeError::ValidationRules(_)),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_validation_serializer_skips_validation_when_disabled() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let schema = validation_schema();
        let ser = AvroSerializer::new(
            &client,
            Some(&schema),
            Some(validating_registry()),
            SerializerConfig::default(),
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };

        let invalid = validation_order("bad", 2, &["a", "b"], "12345", None);
        assert!(ser.serialize(&ser_ctx, invalid).await.is_ok());
    }

    /// Registers a schema whose domain rule always fails, so that the order of the two
    /// failures tells us which phase ran first.
    async fn register_schema_with_failing_domain_rule(client: &MockSchemaRegistryClient) {
        let rule = Rule {
            name: "always-fails".to_string(),
            doc: None,
            kind: Some(Kind::Condition),
            mode: Some(Mode::Write),
            r#type: "CEL".to_string(),
            tags: None,
            params: None,
            expr: Some("message.quantity > 100".to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(RuleSet {
                migration_rules: None,
                domain_rules: Some(vec![rule]),
                encoding_rules: None,
                enable_at: None,
            })),
            schema: VALIDATION_SCHEMA.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
    }

    fn latest_version_config(execution: ValidationRulesExecution) -> SerializerConfig {
        let mut config = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        config.validation_rules_execution = execution;
        config
    }

    #[tokio::test]
    async fn test_validation_runs_before_domain_rules() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        register_schema_with_failing_domain_rule(&client).await;
        let ser = AvroSerializer::new(
            &client,
            None,
            Some(validating_registry()),
            latest_version_config(ValidationRulesExecution::BeforeDomainRules),
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };

        // Both the inline rules and the domain rule fail; validation first means the
        // validation failure is what surfaces.
        let invalid = validation_order("bad", 2, &["a", "b"], "12345", None);
        let err = ser.serialize(&ser_ctx, invalid).await.unwrap_err();
        assert!(
            matches!(err, SerdeError::ValidationRules(_)),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_validation_runs_after_domain_rules() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        register_schema_with_failing_domain_rule(&client).await;
        let ser = AvroSerializer::new(
            &client,
            None,
            Some(validating_registry()),
            latest_version_config(ValidationRulesExecution::AfterDomainRules),
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };

        // Same message, but the domain rule now runs first, so its failure surfaces
        // instead of the validation failure.
        let invalid = validation_order("bad", 2, &["a", "b"], "12345", None);
        let err = ser.serialize(&ser_ctx, invalid).await.unwrap_err();
        assert!(
            !matches!(err, SerdeError::ValidationRules(_)),
            "expected the domain rule to fail, got: {err}"
        );
    }

    const UNION_RECORD_SCHEMA: &str = r#"
    {
        "type": "record", "name": "Outer", "namespace": "test",
        "fields": [
            {"name": "inner", "type": ["null", {
                "type": "record", "name": "Inner",
                "fields": [
                    {"name": "zip", "type": "string",
                     "confluent:rules": [{"name": "zip_rule", "expr": "size(this) == 5"}]}
                ]
            }]}
        ]
    }
    "#;

    const NAMED_REF_SCHEMA: &str = r#"
    {
        "type": "record", "name": "Outer", "namespace": "test",
        "fields": [
            {"name": "first", "type": {
                "type": "record", "name": "Inner",
                "fields": [
                    {"name": "zip", "type": "string",
                     "confluent:rules": [{"name": "zip_rule", "expr": "size(this) == 5"}]}
                ]
            }},
            {"name": "second", "type": "Inner"}
        ]
    }
    "#;

    fn inner_record() -> Value {
        Record(vec![("zip".to_string(), Value::String("abc".to_string()))])
    }

    #[test]
    fn test_validation_descends_into_union_branches() {
        let parsed = apache_avro::Schema::parse_str(UNION_RECORD_SCHEMA).unwrap();
        let validator = CelValidator::new();
        let message = Record(vec![(
            "inner".to_string(),
            Union(1, Box::new(inner_record())),
        )]);

        // The record is reached through a union branch, so the branch schema must be walked
        // against the unwrapped value rather than the union wrapper.
        let violations = validate_message(&validator, &parsed, &[], &message, false);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert_eq!(violations[0].field_path, "inner.zip");
    }

    #[test]
    fn test_validation_skips_null_union_branches() {
        let parsed = apache_avro::Schema::parse_str(UNION_RECORD_SCHEMA).unwrap();
        let validator = CelValidator::new();
        let message = Record(vec![("inner".to_string(), Union(0, Box::new(Value::Null)))]);
        assert_eq!(
            validate_message(&validator, &parsed, &[], &message, false),
            vec![]
        );
    }

    #[test]
    fn test_validation_resolves_inline_named_references() {
        let parsed = apache_avro::Schema::parse_str(NAMED_REF_SCHEMA).unwrap();
        let validator = CelValidator::new();
        let message = Record(vec![
            ("first".to_string(), inner_record()),
            ("second".to_string(), inner_record()),
        ]);

        // `second` reaches the same definition through a Schema::Ref, whose target is
        // declared inline in this schema rather than in the referenced schemas.
        let violations = validate_message(&validator, &parsed, &[], &message, false);
        assert_eq!(violations.len(), 2, "{violations:?}");
        assert_eq!(violations[0].field_path, "first.zip");
        assert_eq!(violations[1].field_path, "second.zip");
    }

    const UNION_OF_RECORDS_SCHEMA: &str = r#"
    {
        "type": "record", "name": "Outer", "namespace": "test",
        "fields": [
            {"name": "choice", "type": [
                {"type": "record", "name": "A", "fields": [
                    {"name": "v", "type": "string",
                     "confluent:rules": [{"name": "ruleA", "expr": "false"}]}
                ]},
                {"type": "record", "name": "B", "fields": [
                    {"name": "v", "type": "string",
                     "confluent:rules": [{"name": "ruleB", "expr": "false"}]}
                ]}
            ]}
        ]
    }
    "#;

    #[test]
    fn test_validation_uses_the_union_branch_the_value_names() {
        let parsed = apache_avro::Schema::parse_str(UNION_OF_RECORDS_SCHEMA).unwrap();
        let validator = CelValidator::new();
        let branch = Record(vec![("v".to_string(), Value::String("x".to_string()))]);

        // Both branches are records of the same shape, so structural matching cannot tell
        // them apart; only the union index says which branch the value actually holds.
        for (index, expected) in [(0u32, "ruleA"), (1u32, "ruleB")] {
            let message = Record(vec![(
                "choice".to_string(),
                Union(index, Box::new(branch.clone())),
            )]);
            let violations = validate_message(&validator, &parsed, &[], &message, false);
            assert_eq!(violations.len(), 1, "index {index}: {violations:?}");
            assert_eq!(violations[0].rule.name, expected, "index {index}");
            assert_eq!(violations[0].field_path, "choice.v");
        }
    }

    #[tokio::test]
    async fn test_cel_field_transforms_nullable_fields() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "plain", "type": "string"},
                {"name": "nullable", "type": ["null", "string"]}
            ]
        }
        "#;
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: None,
            params: None,
            expr: Some("typeName == 'STRING' ; value + '-suffix'".to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let schema = Schema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(RuleSet {
                migration_rules: None,
                domain_rules: Some(vec![rule]),
                encoding_rules: None,
                enable_at: None,
            })),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser = AvroSerializer::new(&client, None, Some(rule_registry), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let message = Record(vec![
            ("plain".to_string(), Value::String("a".to_string())),
            (
                "nullable".to_string(),
                Union(1, Box::new(Value::String("b".to_string()))),
            ),
        ]);
        // The rule must see the value inside the union, not the wrapper, and the
        // transformed value must go back into the union it came from.
        let bytes = ser.serialize(&ser_ctx, message).await.unwrap();

        let deser = AvroDeserializer::new(&client, None, DeserializerConfig::default()).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        let expected = vec![
            ("plain".to_string(), Value::String("a-suffix".to_string())),
            (
                "nullable".to_string(),
                Union(1, Box::new(Value::String("b-suffix".to_string()))),
            ),
        ];
        if let Record(v) = obj2.value {
            assert_eq!(v, expected);
        } else {
            unreachable!();
        }
    }
}
