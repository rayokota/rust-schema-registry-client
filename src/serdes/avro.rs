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
    ValidationRulesExecution, ValidationSchema, append_validation_path, evaluate_validation_rule,
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
            apache_avro::writer::datum::GenericDatumWriter::builder(&schema_tuple.0)
                .schemata(schemata_with_root(&schema_tuple.0, &schema_tuple.1))?
                .build()?
                .write_value_to_vec(value)?
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
            apache_avro::Schema::Record(r) => Ok(match r.name.namespace() {
                Some(ns) => format!("{ns}.{}", r.name.name()),
                None => r.name.name().to_string(),
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
                apache_avro::reader::datum::GenericDatumReader::builder(&writer_schema)
                    .writer_schemata(schemata_with_root(&writer_schema, &writer_named))?
                    .build()?
                    .read_value(&mut reader)?
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
                // No reader schema, deliberately: with no migration the reader schema *is* the
                // writer schema, cloned from it a few lines above, so resolution has nothing to
                // resolve. Providing one anyway is what performs it.
                //
                // And resolution cannot be asked for here. apache-avro rejects a decimal whose
                // encoded length is shorter than its declared precision would need:
                // `max_prec_for_len(2)` is 4, so two bytes in a `precision: 8` field fails with
                // "Precision 8 too small to hold decimal values with 2 bytes". That is every
                // decimal small enough to fit in fewer bytes than its precision allows - 12.34 in
                // a precision-8 field - whichever client wrote the bytes.
                apache_avro::reader::datum::GenericDatumReader::builder(&writer_schema)
                    .writer_schemata(schemata_with_root(&writer_schema, &writer_named))?
                    .build()?
                    .read_value(&mut reader)?
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
            apache_avro::Schema::Record(r) => Ok(match r.name.namespace() {
                Some(ns) => format!("{ns}.{}", r.name.name()),
                None => r.name.name().to_string(),
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

/// The schema list handed to apache-avro's reader/writer. `schemata` *replaces* the default
/// root-derived resolution rather than adding to it, so the root has to be in the list or a
/// `Schema::Ref` to a type the root itself declares - a named type used twice - stays unresolved.
/// Entries resolve in order against the ones before them, so the root goes last, after the
/// referenced schemas `resolve_named_schema` already emitted in dependency order.
fn schemata_with_root<'a>(
    root: &'a apache_avro::Schema,
    named: &'a [apache_avro::Schema],
) -> Vec<&'a apache_avro::Schema> {
    named.iter().chain(std::iter::once(root)).collect()
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
                // A condition's result is a verdict on the field, not a replacement for it, so
                // it does not belong inside the union: re-wrapping hid the bare
                // `Value::Boolean(false)` that the record-field check tests for, and every
                // false `CEL_FIELD` condition over a nullable field passed silently. Plain
                // fields never went through here, which is why the existing tests missed it.
                if ctx.rule.kind == Some(Kind::Condition) {
                    return Ok(result);
                }
                // Which branch the result belongs to follows from the value, not from the
                // branch it arrived on - the reference keeps no branch at all and resolves it
                // from the datum. The writer validates against `variants()[index]`, so a rule
                // that changes the value's type is rejected under the old index. Keep the
                // arriving branch while it still accepts the result, so two structurally
                // identical variants are never swapped.
                let index = match union.variants().get(*index as usize) {
                    Some(variant) if result.validate(variant) => *index,
                    _ => resolve_union(union, &result)
                        .map(|(i, _)| i as u32)
                        .unwrap_or(*index),
                };
                return Ok(Value::Union(index, Box::new(result)));
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
        apache_avro::Schema::Uuid(_) => FieldType::String,
        apache_avro::Schema::Date => FieldType::Int,
        apache_avro::Schema::TimeMillis => FieldType::Int,
        apache_avro::Schema::TimeMicros => FieldType::Long,
        apache_avro::Schema::TimestampMillis => FieldType::Long,
        apache_avro::Schema::TimestampMicros => FieldType::Long,
        apache_avro::Schema::TimestampNanos => FieldType::Long,
        apache_avro::Schema::LocalTimestampMillis => FieldType::Long,
        apache_avro::Schema::LocalTimestampMicros => FieldType::Long,
        apache_avro::Schema::LocalTimestampNanos => FieldType::Long,
        apache_avro::Schema::Duration(_) => FieldType::Fixed,
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
                schema,
                named_schemas,
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
                    &field.schema,
                    named_schemas,
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

/// Unwraps a union value and narrows the schema to the branch it took, keeping the pair
/// consistent. A named `Schema::Ref` never denotes a union (Avro names only records, enums and
/// fixed), so the direct case is the whole story.
fn unwrap_union_with_schema<'a>(
    schema: &'a apache_avro::Schema,
    value: &'a Value,
) -> (&'a apache_avro::Schema, &'a Value) {
    match (schema, value) {
        (apache_avro::Schema::Union(u), Value::Union(idx, inner)) => {
            match u.variants().get(*idx as usize) {
                Some(branch) => unwrap_union_with_schema(branch, inner),
                None => (schema, unwrap_union(inner)),
            }
        }
        (_, Value::Union(_, inner)) => unwrap_union_with_schema(schema, inner),
        _ => (schema, value),
    }
}

/// Evaluates the given rules against `value`, skipping null values to honor the
/// skip-on-null contract. Returns whether the walk should stop.
fn evaluate_rules(
    executor: &dyn ValidationRuleExecutor,
    rules: Vec<ValidationRule>,
    schema: &apache_avro::Schema,
    named_schemas: &HashMap<Name, &apache_avro::Schema>,
    value: &Value,
    path: &str,
    fail_fast: bool,
    violations: &mut Vec<ValidationRuleError>,
) -> bool {
    // A nullable field arrives as Union(idx, inner) against a union schema. Unwrapping the value
    // drops the branch index, and the schema-aware conversion can only follow a union while the
    // value still carries it - so narrow the schema to the branch here as well. Without this a
    // nullable decimal lost its scale and read 12.34 as 1234.
    let (schema, value) = unwrap_union_with_schema(schema, value);
    if rules.is_empty() || matches!(value, Value::Null) {
        return false;
    }
    let serde_value = SerdeValue::Avro(value.clone());
    for rule in &rules {
        // The schema travels with the value: an Avro decimal is unscaled bytes and its
        // scale lives only here, so a rule bound without it reads 12.34 as 1234.
        evaluate_validation_rule(
            executor,
            rule,
            Some(ValidationSchema::Avro(schema, named_schemas)),
            &serde_value,
            path,
            violations,
        );
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
    union.find_schema_with_known_schemata::<apache_avro::Schema>(message, None, None)
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
    async fn test_reused_named_type() {
        // A named type used twice parses as a `Schema::Ref` for the second use, which only
        // resolves if the root is in the schema list handed to the writer and reader.
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "h1", "type": {"type": "record", "name": "Helper",
                    "fields": [{"name": "x", "type": "int"}]}},
                {"name": "h2", "type": "Helper"}
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
        let helper = |n| Record(vec![("x".to_string(), Value::Int(n))]);
        let fields = vec![("h1".to_string(), helper(1)), ("h2".to_string(), helper(2))];
        let obj = Record(fields.clone());
        let ser =
            AvroSerializer::new(&client, Some(&schema), None, SerializerConfig::default()).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = AvroDeserializer::new(&client, None, DeserializerConfig::default()).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        if let Record(v) = obj2.value {
            assert_eq!(v, fields);
        } else {
            panic!("expected record")
        }
    }

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

    const MULTI_BRANCH_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "U",
        "fields": [{"name": "u", "type": ["string", "int"]}]
    }
    "#;

    /// A message-level result lands on the union branch that *accepts* it, not on the first
    /// non-null one.
    ///
    /// `union_variant_index` took the first non-null branch unconditionally - right for the
    /// `[null, T]` nullable shape, wrong wherever the union offers a choice. An int result for
    /// `["string", "int"]` went to the string branch and the writer refused the record with
    /// "Value does not match schema", where the reference writes it to the int branch.
    #[tokio::test]
    async fn test_cel_message_result_picks_the_accepting_union_branch() {
        for (name, expr, want) in [
            (
                "int takes the int branch",
                r#"{"u": 7}"#,
                Value::Union(1, Box::new(Value::Int(7))),
            ),
            (
                "string still takes the string branch",
                r#"{"u": "kept"}"#,
                Value::Union(0, Box::new(Value::String("kept".to_string()))),
            ),
        ] {
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
                name: "r".to_string(),
                doc: None,
                kind: Some(Kind::Transform),
                mode: Some(Mode::Write),
                r#type: "CEL".to_string(),
                tags: None,
                params: None,
                expr: Some(expr.to_string()),
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
                schema: MULTI_BRANCH_SCHEMA.to_string(),
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
            let obj = Record(vec![(
                "u".to_string(),
                Value::Union(0, Box::new(Value::String("a".to_string()))),
            )]);
            let bytes = ser
                .serialize(&ser_ctx, obj)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e:?}"));
            let deser =
                AvroDeserializer::new(&client, Some(rule_registry), DeserializerConfig::default())
                    .unwrap();
            let Record(fields) = deser.deserialize(&ser_ctx, &bytes).await.unwrap().value else {
                panic!("{name}: expected a record");
            };
            let (_, v) = fields.iter().find(|(n, _)| n == "u").unwrap();
            assert_eq!(*v, want, "{name}");
        }
    }

    /// Runs a message-level CEL transform and reports whether the record serialized.
    async fn message_transform_ok(schema_str: &str, expr: &str, seed: Value) -> bool {
        message_transform_err(schema_str, expr, seed)
            .await
            .is_none()
    }

    /// The same, returning the failure text so a test can pin which layer refused the record.
    async fn message_transform_err(schema_str: &str, expr: &str, seed: Value) -> Option<String> {
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
            name: "r".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL".to_string(),
            tags: None,
            params: None,
            expr: Some(expr.to_string()),
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
        let reg = RuleRegistry::new();
        reg.register_executor(CelExecutor::new());
        let ser = AvroSerializer::new(&client, None, Some(reg), ser_conf).unwrap();
        let ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        ser.serialize(&ctx, Record(vec![("u".to_string(), seed)]))
            .await
            .err()
            .map(|e| format!("{e:?}"))
    }

    /// An integer too wide for the field it is written to is refused, not truncated.
    ///
    /// The cast was `*v as i32`, and a union made it reachable without any branch accepting the
    /// value: `branch_accepts` range-checks an int branch, but a value nothing accepted still
    /// fell back to the first non-null branch, so 2147483648 for `["int", "string"]` was written
    /// as -2147483648. The reference range-checks in `narrowToInt` and throws
    /// `UnresolvedUnionException` when no branch accepts.
    #[tokio::test]
    async fn test_an_out_of_range_int_result_is_refused() {
        const UNION: &str =
            r#"{"type":"record","name":"U","fields":[{"name":"u","type":["int","string"]}]}"#;
        const PLAIN: &str = r#"{"type":"record","name":"U","fields":[{"name":"u","type":"int"}]}"#;
        let union_seed = || Value::Union(0, Box::new(Value::Int(1)));

        assert!(
            !message_transform_ok(UNION, r#"{"u": 2147483648}"#, union_seed()).await,
            "an out-of-range int was written to the int branch anyway"
        );
        assert!(
            !message_transform_ok(PLAIN, r#"{"u": 2147483648}"#, Value::Int(1)).await,
            "an out-of-range int was truncated into the field"
        );
        // The twins: the same rule one below the boundary still serializes, so the two above
        // cannot be passing because the transform stopped working.
        assert!(message_transform_ok(UNION, r#"{"u": 2147483647}"#, union_seed()).await);
        assert!(message_transform_ok(PLAIN, r#"{"u": 2147483647}"#, Value::Int(1)).await);
    }

    /// A result no branch of a multi-branch union accepts is refused rather than forced onto one.
    ///
    /// The reference throws `UnresolvedUnionException`; a union with a single non-null branch
    /// keeps the old fallback, as JS does, since that branch cannot be mis-selected.
    #[tokio::test]
    async fn test_a_result_no_union_branch_accepts_is_refused() {
        // Pinned on the message, not just on failure: forcing the value onto the first branch
        // also failed, but from inside apache-avro and without naming the union.
        let err = message_transform_err(
            r#"{"type":"record","name":"U","fields":[{"name":"u","type":["int","boolean"]}]}"#,
            r#"{"u": "text"}"#,
            Value::Union(0, Box::new(Value::Int(1))),
        )
        .await
        .expect("a string was forced onto a numeric branch");
        assert!(
            err.contains("does not match any branch of union"),
            "refused by the writer rather than by branch resolution: {err}"
        );
        assert!(
            message_transform_ok(
                r#"{"type":"record","name":"U","fields":[{"name":"u","type":["int","string"]}]}"#,
                r#"{"u": "text"}"#,
                Value::Union(0, Box::new(Value::Int(1)))
            )
            .await
        );
    }

    /// An integer result reaches Avro's integer-backed logical types.
    ///
    /// The reference accepts a plain integer at a `date` or `timestamp-millis` branch before it
    /// looks at the logical type at all - `branchAccepts` switches on the *base* type, INT or
    /// LONG. apache-avro hoists each logical type into its own `Schema` variant, so the
    /// schema-driven arms matched none of them: a rule returning an integer for a `date`,
    /// `time-millis`, `timestamp-nanos` or `local-timestamp-nanos` field was refused outright,
    /// and every one of them was unreachable inside a union.
    #[tokio::test]
    async fn test_integer_result_reaches_an_integer_backed_logical_type() {
        const TYPES: [&str; 9] = [
            r#"{"type":"int","logicalType":"date"}"#,
            r#"{"type":"int","logicalType":"time-millis"}"#,
            r#"{"type":"long","logicalType":"time-micros"}"#,
            r#"{"type":"long","logicalType":"timestamp-millis"}"#,
            r#"{"type":"long","logicalType":"timestamp-micros"}"#,
            r#"{"type":"long","logicalType":"timestamp-nanos"}"#,
            r#"{"type":"long","logicalType":"local-timestamp-millis"}"#,
            r#"{"type":"long","logicalType":"local-timestamp-micros"}"#,
            r#"{"type":"long","logicalType":"local-timestamp-nanos"}"#,
        ];
        for field in TYPES {
            // Bare, then in a union where nothing else could have been selected by accident.
            for shape in [field.to_string(), format!(r#"[{field},"string"]"#)] {
                let schema = format!(
                    r#"{{"type":"record","name":"U","fields":[{{"name":"u","type":{shape}}}]}}"#
                );
                // The seed is a string so that a union arrives on the *other* branch: the
                // result has to move it, which is what the old code could not do.
                let seed = if shape.starts_with('[') {
                    Value::Union(1, Box::new(Value::String("x".to_string())))
                } else {
                    Value::Long(1)
                };
                if let Some(err) = message_transform_err(&schema, r#"{"u": 20000}"#, seed).await {
                    panic!("{shape}: {err}");
                }
            }
        }
    }

    /// The range check reaches them too, and names the width the *field* has rather than an
    /// intermediate one - a CEL uint is checked against int, not against long first.
    #[tokio::test]
    async fn test_an_out_of_range_integer_names_the_fields_width() {
        for (field, expr) in [
            (
                r#"{"type":"int","logicalType":"date"}"#,
                r#"{"u": 2147483648}"#,
            ),
            ("\"int\"", r#"{"u": uint("18446744073709551615")}"#),
        ] {
            let schema = format!(
                r#"{{"type":"record","name":"U","fields":[{{"name":"u","type":{field}}}]}}"#
            );
            let err = message_transform_err(&schema, expr, Value::Int(1))
                .await
                .unwrap_or_else(|| panic!("{field}: an out-of-range integer was accepted"));
            assert!(err.contains("out of range for INT field"), "{field}: {err}");
        }
    }

    /// A 16-byte result reaches a uuid backed by bytes or by fixed.
    ///
    /// The reference's BYTES and FIXED cases accept raw bytes of the declared width whatever the
    /// logical type; apache-avro's `Schema::Uuid` variant took neither, so only a union with one
    /// non-null branch worked - by falling through to the input-shaped conversion rather than by
    /// resolving.
    #[tokio::test]
    async fn test_byte_result_reaches_a_byte_backed_uuid() {
        for field in [
            r#"{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}"#,
            r#"{"type":"bytes","logicalType":"uuid"}"#,
        ] {
            let schema = format!(
                r#"{{"type":"record","name":"U","fields":[{{"name":"u","type":[{field},"string"]}}]}}"#
            );
            let seed = Value::Union(1, Box::new(Value::String("x".to_string())));
            if let Some(err) =
                message_transform_err(&schema, r#"{"u": b"0123456789abcdef"}"#, seed).await
            {
                panic!("{field}: {err}");
            }
        }
    }

    /// Every Avro logical type survives an identity transform, and reaches the rule as a value.
    ///
    /// D49 gave the write side arms for the logical types apache-avro hoists into their own
    /// `Schema` variants. The read side still had none: `from_avro_value`'s trailing arm turned
    /// `Date`, `TimeMillis`, `TimeMicros`, the three local timestamps and `Uuid` into CEL **null**,
    /// so a rule saw the field as absent and echoing it erased it. Only decimal and the three
    /// timestamps had arms. The reference binds the library's own value (a `LocalDate`, a `UUID`)
    /// and C++ states the rule the others follow: everything but decimal and the timestamps is
    /// the plain int, long or string it is encoded as.
    ///
    /// The sibling `label` field is the discriminator - an echoed value that came back intact
    /// would look the same whether the rule ran or not.
    #[tokio::test]
    async fn test_every_logical_type_survives_an_identity_transform() {
        for (name, field, seed) in [
            (
                "uuid-string",
                r#"{"type":"string","logicalType":"uuid"}"#,
                Value::Uuid(uuid::Uuid::nil()),
            ),
            (
                "uuid-fixed",
                r#"{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}"#,
                Value::Uuid(uuid::Uuid::nil()),
            ),
            (
                "uuid-bytes",
                r#"{"type":"bytes","logicalType":"uuid"}"#,
                Value::Uuid(uuid::Uuid::nil()),
            ),
            (
                "date",
                r#"{"type":"int","logicalType":"date"}"#,
                Value::Date(20000),
            ),
            (
                "time-millis",
                r#"{"type":"int","logicalType":"time-millis"}"#,
                Value::TimeMillis(123),
            ),
            (
                "time-micros",
                r#"{"type":"long","logicalType":"time-micros"}"#,
                Value::TimeMicros(123),
            ),
            (
                "local-timestamp-millis",
                r#"{"type":"long","logicalType":"local-timestamp-millis"}"#,
                Value::LocalTimestampMillis(123),
            ),
            (
                "local-timestamp-micros",
                r#"{"type":"long","logicalType":"local-timestamp-micros"}"#,
                Value::LocalTimestampMicros(123),
            ),
            (
                "local-timestamp-nanos",
                r#"{"type":"long","logicalType":"local-timestamp-nanos"}"#,
                Value::LocalTimestampNanos(123),
            ),
            (
                "timestamp-millis",
                r#"{"type":"long","logicalType":"timestamp-millis"}"#,
                Value::TimestampMillis(123),
            ),
        ] {
            let schema_str = format!(
                r#"{{"type":"record","name":"U","fields":[{{"name":"u","type":["null",{field}]}},{{"name":"label","type":"string"}}]}}"#
            );
            let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
            let client = MockSchemaRegistryClient::new(client_conf);
            let rule = Rule {
                name: "r".to_string(),
                doc: None,
                kind: Some(Kind::Transform),
                mode: Some(Mode::Write),
                r#type: "CEL".to_string(),
                tags: None,
                params: None,
                expr: Some(
                    r#"{"u": message.u, "label": message.u == null ? "IS-NULL" : "NOT-NULL"}"#
                        .to_string(),
                ),
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
            let reg = RuleRegistry::new();
            reg.register_executor(CelExecutor::new());
            let ser = AvroSerializer::new(
                &client,
                None,
                Some(reg.clone()),
                SerializerConfig::new(
                    false,
                    Some(SchemaSelector::LatestVersion),
                    true,
                    false,
                    HashMap::new(),
                ),
            )
            .unwrap();
            let ser_ctx = SerializationContext {
                topic: "test".to_string(),
                serde_type: SerdeType::Value,
                serde_format: SerdeFormat::Avro,
                headers: None,
            };
            let obj = Record(vec![
                ("u".to_string(), Value::Union(1, Box::new(seed.clone()))),
                ("label".to_string(), Value::String("seed".to_string())),
            ]);
            let bytes = ser
                .serialize(&ser_ctx, obj)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e:?}"));
            let deser =
                AvroDeserializer::new(&client, Some(reg), DeserializerConfig::default()).unwrap();
            let Record(fields) = deser.deserialize(&ser_ctx, &bytes).await.unwrap().value else {
                panic!("{name}: expected a record");
            };
            let field_of = |n: &str| {
                fields
                    .iter()
                    .find(|(k, _)| k == n)
                    .map(|(_, v)| v.clone())
                    .unwrap()
            };
            assert_eq!(
                field_of("label"),
                Value::String("NOT-NULL".to_string()),
                "{name}: the field reached the rule as null"
            );
            assert_eq!(
                field_of("u"),
                Value::Union(1, Box::new(seed)),
                "{name}: the echoed value did not survive"
            );
        }
    }

    /// What `branch_accepts` admits, `to_avro_value_with_schema` must be able to write.
    ///
    /// `branch_accepts` takes a CEL uint at a float or double branch, mirroring the reference's
    /// `value instanceof Number`. The conversion had arms for Int but not UInt, so the value fell
    /// through to the input-shaped fallback, came out a Long, and the writer refused the record.
    /// JS and C++ cannot have this: both convert against the resolved branch with no input value
    /// to shape from, where Rust's fallback consults the value the field already held.
    #[tokio::test]
    async fn test_unsigned_result_reaches_a_float_branch() {
        for (name, schema_str) in [
            (
                "float",
                r#"{"type":"record","name":"U","fields":[{"name":"u","type":["float","string"]}]}"#,
            ),
            (
                "double",
                r#"{"type":"record","name":"U","fields":[{"name":"u","type":["double","string"]}]}"#,
            ),
        ] {
            assert!(
                message_transform_ok(
                    schema_str,
                    r#"{"u": 7u}"#,
                    Value::Union(1, Box::new(Value::String("a".to_string())))
                )
                .await,
                "a uint result must be writable at the {name} branch it was accepted for"
            );
        }
    }

    /// A string reaches a *string-backed* uuid and no other, which is the reference's rule: its
    /// STRING case takes any CharSequence whatever the logical type, and its FIXED case has no
    /// string arm at all. apache-avro models uuid as its own schema variant rather than a logical
    /// annotation, so this has to be said explicitly here where the other clients get it free.
    #[tokio::test]
    async fn test_string_reaches_only_the_string_backed_uuid() {
        const UUID: &str = r#"{"u": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"}"#;
        let seed = || Value::String("f81d4fae-7dec-11d0-a765-00a0c91e6bf6".to_string());
        assert!(
            message_transform_ok(
                r#"{"type":"record","name":"U","fields":[{"name":"u","type":{"type":"string","logicalType":"uuid"}}]}"#,
                UUID,
                seed()
            )
            .await
        );
        assert!(
            !message_transform_ok(
                r#"{"type":"record","name":"U","fields":[{"name":"u","type":{"type":"fixed","name":"F","size":16,"logicalType":"uuid"}}]}"#,
                UUID,
                seed()
            )
            .await,
            "a fixed-backed uuid wants 16 bytes; the reference does not accept a string there"
        );
    }

    /// Replace, not merge: the rule's map is the whole new record, so a field the rule does not
    /// name takes the schema's declared default rather than the value it had on the way in.
    ///
    /// This case existed only on the protobuf side, and its absence hid a real defect elsewhere -
    /// the C++ client seeded its result record from the input before applying the map, so it
    /// merged. Every other C6/C7 case names *all* of a record's fields, which makes merge and
    /// replace indistinguishable.
    ///
    /// Driven end to end through the serializer, not through the executor alone: whether the
    /// record the executor produces is one apache-avro will actually encode is the question, and
    /// an executor-level test cannot see it. Before the fix the omitted field was simply left out
    /// and the writer rejected the record with "Value does not match schema", naming nothing.
    #[tokio::test]
    async fn test_cel_message_transform_unnamed_field_takes_its_default() {
        for (expr, expect_default) in [
            (r#"{"kept": message.kept}"#, true),
            (
                r#"{"kept": message.kept, "withDefault": message.withDefault, "nullable": message.nullable}"#,
                false,
            ),
        ] {
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
                "name": "Defaults",
                "fields": [
                    {"name": "kept", "type": "string"},
                    {"name": "withDefault", "type": "string", "default": "fallback"},
                    {"name": "nullable", "type": ["null", "string"], "default": null}
                ]
            }
            "#;
            let rule = Rule {
                name: "r".to_string(),
                doc: None,
                kind: Some(Kind::Transform),
                mode: Some(Mode::Write),
                r#type: "CEL".to_string(),
                tags: None,
                params: None,
                expr: Some(expr.to_string()),
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
            let obj = Record(vec![
                (
                    "kept".to_string(),
                    Value::String("original-kept".to_string()),
                ),
                (
                    "withDefault".to_string(),
                    Value::String("original-withDefault".to_string()),
                ),
                (
                    "nullable".to_string(),
                    Value::Union(1, Box::new(Value::String("original-nullable".to_string()))),
                ),
            ]);
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
            let out = deser.deserialize(&ser_ctx, &bytes).await.unwrap();

            let Record(fields) = out.value else {
                panic!("expected a record");
            };
            let get = |name: &str| {
                fields
                    .iter()
                    .find(|(k, _)| k == name)
                    .map(|(_, v)| v.clone())
                    .unwrap_or_else(|| panic!("field {name} missing"))
            };
            assert_eq!(get("kept"), Value::String("original-kept".to_string()));
            if expect_default {
                // The declared default, and specifically *not* the input's value - that would
                // be merge.
                assert_eq!(get("withDefault"), Value::String("fallback".to_string()));
                assert_eq!(get("nullable"), Value::Union(0, Box::new(Value::Null)));
            } else {
                // The must-fail twin: naming every field still round trips, so "took the
                // defaults" cannot mean "the transform stopped working".
                assert_eq!(
                    get("withDefault"),
                    Value::String("original-withDefault".to_string())
                );
            }
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

    const NESTED_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "Outer",
        "fields": [
            {"name": "inner", "type": {
                "type": "record", "name": "Inner",
                "fields": [
                    {"name": "amount",
                     "type": {"type":"bytes","logicalType":"decimal","precision":12,"scale":4}},
                    {"name": "label", "type": "string", "confluent:tags": ["LABEL"]}
                ]}}
        ]
    }
    "#;

    /// A value inside a *nested* record reaches a rule the way a root-level one does.
    ///
    /// `message_binding` converted against `parsed_target`, which describes only the root, so a
    /// nested record matched none of its fields and its decimals stayed unscaled: `12.3400`
    /// arrived as `123400`. The reference reads the schema off the record it was handed.
    #[tokio::test]
    async fn test_nested_record_reaches_a_rule_at_its_schema_scale() {
        // 0x01E208 = 123400 unscaled, i.e. 12.3400 at scale 4.
        let unscaled = || Value::Decimal(vec![0x01u8, 0xE2, 0x08].into());
        let label_after =
            |schema: &'static str, expr: &'static str, fields: Vec<(String, Value)>| async move {
                let got = serialize_with_cel_field_transform(schema, expr, fields).await;
                let Ok(Record(fields)) = got.map(|v| v.value) else {
                    panic!("the transform failed");
                };
                fields.iter().find(|(n, _)| n == "inner").map_or_else(
                    || match &fields.iter().find(|(n, _)| n == "label").unwrap().1 {
                        Value::String(s) => s.clone(),
                        other => panic!("label = {other:?}"),
                    },
                    |(_, v)| match v {
                        Value::Record(inner) => {
                            match &inner.iter().find(|(n, _)| n == "label").unwrap().1 {
                                Value::String(s) => s.clone(),
                                other => panic!("inner.label = {other:?}"),
                            }
                        }
                        other => panic!("inner = {other:?}"),
                    },
                )
            };

        let inner = Value::Record(vec![
            ("amount".to_string(), unscaled()),
            ("label".to_string(), Value::String("usd".to_string())),
        ]);
        assert_eq!(
            label_after(
                NESTED_SCHEMA,
                "name == 'label' ; string(message.amount)",
                vec![("inner".to_string(), inner)]
            )
            .await,
            "12.3400",
            "a nested sibling decimal must arrive at its declared scale"
        );

        // The control that localises it: the same field on the root record always worked.
        const FLAT_SCHEMA: &str = r#"{"type":"record","name":"Flat","fields":[
            {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":12,"scale":4}},
            {"name":"label","type":"string","confluent:tags":["LABEL"]}]}"#;
        assert_eq!(
            label_after(
                FLAT_SCHEMA,
                "name == 'label' ; string(message.amount)",
                vec![
                    ("amount".to_string(), unscaled()),
                    ("label".to_string(), Value::String("usd".to_string())),
                ]
            )
            .await,
            "12.3400"
        );
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

    const NULLABLE_STR_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "strField", "type": ["null", "string"]}
        ]
    }
    "#;

    /// As [`serialize_with_cel_field_condition`], but a transform rule, round-tripped so the
    /// assertion sees the branch that actually reached the wire.
    async fn serialize_with_cel_field_transform(
        schema_str: &str,
        expr: &str,
        fields: Vec<(String, Value)>,
    ) -> Result<NamedValue, SerdeError> {
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
            expr: Some(expr.to_string()),
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
        let ser =
            AvroSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, Record(fields)).await?;
        let deser =
            AvroDeserializer::new(&client, Some(rule_registry), DeserializerConfig::default())
                .unwrap();
        deser.deserialize(&ser_ctx, &bytes).await
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

    const NULLABLE_TS_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "tsField",
             "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]}
        ]
    }
    "#;

    /// A `CEL_FIELD` condition over a *nullable* field must still be able to fail. The union arm
    /// of the walk used to re-wrap the rule's result, so the bare `Value::Boolean(false)` the
    /// record-field check tests for arrived as `Union(1, Boolean(false))` and every false
    /// condition passed. Only union-typed fields went through that path, so the plain-field
    /// tests above could not see it.
    #[tokio::test]
    async fn test_cel_field_condition_fails_on_nullable_field() {
        let present = vec![(
            "tsField".to_string(),
            Value::Union(1, Box::new(Value::TimestampMillis(1000))),
        )];
        let r = serialize_with_cel_field_condition(
            NULLABLE_TS_SCHEMA,
            "name == 'tsField' ; timestamp(value) > now",
            present.clone(),
        )
        .await;
        assert!(
            r.is_err(),
            "a false condition on a nullable field must fail"
        );

        let r = serialize_with_cel_field_condition(
            NULLABLE_TS_SCHEMA,
            "name == 'tsField' ; timestamp(value) < now",
            present,
        )
        .await;
        assert!(r.is_ok(), "a true condition on a nullable field must pass");
    }

    const INT_OR_STR_SCHEMA: &str = r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "strField", "type": ["int", "string"]}
        ]
    }
    "#;

    /// A rule that changes the value's type moves it off a branch that cannot hold it. The
    /// reference keeps no branch at all, so the string lands on the string branch; keeping
    /// index 0 makes the writer reject the record instead.
    #[tokio::test]
    async fn test_cel_field_transform_re_resolves_a_changed_branch() {
        let got = serialize_with_cel_field_transform(
            INT_OR_STR_SCHEMA,
            "name == 'strField' ; 'moved'",
            vec![(
                "strField".to_string(),
                Value::Union(0, Box::new(Value::Int(1))),
            )],
        )
        .await;
        let fields = match got.map(|v| v.value) {
            Ok(Record(fields)) => fields,
            other => panic!("the transform failed: {other:?}"),
        };
        let (_, v) = fields.iter().find(|(n, _)| n == "strField").unwrap();
        assert_eq!(
            *v,
            Value::Union(1, Box::new(Value::String("moved".to_string())))
        );
    }

    /// A `CEL_FIELD` transform that fills, or clears, a null union branch must move the value
    /// to the branch it now belongs to. Re-wrapping under the branch it arrived on produced
    /// `Union(null_index, String)`, which the writer rejects outright.
    #[tokio::test]
    async fn test_cel_field_transform_moves_a_null_branch() {
        for (name, input, expr, want) in [
            (
                "fills the null branch",
                Value::Union(0, Box::new(Value::Null)),
                "name == 'strField' ; 'recovered'",
                Value::Union(1, Box::new(Value::String("recovered".to_string()))),
            ),
            (
                "clears the value branch",
                Value::Union(1, Box::new(Value::String("a".to_string()))),
                "name == 'strField' ; null",
                Value::Union(0, Box::new(Value::Null)),
            ),
            (
                "leaves a present value on its own branch",
                Value::Union(1, Box::new(Value::String("a".to_string()))),
                "name == 'strField' ; value + '!'",
                Value::Union(1, Box::new(Value::String("a!".to_string()))),
            ),
        ] {
            let got = serialize_with_cel_field_transform(
                NULLABLE_STR_SCHEMA,
                expr,
                vec![("strField".to_string(), input)],
            )
            .await;
            let Ok(Record(fields)) = got.map(|v| v.value) else {
                panic!("{name}: the transform failed");
            };
            let (_, v) = fields.iter().find(|(n, _)| n == "strField").unwrap();
            assert_eq!(*v, want, "{name}");
        }
    }

    #[tokio::test]
    async fn test_cel_field_timestamp_value() {
        // The field rule's `value` binding must be a self-describing timestamp, so the 1-arg
        // `timestamp(value)` works (no unit literal).
        let r = serialize_with_cel_field_condition(
            TS_SCHEMA,
            "name == 'tsField' ; timestamp(value) < now",
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
            expr: Some("name == 'tsField' ; timestamp(value)".to_string()),
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

    /// Cross-client parity: an Avro `decimal` logical type is usable as a Decimal with **no
    /// `decimal(...)` call**, and the wrapped form keeps working alongside it. The boundary
    /// applies the schema's scale and produces a `Value::Opaque(CelDecimal)`, which is this
    /// client's in-CEL decimal representation, so `decimals.*` accept it directly.
    #[tokio::test]
    async fn test_cel_decimal_needs_no_constructor() {
        for expr in [
            // Bare: no constructor call on the field.
            "decimals.eq(message.decField, decimal(\"12.34\"))",
            "decimals.gt(message.decField, decimal(\"10.00\"))",
            // The wrapped form must keep working (decimal(...) re-entry).
            "decimals.eq(decimal(message.decField), decimal(\"12.34\"))",
            // `==` is numeric on it: 12.34 equals 12.340 despite the differing scale.
            "message.decField == decimal(\"12.340\")",
            // The schema's scale is applied, not guessed: as scale 0 this would be 1234.
            "decimals.lt(message.decField, decimal(\"100\"))",
        ] {
            let r = serialize_with_cel_condition(DECIMAL_SCHEMA, expr, decimal_field_12_34()).await;
            assert!(r.is_ok(), "{expr}: {r:?}");
        }
        // Negative control: a false comparison must fail.
        let r = serialize_with_cel_condition(
            DECIMAL_SCHEMA,
            "decimals.gt(message.decField, decimal(\"100\"))",
            decimal_field_12_34(),
        )
        .await;
        assert!(r.is_err());
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
            "timestamp(message.tsField) < now",
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
            "timestamp(message.tsField) > now",
            vec![("tsField".to_string(), Value::TimestampMillis(1000))],
        )
        .await;
        assert!(r.is_err());
    }

    /// Cross-client parity: an Avro timestamp logical type is usable as a timestamp with **no
    /// constructor call at all**. The boundary converts it to `Value::Timestamp`, so it is
    /// comparable against `now` and carries the timestamp accessors. Every one of the seven
    /// clients has this test; the constructor is only needed for a plain numeric field whose
    /// unit the schema cannot supply.
    #[tokio::test]
    async fn test_cel_timestamp_millis_needs_no_constructor() {
        let schema_str = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "tsField", "type": {"type": "long", "logicalType": "timestamp-millis"}}
            ]
        }
        "#;
        // Bare comparison against `now`, and the negative control that proves it compares.
        let past = vec![("tsField".to_string(), Value::TimestampMillis(1000))];
        assert!(
            serialize_with_cel_condition(schema_str, "message.tsField < now", past)
                .await
                .is_ok()
        );
        let future = vec![(
            "tsField".to_string(),
            Value::TimestampMillis(4_102_444_800_000),
        )];
        assert!(
            serialize_with_cel_condition(schema_str, "message.tsField < now", future)
                .await
                .is_err()
        );
        // The schema's millis unit is applied, not guessed, and the accessors work directly.
        let exact = vec![(
            "tsField".to_string(),
            Value::TimestampMillis(1_700_000_000_123),
        )];
        assert!(
            serialize_with_cel_condition(
                schema_str,
                "message.tsField == timestamp(\"2023-11-14T22:13:20.123Z\")",
                exact.clone(),
            )
            .await
            .is_ok()
        );
        assert!(
            serialize_with_cel_condition(
                schema_str,
                "message.tsField.getFullYear() == 2023",
                exact,
            )
            .await
            .is_ok()
        );
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

    /// An inline rule must see an Avro decimal at its schema scale, not the raw unscaled
    /// integer. The scale lives only in the schema, so binding the value alone read 12.34 as
    /// 1234 — silently, with no error, at both the record and the field level.
    #[test]
    fn inline_rules_see_decimals_at_schema_scale() {
        const DECIMAL_SCHEMA: &str = r#"{
            "type": "record",
            "name": "R",
            "confluent:rules": [
                {"name": "msgScaled", "expr": "string(this.amount) == '12.34'"}
            ],
            "fields": [
                {
                    "name": "amount",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 8,
                        "scale": 2
                    },
                    "confluent:rules": [
                        {"name": "fldScaled", "expr": "string(this) == '12.34'"}
                    ]
                }
            ]
        }"#;

        let parsed = apache_avro::Schema::parse_str(DECIMAL_SCHEMA).unwrap();
        // 0x04D2 = 1234 unscaled; at scale 2 that is 12.34.
        let message = Value::Record(vec![(
            "amount".to_string(),
            Value::Decimal(apache_avro::Decimal::from(vec![0x04u8, 0xd2])),
        )]);

        let violations = validate_message(&CelValidator::new(), &parsed, &[], &message, false);
        assert!(
            violations.is_empty(),
            "expected the decimal to read as 12.34 at both levels, got {violations:?}"
        );
    }

    /// The must-fail twin. Without it the test above would also pass if no rule ran at all —
    /// which is how this hid: the rules fired and quietly compared the wrong number.
    #[test]
    fn inline_decimal_rules_still_fire() {
        const DECIMAL_SCHEMA_N: &str = r#"{
            "type": "record",
            "name": "R",
            "confluent:rules": [
                {"name": "msgUnscaled", "expr": "string(this.amount) == '1234'"}
            ],
            "fields": [
                {
                    "name": "amount",
                    "type": {
                        "type": "bytes",
                        "logicalType": "decimal",
                        "precision": 8,
                        "scale": 2
                    },
                    "confluent:rules": [
                        {"name": "fldUnscaled", "expr": "string(this) == '1234'"}
                    ]
                }
            ]
        }"#;

        let parsed = apache_avro::Schema::parse_str(DECIMAL_SCHEMA_N).unwrap();
        let message = Value::Record(vec![(
            "amount".to_string(),
            Value::Decimal(apache_avro::Decimal::from(vec![0x04u8, 0xd2])),
        )]);

        let violations = validate_message(&CelValidator::new(), &parsed, &[], &message, false);
        let mut names: Vec<&str> = violations.iter().map(|v| v.rule.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec!["fldUnscaled", "msgUnscaled"],
            "the unscaled reading must now fail at both levels"
        );
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

    // ---- Avro message-level CEL transforms over the value types (C6/C7) ------------------

    const AVRO_VALUE_TYPES: &str = r#"{
        "type": "record",
        "name": "R",
        "fields": [
            {"name": "amount",
             "type": {"type": "bytes", "logicalType": "decimal", "precision": 8, "scale": 2}},
            {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
            {"name": "data",
             "type": {"type": "record", "name": "Variant", "namespace": "confluent.type",
                      "logicalType": "variant",
                      "fields": [{"name": "metadata", "type": "bytes"},
                                 {"name": "value", "type": "bytes"}]}},
            {"name": "label", "type": "string"}
        ]
    }"#;

    fn avro_fixture_record() -> Value {
        let variant = crate::serdes::variant::Variant::parse_json(r#"{"name":"alice"}"#).unwrap();
        Value::Record(vec![
            // 0x04D2 = 1234 unscaled, i.e. 12.34 at scale 2.
            (
                "amount".to_string(),
                Value::Decimal(apache_avro::Decimal::from(vec![0x04u8, 0xd2])),
            ),
            ("ts".to_string(), Value::TimestampMillis(1_700_000_000_123)),
            (
                "data".to_string(),
                Value::Record(vec![
                    (
                        "metadata".to_string(),
                        Value::Bytes(variant.metadata_bytes().to_vec()),
                    ),
                    (
                        "value".to_string(),
                        Value::Bytes(variant.value_bytes().to_vec()),
                    ),
                ]),
            ),
            ("label".to_string(), Value::String("hi".to_string())),
        ])
    }

    fn avro_transform(expr: &str) -> Value {
        let parsed = apache_avro::Schema::parse_str(AVRO_VALUE_TYPES).unwrap();
        let rule = Rule {
            name: "r".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL".to_string(),
            tags: None,
            params: None,
            expr: Some(expr.to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let mut ctx = RuleContext::new(
            None,
            SerializationContext {
                topic: "test".to_string(),
                serde_type: SerdeType::Value,
                serde_format: SerdeFormat::Avro,
                headers: None,
            },
            None,
            None,
            Some(SerdeSchema::Avro((parsed.clone(), Vec::new()))),
            "test-value".to_string(),
            Mode::Write,
            rule.clone(),
            0,
            vec![rule],
            None,
            None,
        );
        let input = SerdeValue::Avro(avro_fixture_record());
        let executor = CelExecutor::new();
        let mut args = HashMap::new();
        args.insert(
            "message".to_string(),
            executor.message_binding(&ctx, &input),
        );
        match executor.execute(&mut ctx, &input, &args).unwrap() {
            SerdeValue::Avro(v) => v,
            other => panic!("expected an Avro record, got {other:?}"),
        }
    }

    /// As `avro_transform`, but hands back the executor's error instead of unwrapping.
    fn avro_transform_result(expr: &str) -> Result<Value, crate::serdes::serde::SerdeError> {
        let parsed = apache_avro::Schema::parse_str(AVRO_VALUE_TYPES).unwrap();
        let rule = Rule {
            name: "r".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL".to_string(),
            tags: None,
            params: None,
            expr: Some(expr.to_string()),
            on_success: None,
            on_failure: None,
            disabled: None,
        };
        let mut ctx = RuleContext::new(
            None,
            SerializationContext {
                topic: "test".to_string(),
                serde_type: SerdeType::Value,
                serde_format: SerdeFormat::Avro,
                headers: None,
            },
            None,
            None,
            Some(SerdeSchema::Avro((parsed.clone(), Vec::new()))),
            "test-value".to_string(),
            Mode::Write,
            rule.clone(),
            0,
            vec![rule],
            None,
            None,
        );
        let input = SerdeValue::Avro(avro_fixture_record());
        let executor = CelExecutor::new();
        let mut args = HashMap::new();
        args.insert(
            "message".to_string(),
            executor.message_binding(&ctx, &input),
        );
        match executor.execute(&mut ctx, &input, &args)? {
            SerdeValue::Avro(v) => Ok(v),
            other => panic!("expected an Avro record, got {other:?}"),
        }
    }

    fn avro_fixture_field(record: &Value, name: &str) -> Value {
        let Value::Record(fields) = record else {
            panic!("expected a record, got {record:?}");
        };
        fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| panic!("field {name} missing"))
    }

    /// A computed variant must be written back into the variant record.
    ///
    /// `to_avro_value_with_schema` had an `Opaque` arm for the decimal type name and none for
    /// the Variant opaque, so a computed variant fell through to the loose conversion and was
    /// written back as Avro `null` - silently replacing the field.
    #[test]
    fn avro_message_transform_writes_back_a_computed_variant() {
        let out = avro_transform(
            r#"{"amount": message.amount, "ts": message.ts, "data": variants.parseJson("{\"name\":\"bob\"}"), "label": message.label}"#,
        );

        let data = avro_fixture_field(&out, "data");
        assert!(
            !matches!(data, Value::Null),
            "the computed variant was written back as null"
        );
        let Value::Record(fields) = &data else {
            panic!("expected a variant record, got {data:?}");
        };
        let bytes = |name: &str| match fields.iter().find(|(k, _)| k == name) {
            Some((_, Value::Bytes(b))) => b.clone(),
            other => panic!("{name} is not bytes: {other:?}"),
        };
        let variant = crate::serdes::variant::Variant::new(bytes("value"), bytes("metadata"));
        assert_eq!(variant.to_json().unwrap(), r#"{"name":"bob"}"#);
    }

    /// The other two value types were already correct; asserted alongside so a regression in
    /// either shows up here rather than only in the protobuf tests.
    #[test]
    fn avro_message_transform_writes_back_decimal_and_timestamp() {
        let out = avro_transform(
            r#"{"amount": decimals.add(decimal(message.amount), decimal("1.00")), "ts": message.ts + duration("60s"), "data": message.data, "label": message.label}"#,
        );

        // 0x0536 = 1334, i.e. 13.34 at scale 2.
        match avro_fixture_field(&out, "amount") {
            Value::Decimal(d) => {
                assert_eq!(Vec::<u8>::try_from(d).unwrap(), vec![0x05u8, 0x36]);
            }
            other => panic!("expected a decimal, got {other:?}"),
        }
        assert_eq!(
            avro_fixture_field(&out, "ts"),
            Value::TimestampMillis(1_700_000_060_123)
        );
    }

    /// The pass-through case: an identity transform must leave all three untouched.
    #[test]
    fn avro_message_transform_pass_through() {
        let out = avro_transform(
            r#"{"amount": message.amount, "ts": message.ts, "data": message.data, "label": message.label}"#,
        );

        match avro_fixture_field(&out, "amount") {
            Value::Decimal(d) => {
                assert_eq!(Vec::<u8>::try_from(d).unwrap(), vec![0x04u8, 0xd2])
            }
            other => panic!("expected a decimal, got {other:?}"),
        }
        assert_eq!(
            avro_fixture_field(&out, "ts"),
            Value::TimestampMillis(1_700_000_000_123)
        );
        assert!(!matches!(avro_fixture_field(&out, "data"), Value::Null));
    }

    /// The other half of replace semantics: a field the rule does not name and that has **no
    /// declared default** is an error, not a silently partial record. The fixture schema declares
    /// no defaults, so every field the rule omits lands here.
    ///
    /// The end-to-end counterpart, covering the case where a default *is* declared, is
    /// `test_cel_message_transform_unnamed_field_takes_its_default`.
    #[test]
    fn avro_message_transform_unnamed_field_without_a_default_is_an_error() {
        let err = avro_transform_result(r#"{"label": message.label}"#)
            .expect_err("a record missing three fields with no defaults must not be produced");

        let msg = format!("{err:?}");
        assert!(
            msg.contains("amount"),
            "the error must name the field it could not fill: {msg}"
        );
        assert!(msg.contains("no default value"), "{msg}");
    }

    /// The must-fail twin: naming every field still round-trips. Without it, "the other fields
    /// are gone" is equally consistent with the transform having stopped working altogether.
    #[test]
    fn avro_message_transform_naming_every_field_round_trips() {
        let out = avro_transform(
            r#"{"amount": message.amount, "ts": message.ts, "data": message.data, "label": message.label}"#,
        );

        let Value::Record(fields) = &out else {
            panic!("expected a record, got {out:?}");
        };
        let names: Vec<&str> = fields.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, vec!["amount", "ts", "data", "label"]);
        assert_eq!(
            avro_fixture_field(&out, "ts"),
            Value::TimestampMillis(1_700_000_000_123)
        );
    }
}

#[cfg(test)]
mod decimal_round_trip {
    //! A decimal encoded in fewer bytes than its declared precision allows.
    //!
    //! apache-avro's decimal resolution rejects a value whose encoded length is too short for the
    //! declared precision: two bytes in a `precision: 8` field fails with "Precision 8 too small
    //! to hold decimal values with 2 bytes". Avro encodes a bytes-backed decimal as a
    //! variable-length two's-complement integer, so that rejects every decimal small enough to fit
    //! in fewer bytes than its precision allows, whichever client wrote it. The check is the same
    //! in 0.21 and 0.22.
    //!
    //! The fix is that the deserializer no longer asks for resolution when there is nothing to
    //! resolve: with no migration, the reader schema is a clone of the writer schema.
    use super::*;
    use crate::rest::client_config::ClientConfig;
    use crate::rest::mock_schema_registry_client::MockSchemaRegistryClient;
    use crate::rest::models::{RuleSet, Schema as SrSchema};
    use crate::rest::schema_registry_client::Client;
    use crate::serdes::config::SchemaSelector;
    use apache_avro::types::Value as AvValue;

    const DECIMAL_SCHEMA: &str = r#"{"type":"record","name":"DecRec","fields":[
        {"name":"amount","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}}]}"#;

    pub(super) const ARRAY_SCHEMA: &str = r#"{"type":"record","name":"DecArr","fields":[
        {"name":"amounts","type":{"type":"array","items":
            {"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},
         "confluent:tags":["AMOUNTS"]},
        {"name":"label","type":"string"}]}"#;

    pub(super) fn ctx() -> SerializationContext {
        SerializationContext {
            topic: "dec".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Avro,
            headers: None,
        }
    }

    pub(super) async fn register(
        schema_str: &str,
        rule_set: Option<Box<RuleSet>>,
    ) -> MockSchemaRegistryClient {
        let client = MockSchemaRegistryClient::new(ClientConfig::new(vec!["mock://".to_string()]));
        let schema = SrSchema {
            schema_type: Some("AVRO".to_string()),
            references: None,
            metadata: None,
            rule_set,
            schema: schema_str.to_string(),
        };
        client
            .register_schema("dec-value", &schema, false)
            .await
            .unwrap();
        client
    }

    pub(super) fn ser_conf() -> SerializerConfig {
        SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            false,
            HashMap::new(),
        )
    }

    /// Round-trips through the real serializer and deserializer, with no rules.
    async fn round_trip(schema_str: &str, obj: AvValue) -> Result<AvValue, SerdeError> {
        let client = register(schema_str, None).await;
        let ser = AvroSerializer::new(&client, None, None, ser_conf()).unwrap();
        let bytes = ser.serialize(&ctx(), obj).await?;
        let deser = AvroDeserializer::new(&client, None, DeserializerConfig::default()).unwrap();
        Ok(deser.deserialize(&ctx(), &bytes).await?.value)
    }

    /// Reads the unscaled value back, sign-extended the way the wire format defines it. Reading it
    /// unsigned is what lets a negative value come back as a large positive one.
    pub(super) fn unscaled(v: &AvValue) -> i128 {
        match v {
            AvValue::Decimal(d) => {
                let bytes: Vec<u8> = d.try_into().expect("decimal bytes");
                let mut n: i128 = 0;
                for b in &bytes {
                    n = (n << 8) | *b as i128;
                }
                if bytes.first().is_some_and(|b| b & 0x80 != 0) {
                    n -= 1i128 << (8 * bytes.len());
                }
                n
            }
            other => panic!("expected a decimal, got {other:?}"),
        }
    }

    fn only_field(v: AvValue) -> AvValue {
        let AvValue::Record(fields) = v else {
            panic!("expected a record")
        };
        fields.into_iter().next().expect("a field").1
    }

    /// 1234 needs two bytes where the field's precision of 8 would allow four.
    #[tokio::test]
    async fn a_decimal_shorter_than_its_precision_round_trips() {
        let obj = AvValue::Record(vec![(
            "amount".to_string(),
            AvValue::Decimal(apache_avro::Decimal::from(vec![0x04u8, 0xD2])),
        )]);

        let back = round_trip(DECIMAL_SCHEMA, obj).await.expect("round trip");
        assert_eq!(unscaled(&only_field(back)), 1234);
    }

    /// The must-pass twin: a value already padded to the full width worked before, so it is what
    /// separates "the decimal path works" from "only the short case was broken".
    #[tokio::test]
    async fn a_padded_decimal_still_round_trips() {
        let obj = AvValue::Record(vec![(
            "amount".to_string(),
            AvValue::Decimal(apache_avro::Decimal::from(vec![0x00u8, 0x00, 0x04, 0xD2])),
        )]);

        let back = round_trip(DECIMAL_SCHEMA, obj).await.expect("round trip");
        assert_eq!(unscaled(&only_field(back)), 1234);
    }

    /// A negative value in one byte, because sign extension is where a length-sensitive decimal
    /// path goes wrong quietly rather than loudly.
    #[tokio::test]
    async fn a_negative_single_byte_decimal_round_trips() {
        let obj = AvValue::Record(vec![(
            "amount".to_string(),
            AvValue::Decimal(apache_avro::Decimal::from(vec![0xDEu8])),
        )]);

        let back = round_trip(DECIMAL_SCHEMA, obj).await.expect("round trip");
        assert_eq!(unscaled(&only_field(back)), -34);
    }
}

#[cfg(test)]
#[cfg(feature = "rules")]
mod decimal_round_trip_cel {
    //! A field transform over an array of decimals, which the read-back defect above blocked.
    //! Separate module because it needs the `rules` feature, while the defect is in the core read
    //! path and its tests must run without it.
    use super::decimal_round_trip::{ARRAY_SCHEMA, ctx, register, ser_conf, unscaled};
    use super::*;
    use crate::rest::models::{Rule, RuleSet};
    use crate::rules::cel::cel_field_executor::CelFieldExecutor;
    use apache_avro::types::Value as AvValue;

    #[tokio::test]
    async fn a_field_transform_over_an_array_of_decimals_round_trips() {
        let rule_set = Some(Box::new(RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![Rule {
                name: "r".to_string(),
                doc: None,
                kind: Some(Kind::Transform),
                mode: Some(Mode::Write),
                r#type: "CEL_FIELD".to_string(),
                tags: Some(vec!["AMOUNTS".to_string()]),
                params: None,
                expr: Some(
                    r#"name == "amounts" ; decimals.add(decimal(value), decimal("1.00"))"#
                        .to_string(),
                ),
                on_success: None,
                on_failure: None,
                disabled: None,
            }]),
            encoding_rules: None,
            enable_at: None,
        }));
        let client = register(ARRAY_SCHEMA, rule_set).await;
        let registry = RuleRegistry::new();
        registry.register_executor(CelFieldExecutor::new());
        let ser = AvroSerializer::new(&client, None, Some(registry.clone()), ser_conf()).unwrap();
        let obj = AvValue::Record(vec![
            (
                "amounts".to_string(),
                AvValue::Array(vec![
                    AvValue::Decimal(apache_avro::Decimal::from(vec![0x00u8, 0x6F])),
                    AvValue::Decimal(apache_avro::Decimal::from(vec![0x00u8, 0xDE])),
                ]),
            ),
            ("label".to_string(), AvValue::String("hi".to_string())),
        ]);
        let bytes = ser.serialize(&ctx(), obj).await.expect("serialize");
        let deser =
            AvroDeserializer::new(&client, Some(registry), DeserializerConfig::default()).unwrap();
        let back = deser
            .deserialize(&ctx(), &bytes)
            .await
            .expect("deserialize")
            .value;

        let AvValue::Record(fields) = back else {
            panic!("expected a record")
        };
        let AvValue::Array(items) = &fields[0].1 else {
            panic!("expected an array")
        };
        assert_eq!(
            items.iter().map(unscaled).collect::<Vec<_>>(),
            vec![211, 322],
            "1.11 and 2.22 each plus 1.00, at scale 2"
        );
    }
}

#[cfg(test)]
#[cfg(feature = "rules")]
mod nested_variant {
    //! A rule reading a variant inside a nested record.
    //!
    //! The variant is *defined* at the nested position rather than referenced by name. That
    //! matters: apache-avro panics resolving a by-name reference to `confluent.type.Variant` in
    //! nested position, so a schema that defines it once and references it elsewhere cannot be
    //! parsed here at all. Defining it in place is a schema the client can read, and it is what
    //! makes this capability measurable.
    use super::decimal_round_trip::{ctx, register, ser_conf};
    use super::*;
    use crate::rest::models::{Rule, RuleSet};
    use crate::rules::cel::cel_executor::CelExecutor;
    use crate::serdes::variant::Variant;
    use apache_avro::types::Value as AvValue;

    const NESTED_VARIANT: &str = r#"{"type":"record","name":"NestedVariant","fields":[
        {"name":"nested","type":{"type":"record","name":"Inner","fields":[
            {"name":"data","type":{"type":"record","name":"confluent.type.Variant","fields":[
                {"name":"metadata","type":"bytes"},{"name":"value","type":"bytes"}]}}]}}]}"#;

    async fn serialize_under_condition(expr: &str) -> Result<Vec<u8>, SerdeError> {
        let rule_set = Some(Box::new(RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![Rule {
                name: "r".to_string(),
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
            }]),
            encoding_rules: None,
            enable_at: None,
        }));
        let client = register(NESTED_VARIANT, rule_set).await;
        let registry = RuleRegistry::new();
        registry.register_executor(CelExecutor::new());
        let ser = AvroSerializer::new(&client, None, Some(registry), ser_conf()).unwrap();
        let v = Variant::parse_json(r#"{"name":"alice"}"#).expect("parse");
        let obj = AvValue::Record(vec![(
            "nested".to_string(),
            AvValue::Record(vec![(
                "data".to_string(),
                AvValue::Record(vec![
                    (
                        "metadata".to_string(),
                        AvValue::Bytes(v.metadata_bytes().to_vec()),
                    ),
                    (
                        "value".to_string(),
                        AvValue::Bytes(v.value_bytes().to_vec()),
                    ),
                ]),
            )]),
        )]);
        ser.serialize(&ctx(), obj).await
    }

    #[tokio::test]
    async fn a_condition_reads_a_variant_inside_a_nested_record() {
        assert!(
            serialize_under_condition(r#"variants.type(message.nested.data) == "object""#)
                .await
                .is_ok()
        );
    }

    /// The twin: without it the test above is satisfied by a rule that never ran, since a variant
    /// the walk failed to reach would report no violation either.
    #[tokio::test]
    async fn the_same_condition_fails_when_it_should() {
        assert!(
            serialize_under_condition(r#"variants.type(message.nested.data) == "array""#)
                .await
                .is_err()
        );
    }
}
