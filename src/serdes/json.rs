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
use async_recursion::async_recursion;
use base64::Engine;
use dashmap::DashMap;
use futures::future::FutureExt;
use jsonschema::{ValidationError, Validator, validator_for};
use referencing::{Draft, Registry, Resolver, Resource, ResourceRef};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct JsonSerde {
    parsed_schemas: DashMap<Schema, (Value, Registry)>,
    validators: DashMap<Schema, Arc<Validator>>,
    subject_cache: DashMap<SubjectCacheKey, Option<String>>,
}

#[derive(Clone)]
pub struct JsonSerializer<'a, T: Client> {
    schema: Option<&'a Schema>,
    base: BaseSerializer<'a, T>,
    serde: JsonSerde,
    subject_name_strategy_type: SubjectNameStrategyType,
}

impl<'a, T: Client> std::fmt::Debug for JsonSerializer<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonSerializer")
            .field("schema", &self.schema)
            .field("serde", &self.serde)
            .field(
                "subject_name_strategy_type",
                &self.subject_name_strategy_type,
            )
            .finish_non_exhaustive()
    }
}

impl<'a, T: Client + Sync> JsonSerializer<'a, T> {
    pub fn new(
        client: &'a T,
        schema: Option<&'a Schema>,
        rule_registry: Option<RuleRegistry>,
        serializer_config: SerializerConfig,
    ) -> Result<JsonSerializer<'a, T>, SerdeError> {
        for executor in get_executors(rule_registry.as_ref()) {
            executor.configure(client.config(), &serializer_config.rule_config)?;
        }
        Ok(JsonSerializer {
            schema,
            base: BaseSerializer::new(Serde::new(client, rule_registry), serializer_config.clone()),
            serde: JsonSerde {
                parsed_schemas: DashMap::new(),
                validators: DashMap::new(),
                subject_cache: DashMap::new(),
            },
            subject_name_strategy_type: serializer_config.subject_name_strategy_type,
        })
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
            schema_id = SchemaId::new(SerdeFormat::Json, schema.id, schema.guid.clone(), None)?;
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
                schema_id = SchemaId::new(SerdeFormat::Json, rs.id, rs.guid.clone(), None)?;
            } else {
                let rs = self
                    .base
                    .serde
                    .client
                    .get_by_schema(&subject, schema, self.base.config.normalize_schemas, false)
                    .await?;
                schema_id = SchemaId::new(SerdeFormat::Json, rs.id, rs.guid.clone(), None)?;
            }
        }

        let schema;
        let parsed_schema;
        let ref_registry;
        if let Some(ref latest_schema) = latest_schema {
            schema = latest_schema.to_schema();
            (parsed_schema, ref_registry) = self.get_parsed_schema(&schema).await?;
            if self
                .base
                .validation_enabled(Some(ValidationRulesExecution::BeforeDomainRules))
            {
                self.validate_inline_rules(&parsed_schema, &ref_registry, &value)?;
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
                    Some(&latest_schema.to_schema()),
                    Some(&SerdeSchema::Json((
                        parsed_schema.clone(),
                        ref_registry.clone(),
                    ))),
                    &SerdeValue::Json(value),
                    Some(Arc::new(field_transformer)),
                )
                .await?;
            value = match serde_value {
                SerdeValue::Json(value) => value,
                _ => return Err(Serialization("unexpected serde value".to_string())),
            };
            if self
                .base
                .validation_enabled(Some(ValidationRulesExecution::AfterDomainRules))
            {
                self.validate_inline_rules(&parsed_schema, &ref_registry, &value)?;
            }
        } else {
            schema = self
                .schema
                .ok_or(Serialization("schema needs to be set".to_string()))?
                .clone();
            (parsed_schema, ref_registry) = self.get_parsed_schema(&schema).await?;
            // No domain rules run on this path, so there is a single validation point
            // regardless of the configured phase.
            if self.base.validation_enabled(None) {
                self.validate_inline_rules(&parsed_schema, &ref_registry, &value)?;
            }
        }

        if self.base.config.validate {
            let validator = self
                .get_validator(&schema, &parsed_schema, ref_registry)
                .await?;
            validator.validate(&value)?;
        }

        let mut encoded_bytes = serde_json::to_vec(&value)?;
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
                        &SerdeValue::new_bytes(SerdeFormat::Json, &encoded_bytes),
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
        parsed_schema: &Value,
        ref_registry: &Registry,
        value: &Value,
    ) -> Result<(), SerdeError> {
        let executor = self.base.validation_executor()?;
        let root_resource_ref = ResourceRef::from_contents(parsed_schema);
        let base_uri = root_resource_ref.id().unwrap_or("").to_string();
        let ref_registry = ref_registry.clone().try_with_resource(
            base_uri.clone(),
            Resource::from_contents(parsed_schema.clone()),
        )?;
        let ref_resolver = ref_registry.try_resolver(&base_uri)?;
        raise_validation_violations(validate_message(
            executor.as_ref(),
            parsed_schema,
            &ref_registry,
            &ref_resolver,
            value,
            self.base.config.validation_rules_fail_fast,
        )?)
    }

    async fn get_parsed_schema(&self, schema: &Schema) -> Result<(Value, Registry), SerdeError> {
        let result = self.serde.parsed_schemas.get(schema);
        if let Some((parsed_schema, ref_registry)) = result.as_deref() {
            return Ok((parsed_schema.clone(), ref_registry.clone()));
        }
        let ref_registry = resolve_named_schema(schema, self.base.serde.client, None).await?;
        let parsed_schema: Value = serde_json::from_str(&schema.schema)?;
        self.serde.parsed_schemas.insert(
            schema.clone(),
            (parsed_schema.clone(), ref_registry.clone()),
        );
        Ok((parsed_schema, ref_registry))
    }

    pub async fn get_record_name(&self, schema: &Schema) -> Result<String, SerdeError> {
        let (parsed_schema, _) = self.get_parsed_schema(schema).await?;
        parsed_schema
            .get("title")
            .and_then(|t| t.as_str())
            .map(|t| t.to_string())
            .ok_or_else(|| Serialization("Schema does not have a 'title' field".to_string()))
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

    async fn get_validator(
        &self,
        schema: &Schema,
        parsed_schema: &Value,
        ref_registry: Registry,
    ) -> Result<Arc<Validator>, SerdeError> {
        let result = self.serde.validators.get(schema);
        if let Some(validator) = result.as_deref() {
            return Ok(validator.clone());
        }
        let validator = Arc::new(
            jsonschema::options()
                .with_registry(ref_registry)
                .build(parsed_schema)?,
        );
        self.serde
            .validators
            .insert(schema.clone(), validator.clone());
        Ok(validator)
    }

    fn close(&mut self) {}
}

impl<'a> From<ValidationError<'a>> for SerdeError {
    fn from(value: ValidationError<'a>) -> Self {
        Serialization(value.to_string())
    }
}

async fn transform_fields(
    ctx: &mut RuleContext,
    value: &SerdeValue,
) -> Result<SerdeValue, SerdeError> {
    if let Some(SerdeSchema::Json((s, ref_registry))) = ctx.parsed_target.clone()
        && let SerdeValue::Json(v) = value
    {
        let root_resource_ref = ResourceRef::from_contents(&s);
        let base_uri = root_resource_ref.id().unwrap_or("").to_string();
        let ref_registry = ref_registry
            .clone()
            .try_with_resource(base_uri.clone(), Resource::from_contents(s.clone()))?;
        let ref_resolver = ref_registry.try_resolver(&base_uri)?;
        let value = transform(ctx, &s, &ref_registry, &ref_resolver, "$", v).await?;
        return Ok(SerdeValue::Json(value));
    }
    Ok(value.clone())
}

#[derive(Clone)]
pub struct JsonDeserializer<'a, T: Client> {
    base: BaseDeserializer<'a, T>,
    serde: JsonSerde,
    subject_name_strategy_type: SubjectNameStrategyType,
}

impl<'a, T: Client> std::fmt::Debug for JsonDeserializer<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonDeserializer")
            .field("serde", &self.serde)
            .field(
                "subject_name_strategy_type",
                &self.subject_name_strategy_type,
            )
            .finish_non_exhaustive()
    }
}

impl<'a, T: Client + Sync> JsonDeserializer<'a, T> {
    pub fn new(
        client: &'a T,
        rule_registry: Option<RuleRegistry>,
        deserializer_config: DeserializerConfig,
    ) -> Result<JsonDeserializer<'a, T>, SerdeError> {
        for executor in get_executors(rule_registry.as_ref()) {
            executor.configure(client.config(), &deserializer_config.rule_config)?;
        }
        Ok(JsonDeserializer {
            base: BaseDeserializer::new(
                Serde::new(client, rule_registry),
                deserializer_config.clone(),
            ),
            serde: JsonSerde {
                parsed_schemas: DashMap::new(),
                validators: DashMap::new(),
                subject_cache: DashMap::new(),
            },
            subject_name_strategy_type: deserializer_config.subject_name_strategy_type,
        })
    }

    pub async fn deserialize(
        &self,
        ctx: &SerializationContext,
        data: &[u8],
    ) -> Result<Value, SerdeError> {
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

        let mut schema_id = SchemaId::new(SerdeFormat::Json, None, None, None)?;
        let id_deser = self.base.config.schema_id_deserializer;
        let bytes_read = id_deser(data, ctx, &mut schema_id)?;
        let mut data = &data[bytes_read..];

        let writer_schema_raw = self
            .base
            .get_writer_schema(&schema_id, initial_subject.as_deref(), None)
            .await?;
        let (writer_schema, writer_ref_registry) =
            self.get_parsed_schema(&writer_schema_raw).await?;

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
                    &SerdeValue::new_bytes(SerdeFormat::Json, data),
                    None,
                )
                .await?
                .as_bytes();
            data = &serde_value;
        }

        let migrations;
        let reader_schema_raw;
        let reader_schema;
        let reader_ref_registry;
        if let Some(ref latest_schema) = latest_schema {
            migrations = self
                .base
                .serde
                .get_migrations(&subject, &writer_schema_raw, latest_schema, None)
                .await?;
            reader_schema_raw = latest_schema.to_schema();
            (reader_schema, reader_ref_registry) =
                self.get_parsed_schema(&reader_schema_raw).await?;
        } else {
            migrations = Vec::new();
            reader_schema_raw = writer_schema_raw.clone();
            reader_schema = writer_schema.clone();
            reader_ref_registry = writer_ref_registry.clone();
        }

        let mut value = serde_json::from_slice(data)?;
        if !migrations.is_empty() {
            let serde_value = self
                .base
                .serde
                .execute_migrations(ctx, &subject, &migrations, &SerdeValue::Json(value))
                .await?;
            value = match serde_value {
                SerdeValue::Json(v) => v,
                _ => return Err(Serialization("unexpected serde value".to_string())),
            }
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
                Some(&SerdeSchema::Json((
                    reader_schema.clone(),
                    reader_ref_registry.clone(),
                ))),
                &SerdeValue::Json(value),
                Some(Arc::new(field_transformer)),
            )
            .await?;
        value = match serde_value {
            SerdeValue::Json(value) => value,
            _ => return Err(Serialization("unexpected serde value".to_string())),
        };

        if self.base.config.validate {
            let validator = self
                .get_validator(&reader_schema_raw, &reader_schema, reader_ref_registry)
                .await?;
            validator.validate(&value)?;
        }

        Ok(value)
    }

    async fn get_parsed_schema(&self, schema: &Schema) -> Result<(Value, Registry), SerdeError> {
        let result = self.serde.parsed_schemas.get(schema);
        if let Some((parsed_schema, ref_registry)) = result.as_deref() {
            return Ok((parsed_schema.clone(), ref_registry.clone()));
        }
        let ref_registry = resolve_named_schema(schema, self.base.serde.client, None).await?;
        let parsed_schema: Value = serde_json::from_str(&schema.schema)?;
        self.serde.parsed_schemas.insert(
            schema.clone(),
            (parsed_schema.clone(), ref_registry.clone()),
        );
        Ok((parsed_schema, ref_registry))
    }

    async fn get_validator(
        &self,
        schema: &Schema,
        parsed_schema: &Value,
        ref_registry: Registry,
    ) -> Result<Arc<Validator>, SerdeError> {
        let result = self.serde.validators.get(schema);
        if let Some(validator) = result.as_deref() {
            return Ok(validator.clone());
        }
        let validator = Arc::new(
            jsonschema::options()
                .with_registry(ref_registry)
                .build(parsed_schema)?,
        );
        self.serde
            .validators
            .insert(schema.clone(), validator.clone());
        Ok(validator)
    }

    pub async fn get_record_name(&self, schema: &Schema) -> Result<String, SerdeError> {
        let (parsed_schema, _) = self.get_parsed_schema(schema).await?;
        parsed_schema
            .get("title")
            .and_then(|t| t.as_str())
            .map(|t| t.to_string())
            .ok_or_else(|| Serialization("Schema does not have a 'title' field".to_string()))
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
async fn resolve_named_schema<T>(
    schema: &Schema,
    client: &T,
    ref_registry: Option<Registry>,
) -> Result<Registry, SerdeError>
where
    T: Client + Sync,
{
    let mut ref_registry = if let Some(r) = ref_registry {
        r
    } else {
        Registry::options().build(Vec::<(String, Resource)>::new().into_iter())?
    };
    if let Some(refs) = schema.references.as_ref() {
        let mut resources = Vec::new();
        for r in refs {
            let ref_schema = client
                .get_version(
                    &r.subject.clone().unwrap_or_default(),
                    r.version.unwrap_or(-1),
                    true,
                    None,
                )
                .await?;
            ref_registry =
                resolve_named_schema(&ref_schema.to_schema(), client, Some(ref_registry.clone()))
                    .await?;
            let ref_schema_val: Value =
                serde_json::from_str(&ref_schema.schema.clone().unwrap_or_default())?;
            let resource = Resource::from_contents(ref_schema_val.clone());
            resources.push((r.name.clone().unwrap_or_default(), resource));
        }
        // TODO fix draft default?
        ref_registry = ref_registry.try_with_resources(resources.into_iter(), Draft::default())?;
    }
    Ok(ref_registry)
}

#[async_recursion]
async fn transform(
    ctx: &mut RuleContext,
    schema: &Value,
    ref_registry: &Registry,
    ref_resolver: &Resolver,
    path: &str,
    message: &Value,
) -> Result<Value, SerdeError> {
    if let Value::Object(map) = schema {
        // A type union is narrowed to the member the value satisfies before dispatching, as
        // the validation walk does.
        if let Some(narrowed) = narrow_subtype(schema, message, ref_registry) {
            return transform(ctx, &narrowed, ref_registry, ref_resolver, path, message).await;
        }
        let all_of = map.get("allOf").and_then(|v| {
            if let Value::Array(a) = v {
                Some(a)
            } else {
                None
            }
        });
        let any_of = map.get("anyOf").and_then(|v| {
            if let Value::Array(a) = v {
                Some(a)
            } else {
                None
            }
        });
        let one_of = map.get("oneOf").and_then(|v| {
            if let Value::Array(a) = v {
                Some(a)
            } else {
                None
            }
        });
        if all_of.is_some() || any_of.is_some() || one_of.is_some() {
            let mut current = message.clone();
            if let Some(subschemas) = all_of {
                for subschema in subschemas {
                    current = transform(ctx, subschema, ref_registry, ref_resolver, path, &current)
                        .await?;
                }
            } else if let Some(subschemas) = one_of {
                for subschema in subschemas {
                    if validate_subschema(subschema, &current, ref_registry) {
                        current =
                            transform(ctx, subschema, ref_registry, ref_resolver, path, &current)
                                .await?;
                        break;
                    }
                }
            } else if let Some(subschemas) = any_of {
                for subschema in subschemas {
                    if validate_subschema(subschema, &current, ref_registry) {
                        current =
                            transform(ctx, subschema, ref_registry, ref_resolver, path, &current)
                                .await?;
                    }
                }
            }
            // Also visit sibling properties/items at this level
            // (siblings to allOf/anyOf/oneOf).
            if let Some(Value::Object(props)) = map.get("properties")
                && let Value::Object(message_obj) = &current
            {
                let mut new_message = message_obj.clone();
                for (prop_name, prop_schema) in props {
                    let new_value = transform_field_with_ctx(
                        ctx,
                        path,
                        prop_name,
                        &new_message,
                        prop_schema,
                        ref_registry,
                        ref_resolver,
                    )
                    .await?;
                    if let Some(new_value) = new_value {
                        new_message.insert(prop_name.clone(), new_value);
                    }
                }
                current = Value::Object(new_message);
            }
            if let Some(items) = map.get("items")
                && let Value::Array(_) = &current
            {
                current = transform(ctx, items, ref_registry, ref_resolver, path, &current).await?;
            }
            return Ok(current);
        }
        if let Some(items) = map.get("items")
            && let Value::Array(_) = message
        {
            return transform(ctx, items, ref_registry, ref_resolver, path, message).await;
        }
        if let Some(reference) = map.get("$ref") {
            let Some(reference) = reference.as_str() else {
                return Err(SerdeError::Serialization(format!(
                    "$ref must be a string, found {reference}"
                )));
            };
            let ref_schema = ref_resolver.lookup(reference)?;
            // Recurse with the resolver the lookup returned, not the original one: it is
            // scoped to the referenced resource, so a relative or fragment-only `$ref`
            // inside it resolves against the right base URI.
            return transform(
                ctx,
                ref_schema.contents(),
                ref_registry,
                ref_schema.resolver(),
                path,
                message,
            )
            .await;
        }
        let field_type = get_type(schema);
        if field_type == FieldType::Record
            && let Some(Value::Object(props)) = map.get("properties")
            && let Value::Object(message) = message
        {
            let mut new_message = message.clone();
            for (prop_name, prop_schema) in props {
                let new_value = transform_field_with_ctx(
                    ctx,
                    path,
                    prop_name,
                    &new_message,
                    prop_schema,
                    ref_registry,
                    ref_resolver,
                )
                .await?;
                if let Some(new_value) = new_value {
                    new_message.insert(prop_name.clone(), new_value);
                }
            }
            return Ok(Value::Object(new_message));
        }
    }
    if let Some(field_ctx) = ctx.current_field() {
        field_ctx.set_field_type(get_type(schema));
        let rule_tags = ctx
            .rule
            .tags
            .clone()
            .map(|v| HashSet::from_iter(v.into_iter()));
        if rule_tags.is_none_or(|tags| !tags.is_disjoint(&field_ctx.tags)) {
            let message_value = SerdeValue::Json(message.clone());
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
                if let SerdeValue::Json(v) = new_value {
                    return Ok(v);
                }
            }
        }
    }
    Ok(message.clone())
}

async fn transform_field_with_ctx(
    ctx: &mut RuleContext,
    path: &str,
    prop_name: &str,
    message: &serde_json::Map<String, Value>,
    prop_schema: &Value,
    ref_registry: &Registry,
    ref_resolver: &Resolver<'_>,
) -> Result<Option<Value>, SerdeError> {
    let full_name = path.to_string() + "." + prop_name;
    let message_value = SerdeValue::Json(Value::Object(message.clone()));
    ctx.enter_field(
        message_value,
        full_name.clone(),
        prop_name.to_string(),
        get_type(prop_schema),
        get_inline_tags(prop_schema),
    );
    if let Some(value) = message.get(prop_name) {
        let new_value = transform(
            ctx,
            prop_schema,
            ref_registry,
            ref_resolver,
            &full_name,
            value,
        )
        .await?;
        if let Some(Kind::Condition) = ctx.rule.kind
            && let Value::Bool(b) = new_value
            && !b
        {
            return Err(SerdeError::RuleCondition(Box::new(ctx.rule.clone())));
        }
        ctx.exit_field();
        return Ok(Some(new_value));
    }
    ctx.exit_field();
    Ok(None)
}

/// Walks `message` against `schema`, evaluating every inline `confluent:rules` CHECK
/// constraint encountered and collecting all failures. Read-only — the message is not
/// modified.
///
/// Two kinds of rules are evaluated:
///   - Object-level (`confluent:rules` on an object schema) — `this` is the object.
///   - Property-level (`confluent:rules` on a property schema) — `this` is the property
///     value. Honors the skip-on-null contract: a property that is absent or null does
///     not have its rules invoked.
///
/// Failures carry their location rooted at `$` to match the JVM client (e.g. `$.addr.zip`,
/// `$.tags[3]`). The walk continues after each failure so callers see the full set rather
/// than only the first, unless `fail_fast` is set.
fn validate_message(
    executor: &dyn ValidationRuleExecutor,
    schema: &Value,
    ref_registry: &Registry,
    ref_resolver: &Resolver,
    message: &Value,
    fail_fast: bool,
) -> Result<Vec<ValidationRuleError>, SerdeError> {
    let mut violations = Vec::new();
    validate_with_rules(
        executor,
        schema,
        ref_registry,
        ref_resolver,
        "$",
        message,
        fail_fast,
        &mut violations,
    )?;
    Ok(violations)
}

/// Mirrors [`transform`]'s dispatch shape: the combined keywords (allOf/anyOf/oneOf) with
/// their sibling properties/items, then items, then `$ref`, then object properties.
#[allow(clippy::too_many_arguments)]
fn validate_with_rules(
    executor: &dyn ValidationRuleExecutor,
    schema: &Value,
    ref_registry: &Registry,
    ref_resolver: &Resolver,
    path: &str,
    message: &Value,
    fail_fast: bool,
    violations: &mut Vec<ValidationRuleError>,
) -> Result<(), SerdeError> {
    if fail_fast && !violations.is_empty() {
        return Ok(());
    }
    let Value::Object(map) = schema else {
        return Ok(());
    };

    // A type union is narrowed to the member the value satisfies before anything else, so
    // that the walk dispatches on a single type - as the transform walk does. Narrowing
    // first also keeps this level's rules from being evaluated twice: the recursive call
    // sees the same rules on the narrowed copy.
    if let Some(narrowed) = narrow_subtype(schema, message, ref_registry) {
        return validate_with_rules(
            executor,
            &narrowed,
            ref_registry,
            ref_resolver,
            path,
            message,
            fail_fast,
            violations,
        );
    }

    // Rules declared at this level: `this` is the value at this location.
    if evaluate_rules(
        executor,
        parse_validation_rules(map.get(VALIDATION_RULES_PROP)),
        message,
        path,
        fail_fast,
        violations,
    ) {
        return Ok(());
    }

    let as_array = |key: &str| {
        map.get(key).and_then(|v| {
            if let Value::Array(a) = v {
                Some(a)
            } else {
                None
            }
        })
    };
    let all_of = as_array("allOf");
    let any_of = as_array("anyOf");
    let one_of = as_array("oneOf");
    if all_of.is_some() || any_of.is_some() || one_of.is_some() {
        // allOf branches all apply; for oneOf/anyOf only the branches the value actually
        // satisfies do, otherwise violations would be attributed to a branch the value
        // does not follow.
        if let Some(subschemas) = all_of {
            for subschema in subschemas {
                validate_with_rules(
                    executor,
                    subschema,
                    ref_registry,
                    ref_resolver,
                    path,
                    message,
                    fail_fast,
                    violations,
                )?;
                if fail_fast && !violations.is_empty() {
                    return Ok(());
                }
            }
        } else if let Some(subschemas) = one_of {
            for subschema in subschemas {
                if validate_subschema(subschema, message, ref_registry) {
                    validate_with_rules(
                        executor,
                        subschema,
                        ref_registry,
                        ref_resolver,
                        path,
                        message,
                        fail_fast,
                        violations,
                    )?;
                    break;
                }
            }
        } else if let Some(subschemas) = any_of {
            for subschema in subschemas {
                if validate_subschema(subschema, message, ref_registry) {
                    validate_with_rules(
                        executor,
                        subschema,
                        ref_registry,
                        ref_resolver,
                        path,
                        message,
                        fail_fast,
                        violations,
                    )?;
                    if fail_fast && !violations.is_empty() {
                        return Ok(());
                    }
                }
            }
        }
        if fail_fast && !violations.is_empty() {
            return Ok(());
        }
        // Also visit sibling properties/items at this level
        // (siblings to allOf/anyOf/oneOf).
        return validate_properties(
            executor,
            map,
            ref_registry,
            ref_resolver,
            path,
            message,
            fail_fast,
            violations,
        );
    }

    if let Some(reference) = map.get("$ref").and_then(|v| v.as_str()) {
        // Surface a failed lookup rather than treating the reference as carrying no rules:
        // silently skipping the referenced subtree would let a message serialize without
        // the checks the reference was there to supply. The transform path propagates this
        // failure the same way.
        let ref_schema = ref_resolver.lookup(reference)?;
        // Recurse with the resolver the lookup returned, not the original one: it is scoped
        // to the referenced resource, so a relative or fragment-only `$ref` inside it
        // resolves against the right base URI.
        return validate_with_rules(
            executor,
            ref_schema.contents(),
            ref_registry,
            ref_schema.resolver(),
            path,
            message,
            fail_fast,
            violations,
        );
    }

    validate_properties(
        executor,
        map,
        ref_registry,
        ref_resolver,
        path,
        message,
        fail_fast,
        violations,
    )
}

/// Descends into an object's properties and an array's items.
#[allow(clippy::too_many_arguments)]
fn validate_properties(
    executor: &dyn ValidationRuleExecutor,
    map: &serde_json::Map<String, Value>,
    ref_registry: &Registry,
    ref_resolver: &Resolver,
    path: &str,
    message: &Value,
    fail_fast: bool,
    violations: &mut Vec<ValidationRuleError>,
) -> Result<(), SerdeError> {
    if let Some(Value::Object(props)) = map.get("properties")
        && let Value::Object(message) = message
    {
        for (prop_name, prop_schema) in props {
            let Some(value) = message.get(prop_name) else {
                continue;
            };
            validate_with_rules(
                executor,
                prop_schema,
                ref_registry,
                ref_resolver,
                &append_validation_path(path, prop_name),
                value,
                fail_fast,
                violations,
            )?;
            if fail_fast && !violations.is_empty() {
                return Ok(());
            }
        }
    }
    if let Some(items) = map.get("items")
        && let Value::Array(elements) = message
    {
        for (i, element) in elements.iter().enumerate() {
            validate_with_rules(
                executor,
                items,
                ref_registry,
                ref_resolver,
                &format!("{path}[{i}]"),
                element,
                fail_fast,
                violations,
            )?;
            if fail_fast && !violations.is_empty() {
                return Ok(());
            }
        }
    }
    Ok(())
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
    if rules.is_empty() || value.is_null() {
        return false;
    }
    let serde_value = SerdeValue::Json(value.clone());
    for rule in &rules {
        evaluate_validation_rule(executor, rule, &serde_value, path, violations);
        if fail_fast && !violations.is_empty() {
            return true;
        }
    }
    false
}

/// Narrows a schema whose `type` is a union to the first member the message satisfies,
/// returning a copy with that single type. A union is not a type the walks can dispatch on -
/// without narrowing, a nullable field (`"type": ["null", "string"]`) is typed as Combined
/// and never reaches its own case. The copy leaves the parsed schema untouched, which
/// matters because it is cached and shared across serializations.
fn narrow_subtype(schema: &Value, message: &Value, ref_registry: &Registry) -> Option<Value> {
    let Value::Object(map) = schema else {
        return None;
    };
    let Some(Value::Array(types)) = map.get("type") else {
        return None;
    };
    for member in types {
        let mut candidate = map.clone();
        candidate.insert("type".to_string(), member.clone());
        let candidate = Value::Object(candidate);
        if validate_subschema(&candidate, message, ref_registry) {
            return Some(candidate);
        }
    }
    None
}

fn validate_subschema(subschema: &Value, message: &Value, ref_registry: &Registry) -> bool {
    let validator = jsonschema::options()
        .with_registry(ref_registry.clone())
        .build(subschema);
    if let Ok(validator) = validator {
        validator.validate(message).is_ok()
    } else {
        false
    }
}

fn get_type(schema: &Value) -> FieldType {
    let mut schema_type = "null";
    if let Value::Object(schema) = schema {
        // `const` takes any JSON value and `enum` is always an array, so both are matched
        // by presence rather than by holding a string.
        if schema.get("const").is_some() || schema.get("enum").is_some() {
            return FieldType::Enum;
        } else if let Some(Value::Array(_)) = schema.get("type") {
            // A type union: the walks narrow it to the type the value satisfies before
            // dispatching, so this is only reached when nothing matched.
            return FieldType::Combined;
        } else if let Some(Value::String(s)) = schema.get("type") {
            schema_type = s;
        } else if schema.get("properties").is_some() {
            schema_type = "object";
        }
    } else if let Value::String(schema) = schema {
        schema_type = schema
    };
    match schema_type {
        "object" => {
            if schema.get("properties").is_some() {
                FieldType::Record
            } else {
                FieldType::Map
            }
        }
        "array" => FieldType::Array,
        "string" => FieldType::String,
        "integer" => FieldType::Int,
        "number" => FieldType::Double,
        "boolean" => FieldType::Boolean,
        "null" => FieldType::Null,
        _ => FieldType::Null,
    }
}

fn get_inline_tags(schema: &Value) -> HashSet<String> {
    let mut tag_set = HashSet::new();
    if let Value::Object(schema) = schema
        && let Some(Value::Array(tags)) = schema.get("confluent:tags")
    {
        for tag in tags {
            if let Value::String(tag) = tag {
                tag_set.insert(tag.clone());
            }
        }
    }
    tag_set
}

#[cfg(test)]
#[cfg(feature = "rules")]
mod tests {
    use super::*;
    use crate::rest::client_config::ClientConfig;
    use crate::rest::mock_dek_registry_client::MockDekRegistryClient;
    use crate::rest::mock_schema_registry_client::MockSchemaRegistryClient;
    use crate::rest::models::{Rule, RuleSet, SchemaReference};
    use crate::rules::cel::cel_executor::CelExecutor;
    use crate::rules::cel::cel_field_executor::CelFieldExecutor;
    use crate::rules::cel::cel_validator::CelValidator;
    use crate::rules::encryption::encrypt_executor::{
        EncryptionExecutor, FakeClock, FieldEncryptionExecutor,
    };
    use crate::rules::encryption::localkms::local_driver::LocalKmsDriver;
    use crate::serdes::config::SchemaSelector;
    use crate::serdes::serde::{SerdeFormat, SerdeHeaders, header_schema_id_serializer};
    use std::collections::BTreeMap;

    #[tokio::test]
    async fn test_basic_serialization() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::default();
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": "string",
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
        }
        "#;
        let schema = Schema {
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        let obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        let ser = JsonSerializer::new(
            &client,
            Some(&schema),
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_guid_in_header() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let mut ser_conf = SerializerConfig::default();
        ser_conf.schema_id_serializer = header_schema_id_serializer;
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": "string",
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
        }
        "#;
        let schema = Schema {
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        let obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        let ser = JsonSerializer::new(
            &client,
            Some(&schema),
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: Some(SerdeHeaders::default()),
        };
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_serialize_references() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            true,
            HashMap::new(),
        );
        let ref_schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": "string",
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
        }
        "#;
        let ref_schema = Schema {
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: ref_schema_str.to_string(),
        };
        client
            .register_schema("ref", &ref_schema, false)
            .await
            .unwrap();
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "otherField": {"$ref": "ref"}
            }
        }
        "#;
        let refs = vec![SchemaReference {
            name: Some("ref".to_string()),
            subject: Some("ref".to_string()),
            version: Some(1),
        }];
        let schema = Schema {
            schema_type: Some("JSON".to_string()),
            references: Some(refs),
            metadata: None,
            rule_set: None,
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();

        let obj_str = r#"
        {
            "otherField":
            {
                "intField": 123,
                "doubleField": 45.67,
                "stringField": "hi",
                "booleanField": true,
                "bytesField": "Zm9vYmFy"
            }
        }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_cel_field() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            true,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": "string",
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let mut obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        let mut obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi-suffix",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        obj = serde_json::from_str(obj_str).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_cel_field_with_nullable() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            true,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": ["string", "null"],
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let mut obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        let mut obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi-suffix",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        obj = serde_json::from_str(obj_str).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_cel_field_with_union_of_refs() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            true,
            HashMap::new(),
        );
        let schema_str = r##"{
            "type": "object",
            "properties": {
                "messageType": {
                    "type": "string"
                },
                "version": {
                    "type": "string"
                },
                "payload": {
                    "type": "object",
                    "oneOf": [
                    {
                        "$ref": "#/$defs/authentication_request"
                    },
                    {
                        "$ref": "#/$defs/authentication_status"
                    }
                    ]
                }
            },
            "required": [
            "payload",
            "messageType",
            "version"
            ],
            "$defs": {
                "authentication_request": {
                    "properties": {
                        "messageId": {
                            "type": "string",
                            "confluent:tags": ["PII"]
                        },
                        "timestamp": {
                            "type": "integer",
                            "minimum": 0
                        },
                        "requestId": {
                            "type": "string"
                        }
                    },
                    "required": [
                    "messageId",
                    "timestamp"
                    ]
                },
                "authentication_status": {
                    "properties": {
                        "messageId": {
                            "type": "string",
                            "confluent:tags": ["PII"]
                        },
                        "authType": {
                            "type": [
                            "string",
                            "null"
                            ]
                        }
                    },
                    "required": [
                    "messageId",
                    "authType"
                    ]
                }
            }
        }
        "##;
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: None,
            params: None,
            expr: Some("name == 'messageId' ; value + '-suffix'".to_string()),
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let mut obj_str = r#"
        {
            "messageType": "authentication_request",
            "version": "1.0",
            "payload": {
                "messageId": "12345",
                "timestamp": 1757410647
            }
        }
        "#;
        let mut obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        obj_str = r#"
        {
            "messageType": "authentication_request",
            "version": "1.0",
            "payload": {
                "messageId": "12345-suffix",
                "timestamp": 1757410647
            }
        }
        "#;
        obj = serde_json::from_str(obj_str).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_cel_field_transform_all_of() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            true,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "pins": {
                    "type": "object",
                    "allOf": [
                        {
                            "properties": {
                                "pin": {
                                    "confluent:tags": ["PII"],
                                    "type": ["string", "null"]
                                }
                            }
                        },
                        {
                            "properties": {
                                "npin": {
                                    "confluent:tags": ["PII"],
                                    "type": ["string", "null"]
                                }
                            }
                        }
                    ]
                }
            }
        }
        "#;
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: Some(vec!["PII".to_string()]),
            params: None,
            expr: Some("value + '-suffix'".to_string()),
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let obj_str = r#"
        { "pins": { "pin": "P123456789", "npin": "NP00012345678" } }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let expected_str = r#"
        { "pins": { "pin": "P123456789-suffix", "npin": "NP00012345678-suffix" } }
        "#;
        let expected: Value = serde_json::from_str(expected_str).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, expected);
    }

    #[tokio::test]
    async fn test_cel_field_transform_nested_any_of() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            true,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "pins": {
                    "type": "object",
                    "anyOf": [
                        {
                            "properties": {
                                "pin": {
                                    "confluent:tags": ["PII"],
                                    "type": ["string", "null"]
                                }
                            }
                        },
                        {
                            "properties": {
                                "npin": {
                                    "confluent:tags": ["PII"],
                                    "type": ["string", "null"]
                                }
                            }
                        }
                    ]
                }
            }
        }
        "#;
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: Some(vec!["PII".to_string()]),
            params: None,
            expr: Some("value + '-suffix'".to_string()),
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let obj_str = r#"
        { "pins": { "pin": "P123456789", "npin": "NP00012345678" } }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let expected_str = r#"
        { "pins": { "pin": "P123456789-suffix", "npin": "NP00012345678-suffix" } }
        "#;
        let expected: Value = serde_json::from_str(expected_str).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, expected);
    }

    #[tokio::test]
    async fn test_cel_field_transform_sibling_any_of() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            true,
            true,
            HashMap::new(),
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "pins": {
                    "type": "object",
                    "anyOf": [
                        { "required": ["pin"] },
                        { "required": ["npin"] }
                    ],
                    "properties": {
                        "pin": {
                            "confluent:tags": ["PII"],
                            "type": ["string", "null"]
                        },
                        "npin": {
                            "confluent:tags": ["PII"],
                            "type": ["string", "null"]
                        }
                    }
                }
            }
        }
        "#;
        let rule = Rule {
            name: "test-cel".to_string(),
            doc: None,
            kind: Some(Kind::Transform),
            mode: Some(Mode::Write),
            r#type: "CEL_FIELD".to_string(),
            tags: Some(vec!["PII".to_string()]),
            params: None,
            expr: Some("value + '-suffix'".to_string()),
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let obj_str = r#"
        { "pins": { "pin": "P123456789", "npin": "NP00012345678" } }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let expected_str = r#"
        { "pins": { "pin": "P123456789-suffix", "npin": "NP00012345678-suffix" } }
        "#;
        let expected: Value = serde_json::from_str(expected_str).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, expected);
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
            true,
            rule_conf,
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": "string",
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();
        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    /// A nullable field is the ordinary shape for an optional encrypted field. Its type is
    /// a union, which is not a type the walk can dispatch on: without narrowing it to the
    /// member the value satisfies, the field is reported to the executor as Null and
    /// encryption fails with "unsupported field type".
    #[tokio::test]
    async fn test_encryption_with_nullable_field() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            true,
            rule_conf,
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": ["string", "null"],
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();
        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
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
            true,
            rule_conf,
        );
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": "string",
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
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
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let obj_str = r#"
        {
            "intField": 123,
            "doubleField": 45.67,
            "stringField": "hi",
            "booleanField": true,
            "bytesField": "Zm9vYmFy"
        }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(EncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();
        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_encryption_with_references() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let ser_conf = SerializerConfig::new(
            false,
            Some(SchemaSelector::LatestVersion),
            false,
            true,
            rule_conf,
        );
        let ref_schema_str = r#"
        {
            "type": "object",
            "properties": {
                "intField": {"type": "integer"},
                "doubleField": {"type": "number"},
                "stringField": {
                    "type": "string",
                    "confluent:tags": ["PII"]
                },
                "booleanField": {"type": "boolean"},
                "bytesField": {
                    "type": "string",
                    "contentEncoding": "base64",
                    "confluent:tags": ["PII"]
                }
            }
        }
        "#;
        let ref_schema = Schema {
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: ref_schema_str.to_string(),
        };
        client
            .register_schema("ref", &ref_schema, false)
            .await
            .unwrap();
        let schema_str = r#"
        {
            "type": "object",
            "properties": {
                "otherField": {"$ref": "ref"}
            }
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
        let refs = vec![SchemaReference {
            name: Some("ref".to_string()),
            subject: Some("ref".to_string()),
            version: Some(1),
        }];
        let schema = Schema {
            schema_type: Some("JSON".to_string()),
            references: Some(refs),
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_str.to_string(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();

        let obj_str = r#"
        {
            "otherField":
            {
                "intField": 123,
                "doubleField": 45.67,
                "stringField": "hi",
                "booleanField": true,
                "bytesField": "Zm9vYmFy"
            }
        }
        "#;
        let obj: Value = serde_json::from_str(obj_str).unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();
        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    const JSON_SCHEMA_STR: &str = r#"
    {
        "title": "DemoSchema",
        "type": "object",
        "properties": {
            "intField": {"type": "integer"},
            "stringField": {"type": "string"}
        }
    }"#;

    fn json_demo_schema() -> Schema {
        Schema {
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: JSON_SCHEMA_STR.to_string(),
        }
    }

    fn json_demo_obj() -> Value {
        serde_json::json!({"intField": 123, "stringField": "hi"})
    }

    fn json_make_association(
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
    async fn test_json_serde_with_associated_name_strategy() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = json_demo_schema();
        client
            .register_schema("my-custom-subject", &schema, false)
            .await
            .unwrap();
        client
            .create_association(&json_make_association(
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
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let obj = json_demo_obj();
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        let deser =
            JsonDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_json_serde_with_associated_name_strategy_fallback_to_topic() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = json_demo_schema();
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
        let ser =
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let obj = json_demo_obj();
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();

        let deser = JsonDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_json_serde_with_associated_name_strategy_fallback_none() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

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
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let result = ser.serialize(&ser_ctx, json_demo_obj()).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Could not determine subject")
        );
    }

    #[tokio::test]
    async fn test_json_serde_with_associated_name_strategy_multiple_associations() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = json_demo_schema();
        client
            .register_schema("subject1", &schema, false)
            .await
            .unwrap();
        client
            .register_schema("subject2", &schema, false)
            .await
            .unwrap();
        client
            .create_association(&json_make_association(
                "lkc-123:topic1",
                "subject1",
                "value",
            ))
            .await
            .unwrap();
        client
            .create_association(&json_make_association(
                "lkc-456:topic1",
                "subject2",
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
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let result = ser.serialize(&ser_ctx, json_demo_obj()).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("multiple associated subjects found")
        );
    }

    #[tokio::test]
    async fn test_json_serde_with_associated_name_strategy_with_kafka_cluster_id() {
        use crate::rest::models::{
            AssociationCreateOrUpdateInfo, AssociationCreateOrUpdateRequest,
        };

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = json_demo_schema();
        client
            .register_schema("my-custom-subject", &schema, false)
            .await
            .unwrap();

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
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let obj = json_demo_obj();
        let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        deser_conf.strategy_config = HashMap::from([(
            KAFKA_CLUSTER_ID_CONFIG.to_string(),
            "lkc-my-cluster".to_string(),
        )]);
        let deser =
            JsonDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();
        let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_json_serde_with_associated_name_strategy_caching() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);

        let schema = json_demo_schema();
        client
            .register_schema("my-cached-subject", &schema, false)
            .await
            .unwrap();
        client
            .create_association(&json_make_association(
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
            JsonSerializer::new(&client, None, Some(rule_registry.clone()), ser_conf).unwrap();
        let ser_ctx = SerializationContext {
            topic: "topic1".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let obj = json_demo_obj();

        let mut deser_conf = DeserializerConfig::default();
        deser_conf.subject_name_strategy_type = SubjectNameStrategyType::Associated;
        let deser =
            JsonDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        for _ in 0..5 {
            let bytes = ser.serialize(&ser_ctx, obj.clone()).await.unwrap();
            let obj2 = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
            assert_eq!(obj2, obj);
        }
    }

    const VALIDATION_SCHEMA: &str = r##"
    {
        "type": "object",
        "confluent:rules": [
            {"name": "quantity_matches_items",
             "expr": "this.quantity == size(this.items)"}
        ],
        "properties": {
            "id": {
                "type": "string",
                "confluent:rules": [
                    {"name": "id_prefix", "expr": "this.startsWith('ord-')"}
                ]
            },
            "quantity": {
                "type": "integer",
                "confluent:rules": [
                    {"name": "positive_quantity", "expr": "this > 0"}
                ]
            },
            "items": {"type": "array", "items": {"type": "string"}},
            "address": {"$ref": "#/definitions/Address"}
        },
        "definitions": {
            "Address": {
                "type": "object",
                "properties": {
                    "zip": {
                        "type": "string",
                        "confluent:rules": [
                            {"name": "zip_digits",
                             "expr": "this.matches('^[0-9]{5}$') ? '' : 'zip must be 5 digits'"}
                        ]
                    }
                }
            }
        }
    }
    "##;

    fn json_order(id: &str, quantity: i64, items: &[&str], zip: &str) -> Value {
        serde_json::json!({
            "id": id,
            "quantity": quantity,
            "items": items,
            "address": {"zip": zip},
        })
    }

    fn validation_schema() -> Schema {
        Schema {
            schema_type: Some("JSON".to_string()),
            references: None,
            metadata: None,
            rule_set: None,
            schema: VALIDATION_SCHEMA.to_string(),
        }
    }

    fn validate_json(message: &Value, fail_fast: bool) -> Vec<ValidationRuleError> {
        let parsed: Value = serde_json::from_str(VALIDATION_SCHEMA).unwrap();
        let ref_registry = Registry::options()
            .build(Vec::<(String, Resource)>::new().into_iter())
            .unwrap();
        let base_uri = ResourceRef::from_contents(&parsed)
            .id()
            .unwrap_or("")
            .to_string();
        let ref_registry = ref_registry
            .try_with_resource(base_uri.clone(), Resource::from_contents(parsed.clone()))
            .unwrap();
        let ref_resolver = ref_registry.try_resolver(&base_uri).unwrap();
        let validator = CelValidator::new();
        validate_message(
            &validator,
            &parsed,
            &ref_registry,
            &ref_resolver,
            message,
            fail_fast,
        )
        .unwrap()
    }

    /// A `$ref` into another resource has to be followed with the resolver the lookup
    /// returned: that resolver is scoped to the referenced resource, so a fragment-only
    /// `$ref` inside it (`#/$defs/...`) resolves against that resource rather than the root
    /// document, which does not have those definitions at all.
    #[test]
    fn test_validation_follows_refs_scoped_to_the_referenced_resource() {
        let root: Value = serde_json::from_str(
            r#"{
                "type": "object",
                "properties": { "p": { "$ref": "https://example.com/inner.json" } }
            }"#,
        )
        .unwrap();
        let inner: Value = serde_json::from_str(
            r##"{
                "$id": "https://example.com/inner.json",
                "type": "object",
                "properties": { "x": { "$ref": "#/$defs/tagged" } },
                "$defs": {
                    "tagged": {
                        "type": "string",
                        "confluent:rules": [ { "name": "r", "expr": "false" } ]
                    }
                }
            }"##,
        )
        .unwrap();
        // Registered the way the serde registers a schema and its references.
        let ref_registry = Registry::options()
            .build(Vec::<(String, Resource)>::new().into_iter())
            .unwrap();
        let ref_registry = ref_registry
            .try_with_resources(
                vec![
                    (
                        "https://example.com/inner.json".to_string(),
                        Resource::from_contents(inner),
                    ),
                    ("".to_string(), Resource::from_contents(root.clone())),
                ]
                .into_iter(),
                Draft::default(),
            )
            .unwrap();
        let ref_resolver = ref_registry.try_resolver("").unwrap();
        let message: Value = serde_json::from_str(r#"{ "p": { "x": "hi" } }"#).unwrap();

        let validator = CelValidator::new();
        let violations = validate_message(
            &validator,
            &root,
            &ref_registry,
            &ref_resolver,
            &message,
            false,
        )
        .unwrap();

        assert_eq!(violations.len(), 1, "{violations:?}");
        assert_eq!(violations[0].rule.name, "r");
        assert_eq!(violations[0].field_path, "$.p.x");
    }

    fn find_violation<'a>(
        violations: &'a [ValidationRuleError],
        name: &str,
    ) -> &'a ValidationRuleError {
        violations
            .iter()
            .find(|v| v.rule.name == name)
            .unwrap_or_else(|| panic!("no violation named {name} in {violations:?}"))
    }

    fn validating_registry() -> RuleRegistry {
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelExecutor::new());
        rule_registry.register_validation_executor(CelValidator::new());
        rule_registry
    }

    #[test]
    fn test_validation_valid_object_has_no_violations() {
        let message = json_order("ord-1", 2, &["a", "b"], "12345");
        assert_eq!(validate_json(&message, false), vec![]);
    }

    #[test]
    fn test_validation_collects_every_violation_with_dollar_rooted_paths() {
        let message = json_order("x", 0, &["a"], "abc");
        let violations = validate_json(&message, false);

        // Properties are walked in the schema's key order, so assert on the set of
        // violations rather than their sequence.
        assert_eq!(violations.len(), 4, "{violations:?}");
        assert_eq!(
            find_violation(&violations, "quantity_matches_items").field_path,
            "$"
        );
        assert_eq!(find_violation(&violations, "id_prefix").field_path, "$.id");
        assert_eq!(
            find_violation(&violations, "positive_quantity").field_path,
            "$.quantity"
        );
        // Resolved through "$ref".
        assert_eq!(
            find_violation(&violations, "zip_digits").field_path,
            "$.address.zip"
        );
    }

    #[test]
    fn test_validation_reports_unresolvable_references() {
        // A reference that cannot be resolved must not be treated as "no rules here":
        // that would let the message through without the checks it was meant to supply.
        let schema: Value = serde_json::from_str(
            r##"{"type": "object", "properties": {"a": {"$ref": "#/definitions/Missing"}}}"##,
        )
        .unwrap();
        let ref_registry = Registry::options()
            .build(Vec::<(String, Resource)>::new().into_iter())
            .unwrap();
        let base_uri = ResourceRef::from_contents(&schema)
            .id()
            .unwrap_or("")
            .to_string();
        let ref_registry = ref_registry
            .try_with_resource(base_uri.clone(), Resource::from_contents(schema.clone()))
            .unwrap();
        let ref_resolver = ref_registry.try_resolver(&base_uri).unwrap();
        let validator = CelValidator::new();

        let message = serde_json::json!({"a": {"x": 1}});
        let result = validate_message(
            &validator,
            &schema,
            &ref_registry,
            &ref_resolver,
            &message,
            false,
        );
        assert!(result.is_err(), "expected the lookup failure to surface");
    }

    #[test]
    fn test_validation_fail_fast_stops_at_first_violation() {
        let message = json_order("x", 0, &["a"], "abc");
        assert_eq!(validate_json(&message, true).len(), 1);
    }

    #[test]
    fn test_validation_skips_absent_and_null_properties() {
        let mut message = serde_json::json!({"quantity": 1, "items": ["a"]});
        assert_eq!(validate_json(&message, false), vec![]);

        message["id"] = Value::Null;
        assert_eq!(validate_json(&message, false), vec![]);
    }

    #[tokio::test]
    async fn test_validation_serializer_rejects_invalid_message() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let mut ser_conf = SerializerConfig::default();
        ser_conf.validation_rules_execution = ValidationRulesExecution::AfterDomainRules;
        let schema = validation_schema();
        let ser = JsonSerializer::new(
            &client,
            Some(&schema),
            Some(validating_registry()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Json,
            headers: None,
        };

        let valid = json_order("ord-1", 2, &["a", "b"], "12345");
        assert!(ser.serialize(&ser_ctx, valid).await.is_ok());

        let invalid = json_order("bad", 2, &["a", "b"], "12345");
        let err = ser.serialize(&ser_ctx, invalid).await.unwrap_err();
        assert!(
            matches!(err, SerdeError::ValidationRules(_)),
            "unexpected error: {err}"
        );
    }
}
