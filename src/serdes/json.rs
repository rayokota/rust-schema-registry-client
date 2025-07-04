use crate::rest::models::{Kind, Mode};
use crate::rest::models::{Phase, Schema};
use crate::rest::schema_registry_client::Client;
use crate::serdes::config::{DeserializerConfig, SerializerConfig};
use crate::serdes::rule_registry::RuleRegistry;
use crate::serdes::serde::SerdeError::Serialization;
use crate::serdes::serde::{
    BaseDeserializer, BaseSerializer, FieldTransformer, FieldType, RuleContext, SchemaId, Serde,
    SerdeError, SerdeFormat, SerdeSchema, SerdeType, SerdeValue, SerializationContext,
    get_executor, get_executors,
};
use async_recursion::async_recursion;
use base64::Engine;
use dashmap::DashMap;
use futures::future::FutureExt;
use jsonschema::{ValidationError, Validator, validator_for};
use referencing::{Draft, Registry, Resolver, Resource};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub(crate) struct JsonSerde {
    parsed_schemas: DashMap<Schema, (Value, Registry)>,
    validators: DashMap<Schema, Arc<Validator>>,
}

#[derive(Clone, Debug)]
pub struct JsonSerializer<'a, T: Client> {
    schema: Option<&'a Schema>,
    base: BaseSerializer<'a, T>,
    serde: JsonSerde,
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
            base: BaseSerializer::new(Serde::new(client, rule_registry), serializer_config),
            serde: JsonSerde {
                parsed_schemas: DashMap::new(),
                validators: DashMap::new(),
            },
        })
    }

    pub async fn serialize(
        &self,
        ctx: &SerializationContext,
        value: Value,
    ) -> Result<Vec<u8>, SerdeError> {
        let mut value = value;
        let strategy = self.base.config.subject_name_strategy;
        let subject = strategy(&ctx.topic, &ctx.serde_type, self.schema);
        let subject = subject.ok_or(Serialization(
            "subject name strategy returned None".to_string(),
        ))?;
        let latest_schema = self
            .base
            .serde
            .get_reader_schema(&subject, None, &self.base.config.use_schema)
            .await?;

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
            let field_transformer: FieldTransformer =
                Box::new(|ctx, field_executor_type, value| {
                    transform_fields(ctx, field_executor_type, value).boxed()
                });
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
        } else {
            schema = self
                .schema
                .ok_or(Serialization("schema needs to be set".to_string()))?
                .clone();
            (parsed_schema, ref_registry) = self.get_parsed_schema(&schema).await?;
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
            if let Some(ref rule_set) = schema.rule_set {
                if rule_set.encoding_rules.is_some() {
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
        }

        let id_ser = self.base.config.schema_id_serializer;
        id_ser(&encoded_bytes, ctx, &schema_id)
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

    fn close(&mut self) {}
}

impl<'a> From<ValidationError<'a>> for SerdeError {
    fn from(value: ValidationError<'a>) -> Self {
        Serialization(value.to_string())
    }
}

async fn transform_fields(
    ctx: &mut RuleContext,
    field_executor_type: &str,
    value: &SerdeValue,
) -> Result<SerdeValue, SerdeError> {
    if let Some(SerdeSchema::Json((s, ref_registry))) = ctx.parsed_target.clone() {
        if let SerdeValue::Json(v) = value {
            let root_resource = Resource::from_contents(s.clone())?;
            let base_uri = root_resource.id().unwrap_or("").to_string();
            let ref_registry = ref_registry
                .clone()
                .try_with_resource(base_uri.clone(), root_resource)?;
            let ref_resolver = ref_registry.try_resolver(&base_uri)?;
            let value = transform(
                ctx,
                &s,
                &ref_registry,
                &ref_resolver,
                "$",
                v,
                field_executor_type,
            )
            .await?;
            return Ok(SerdeValue::Json(value));
        }
    }
    Ok(value.clone())
}

#[derive(Clone, Debug)]
pub struct JsonDeserializer<'a, T: Client> {
    base: BaseDeserializer<'a, T>,
    serde: JsonSerde,
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
            base: BaseDeserializer::new(Serde::new(client, rule_registry), deserializer_config),
            serde: JsonSerde {
                parsed_schemas: DashMap::new(),
                validators: DashMap::new(),
            },
        })
    }

    pub async fn deserialize(
        &self,
        ctx: &SerializationContext,
        data: &[u8],
    ) -> Result<Value, SerdeError> {
        let strategy = self.base.config.subject_name_strategy;
        let mut subject = strategy(&ctx.topic, &ctx.serde_type, None);
        let mut latest_schema = None;
        let has_subject = subject.is_some();
        if has_subject {
            latest_schema = self
                .base
                .serde
                .get_reader_schema(
                    subject.as_ref().unwrap(),
                    None,
                    &self.base.config.use_schema,
                )
                .await?;
        }

        let mut schema_id = SchemaId::new(SerdeFormat::Json, None, None, None)?;
        let id_deser = self.base.config.schema_id_deserializer;
        let bytes_read = id_deser(data, ctx, &mut schema_id)?;
        let mut data = &data[bytes_read..];

        let writer_schema_raw = self
            .base
            .get_writer_schema(&schema_id, subject.as_deref(), None)
            .await?;
        let (writer_schema, writer_ref_registry) =
            self.get_parsed_schema(&writer_schema_raw).await?;

        if !has_subject {
            subject = strategy(&ctx.topic, &ctx.serde_type, Some(&writer_schema_raw));
            if let Some(subject) = subject.as_ref() {
                latest_schema = self
                    .base
                    .serde
                    .get_reader_schema(subject, None, &self.base.config.use_schema)
                    .await?;
            }
        }
        let subject = subject.unwrap();
        let serde_value;
        if let Some(ref rule_set) = writer_schema_raw.rule_set {
            if rule_set.encoding_rules.is_some() {
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

        let field_transformer: FieldTransformer = Box::new(|ctx, field_executor_type, value| {
            transform_fields(ctx, field_executor_type, value).boxed()
        });
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
            let resource = Resource::from_contents(ref_schema_val.clone())?;
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
    field_executor_type: &str,
) -> Result<Value, SerdeError> {
    if let Value::Object(map) = schema {
        if let Some(Value::Array(subschemas)) = map.get("allOf") {
            if let Some(subschema) = validate_subschemas(subschemas, message, ref_registry) {
                return transform(
                    ctx,
                    subschema,
                    ref_registry,
                    ref_resolver,
                    path,
                    message,
                    field_executor_type,
                )
                .await;
            }
        }
        if let Some(Value::Array(subschemas)) = map.get("anyOf") {
            if let Some(subschema) = validate_subschemas(subschemas, message, ref_registry) {
                return transform(
                    ctx,
                    subschema,
                    ref_registry,
                    ref_resolver,
                    path,
                    message,
                    field_executor_type,
                )
                .await;
            }
        }
        if let Some(Value::Array(subschemas)) = map.get("oneOf") {
            if let Some(subschema) = validate_subschemas(subschemas, message, ref_registry) {
                return transform(
                    ctx,
                    subschema,
                    ref_registry,
                    ref_resolver,
                    path,
                    message,
                    field_executor_type,
                )
                .await;
            }
        }
        if let Some(items) = map.get("items") {
            if let Value::Array(_) = message {
                return transform(
                    ctx,
                    items,
                    ref_registry,
                    ref_resolver,
                    path,
                    message,
                    field_executor_type,
                )
                .await;
            }
        }
        if let Some(reference) = map.get("$ref") {
            let ref_schema = ref_resolver.lookup(reference.as_str().unwrap())?;
            let ref_schema = ref_schema.contents();
            return transform(
                ctx,
                ref_schema,
                ref_registry,
                ref_resolver,
                path,
                message,
                field_executor_type,
            )
            .await;
        }
        let field_type = get_type(schema);
        if field_type == FieldType::Record {
            if let Some(Value::Object(props)) = map.get("properties") {
                if let Value::Object(message) = message {
                    let mut new_message = serde_json::Map::new();
                    for (prop_name, prop_schema) in props {
                        let new_value = transform_field_with_ctx(
                            ctx,
                            path,
                            prop_name,
                            message,
                            prop_schema,
                            ref_registry,
                            ref_resolver,
                            field_executor_type,
                        )
                        .await?;
                        new_message.insert(prop_name.clone(), new_value);
                    }
                    return Ok(Value::Object(new_message));
                }
            }
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
            let executor = get_executor(ctx.rule_registry.as_ref(), field_executor_type);
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
    field_executor_type: &str,
) -> Result<Value, SerdeError> {
    let full_name = path.to_string() + "." + prop_name;
    let message_value = SerdeValue::Json(Value::Object(message.clone()));
    ctx.enter_field(
        message_value,
        full_name.clone(),
        prop_name.to_string(),
        get_type(prop_schema),
        get_inline_tags(prop_schema),
    );
    let value = message.get(prop_name).unwrap_or(&Value::Null);
    let new_value = transform(
        ctx,
        prop_schema,
        ref_registry,
        ref_resolver,
        &full_name,
        value,
        field_executor_type,
    )
    .await?;
    if let Some(Kind::Condition) = ctx.rule.kind {
        if let Value::Bool(b) = new_value {
            if !b {
                return Err(SerdeError::RuleCondition(Box::new(ctx.rule.clone())));
            }
        }
    }
    ctx.exit_field();
    Ok(new_value)
}

fn validate_subschemas<'a>(
    subschemas: &'a [Value],
    message: &Value,
    ref_registry: &Registry,
) -> Option<&'a Value> {
    subschemas.iter().find(|&subschema| {
        let validator = jsonschema::options()
            .with_registry(ref_registry.clone())
            .build(subschema);
        if let Ok(validator) = validator {
            validator.validate(message).is_ok()
        } else {
            false
        }
    })
}

fn get_type(schema: &Value) -> FieldType {
    if let Value::Array(_) = schema {
        return FieldType::Combined;
    }
    let mut schema_type = "null";
    if let Value::Object(schema) = schema {
        if let Some(Value::String(_)) = schema.get("const") {
            return FieldType::Enum;
        } else if let Some(Value::String(_)) = schema.get("enum") {
            return FieldType::Enum;
        } else if let Some(Value::String(s)) = schema.get("type") {
            schema_type = s;
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
    if let Value::Object(schema) = schema {
        if let Some(Value::Array(tags)) = schema.get("confluent:tags") {
            for tag in tags {
                if let Value::String(tag) = tag {
                    tag_set.insert(tag.clone());
                }
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
    use crate::rules::cel::cel_field_executor::CelFieldExecutor;
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
            false,
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
            false,
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
            false,
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
            false,
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
}
