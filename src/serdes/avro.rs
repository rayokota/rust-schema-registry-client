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
}

#[derive(Clone, Debug)]
pub struct AvroSerializer<'a, T: Client> {
    schema: Option<&'a Schema>,
    base: BaseSerializer<'a, T>,
    serde: AvroSerde,
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
            base: BaseSerializer::new(Serde::new(client, rule_registry), serializer_config),
            serde: AvroSerde {
                parsed_schemas: DashMap::new(),
            },
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
                    Some(&schema),
                    Some(&SerdeSchema::Avro(schema_tuple.clone())),
                    &SerdeValue::Avro(value),
                    Some(Arc::new(field_transformer)),
                )
                .await?;
            value = match serde_value {
                SerdeValue::Avro(value) => value,
                _ => return Err(Serialization("unexpected serde value".to_string())),
            }
        } else {
            let schema = self
                .schema
                .ok_or(Serialization("schema needs to be set".to_string()))?;
            schema_tuple = self.get_parsed_schema(schema).await?;
        }

        let mut encoded_bytes = apache_avro::to_avro_datum_schemata(
            &schema_tuple.0,
            schema_tuple.1.iter().collect(),
            value,
        )?;
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
                            &SerdeValue::new_bytes(SerdeFormat::Avro, &encoded_bytes),
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
            &schemas.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )?;
        self.serde
            .parsed_schemas
            .insert(schema.clone(), parsed_schema.clone());
        Ok(parsed_schema)
    }

    fn close(&mut self) {}
}

async fn transform_fields(
    ctx: &mut RuleContext,
    field_executor_type: &str,
    value: &SerdeValue,
) -> Result<SerdeValue, SerdeError> {
    if let Some(SerdeSchema::Avro((s, named))) = ctx.parsed_target.clone() {
        if let SerdeValue::Avro(v) = value {
            let value = transform(ctx, &s, &named, v, field_executor_type).await?;
            return Ok(SerdeValue::Avro(value));
        }
    }
    Ok(value.clone())
}

#[derive(Clone, Debug, PartialEq)]
pub struct NamedValue {
    pub name: Option<Name>,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub struct AvroDeserializer<'a, T: Client> {
    base: BaseDeserializer<'a, T>,
    serde: AvroSerde,
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
            base: BaseDeserializer::new(Serde::new(client, rule_registry), deserializer_config),
            serde: AvroSerde {
                parsed_schemas: DashMap::new(),
            },
        })
    }

    pub async fn deserialize(
        &self,
        ctx: &SerializationContext,
        data: &[u8],
    ) -> Result<NamedValue, SerdeError> {
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

        let mut schema_id = SchemaId::new(SerdeFormat::Avro, None, None, None)?;
        let id_deser = self.base.config.schema_id_deserializer;
        let bytes_read = id_deser(data, ctx, &mut schema_id)?;
        let mut data = &data[bytes_read..];

        let writer_schema_raw = self
            .base
            .get_writer_schema(&schema_id, subject.as_deref(), None)
            .await?;
        let (writer_schema, writer_named) = self.get_parsed_schema(&writer_schema_raw).await?;

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
                        &SerdeValue::new_bytes(SerdeFormat::Avro, data),
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
            value = apache_avro::from_avro_datum_schemata(
                &writer_schema,
                writer_named.iter().collect(),
                &mut reader,
                None,
            )?;
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
            value = apache_avro::from_avro_datum_reader_schemata(
                &writer_schema,
                writer_named.iter().collect(),
                &mut reader,
                Some(&reader_schema),
                reader_named.iter().collect(),
            )?;
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
            &schemas.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )?;
        self.serde
            .parsed_schemas
            .insert(schema.clone(), parsed_schema.clone());
        Ok(parsed_schema)
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
    field_executor_type: &str,
) -> Result<Value, SerdeError> {
    match schema {
        apache_avro::Schema::Union(union) => {
            let subschema = resolve_union(union, message);
            if subschema.is_none() {
                return Ok(message.clone());
            }
            let result = transform(
                ctx,
                subschema.unwrap().1,
                named_schemas,
                message,
                field_executor_type,
            )
            .await?;
            return Ok(result);
        }
        apache_avro::Schema::Array(array) => {
            if let Value::Array(items) = message {
                let mut result = Vec::with_capacity(items.len());
                for item in items {
                    let item =
                        transform(ctx, &array.items, named_schemas, item, field_executor_type)
                            .await?;
                    result.push(item);
                }
                return Ok(Value::Array(result));
            }
        }
        apache_avro::Schema::Map(map) => {
            if let Value::Map(values) = message {
                let mut result: HashMap<String, Value> = HashMap::with_capacity(values.len());
                for (key, value) in values {
                    let value =
                        transform(ctx, &map.types, named_schemas, value, field_executor_type)
                            .await?;
                    result.insert(key.clone(), value);
                }
                return Ok(Value::Map(result));
            }
        }
        apache_avro::Schema::Record(record) => {
            if let Value::Record(fields) = message {
                let mut result = Vec::with_capacity(fields.len());
                for field in fields {
                    let field = transform_field_with_ctx(
                        ctx,
                        record,
                        named_schemas,
                        field,
                        fields,
                        field_executor_type,
                    )
                    .await?;
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
            let executor = get_executor(ctx.rule_registry.as_ref(), field_executor_type);
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
    field_executor_type: &str,
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
    );
    let new_value = transform(
        ctx,
        &field_schema.schema,
        named_schemas,
        &field.1,
        field_executor_type,
    )
    .await?;
    if let Some(Kind::Condition) = ctx.rule.kind {
        if let Value::Boolean(b) = new_value {
            if !b {
                return Err(SerdeError::RuleCondition(Box::new(ctx.rule.clone())));
            }
        }
    }
    ctx.exit_field();
    Ok((field.0.clone(), new_value))
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
    use crate::rules::cel::cel_field_executor::CelFieldExecutor;
    use crate::rules::encryption::encrypt_executor::{
        EncryptionExecutor, FakeClock, FieldEncryptionExecutor,
    };
    use crate::rules::encryption::localkms::local_driver::LocalKmsDriver;
    use crate::rules::jsonata::jsonata_executor::JsonataExecutor;
    use crate::serdes::config::SchemaSelector;
    use crate::serdes::serde::{SerdeFormat, SerdeHeaders, header_schema_id_serializer};
    use apache_avro::types::Value::{Record, Union};
    use std::collections::BTreeMap;

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
        dek_client.register_kek(kek_req).await.unwrap();

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
        dek_client.register_kek(kek_req).await.unwrap();

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
        dek_client.register_kek(kek_req).await.unwrap();

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
}
