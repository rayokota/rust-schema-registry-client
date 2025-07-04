use crate::DESCRIPTOR_POOL;
use crate::rest::models::{Kind, Mode, Phase};
use crate::rest::models::{Schema, SchemaReference};
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
use base64::prelude::BASE64_STANDARD;
use dashmap::DashMap;
use futures::future::FutureExt;
use prost::{Message, bytes};
use prost_reflect::prost_types::{DescriptorProto, FileDescriptorProto};
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, FileDescriptor, MessageDescriptor,
    ReflectMessage, SerializeOptions, Value,
};
use prost_types::FileDescriptorSet;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{BufReader, Cursor};
use std::sync::Arc;

pub mod confluent {
    include!("../codegen/confluent.rs");
    pub mod r#type {
        include!("../codegen/confluent.r#type.rs");
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProtobufSerde {
    parsed_schemas: DashMap<Schema, (FileDescriptor, DescriptorPool)>,
}

#[derive(Clone, Debug)]
pub struct ProtobufSerializer<'a, T: Client> {
    reference_subject_name_strategy: ReferenceSubjectNameStrategy,
    base: BaseSerializer<'a, T>,
    serde: ProtobufSerde,
}

pub type ReferenceSubjectNameStrategy = fn(ref_name: &str, serde_type: &SerdeType) -> String;

pub fn default_reference_subject_name_strategy(ref_name: &str, serde_type: &SerdeType) -> String {
    ref_name.to_string()
}

impl<'a, T: Client + Sync> ProtobufSerializer<'a, T> {
    pub fn new(
        client: &'a T,
        rule_registry: Option<RuleRegistry>,
        serializer_config: SerializerConfig,
    ) -> Result<ProtobufSerializer<'a, T>, SerdeError> {
        Self::with_reference_subject_name_strategy(
            client,
            default_reference_subject_name_strategy,
            rule_registry,
            serializer_config,
        )
    }

    pub fn with_reference_subject_name_strategy(
        client: &'a T,
        reference_subject_name_strategy: ReferenceSubjectNameStrategy,
        rule_registry: Option<RuleRegistry>,
        serializer_config: SerializerConfig,
    ) -> Result<ProtobufSerializer<'a, T>, SerdeError> {
        for executor in get_executors(rule_registry.as_ref()) {
            executor.configure(client.config(), &serializer_config.rule_config)?;
        }
        Ok(ProtobufSerializer {
            reference_subject_name_strategy,
            base: BaseSerializer::new(Serde::new(client, rule_registry), serializer_config),
            serde: ProtobufSerde {
                parsed_schemas: DashMap::new(),
            },
        })
    }

    pub async fn serialize_with_file_desc_set<M: Message>(
        &self,
        ctx: &SerializationContext,
        value: &M,
        message_type_name: &str,
        fds: FileDescriptorSet,
    ) -> Result<Vec<u8>, SerdeError> {
        let pool = DescriptorPool::from_file_descriptor_set(fds)?;
        let md = pool
            .get_message_by_name(message_type_name)
            .ok_or(Serialization(format!(
                "message descriptor {message_type_name} not found"
            )))?;
        self.serialize_with_message_desc(ctx, value, &md).await
    }

    pub async fn serialize<M: ReflectMessage>(
        &self,
        ctx: &SerializationContext,
        value: &M,
    ) -> Result<Vec<u8>, SerdeError> {
        self.serialize_with_message_desc(ctx, value, &value.descriptor())
            .await
    }

    pub async fn serialize_with_message_desc<M: Message>(
        &self,
        ctx: &SerializationContext,
        value: &M,
        md: &MessageDescriptor,
    ) -> Result<Vec<u8>, SerdeError> {
        let strategy = self.base.config.subject_name_strategy;
        // TODO pass schema (instead of None) to strategy later?
        let subject = strategy(&ctx.topic, &ctx.serde_type, None);
        let subject = subject.ok_or(Serialization(
            "subject name strategy returned None".to_string(),
        ))?;
        let latest_schema = self
            .base
            .serde
            .get_reader_schema(&subject, Some("serialized"), &self.base.config.use_schema)
            .await?;

        let mut schema_id;
        if let Some(ref schema) = latest_schema {
            schema_id = SchemaId::new(SerdeFormat::Protobuf, schema.id, schema.guid.clone(), None)?;
        } else {
            let references = self.resolve_dependencies(ctx, &md.parent_file()).await?;
            let schema = Schema {
                schema_type: Some("PROTOBUF".to_string()),
                references: Some(references),
                metadata: None,
                rule_set: None,
                schema: schema_to_str(&md.parent_file())?,
            };
            if self.base.config.auto_register_schemas {
                let rs = self
                    .base
                    .serde
                    .client
                    .register_schema(&subject, &schema, self.base.config.normalize_schemas)
                    .await?;
                schema_id = SchemaId::new(SerdeFormat::Protobuf, rs.id, rs.guid.clone(), None)?;
            } else {
                let rs = self
                    .base
                    .serde
                    .client
                    .get_by_schema(&subject, &schema, self.base.config.normalize_schemas, false)
                    .await?;
                schema_id = SchemaId::new(SerdeFormat::Protobuf, rs.id, rs.guid.clone(), None)?;
            }
        }

        let fd;
        let pool;
        let mut encoded_bytes = Vec::new();
        if let Some(ref latest_schema) = latest_schema {
            let schema = latest_schema.to_schema();
            (fd, pool) = self.get_parsed_schema(&schema).await?;
            let field_transformer: FieldTransformer =
                Box::new(|ctx, field_executor_type, value| {
                    transform_fields(ctx, field_executor_type, value).boxed()
                });
            let mut msg = DynamicMessage::new(md.clone());
            msg.transcode_from(value)?;
            let serde_value = self
                .base
                .serde
                .execute_rules(
                    ctx,
                    &subject,
                    Mode::Write,
                    None,
                    Some(&schema),
                    Some(&SerdeSchema::Protobuf(fd.clone())),
                    &SerdeValue::Protobuf(Value::Message(msg)),
                    Some(Arc::new(field_transformer)),
                )
                .await?;
            msg = match serde_value {
                SerdeValue::Protobuf(Value::Message(msg)) => msg,
                _ => return Err(Serialization("unexpected serde value".to_string())),
            };
            msg.encode(&mut encoded_bytes)?;
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
                            &SerdeValue::new_bytes(SerdeFormat::Protobuf, &encoded_bytes),
                            None,
                        )
                        .await?
                        .as_bytes();
                }
            }
        } else {
            value.encode(&mut encoded_bytes)?;
        }

        schema_id.message_indexes = Some(self.to_index_array(md)?);
        let id_ser = self.base.config.schema_id_serializer;
        id_ser(&encoded_bytes, ctx, &schema_id)
    }

    #[async_recursion]
    async fn resolve_dependencies(
        &self,
        ctx: &SerializationContext,
        file_desc: &FileDescriptor,
    ) -> Result<Vec<SchemaReference>, SerdeError> {
        let mut references = Vec::new();
        for dep in file_desc.dependencies() {
            if is_builtin(dep.name()) {
                continue;
            }
            let dep_refs = self.resolve_dependencies(ctx, &dep).await?;
            let strategy = self.reference_subject_name_strategy;
            let subject = strategy(dep.name(), &ctx.serde_type);
            let schema = Schema {
                schema_type: Some("PROTOBUF".to_string()),
                references: Some(dep_refs),
                metadata: None,
                rule_set: None,
                schema: schema_to_str(&dep)?,
            };
            if self.base.config.auto_register_schemas {
                self.base
                    .serde
                    .client
                    .register_schema(&subject, &schema, self.base.config.normalize_schemas)
                    .await?;
            }

            let reference = self
                .base
                .serde
                .client
                .get_by_schema(&subject, &schema, self.base.config.normalize_schemas, false)
                .await?;
            references.push(SchemaReference {
                name: Some(dep.name().to_string()),
                subject: Some(subject),
                version: reference.version,
            });
        }
        Ok(references)
    }

    fn to_index_array(&self, desc: &MessageDescriptor) -> Result<Vec<i32>, SerdeError> {
        let mut msg_idx = VecDeque::new();

        // Walk the nested MessageDescriptor tree up to the root.
        let mut previous = desc.clone();
        let mut current = previous;
        let mut found = false;
        while current.parent_message().is_some() {
            previous = current;
            current = previous
                .parent_message()
                .ok_or(Serialization("parent message not found".to_string()))?;
            for (idx, node) in current.child_messages().enumerate() {
                if node == previous {
                    msg_idx.push_front(idx as i32);
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(Serialization(
                    "nested MessageDescriptor not found".to_string(),
                ));
            }
        }

        // Add the index of the root MessageDescriptor in the FileDescriptor.
        found = false;
        for (idx, node) in current.parent_file().messages().enumerate() {
            if node == current {
                msg_idx.push_front(idx as i32);
                found = true;
                break;
            }
        }
        if !found {
            return Err(Serialization(
                "MessageDescriptor not found in file".to_string(),
            ));
        }
        Ok(msg_idx.into_iter().collect())
    }

    async fn get_parsed_schema(
        &self,
        schema: &Schema,
    ) -> Result<(FileDescriptor, DescriptorPool), SerdeError> {
        let result = self.serde.parsed_schemas.get(schema);
        if let Some((parsed_schema, pool)) = result.as_deref() {
            return Ok((parsed_schema.clone(), pool.clone()));
        }
        // Get a copy of the global descriptor pool with the Google well-known types.
        let mut pool = DESCRIPTOR_POOL.clone();
        resolve_named_schema(
            schema,
            self.base.serde.client,
            &mut pool,
            &mut HashSet::new(),
        )
        .await?;
        let fd_proto = str_to_proto(&mut pool, "default", &schema.schema)?;
        self.serde
            .parsed_schemas
            .insert(schema.clone(), (fd_proto.clone(), pool.clone()));
        Ok((fd_proto, pool))
    }

    fn close(&mut self) {}
}

fn schema_to_str(fd: &FileDescriptor) -> Result<String, SerdeError> {
    let mut buf = Vec::new();
    fd.encode(&mut buf)?;
    Ok(BASE64_STANDARD.encode(&buf))
}

fn str_to_proto(
    pool: &mut DescriptorPool,
    name: &str,
    s: &str,
) -> Result<FileDescriptor, SerdeError> {
    let result = BASE64_STANDARD.decode(s).map_err(|e| {
        Serialization(format!(
            "failed to decode base64 schema string for {name}: {e}"
        ))
    });
    decode_file_descriptor_proto_with_name(pool, name, result.unwrap())?;
    pool.get_file_by_name(name)
        .ok_or(Serialization("file descriptor not found".to_string()))
}

// See https://github.com/andrewhickman/prost-reflect/issues/152 and
// https://github.com/protocolbuffers/protobuf/issues/9257
fn decode_file_descriptor_proto_with_name(
    pool: &mut DescriptorPool,
    name: &str,
    mut bytes: Vec<u8>,
) -> Result<(), SerdeError> {
    FileDescriptorProto {
        name: Some(name.to_owned()),
        ..Default::default()
    }
    .encode(&mut bytes)?;
    pool.decode_file_descriptor_proto(bytes.as_slice())?;
    Ok(())
}

async fn transform_fields(
    ctx: &mut RuleContext,
    field_executor_type: &str,
    value: &SerdeValue,
) -> Result<SerdeValue, SerdeError> {
    if let Some(SerdeSchema::Protobuf(s)) = ctx.parsed_target.clone() {
        if let SerdeValue::Protobuf(v) = value {
            if let Value::Message(message) = v {
                let pool = s.parent_pool();
                let desc = pool
                    .get_message_by_name(message.descriptor().full_name())
                    .ok_or(Serialization(format!(
                        "message descriptor {} not found",
                        message.descriptor().full_name()
                    )))?;
                let value = transform(ctx, &desc, v, field_executor_type).await?;
                return Ok(SerdeValue::Protobuf(value));
            }
        }
    }
    Ok(value.clone())
}

#[derive(Clone, Debug)]
pub struct ProtobufDeserializer<'a, T: Client> {
    base: BaseDeserializer<'a, T>,
    serde: ProtobufSerde,
}

impl<'a, T: Client + Sync> ProtobufDeserializer<'a, T> {
    pub fn new(
        client: &'a T,
        rule_registry: Option<RuleRegistry>,
        deserializer_config: DeserializerConfig,
    ) -> Result<ProtobufDeserializer<'a, T>, SerdeError> {
        for executor in get_executors(rule_registry.as_ref()) {
            executor.configure(client.config(), &deserializer_config.rule_config)?;
        }
        Ok(ProtobufDeserializer {
            base: BaseDeserializer::new(Serde::new(client, rule_registry), deserializer_config),
            serde: ProtobufSerde {
                parsed_schemas: DashMap::new(),
            },
        })
    }

    pub async fn deserialize<M: Message + Default>(
        &self,
        ctx: &SerializationContext,
        data: &[u8],
    ) -> Result<M, SerdeError> {
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
                    Some("serialized"),
                    &self.base.config.use_schema,
                )
                .await?;
        }

        let mut schema_id = SchemaId::new(SerdeFormat::Protobuf, None, None, None)?;
        let id_deser = self.base.config.schema_id_deserializer;
        let bytes_read = id_deser(data, ctx, &mut schema_id)?;
        let msg_index = schema_id.message_indexes.clone().unwrap_or_default();
        let mut data = &data[bytes_read..];

        let writer_schema_raw = self
            .base
            .get_writer_schema(&schema_id, subject.as_deref(), Some("serialized"))
            .await?;
        let (mut writer_schema, mut pool) = self.get_parsed_schema(&writer_schema_raw).await?;
        let writer_desc = self.get_message_desc(&pool, &writer_schema, &msg_index)?;

        if !has_subject {
            subject = strategy(&ctx.topic, &ctx.serde_type, Some(&writer_schema_raw));
            if let Some(subject) = subject.as_ref() {
                latest_schema = self
                    .base
                    .serde
                    .get_reader_schema(subject, Some("serialized"), &self.base.config.use_schema)
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
                        &SerdeValue::new_bytes(SerdeFormat::Protobuf, data),
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
        if let Some(ref latest_schema) = latest_schema {
            migrations = self
                .base
                .serde
                .get_migrations(&subject, &writer_schema_raw, latest_schema, None)
                .await?;
            reader_schema_raw = latest_schema.to_schema();
            (reader_schema, pool) = self.get_parsed_schema(&reader_schema_raw).await?;
        } else {
            migrations = Vec::new();
            reader_schema_raw = writer_schema_raw.clone();
            reader_schema = writer_schema.clone();
        }

        // Initialize reader desc to first message in file
        let mut reader_desc = self.get_message_desc(&pool, &reader_schema, &[0i32])?;
        // Attempt to find a reader desc with the same name as the writer
        reader_desc = pool
            .get_message_by_name(writer_desc.full_name())
            .unwrap_or(reader_desc);

        let mut msg: DynamicMessage;
        if !migrations.is_empty() {
            let reader = Cursor::new(data);
            msg = DynamicMessage::decode(writer_desc, reader)?;
            let mut serializer = serde_json::Serializer::new(vec![]);
            let options = SerializeOptions::new().skip_default_fields(false);
            msg.serialize_with_options(&mut serializer, &options)?;
            let json: serde_json::Value = serde_json::from_slice(&serializer.into_inner())?;
            let mut serde_value = SerdeValue::Json(json);
            serde_value = self
                .base
                .serde
                .execute_migrations(ctx, &subject, &migrations, &serde_value)
                .await?;
            let json_str = match serde_value {
                SerdeValue::Json(v) => serde_json::to_string(&v)?,
                _ => return Err(Serialization("unexpected serde value".to_string())),
            };
            let mut deserializer = serde_json::de::Deserializer::from_str(&json_str);
            msg = DynamicMessage::deserialize(reader_desc, &mut deserializer)?;
            deserializer.end()?;
        } else {
            let mut reader = Cursor::new(&data);
            msg = DynamicMessage::decode(reader_desc, &mut reader)?;
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
                Some(&SerdeSchema::Protobuf(reader_schema.clone())),
                &SerdeValue::Protobuf(Value::Message(msg)),
                Some(Arc::new(field_transformer)),
            )
            .await?;
        msg = match serde_value {
            SerdeValue::Protobuf(Value::Message(msg)) => msg,
            _ => return Err(Serialization("unexpected serde value".to_string())),
        };

        let result: M = msg.transcode_to()?;
        Ok(result)
    }

    fn get_message_desc(
        &self,
        pool: &DescriptorPool,
        fd: &FileDescriptor,
        msg_index: &[i32],
    ) -> Result<MessageDescriptor, SerdeError> {
        let file_desc_proto = fd.file_descriptor_proto();
        let (full_name, desc_proto) =
            self.get_message_desc_proto_file("", file_desc_proto, msg_index)?;
        let package = file_desc_proto.package.clone().unwrap_or_default();
        let qualified_name = if package.is_empty() {
            full_name
        } else {
            package + "." + &full_name
        };
        let msg = pool
            .get_message_by_name(&qualified_name)
            .ok_or(Serialization(format!(
                "message descriptor {qualified_name} not found"
            )))?;
        Ok(msg)
    }

    fn get_message_desc_proto_file(
        &self,
        path: &str,
        desc: &FileDescriptorProto,
        msg_index: &[i32],
    ) -> Result<(String, DescriptorProto), SerdeError> {
        let index = msg_index[0] as usize;
        let msg = desc.message_type.get(index).ok_or(Serialization(format!(
            "message descriptor not found at index {index}"
        )))?;
        let name = msg.name.clone().unwrap_or(String::new());
        let path = if !path.is_empty() && !name.is_empty() {
            path.to_string() + "." + &name
        } else {
            name
        };
        if msg_index.len() == 1 {
            Ok((path, msg.clone()))
        } else {
            self.get_message_desc_proto_nested(&path, msg, &msg_index[1..])
        }
    }

    fn get_message_desc_proto_nested(
        &self,
        path: &str,
        desc: &DescriptorProto,
        msg_index: &[i32],
    ) -> Result<(String, DescriptorProto), SerdeError> {
        let index = msg_index[0] as usize;
        let msg = desc.nested_type.get(index).ok_or(Serialization(format!(
            "message descriptor not found at index {index}"
        )))?;
        let name = msg.name.clone().unwrap_or(String::new());
        let path = if !path.is_empty() && !name.is_empty() {
            path.to_string() + "." + &name
        } else {
            name
        };
        if msg_index.len() == 1 {
            Ok((path, msg.clone()))
        } else {
            self.get_message_desc_proto_nested(&path, msg, &msg_index[1..])
        }
    }

    async fn get_parsed_schema(
        &self,
        schema: &Schema,
    ) -> Result<(FileDescriptor, DescriptorPool), SerdeError> {
        let result = self.serde.parsed_schemas.get(schema);
        if let Some((parsed_schema, pool)) = result.as_deref() {
            return Ok((parsed_schema.clone(), pool.clone()));
        }
        // Get a copy of the global descriptor pool with the Google well-known types.
        let mut pool = DESCRIPTOR_POOL.clone();
        resolve_named_schema(
            schema,
            self.base.serde.client,
            &mut pool,
            &mut HashSet::new(),
        )
        .await?;
        let fd_proto = str_to_proto(&mut pool, "default", &schema.schema)?;
        self.serde
            .parsed_schemas
            .insert(schema.clone(), (fd_proto.clone(), pool.clone()));
        Ok((fd_proto, pool))
    }
}

#[async_recursion]
async fn resolve_named_schema<'a, T>(
    schema: &Schema,
    client: &'a T,
    pool: &mut DescriptorPool,
    visited: &mut HashSet<String>,
) -> Result<(), SerdeError>
where
    T: Client + Sync,
{
    if let Some(refs) = schema.references.as_ref() {
        for r in refs {
            let name = r.name.clone().unwrap_or_default();
            if is_builtin(&name) || visited.contains(&name) {
                continue;
            }
            visited.insert(name.clone());
            let ref_schema = client
                .get_version(
                    &r.subject.clone().unwrap_or_default(),
                    r.version.unwrap_or(-1),
                    true,
                    Some("serialized"),
                )
                .await?;
            resolve_named_schema(&ref_schema.to_schema(), client, pool, visited).await?;
            str_to_proto(pool, &name, &ref_schema.schema.clone().unwrap_or_default())?;
        }
    }
    Ok(())
}

#[async_recursion]
async fn transform(
    ctx: &mut RuleContext,
    descriptor: &MessageDescriptor,
    message: &Value,
    field_executor_type: &str,
) -> Result<Value, SerdeError> {
    match message {
        Value::List(items) => {
            let mut result = Vec::with_capacity(items.len());
            for item in items {
                let item = transform(ctx, descriptor, item, field_executor_type).await?;
                result.push(item);
            }
            return Ok(Value::List(result));
        }
        Value::Map(map) => {
            let mut result = HashMap::new();
            for (key, value) in map {
                let value = transform(ctx, descriptor, value, field_executor_type).await?;
                result.insert(key.clone(), value);
            }
            return Ok(Value::Map(result));
        }
        Value::Message(message) => {
            let mut result = message.clone();
            for fd in descriptor.fields() {
                let field =
                    transform_field_with_ctx(ctx, &fd, descriptor, message, field_executor_type)
                        .await?;
                if let Some(field) = field {
                    result.set_field(&fd, field);
                }
            }
            return Ok(Value::Message(result));
        }
        _ => {
            if let Some(field_ctx) = ctx.current_field() {
                let rule_tags = ctx
                    .rule
                    .tags
                    .clone()
                    .map(|v| HashSet::from_iter(v.into_iter()));
                if rule_tags.is_none_or(|tags| !tags.is_disjoint(&field_ctx.tags)) {
                    let message_value = SerdeValue::Protobuf(message.clone());
                    let executor = get_executor(ctx.rule_registry.as_ref(), field_executor_type);
                    if let Some(executor) = executor {
                        let field_executor =
                            executor
                                .as_field_rule_executor()
                                .ok_or(SerdeError::Rule(format!(
                                    "executor {field_executor_type} is not a field rule executor"
                                )))?;
                        let new_value = field_executor.transform_field(ctx, &message_value).await?;
                        if let SerdeValue::Protobuf(v) = new_value {
                            return Ok(v);
                        }
                    }
                }
            }
        }
    }
    Ok(message.clone())
}

async fn transform_field_with_ctx(
    ctx: &mut RuleContext,
    fd: &FieldDescriptor,
    desc: &MessageDescriptor,
    message: &DynamicMessage,
    field_executor_type: &str,
) -> Result<Option<Value>, SerdeError> {
    let message_value = SerdeValue::Protobuf(Value::Message(message.clone()));
    ctx.enter_field(
        message_value,
        fd.full_name().to_string(),
        fd.name().to_string(),
        get_type(fd),
        get_inline_tags(fd),
    );
    if fd.containing_oneof().is_some() && !message.has_field(fd) {
        // skip oneof fields that are not set
        return Ok(None);
    }
    let value = message.get_field(fd);
    let new_value = transform(ctx, desc, &value, field_executor_type).await?;
    if let Some(Kind::Condition) = ctx.rule.kind {
        if let Value::Bool(b) = new_value {
            if !b {
                return Err(SerdeError::RuleCondition(Box::new(ctx.rule.clone())));
            }
        }
    }
    ctx.exit_field();
    Ok(Some(new_value))
}

fn get_type(fd: &FieldDescriptor) -> FieldType {
    if fd.is_map() {
        return FieldType::Map;
    }
    match fd.kind() {
        prost_reflect::Kind::Message(_) => FieldType::Record,
        prost_reflect::Kind::Enum(_) => FieldType::Enum,
        prost_reflect::Kind::String => FieldType::String,
        prost_reflect::Kind::Bytes => FieldType::Bytes,
        prost_reflect::Kind::Int32 => FieldType::Int,
        prost_reflect::Kind::Sint32 => FieldType::Int,
        prost_reflect::Kind::Uint32 => FieldType::Int,
        prost_reflect::Kind::Fixed32 => FieldType::Int,
        prost_reflect::Kind::Sfixed32 => FieldType::Int,
        prost_reflect::Kind::Int64 => FieldType::Long,
        prost_reflect::Kind::Sint64 => FieldType::Long,
        prost_reflect::Kind::Uint64 => FieldType::Long,
        prost_reflect::Kind::Fixed64 => FieldType::Long,
        prost_reflect::Kind::Sfixed64 => FieldType::Long,
        prost_reflect::Kind::Float => FieldType::Float,
        prost_reflect::Kind::Double => FieldType::Double,
        prost_reflect::Kind::Bool => FieldType::Boolean,
    }
}

fn get_inline_tags(fd: &FieldDescriptor) -> HashSet<String> {
    let mut tag_set = HashSet::new();
    let field_ext = DESCRIPTOR_POOL
        .get_extension_by_name("confluent.field_meta")
        .unwrap();
    if fd.options().has_extension(&field_ext) {
        if let Some(v) = fd.options().get_extension(&field_ext).as_message() {
            if let Some(tags) = v.get_field_by_name("tags") {
                if let Some(tags) = tags.as_list() {
                    for tag in tags {
                        if let Some(tag) = tag.as_str() {
                            tag_set.insert(tag.to_string());
                        }
                    }
                }
            }
        }
    }
    tag_set
}

fn is_builtin(name: &str) -> bool {
    name.starts_with("confluent/")
        || name.starts_with("google/protobuf/")
        || name.starts_with("google/type/")
}

#[cfg(test)]
#[cfg(feature = "rules")]
mod tests {
    use super::*;
    use crate::TEST_FILE_DESCRIPTOR_SET;
    use crate::rest::client_config::ClientConfig;
    use crate::rest::mock_dek_registry_client::MockDekRegistryClient;
    use crate::rest::mock_schema_registry_client::MockSchemaRegistryClient;
    use crate::rest::models::{Rule, RuleSet};
    use crate::rest::schema_registry_client::SchemaRegistryClient;
    use crate::rules::cel::cel_field_executor::CelFieldExecutor;
    use crate::rules::encryption::encrypt_executor::{
        EncryptionExecutor, FakeClock, FieldEncryptionExecutor,
    };
    use crate::rules::encryption::localkms::local_driver::LocalKmsDriver;
    use crate::serdes::config::SchemaSelector;
    use crate::serdes::protobuf::tests::test::Author;
    use crate::serdes::protobuf::tests::test::DependencyMessage;
    use crate::serdes::protobuf::tests::test::TestMessage;
    use crate::serdes::protobuf::tests::test::author::PiiOneof;
    use crate::serdes::serde::{SerdeFormat, SerdeHeaders, header_schema_id_serializer};
    use std::collections::BTreeMap;

    pub(crate) mod test {
        include!("../codegen/test/test.rs");
    }

    #[tokio::test]
    async fn test_basic_serialization() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::default();
        let obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
        let rule_registry = RuleRegistry::new();
        let ser = ProtobufSerializer::with_reference_subject_name_strategy(
            &client,
            default_reference_subject_name_strategy,
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Protobuf,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, &obj).await.unwrap();

        let deser = ProtobufDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2: Author = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_guid_in_header() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let mut ser_conf = SerializerConfig::default();
        ser_conf.schema_id_serializer = header_schema_id_serializer;
        let obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
        let rule_registry = RuleRegistry::new();
        let ser = ProtobufSerializer::with_reference_subject_name_strategy(
            &client,
            default_reference_subject_name_strategy,
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Protobuf,
            headers: Some(SerdeHeaders::default()),
        };
        let bytes = ser.serialize(&ser_ctx, &obj).await.unwrap();

        let deser = ProtobufDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2: Author = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_basic_serialization_with_file_desc_set() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::default();
        let obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
        let rule_registry = RuleRegistry::new();
        let ser = ProtobufSerializer::with_reference_subject_name_strategy(
            &client,
            default_reference_subject_name_strategy,
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Protobuf,
            headers: None,
        };
        let fds: FileDescriptorSet = FileDescriptorSet::decode(TEST_FILE_DESCRIPTOR_SET).unwrap();
        let bytes = ser
            .serialize_with_file_desc_set(&ser_ctx, &obj, "test.Author", fds)
            .await
            .unwrap();

        let deser = ProtobufDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2: Author = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_serialize_reference() {
        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let ser_conf = SerializerConfig::default();
        let msg = TestMessage {
            test_string: "hi".to_string(),
            test_bool: true,
            test_bytes: vec![1u8, 2u8, 3u8],
            test_double: 1.23,
            test_float: 3.45,
            test_fixed32: 67,
            test_fixed64: 89,
            test_int32: 100,
            test_int64: 200,
            test_sfixed32: 300,
            test_sfixed64: 400,
            test_sint32: 500,
            test_sint64: 600,
            test_uint32: 700,
            test_uint64: 800,
        };
        let obj = DependencyMessage {
            is_active: true,
            test_message: Some(msg),
        };
        let rule_registry = RuleRegistry::new();
        let ser = ProtobufSerializer::with_reference_subject_name_strategy(
            &client,
            default_reference_subject_name_strategy,
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Protobuf,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, &obj).await.unwrap();

        let deser = ProtobufDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();
        let obj2: DependencyMessage = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
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
        let mut obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
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
        let rule_set = RuleSet {
            migration_rules: None,
            domain_rules: Some(vec![rule]),
            encoding_rules: None,
        };
        let schema = Schema {
            schema_type: Some("PROTOBUF".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_to_str(&obj.descriptor().parent_file()).unwrap(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(CelFieldExecutor::new());
        let ser = ProtobufSerializer::with_reference_subject_name_strategy(
            &client,
            default_reference_subject_name_strategy,
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Protobuf,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, &obj).await.unwrap();

        let deser = ProtobufDeserializer::new(
            &client,
            Some(rule_registry.clone()),
            DeserializerConfig::default(),
        )
        .unwrap();

        obj = Author {
            name: "Kafka-suffix".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec![
                "Metamorphosis-suffix".to_string(),
                "The Trial-suffix".to_string(),
            ],
            pii_oneof: Some(PiiOneof::OneofString("oneof-suffix".to_string())),
        };
        let obj2: Author = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_encryption() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let ser_conf = SerializerConfig::new(false, None, true, false, rule_conf.clone());
        let deser_conf = DeserializerConfig::new(None, false, rule_conf);
        let mut obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
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
            schema_type: Some("PROTOBUF".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_to_str(&obj.descriptor().parent_file()).unwrap(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(FieldEncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser = ProtobufSerializer::with_reference_subject_name_strategy(
            &client,
            default_reference_subject_name_strategy,
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Protobuf,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, &obj).await.unwrap();

        let deser =
            ProtobufDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
        let obj2: Author = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }

    #[tokio::test]
    async fn test_payload_encryption() {
        LocalKmsDriver::register();

        let client_conf = ClientConfig::new(vec!["mock://".to_string()]);
        let client = MockSchemaRegistryClient::new(client_conf);
        let rule_conf = HashMap::from([("secret".to_string(), "mysecret".to_string())]);
        let ser_conf = SerializerConfig::new(false, None, true, false, rule_conf.clone());
        let deser_conf = DeserializerConfig::new(None, false, rule_conf);
        let mut obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
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
            schema_type: Some("PROTOBUF".to_string()),
            references: None,
            metadata: None,
            rule_set: Some(Box::new(rule_set)),
            schema: schema_to_str(&obj.descriptor().parent_file()).unwrap(),
        };
        client
            .register_schema("test-value", &schema, false)
            .await
            .unwrap();
        let rule_registry = RuleRegistry::new();
        rule_registry.register_executor(EncryptionExecutor::<MockDekRegistryClient>::new(
            FakeClock::new(0),
        ));
        let ser = ProtobufSerializer::with_reference_subject_name_strategy(
            &client,
            default_reference_subject_name_strategy,
            Some(rule_registry.clone()),
            ser_conf,
        )
        .unwrap();
        let ser_ctx = SerializationContext {
            topic: "test".to_string(),
            serde_type: SerdeType::Value,
            serde_format: SerdeFormat::Protobuf,
            headers: None,
        };
        let bytes = ser.serialize(&ser_ctx, &obj).await.unwrap();

        let deser =
            ProtobufDeserializer::new(&client, Some(rule_registry.clone()), deser_conf).unwrap();

        obj = Author {
            name: "Kafka".to_string(),
            id: 123,
            picture: vec![1u8, 2u8, 3u8],
            works: vec!["Metamorphosis".to_string(), "The Trial".to_string()],
            pii_oneof: Some(PiiOneof::OneofString("oneof".to_string())),
        };
        let obj2: Author = deser.deserialize(&ser_ctx, &bytes).await.unwrap();
        assert_eq!(obj2, obj);
    }
}
