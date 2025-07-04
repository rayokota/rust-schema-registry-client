use crate::rules::cel::cel_lib::default_context;
use crate::serdes::serde::{RuleBase, RuleContext, RuleExecutor, SerdeError, SerdeValue};
use async_trait::async_trait;
use cel_interpreter::objects::{Key, Map};
use cel_interpreter::{Context, ExecutionError, ParseError, Program, Value};
use dashmap::DashMap;
use prost::bytes::Bytes;
use prost_reflect::MapKey;
use std::collections::HashMap;
use std::sync::Arc;

pub struct CelExecutor {
    cache: DashMap<String, Program>,
}

impl RuleBase for CelExecutor {
    fn get_type(&self) -> &'static str {
        "CEL"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Default for CelExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CelExecutor {
    pub fn new() -> Self {
        CelExecutor {
            cache: DashMap::new(),
        }
    }

    pub(crate) fn execute(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
        args: &HashMap<String, Value>,
    ) -> Result<SerdeValue, SerdeError> {
        let mut expr = ctx.rule.expr.clone().ok_or(SerdeError::Rule(
            "rule does not contain an expression".to_string(),
        ))?;
        let parts: Vec<&str> = expr.split(";").collect();
        if parts.len() > 1 {
            let guard = parts[0];
            if !guard.is_empty() {
                let guard_result = self.execute_rule(ctx, msg, guard, args)?;
                if !guard_result.as_bool() {
                    return Ok(msg.clone());
                }
            }
            expr = parts[1].to_string();
        }
        self.execute_rule(ctx, msg, &expr, args)
    }

    fn execute_rule(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
        expr: &str,
        args: &HashMap<String, Value>,
    ) -> Result<SerdeValue, SerdeError> {
        let mut prog = self.cache.get(expr);
        if prog.is_none() {
            let program = Program::compile(expr)?;
            self.cache.insert(expr.to_string(), program);
        }
        prog = self.cache.get(expr);
        let prog = prog.ok_or(SerdeError::Rule("failed to compile program".to_string()))?;
        let mut context = default_context();
        for (k, v) in args {
            context.add_variable_from_value(k.clone(), v.clone());
        }
        let result = prog.value().execute(&context)?;
        Ok(to_serde_value(msg, &result))
    }

    pub fn register() {
        crate::serdes::rule_registry::register_rule_executor(CelExecutor::new());
    }
}

#[async_trait]
impl RuleExecutor for CelExecutor {
    async fn transform(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        let mut args = HashMap::new();
        args.insert("msg".to_string(), from_serde_value(msg));
        self.execute(ctx, msg, &args)
    }
}

pub fn from_serde_value(value: &SerdeValue) -> Value {
    match value {
        SerdeValue::Avro(v) => from_avro_value(v),
        SerdeValue::Protobuf(v) => from_protobuf_value(v),
        SerdeValue::Json(v) => from_json_value(v),
    }
}

fn from_avro_value(value: &apache_avro::types::Value) -> Value {
    match value {
        apache_avro::types::Value::Boolean(v) => Value::Bool(*v),
        apache_avro::types::Value::Int(v) => Value::Int(*v as i64),
        apache_avro::types::Value::Long(v) => Value::Int(*v),
        apache_avro::types::Value::Float(v) => Value::Float(*v as f64),
        apache_avro::types::Value::Double(v) => Value::Float(*v),
        apache_avro::types::Value::String(v) => Value::String(Arc::new((*v).clone())),
        apache_avro::types::Value::Bytes(v) => Value::Bytes(Arc::new(v.to_vec())),
        apache_avro::types::Value::Fixed(_, v) => Value::Bytes(Arc::new(v.to_vec())),
        apache_avro::types::Value::Enum(_, v) => Value::String(Arc::new((*v).clone())),
        apache_avro::types::Value::Array(v) => {
            Value::List(Arc::new(v.iter().map(from_avro_value).collect()))
        }
        apache_avro::types::Value::Map(v) => Value::Map(Map {
            map: Arc::new(
                v.iter()
                    .map(|(k, v)| (Key::String(Arc::new(k.clone())), from_avro_value(v)))
                    .collect(),
            ),
        }),
        apache_avro::types::Value::Record(v) => {
            let mut map: HashMap<Key, Value> = HashMap::with_capacity(v.len());
            for (k, v) in v {
                map.insert(Key::String(Arc::new(k.clone())), from_avro_value(v));
            }
            Value::Map(Map { map: Arc::new(map) })
        }
        apache_avro::types::Value::Null => Value::Null,
        _ => Value::Null,
    }
}

fn from_protobuf_value(value: &prost_reflect::Value) -> Value {
    match value {
        prost_reflect::Value::Bool(v) => Value::Bool(*v),
        prost_reflect::Value::I32(v) => Value::Int(*v as i64),
        prost_reflect::Value::I64(v) => Value::Int(*v),
        prost_reflect::Value::U32(v) => Value::Int(*v as i64),
        prost_reflect::Value::U64(v) => Value::Int(*v as i64),
        prost_reflect::Value::F32(v) => Value::Float(*v as f64),
        prost_reflect::Value::F64(v) => Value::Float(*v),
        prost_reflect::Value::String(v) => Value::String(Arc::new(v.clone())),
        prost_reflect::Value::Bytes(v) => Value::Bytes(Arc::new(v.to_vec())),
        prost_reflect::Value::EnumNumber(v) => Value::Int(*v as i64),
        prost_reflect::Value::Message(msg) => {
            let mut map: HashMap<Key, Value> = HashMap::with_capacity(msg.fields().count());
            for (fd, v) in msg.fields() {
                map.insert(
                    Key::String(Arc::new(fd.name().to_string())),
                    from_protobuf_value(v),
                );
            }
            Value::Map(Map { map: Arc::new(map) })
        }
        prost_reflect::Value::List(v) => {
            Value::List(Arc::new(v.iter().map(from_protobuf_value).collect()))
        }
        prost_reflect::Value::Map(v) => {
            let map = v
                .iter()
                .map(|(k, v)| (from_protobuf_map_key(k), from_protobuf_value(v)))
                .collect();
            Value::Map(Map { map: Arc::new(map) })
        }
    }
}

fn from_protobuf_map_key(value: &MapKey) -> Key {
    match value {
        MapKey::Bool(v) => Key::Bool(*v),
        MapKey::I32(v) => Key::Int(*v as i64),
        MapKey::I64(v) => Key::Int(*v),
        MapKey::U32(v) => Key::Uint(*v as u64),
        MapKey::U64(v) => Key::Uint(*v),
        MapKey::String(v) => Key::String(Arc::new(v.clone())),
    }
}

fn from_json_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Bool(v) => Value::Bool(*v),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                Value::Int(i)
            } else if let Some(f) = v.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(v) => Value::String(Arc::new(v.clone())),
        serde_json::Value::Array(v) => {
            Value::List(Arc::new(v.iter().map(from_json_value).collect()))
        }
        serde_json::Value::Object(v) => {
            let map = v
                .iter()
                .map(|(k, v)| (Key::String(Arc::new(k.clone())), from_json_value(v)))
                .collect();
            Value::Map(Map { map: Arc::new(map) })
        }
        serde_json::Value::Null => Value::Null,
    }
}

pub fn to_serde_value(input: &SerdeValue, value: &Value) -> SerdeValue {
    match input {
        SerdeValue::Avro(v) => SerdeValue::Avro(to_avro_value(v, value)),
        SerdeValue::Protobuf(v) => SerdeValue::Protobuf(to_protobuf_value(v, value)),
        SerdeValue::Json(v) => SerdeValue::Json(to_json_value(v, value)),
    }
}

fn to_avro_value(input: &apache_avro::types::Value, value: &Value) -> apache_avro::types::Value {
    match value {
        Value::Bool(v) => apache_avro::types::Value::Boolean(*v),
        Value::Int(v) => {
            if let apache_avro::types::Value::Int(_) = input {
                apache_avro::types::Value::Int(*v as i32)
            } else {
                apache_avro::types::Value::Long(*v)
            }
        }
        Value::UInt(v) => {
            if let apache_avro::types::Value::Int(_) = input {
                apache_avro::types::Value::Int(*v as i32)
            } else {
                apache_avro::types::Value::Long(*v as i64)
            }
        }
        Value::Float(v) => {
            if let apache_avro::types::Value::Float(_) = input {
                apache_avro::types::Value::Float(*v as f32)
            } else {
                apache_avro::types::Value::Double(*v)
            }
        }
        Value::String(v) => {
            if let apache_avro::types::Value::Enum(s, _) = input {
                apache_avro::types::Value::Enum(*s, v.to_string())
            } else {
                apache_avro::types::Value::String(v.to_string())
            }
        }
        Value::Bytes(v) => {
            if let apache_avro::types::Value::Fixed(s, _) = input {
                apache_avro::types::Value::Fixed(*s, (**v).clone())
            } else {
                apache_avro::types::Value::Bytes((**v).clone())
            }
        }
        Value::List(v) => apache_avro::types::Value::Array(
            (**v)
                .clone()
                .into_iter()
                .map(|x| to_avro_value(input, &x))
                .collect(),
        ),
        Value::Map(v) => {
            let iter = (*v.map).clone().into_iter().map(|(k, v)| {
                let key = if let Key::String(s) = k {
                    s.to_string()
                } else {
                    k.to_string()
                };
                (key, to_avro_value(input, &v))
            });
            if let apache_avro::types::Value::Record(_) = input {
                apache_avro::types::Value::Record(iter.collect())
            } else {
                apache_avro::types::Value::Map(iter.collect())
            }
        }
        Value::Null => apache_avro::types::Value::Null,
        _ => apache_avro::types::Value::Null,
    }
}

fn to_protobuf_value(input: &prost_reflect::Value, value: &Value) -> prost_reflect::Value {
    match value {
        Value::Bool(v) => prost_reflect::Value::Bool(*v),
        Value::Int(v) => match input {
            prost_reflect::Value::I32(_) => prost_reflect::Value::I32(*v as i32),
            prost_reflect::Value::I64(_) => prost_reflect::Value::I64(*v),
            prost_reflect::Value::U32(_) => prost_reflect::Value::U32(*v as u32),
            prost_reflect::Value::U64(_) => prost_reflect::Value::U64(*v as u64),
            prost_reflect::Value::EnumNumber(_) => prost_reflect::Value::EnumNumber(*v as i32),
            _ => prost_reflect::Value::I64(*v),
        },
        Value::UInt(v) => match input {
            prost_reflect::Value::I32(_) => prost_reflect::Value::I32(*v as i32),
            prost_reflect::Value::I64(_) => prost_reflect::Value::I64(*v as i64),
            prost_reflect::Value::U32(_) => prost_reflect::Value::U32(*v as u32),
            prost_reflect::Value::U64(_) => prost_reflect::Value::U64(*v),
            prost_reflect::Value::EnumNumber(_) => prost_reflect::Value::EnumNumber(*v as i32),
            _ => prost_reflect::Value::U64(*v),
        },
        Value::Float(v) => {
            if let prost_reflect::Value::F32(_) = input {
                prost_reflect::Value::F32(*v as f32)
            } else {
                prost_reflect::Value::F64(*v)
            }
        }
        Value::String(v) => prost_reflect::Value::String(v.to_string()),
        Value::Bytes(v) => prost_reflect::Value::Bytes(Bytes::from((**v).clone())),
        Value::List(v) => prost_reflect::Value::List(
            (**v)
                .clone()
                .into_iter()
                .map(|x| to_protobuf_value(input, &x))
                .collect(),
        ),
        Value::Map(v) => {
            let iter = (*v.map).clone().into_iter().map(|(k, v)| {
                let key = to_protobuf_map_key(&k);
                (key, to_protobuf_value(input, &v))
            });
            prost_reflect::Value::Map(iter.collect())
        }
        Value::Null => prost_reflect::Value::Bytes(Bytes::from(Vec::new())),
        _ => prost_reflect::Value::Bytes(Bytes::from(Vec::new())),
    }
}

fn to_protobuf_map_key(value: &Key) -> MapKey {
    match value {
        Key::Bool(v) => MapKey::Bool(*v),
        Key::Int(v) => MapKey::I64(*v),
        Key::Uint(v) => MapKey::U64(*v),
        Key::String(v) => MapKey::String(v.to_string()),
    }
}

fn to_json_value(input: &serde_json::Value, value: &Value) -> serde_json::Value {
    match value {
        Value::Bool(v) => serde_json::Value::Bool(*v),
        Value::Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        Value::UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        Value::Float(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v).unwrap()),
        Value::String(v) => serde_json::Value::String(v.to_string()),
        Value::List(v) => serde_json::Value::Array(
            (**v)
                .clone()
                .into_iter()
                .map(|x| to_json_value(input, &x))
                .collect(),
        ),
        Value::Map(v) => {
            let iter = (*v.map).clone().into_iter().map(|(k, v)| {
                let key = if let Key::String(s) = k {
                    s.to_string()
                } else {
                    k.to_string()
                };
                (key, to_json_value(input, &v))
            });
            serde_json::Value::Object(iter.collect())
        }
        Value::Null => serde_json::Value::Null,
        _ => serde_json::Value::Null,
    }
}

impl From<ExecutionError> for SerdeError {
    fn from(value: ExecutionError) -> Self {
        SerdeError::Rule(format!("CEL execution error: {value}"))
    }
}

impl From<ParseError> for SerdeError {
    fn from(value: ParseError) -> Self {
        SerdeError::Rule(format!("CEL parse error: {value}"))
    }
}
