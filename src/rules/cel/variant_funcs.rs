// Copyright 2026 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! CEL bindings for the `variant(...)` constructor and the `variants.*` accessor functions.
//! A CEL Variant is a `cel::objects::Opaque` wrapping the codec [`Variant`] (mirroring the
//! decimal opaque). Null model: CEL null = absent (a miss, out-of-bounds, or non-variant);
//! a Variant whose top type is NULL = present-but-null (detected by `variants.isNull`).
//!
//! Both an Avro variant field (a record → CEL map) and a Protobuf `confluent.type.Variant`
//! message (→ CEL map) reach CEL as a `Value::Map` with `metadata`/`value` byte entries, so
//! `variant(dyn)` accepts that shape directly - mirroring the Python/Go clients.

use std::sync::Arc;

use cel::extractors::Arguments;
use cel::objects::{Key, Map, Opaque};
use cel::{Context, ExecutionError, Value};

use crate::rules::cel::decimal_funcs::{decimal_value, from_bytes_scale};
use crate::rules::cel::timestamp_funcs::from_epoch;
use crate::rules::cel::variant_path;
use crate::serdes::variant::{Type, Variant};

/// The opaque-type label shared across all Schema Registry CEL clients.
pub const VARIANT_TYPE_NAME: &str = "confluent.type.Variant";

/// A CEL Variant value: an opaque wrapper over the codec [`Variant`].
#[derive(Debug, Clone)]
pub struct CelVariant(pub Variant);

impl PartialEq for CelVariant {
    fn eq(&self, other: &Self) -> bool {
        self.0.metadata_bytes() == other.0.metadata_bytes()
            && self.0.standalone_value_bytes() == other.0.standalone_value_bytes()
    }
}
impl Eq for CelVariant {}

impl Opaque for CelVariant {
    fn runtime_type_name(&self) -> &str {
        VARIANT_TYPE_NAME
    }
}

/// Wraps a [`Variant`] as a CEL opaque value.
pub fn variant_value(v: Variant) -> Value {
    Value::Opaque(Arc::new(CelVariant(v)))
}

fn err(msg: impl Into<String>) -> ExecutionError {
    ExecutionError::FunctionError {
        function: "variants".to_string(),
        message: msg.into(),
    }
}

/// The coarse label `variants.type` returns: integer widths collapse to "int", float/double
/// to "double", decimal widths to "decimal", all timestamp variants to "timestamp".
fn type_label(t: Type) -> &'static str {
    match t {
        Type::Object => "object",
        Type::Array => "array",
        Type::Null => "null",
        Type::Boolean => "boolean",
        Type::Byte | Type::Short | Type::Int | Type::Long => "int",
        Type::Float | Type::Double => "double",
        Type::Decimal4 | Type::Decimal8 | Type::Decimal16 => "decimal",
        Type::Date => "date",
        Type::Time => "time",
        Type::TimestampTz | Type::TimestampNtz | Type::TimestampNanosTz | Type::TimestampNanosNtz => {
            "timestamp"
        }
        Type::String => "string",
        Type::Binary => "bytes",
        Type::Uuid => "uuid",
    }
}

/// Reads a Variant from a CEL map with `metadata`/`value` byte entries (an Avro record or a
/// Protobuf message decoded into CEL). None if the map lacks either byte field.
fn variant_from_map(m: &Map) -> Option<Variant> {
    let get = |k: &str| -> Option<Vec<u8>> {
        match m.map.get(&Key::String(Arc::new(k.to_string()))) {
            Some(Value::Bytes(b)) => Some(b.as_ref().clone()),
            _ => None,
        }
    };
    match (get("value"), get("metadata")) {
        (Some(value), Some(metadata)) => Some(Variant::new(value, metadata)),
        _ => None,
    }
}

/// The `variant(dyn)` dispatch: an opaque Variant, or a map with metadata/value bytes.
/// Rejects strings (use `variants.parseJson`) and null.
fn to_variant(v: &Value) -> Result<Variant, ExecutionError> {
    match v {
        Value::Opaque(o) if o.runtime_type_name() == VARIANT_TYPE_NAME => o
            .downcast_ref::<CelVariant>()
            .map(|cv| cv.0.clone())
            .ok_or_else(|| err("variant: opaque value is not a Variant")),
        Value::Map(m) => variant_from_map(m)
            .ok_or_else(|| err("variant: map missing 'metadata'/'value' byte entries")),
        Value::String(_) => Err(err(
            "variant: cannot convert string to Variant; use variants.parseJson(s)",
        )),
        Value::Null => Err(err("variant: cannot convert null to Variant")),
        _ => Err(err("variant: cannot convert value to Variant")),
    }
}

/// A `variants.*` navigation receiver: CEL null → Ok(None) (passthrough); a variant →
/// Ok(Some); anything else → Err.
fn receiver(v: &Value) -> Result<Option<Variant>, ExecutionError> {
    match v {
        Value::Null => Ok(None),
        Value::Opaque(o) if o.runtime_type_name() == VARIANT_TYPE_NAME => o
            .downcast_ref::<CelVariant>()
            .map(|cv| Some(cv.0.clone()))
            .ok_or_else(|| err("variant: opaque value is not a Variant")),
        Value::Map(m) => variant_from_map(m)
            .map(Some)
            .ok_or_else(|| err("expected a Variant")),
        _ => Err(err("expected a Variant")),
    }
}

// ---- functions ----

fn variant(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    match args.as_slice() {
        [v] => Ok(variant_value(to_variant(v)?)),
        [Value::Bytes(value), Value::Bytes(metadata)] => Ok(variant_value(Variant::new(
            value.as_ref().clone(),
            metadata.as_ref().clone(),
        ))),
        _ => Err(err("variant: expected (dyn) or (bytes, bytes)")),
    }
}

fn variants_parse_json(v: Value) -> Result<Value, ExecutionError> {
    match &v {
        Value::String(s) => Variant::parse_json(s)
            .map(variant_value)
            .map_err(|e| err(format!("variants.parseJson: {e}"))),
        _ => Err(err("variants.parseJson: expected a string")),
    }
}

fn variants_try_parse_json(v: Value) -> Result<Value, ExecutionError> {
    match &v {
        Value::String(s) => Ok(Variant::parse_json(s).map(variant_value).unwrap_or(Value::Null)),
        _ => Ok(Value::Null),
    }
}

fn variants_type(v: Value) -> Result<Value, ExecutionError> {
    match receiver(&v)? {
        None => Ok(Value::Null),
        Some(vv) => Ok(Value::String(Arc::new(type_label(vv.get_type()).to_string()))),
    }
}

fn variants_is_null(v: Value) -> Result<bool, ExecutionError> {
    Ok(match &v {
        Value::Opaque(o) if o.runtime_type_name() == VARIANT_TYPE_NAME => o
            .downcast_ref::<CelVariant>()
            .map(|cv| cv.0.get_type() == Type::Null)
            .unwrap_or(false),
        _ => false,
    })
}

fn variants_path(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let vv = match receiver(&a)? {
        None => return Ok(Value::Null),
        Some(v) => v,
    };
    let path = match &b {
        Value::String(s) => s.as_str(),
        _ => return Err(err("variants.path: expected a string path")),
    };
    match variant_path::walk(&vv, path).map_err(|e| err(format!("variants.path: {e}")))? {
        Some(r) => Ok(variant_value(r)),
        None => Ok(Value::Null),
    }
}

fn variants_field(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let vv = match receiver(&a)? {
        None => return Ok(Value::Null),
        Some(v) => v,
    };
    if vv.get_type() != Type::Object {
        return Ok(Value::Null);
    }
    let key = match &b {
        Value::String(s) => s.as_str(),
        _ => return Err(err("variants.field: expected a string key")),
    };
    Ok(vv.get_field_by_key(key).map(variant_value).unwrap_or(Value::Null))
}

fn variants_index(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let vv = match receiver(&a)? {
        None => return Ok(Value::Null),
        Some(v) => v,
    };
    if vv.get_type() != Type::Array {
        return Ok(Value::Null);
    }
    let idx = match &b {
        Value::Int(i) => *i,
        _ => return Err(err("variants.index: expected an int index")),
    };
    if idx < 0 || idx > i32::MAX as i64 {
        return Ok(Value::Null);
    }
    Ok(vv
        .get_element_at_index(idx as usize)
        .map(variant_value)
        .unwrap_or(Value::Null))
}

/// Backing for `variants.as` (strict) and `variants.tryAs` (soft). On a type mismatch the
/// strict form errors and the soft form returns CEL null; types with no CEL scalar
/// extraction (object/array/null/date/time/uuid) always error.
fn variant_as(a: &Value, b: &Value, null_on_error: bool) -> Result<Value, ExecutionError> {
    let vv = match receiver(a)? {
        None => return Ok(Value::Null),
        Some(v) => v,
    };
    let t = match b {
        Value::String(s) => s.as_str(),
        _ => return Err(err("variants.as: expected a type string")),
    };
    // Not extractable as a CEL scalar - always an error, even in the soft form.
    if matches!(t, "object" | "array" | "null" | "date" | "time" | "uuid") {
        return Err(err(format!(
            "variants.as: type '{t}' is not supported for extraction (use variants.type/\
             variants.path/variants.field/variants.index instead)"
        )));
    }
    let conv = |e: crate::serdes::variant::VariantError| err(format!("variants.as: {e}"));
    let vt = vv.get_type();
    let extracted: Option<Result<Value, ExecutionError>> = match t {
        "string" => (vt == Type::String)
            .then(|| vv.get_string().map(|s| Value::String(Arc::new(s))).map_err(conv)),
        "int" => matches!(vt, Type::Byte | Type::Short | Type::Int | Type::Long)
            .then(|| vv.get_long().map(Value::Int).map_err(conv)),
        "double" => match vt {
            Type::Float => Some(vv.get_float().map(|f| Value::Float(f as f64)).map_err(conv)),
            Type::Double => Some(vv.get_double().map(Value::Float).map_err(conv)),
            _ => None,
        },
        "boolean" => (vt == Type::Boolean)
            .then(|| vv.get_boolean().map(Value::Bool).map_err(conv)),
        "decimal" => matches!(vt, Type::Decimal4 | Type::Decimal8 | Type::Decimal16).then(|| {
            vv.get_decimal_parts()
                .map_err(conv)
                .map(|(bytes, scale)| decimal_value(from_bytes_scale(&bytes, scale as i64)))
        }),
        "timestamp" => matches!(
            vt,
            Type::TimestampTz | Type::TimestampNtz | Type::TimestampNanosTz | Type::TimestampNanosNtz
        )
        .then(|| {
            let unit = if matches!(vt, Type::TimestampTz | Type::TimestampNtz) {
                "micros"
            } else {
                "nanos"
            };
            let raw = vv.get_long().map_err(conv)?;
            from_epoch(raw, unit)
                .map(Value::Timestamp)
                .map_err(|e| err(format!("variants.as: {e}")))
        }),
        "bytes" => (vt == Type::Binary)
            .then(|| vv.get_binary().map(|b| Value::Bytes(Arc::new(b))).map_err(conv)),
        _ => {
            return if null_on_error {
                Ok(Value::Null)
            } else {
                Err(err(format!(
                    "variants.as: unknown type '{t}' (expected one of: string, int, double, \
                     boolean, decimal, timestamp, bytes)"
                )))
            };
        }
    };
    match extracted {
        Some(r) => r,
        None => {
            if null_on_error {
                Ok(Value::Null)
            } else {
                Err(err(format!("variants.as: variant is not {t}-typed")))
            }
        }
    }
}

fn variants_as(a: Value, b: Value) -> Result<Value, ExecutionError> {
    variant_as(&a, &b, false)
}

fn variants_try_as(a: Value, b: Value) -> Result<Value, ExecutionError> {
    variant_as(&a, &b, true)
}

fn variants_to_json(v: Value) -> Result<Value, ExecutionError> {
    match receiver(&v)? {
        None => Ok(Value::Null),
        Some(vv) => vv
            .to_json()
            .map(|s| Value::String(Arc::new(s)))
            .map_err(|e| err(format!("variants.toJson: {e}"))),
    }
}

/// Registers the variant(...) constructor and the variants.* accessor family.
pub fn add_variant_functions(ctx: &mut Context) {
    ctx.add_function("variant", variant);
    ctx.add_function("variants.parseJson", variants_parse_json);
    ctx.add_function("variants.tryParseJson", variants_try_parse_json);
    ctx.add_function("variants.type", variants_type);
    ctx.add_function("variants.isNull", variants_is_null);
    ctx.add_function("variants.path", variants_path);
    ctx.add_function("variants.field", variants_field);
    ctx.add_function("variants.index", variants_index);
    ctx.add_function("variants.as", variants_as);
    ctx.add_function("variants.tryAs", variants_try_as);
    ctx.add_function("variants.toJson", variants_to_json);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::cel::cel_lib::default_context;
    use cel::Program;

    // A JSON document exercising objects, arrays, an explicit null, and nesting.
    const DOC: &str = r#"{"name":"alice","age":30,"explicit":null,"nested":{"x":1},"scores":[10,20,30]}"#;

    fn eval_with(expr: &str, this: Value) -> Value {
        let program = Program::compile(expr).expect("compile");
        let mut ctx = default_context();
        ctx.add_variable_from_value("this", this);
        program.execute(&ctx).expect("execute")
    }

    fn eval_bool(expr: &str, this: Value) -> bool {
        matches!(eval_with(expr, this), Value::Bool(true))
    }

    fn doc_string() -> Value {
        Value::String(Arc::new(DOC.to_string()))
    }

    #[test]
    fn variant_functions() {
        let cases = [
            "variants.type(variants.parseJson(this)) == 'object'",
            "variants.as(variants.field(variants.parseJson(this), 'name'), 'string') == 'alice'",
            "variants.as(variants.field(variants.parseJson(this), 'age'), 'int') == 30",
            // A missing field is CEL null (absent); an explicit JSON null is a present variant-null.
            "variants.field(variants.parseJson(this), 'missing') == null",
            "variants.isNull(variants.field(variants.parseJson(this), 'explicit'))",
            "!variants.isNull(variants.field(variants.parseJson(this), 'missing'))",
            "variants.as(variants.path(variants.parseJson(this), '$.nested.x'), 'int') == 1",
            "variants.as(variants.index(variants.field(variants.parseJson(this), 'scores'), 2), 'int') == 30",
            // tryAs returns CEL null on a type mismatch (age is an int, not a string).
            "variants.tryAs(variants.field(variants.parseJson(this), 'age'), 'string') == null",
            r#"variants.toJson(variants.field(variants.parseJson(this), 'nested')) == '{"x":1}'"#,
        ];
        for expr in cases {
            assert!(eval_bool(expr, doc_string()), "expr failed: {expr}");
        }
    }

    // An Avro variant record, converted through the real Avro→CEL path (from_serde_value →
    // from_avro_value → Value::Map), then consumed by variant(this).
    #[test]
    fn avro_variant_into_cel() {
        use crate::rules::cel::cel_executor::from_serde_value;
        use crate::serdes::serde::SerdeValue;
        use apache_avro::types::Value as AvroValue;

        let pv = Variant::parse_json(DOC).expect("parse");
        let record = AvroValue::Record(vec![
            (
                "metadata".to_string(),
                AvroValue::Bytes(pv.metadata_bytes().to_vec()),
            ),
            (
                "value".to_string(),
                AvroValue::Bytes(pv.value_bytes().to_vec()),
            ),
        ]);
        let this = from_serde_value(&SerdeValue::Avro(record));
        assert!(eval_bool(
            "variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'",
            this,
        ));
    }

    // A confluent.type.Variant proto message, converted through the real Protobuf→CEL path,
    // then consumed by variant(this).
    #[test]
    fn proto_variant_into_cel() {
        use crate::rules::cel::cel_executor::from_protobuf_value_for_test;
        use prost_reflect::{DynamicMessage, Value as ProtoValue};

        let pv = Variant::parse_json(DOC).expect("parse");
        let desc = crate::DESCRIPTOR_POOL
            .get_message_by_name("confluent.type.Variant")
            .expect("variant.proto is compiled into the descriptor pool");
        let mut msg = DynamicMessage::new(desc);
        msg.set_field_by_name(
            "metadata",
            ProtoValue::Bytes(pv.metadata_bytes().to_vec().into()),
        );
        msg.set_field_by_name("value", ProtoValue::Bytes(pv.value_bytes().to_vec().into()));
        let this = from_protobuf_value_for_test(&ProtoValue::Message(msg));
        assert!(eval_bool(
            "variants.as(variants.field(variant(this), 'age'), 'int') == 30",
            this,
        ));
    }
}
