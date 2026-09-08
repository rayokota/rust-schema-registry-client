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
    /// Identity semantics, matching Java, C#, Python, JavaScript and C++, none of which give a
    /// variant a value-equality operator.
    ///
    /// A variant is a dynamically typed container, so comparing encodings is not value
    /// equality: it reports `12.34 != 12.340` (different scale) and `int8(1) != int16(1)`
    /// (different width), and can separate identical documents whose metadata dictionaries
    /// differ. Real value equality needs a decode and a specification for cross-width integers,
    /// decimal scale, int/double comparison and object key order - which `==` does not do.
    ///
    /// So this is true only when both sides are the same value: a `true` is never wrong, while
    /// equal values reached separately compare false. Compare values with `variants.as` or
    /// `variants.toJson` instead.
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
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
        Type::TimestampTz
        | Type::TimestampNtz
        | Type::TimestampNanosTz
        | Type::TimestampNanosNtz => "timestamp",
        Type::String => "string",
        Type::Binary => "bytes",
        Type::Uuid => "uuid",
    }
}

/// A variant whose metadata is empty carries no value at all: a Protobuf field left unset, or an
/// Avro variant record with empty byte fields. `Variant::new` accepts it — the version byte is
/// only read later — so the check belongs here, and callers map it to CEL null rather than
/// letting an accessor fail on a buffer that was never populated.
fn is_absent(v: &Variant) -> bool {
    v.metadata_bytes().is_empty()
}

/// Reads a Variant from a CEL map with `metadata`/`value` byte entries (an Avro record or a
/// Protobuf message decoded into CEL).
///
/// Three outcomes: `None` if the map is not variant-shaped (the caller reports that), `Some(None)`
/// if it is variant-shaped but absent, and `Some(Some(v))` for a readable variant.
fn variant_from_map(m: &Map) -> Option<Option<Variant>> {
    let get = |k: &str| -> Option<Vec<u8>> {
        match m.map.get(&Key::String(Arc::new(k.to_string()))) {
            Some(Value::Bytes(b)) => Some(b.as_ref().clone()),
            _ => None,
        }
    };
    match (get("value"), get("metadata")) {
        (Some(value), Some(metadata)) => Some(if metadata.is_empty() {
            None
        } else {
            Some(Variant::new(value, metadata))
        }),
        _ => None,
    }
}

/// The `variant(dyn)` dispatch: an opaque Variant, or a map with metadata/value bytes.
/// Rejects strings (use `variants.parseJson`) and null. `Ok(None)` means the input is an absent
/// variant, which the caller reports as CEL null.
pub(crate) fn to_variant(v: &Value) -> Result<Option<Variant>, ExecutionError> {
    match v {
        Value::Opaque(o) if o.runtime_type_name() == VARIANT_TYPE_NAME => o
            .downcast_ref::<CelVariant>()
            .map(|cv| {
                let inner = cv.0.clone();
                if is_absent(&inner) { None } else { Some(inner) }
            })
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
            .map(|cv| {
                let inner = cv.0.clone();
                if is_absent(&inner) { None } else { Some(inner) }
            })
            .ok_or_else(|| err("variant: opaque value is not a Variant")),
        Value::Map(m) => variant_from_map(m).ok_or_else(|| err("expected a Variant")),
        _ => Err(err("expected a Variant")),
    }
}

// ---- functions ----

fn variant(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    match args.as_slice() {
        // CEL null passes through (aligns with the Java reference variant(null) -> null).
        [Value::Null] => Ok(Value::Null),
        [v] => Ok(match to_variant(v)? {
            Some(variant) => variant_value(variant),
            // An absent variant reports as CEL null, like the `variant(null)` arm above.
            None => Value::Null,
        }),
        [Value::Bytes(value), Value::Bytes(metadata)] => {
            if metadata.is_empty() {
                // Passing empty metadata explicitly is a rule-authoring mistake rather than an
                // absent field, so it is reported instead of yielding null.
                return Err(err(
                    "variant: metadata is empty, so there is no variant to read",
                ));
            }
            Ok(variant_value(Variant::new(
                value.as_ref().clone(),
                metadata.as_ref().clone(),
            )))
        }
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
        Value::String(s) => Ok(Variant::parse_json(s)
            .map(variant_value)
            .unwrap_or(Value::Null)),
        _ => Ok(Value::Null),
    }
}

fn variants_type(v: Value) -> Result<Value, ExecutionError> {
    match receiver(&v)? {
        None => Ok(Value::Null),
        Some(vv) => Ok(Value::String(Arc::new(
            type_label(vv.get_type()).to_string(),
        ))),
    }
}

fn variants_is_null(v: Value) -> Result<bool, ExecutionError> {
    // Coerces through `receiver` like every other accessor. Matching only the opaque form
    // answered false for the shapes a variant-typed field decodes to — the `Value::Map` an Avro
    // variant record and a protobuf confluent.type.Variant both convert to — which the untyped
    // signature admits, so a bare variant holding an explicit JSON null reported "not null".
    // A non-variant stays false rather than erroring: this predicate never fails.
    Ok(match receiver(&v) {
        Ok(Some(vv)) => vv.get_type() == Type::Null,
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
    Ok(vv
        .get_field_by_key(key)
        .map(variant_value)
        .unwrap_or(Value::Null))
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
        "string" => (vt == Type::String).then(|| {
            vv.get_string()
                .map(|s| Value::String(Arc::new(s)))
                .map_err(conv)
        }),
        "int" => matches!(vt, Type::Byte | Type::Short | Type::Int | Type::Long)
            .then(|| vv.get_long().map(Value::Int).map_err(conv)),
        "double" => match vt {
            Type::Float => Some(vv.get_float().map(|f| Value::Float(f as f64)).map_err(conv)),
            Type::Double => Some(vv.get_double().map(Value::Float).map_err(conv)),
            _ => None,
        },
        "boolean" => (vt == Type::Boolean).then(|| vv.get_boolean().map(Value::Bool).map_err(conv)),
        "decimal" => matches!(vt, Type::Decimal4 | Type::Decimal8 | Type::Decimal16).then(|| {
            vv.get_decimal_parts()
                .map_err(conv)
                .map(|(bytes, scale)| decimal_value(from_bytes_scale(&bytes, scale as i64)))
        }),
        "timestamp" => matches!(
            vt,
            Type::TimestampTz
                | Type::TimestampNtz
                | Type::TimestampNanosTz
                | Type::TimestampNanosNtz
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
        "bytes" => (vt == Type::Binary).then(|| {
            vv.get_binary()
                .map(|b| Value::Bytes(Arc::new(b)))
                .map_err(conv)
        }),
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
    const DOC: &str =
        r#"{"name":"alice","age":30,"explicit":null,"nested":{"x":1},"scores":[10,20,30]}"#;

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
            // variant(null) passes CEL null through (aligns with the Java reference), both
            // directly and composed with a navigation accessor over an absent field.
            "variant(null) == null",
            "variants.field(variant(variants.field(variants.parseJson(this), 'missing')), 'k') == null",
        ];
        for expr in cases {
            assert!(eval_bool(expr, doc_string()), "expr failed: {expr}");
        }
    }

    /// The CEL map shape a variant-typed field decodes to, with the given raw bytes.
    fn variant_map(value: Vec<u8>, metadata: Vec<u8>) -> Value {
        let mut m = std::collections::HashMap::new();
        m.insert(
            Key::String(Arc::new("value".to_string())),
            Value::Bytes(Arc::new(value)),
        );
        m.insert(
            Key::String(Arc::new("metadata".to_string())),
            Value::Bytes(Arc::new(metadata)),
        );
        Value::Map(Map { map: Arc::new(m) })
    }

    /// An *absent* variant — a Protobuf field left unset, or an Avro variant record whose byte
    /// fields are empty — carries no metadata, so there is nothing to read. It reads as CEL null
    /// and every accessor propagates that, rather than `Variant::new` accepting it and an
    /// accessor later indexing into a buffer that was never populated.
    #[test]
    fn absent_variant_reads_as_null() {
        let absent = variant_map(Vec::new(), Vec::new());
        let cases = [
            "variants.type(this) == null",
            // isNull is false, not an error: an absent variant is not a JSON null.
            "!variants.isNull(this)",
            "variants.field(this, 'name') == null",
            "variants.path(this, '$.name') == null",
            // The explicit constructor reports it as CEL null too, like `variant(null)`.
            "variant(this) == null",
        ];
        for expr in cases {
            assert!(eval_bool(expr, absent.clone()), "expr failed: {expr}");
        }
    }

    /// Absent must stay distinguishable from a variant that genuinely holds JSON null: the former
    /// is CEL null, the latter a present variant whose type is NULL.
    #[test]
    fn explicit_null_variant_is_not_absent() {
        assert!(eval_bool(
            "variants.isNull(variants.parseJson('null'))",
            doc_string()
        ));
        assert!(eval_bool(
            "variants.type(variants.parseJson('null')) != null",
            doc_string()
        ));
    }

    /// Passing empty metadata explicitly is a rule-authoring mistake rather than an absent field,
    /// so it is reported instead of yielding null.
    #[test]
    fn variant_from_empty_metadata_bytes_is_rejected() {
        let err = variant(Arguments(Arc::new(vec![
            Value::Bytes(Arc::new(Vec::new())),
            Value::Bytes(Arc::new(Vec::new())),
        ])))
        .expect_err("empty metadata must be rejected");
        assert!(
            format!("{err:?}").contains("metadata is empty"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn try_parse_json_empty_is_null() {
        // Empty / whitespace-only input is a soft failure: variants.tryParseJson maps the
        // VariantError to CEL null rather than propagating an error or panicking.
        assert_eq!(
            variants_try_parse_json(Value::String(Arc::new(String::new()))),
            Ok(Value::Null)
        );
        assert_eq!(
            variants_try_parse_json(Value::String(Arc::new("   ".to_string()))),
            Ok(Value::Null)
        );
        assert_eq!(
            variants_try_parse_json(Value::String(Arc::new("\t\n\r ".to_string()))),
            Ok(Value::Null)
        );
        // parseJson (the strict variant) surfaces the same input as an error.
        assert!(variants_parse_json(Value::String(Arc::new(String::new()))).is_err());
        // End to end through the CEL executor.
        assert!(eval_bool("variants.tryParseJson('') == null", doc_string()));
        assert!(eval_bool(
            "variants.tryParseJson('   ') == null",
            doc_string()
        ));
    }

    #[test]
    fn non_finite_round_trip_through_cel() {
        // Bareword non-finite literals parse and re-render as barewords through the CEL layer.
        assert!(eval_bool(
            "variants.toJson(variants.parseJson('NaN')) == 'NaN'",
            doc_string(),
        ));
        assert!(eval_bool(
            "variants.toJson(variants.parseJson('Infinity')) == 'Infinity'",
            doc_string(),
        ));
        assert!(eval_bool(
            "variants.toJson(variants.parseJson('-Infinity')) == '-Infinity'",
            doc_string(),
        ));
        // Out-of-range magnitude becomes Infinity.
        assert!(eval_bool(
            "variants.toJson(variants.parseJson('1e400')) == 'Infinity'",
            doc_string(),
        ));
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
    /// `variants.isNull` must coerce its receiver like every other accessor. It is declared over
    /// dyn, so a bare variant field reaches it; a receiver check that only accepts the opaque
    /// form answers false for the shapes a variant-typed field decodes to, reporting "not null"
    /// for a variant holding an explicit JSON null. A bare *object* cannot catch this — isNull on
    /// an object is false either way — so only a variant that is itself null discriminates.
    #[test]
    fn variant_is_null_coerces_bare_receiver() {
        use crate::rules::cel::cel_executor::{from_protobuf_value_for_test, from_serde_value};
        use crate::serdes::serde::SerdeValue;
        use apache_avro::types::Value as AvroValue;
        use prost_reflect::{DynamicMessage, Value as ProtoValue};

        for (json, expected) in [("null", true), ("5", false)] {
            let pv = Variant::parse_json(json).expect("parse");

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
            let avro_this = from_serde_value(&SerdeValue::Avro(record));

            let desc = crate::DESCRIPTOR_POOL
                .get_message_by_name("confluent.type.Variant")
                .expect("variant.proto is compiled into the descriptor pool");
            let mut msg = DynamicMessage::new(desc);
            msg.set_field_by_name(
                "metadata",
                ProtoValue::Bytes(pv.metadata_bytes().to_vec().into()),
            );
            msg.set_field_by_name("value", ProtoValue::Bytes(pv.value_bytes().to_vec().into()));
            let proto_this = from_protobuf_value_for_test(&ProtoValue::Message(msg));

            for (kind, this) in [("avro", avro_this), ("proto", proto_this)] {
                assert_eq!(
                    eval_bool("variants.isNull(this)", this.clone()),
                    expected,
                    "{kind}: bare variants.isNull on {json}"
                );
                // The wrapped form has always worked and must keep working.
                assert_eq!(
                    eval_bool("variants.isNull(variant(this))", this.clone()),
                    expected,
                    "{kind}: wrapped variants.isNull on {json}"
                );
            }
        }
    }

    /// Cross-client parity: a variant value is usable with the `variants.*` accessors with **no
    /// `variant(...)` call**, in both formats, and the wrapped form keeps working alongside it.
    /// The accessors take an untyped `Arguments` and coerce through `receiver`, which accepts
    /// both the opaque form and the `Value::Map` that the Avro and Protobuf paths produce.
    #[test]
    fn variant_needs_no_constructor() {
        use crate::rules::cel::cel_executor::{from_protobuf_value_for_test, from_serde_value};
        use crate::serdes::serde::SerdeValue;
        use apache_avro::types::Value as AvroValue;
        use prost_reflect::{DynamicMessage, Value as ProtoValue};

        let pv = Variant::parse_json(DOC).expect("parse");

        // Avro: a variant record through the real Avro -> CEL path.
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
        let avro_this = from_serde_value(&SerdeValue::Avro(record));

        // Protobuf: a confluent.type.Variant message through the real Protobuf -> CEL path.
        let desc = crate::DESCRIPTOR_POOL
            .get_message_by_name("confluent.type.Variant")
            .expect("variant.proto is compiled into the descriptor pool");
        let mut msg = DynamicMessage::new(desc);
        msg.set_field_by_name(
            "metadata",
            ProtoValue::Bytes(pv.metadata_bytes().to_vec().into()),
        );
        msg.set_field_by_name("value", ProtoValue::Bytes(pv.value_bytes().to_vec().into()));
        let proto_this = from_protobuf_value_for_test(&ProtoValue::Message(msg));

        for (kind, this) in [("avro", avro_this), ("proto", proto_this)] {
            for expr in [
                // Bare: no constructor call.
                "variants.type(this) == 'object'",
                "variants.as(variants.field(this, 'name'), 'string') == 'alice'",
                "variants.as(variants.path(this, '$.nested.x'), 'int') == 1",
                // The wrapped form must keep working (variant(...) re-entry).
                "variants.as(variants.field(variant(this), 'name'), 'string') == 'alice'",
                // A missing key is CEL null, not an error.
                "variants.field(this, 'nope') == null",
            ] {
                assert!(eval_bool(expr, this.clone()), "{kind}: {expr}");
            }
            // Negative control.
            assert!(
                !eval_bool(
                    "variants.as(variants.field(this, 'name'), 'string') == 'bob'",
                    this.clone(),
                ),
                "{kind}: negative control held"
            );
        }
    }
}
