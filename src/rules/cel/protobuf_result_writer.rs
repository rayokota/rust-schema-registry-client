//! Rebuilds a protobuf message from the map a message-level `CEL` transform returned.
//!
//! A rule that returns a map is returning **the whole new message**: the transform has replace
//! semantics, not merge. Three consequences a rule author needs to know, and every client has
//! to match:
//!
//! - a field the rule does not name is **dropped**, so a rule naming only the field it changes
//!   discards the rest;
//! - a `null` in the map **clears** its field;
//! - echoing a field that was absent **materialises** it, because reading it produced a value.
//!   Preserve absence with `has(x) ? x : null`.
//!
//! Without this a message-level transform fell through [`super::cel_executor::to_serde_value`],
//! whose protobuf arm turns a CEL map into a `prost_reflect::Value::Map` and whose catch-all
//! turns anything it does not recognise into empty bytes - so decimal and timestamp were not
//! merely unwritten, they were replaced with `b""`.
//!
//! **Mechanism note.** The JVM client rebuilds by rendering the result to JSON and parsing it
//! back. This builds the message directly through `prost_reflect`, because a JSON round trip
//! would base64 every bytes field and format every timestamp only to parse them straight back.
//! The behaviours the JVM client gets free from the JSON mapping - null clearing a field, and a
//! key matching either the declared or the JSON name - are reproduced explicitly below.

use cel::objects::Key;
use cel::Value;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage};

use crate::rules::cel::decimal_funcs::{to_decimal, DECIMAL_TYPE_NAME};
use crate::rules::cel::variant_funcs::{to_variant, VARIANT_TYPE_NAME};
use crate::serdes::serde::SerdeError;

const TIMESTAMP_TYPE_NAME: &str = "google.protobuf.Timestamp";

/// Rebuilds `input`'s message type from `value`, or returns `None` when `value` is not a map
/// (a CONDITION's bool, or a rule that returned a scalar).
pub(crate) fn write_back_protobuf(
    input: &DynamicMessage,
    value: &Value,
) -> Result<Option<DynamicMessage>, SerdeError> {
    let Value::Map(map) = value else {
        return Ok(None);
    };
    let desc = input.descriptor();
    let mut out = DynamicMessage::new(desc.clone());
    fill(&mut out, &desc, map)?;
    Ok(Some(out))
}

fn fill(
    out: &mut DynamicMessage,
    desc: &MessageDescriptor,
    map: &cel::objects::Map,
) -> Result<(), SerdeError> {
    for (key, value) in map.map.iter() {
        let Key::String(name) = key else {
            continue;
        };
        let Some(fd) = find_field(desc, name) else {
            // A key the schema does not declare has nowhere to go. Dropping it matches the JVM
            // client, whose JSON parse ignores unknown fields.
            continue;
        };
        if matches!(value, Value::Null) {
            // An explicit null clears the field, which is how a rule preserves an absent value
            // across a transform that echoes it.
            out.clear_field(&fd);
            continue;
        }
        let converted = to_field_value(&fd, value)?;
        out.set_field(&fd, converted);
    }
    Ok(())
}

/// Resolves a result key by declared name, then by JSON name: a rule may legitimately return
/// either, so matching only the declared name would silently skip a field like `total_amount`.
fn find_field(desc: &MessageDescriptor, name: &str) -> Option<FieldDescriptor> {
    desc.get_field_by_name(name)
        .or_else(|| desc.get_field_by_json_name(name))
}

fn to_field_value(
    fd: &FieldDescriptor,
    value: &Value,
) -> Result<prost_reflect::Value, SerdeError> {
    if fd.is_list() {
        let Value::List(items) = value else {
            return Ok(prost_reflect::Value::List(Vec::new()));
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items.iter() {
            if matches!(item, Value::Null) {
                continue;
            }
            out.push(scalar_or_message(fd, item)?);
        }
        return Ok(prost_reflect::Value::List(out));
    }
    if fd.is_map() {
        let Value::Map(entries) = value else {
            return Ok(prost_reflect::Value::Map(Default::default()));
        };
        let value_fd = fd
            .kind()
            .as_message()
            .and_then(|m| m.get_field_by_name("value"))
            .ok_or_else(|| SerdeError::Rule("map field has no value type".to_string()))?;
        let mut out = std::collections::HashMap::new();
        for (k, v) in entries.map.iter() {
            if matches!(v, Value::Null) {
                continue;
            }
            let Key::String(name) = k else { continue };
            out.insert(
                prost_reflect::MapKey::String(name.to_string()),
                scalar_or_message(&value_fd, v)?,
            );
        }
        return Ok(prost_reflect::Value::Map(out));
    }
    scalar_or_message(fd, value)
}

fn scalar_or_message(
    fd: &FieldDescriptor,
    value: &Value,
) -> Result<prost_reflect::Value, SerdeError> {
    if let Kind::Message(md) = fd.kind() {
        return Ok(prost_reflect::Value::Message(build_message(&md, value)?));
    }
    scalar(fd, value)
}

/// Builds one message value, inverting how the CEL binding read it.
///
/// The three value types do not arrive as maps of their own fields: this client carries a
/// decimal and a variant as CEL opaques and a timestamp as a CEL timestamp, whether the rule
/// computed a new value or merely echoed the field.
/// Encodes a CEL result into one of the value-type messages, when the field is one.
///
/// A `CEL_FIELD` rule over a decimal or timestamp field is handed the whole message and hands
/// back a CEL value, which has to be encoded rather than passed to the generic conversion -
/// that one has no arm for a decimal opaque and turns it into empty bytes.
pub(crate) fn write_back_value_type(
    desc: &MessageDescriptor,
    value: &Value,
) -> Option<Result<DynamicMessage, SerdeError>> {
    let name = desc.full_name();
    if name == DECIMAL_TYPE_NAME || name == TIMESTAMP_TYPE_NAME || name == VARIANT_TYPE_NAME {
        return Some(build_message(desc, value));
    }
    None
}

fn build_message(md: &MessageDescriptor, value: &Value) -> Result<DynamicMessage, SerdeError> {
    let full_name = md.full_name();

    if full_name == DECIMAL_TYPE_NAME {
        // The decimal keeps the scale it computed. Unlike Avro, where the schema fixes the
        // scale and the Avro writer re-quantizes to it, a protobuf confluent.type.Decimal
        // carries its own scale field - so there is nothing to quantize against.
        let decimal = to_decimal(value).map_err(|e| SerdeError::Rule(e.to_string()))?;
        let (unscaled, exponent) = decimal.into_bigint_and_exponent();
        let mut out = DynamicMessage::new(md.clone());
        set_named(&mut out, md, "value",
            prost_reflect::Value::Bytes(unscaled.to_signed_bytes_be().into()));
        set_named(&mut out, md, "scale", prost_reflect::Value::I32(exponent as i32));
        return Ok(out);
    }

    if full_name == VARIANT_TYPE_NAME {
        let variant = to_variant(value)
            .map_err(|e| SerdeError::Rule(e.to_string()))?
            .ok_or_else(|| SerdeError::Rule(
                "cannot write an absent variant; use null to clear the field".to_string()))?;
        let mut out = DynamicMessage::new(md.clone());
        set_named(&mut out, md, "metadata",
            prost_reflect::Value::Bytes(variant.metadata_bytes().to_vec().into()));
        set_named(&mut out, md, "value",
            prost_reflect::Value::Bytes(variant.value_bytes().to_vec().into()));
        return Ok(out);
    }

    if full_name == TIMESTAMP_TYPE_NAME {
        let Value::Timestamp(ts) = value else {
            return Err(SerdeError::Rule(format!(
                "expected a timestamp for {full_name}"
            )));
        };
        let mut out = DynamicMessage::new(md.clone());
        set_named(&mut out, md, "seconds", prost_reflect::Value::I64(ts.timestamp()));
        set_named(&mut out, md, "nanos",
            prost_reflect::Value::I32(ts.timestamp_subsec_nanos() as i32));
        return Ok(out);
    }

    // A nested message the rule rebuilt field by field.
    let Value::Map(map) = value else {
        return Err(SerdeError::Rule(format!(
            "cannot write this value to {full_name}"
        )));
    };
    let mut out = DynamicMessage::new(md.clone());
    fill(&mut out, md, map)?;
    Ok(out)
}

fn set_named(
    out: &mut DynamicMessage,
    md: &MessageDescriptor,
    name: &str,
    value: prost_reflect::Value,
) {
    if let Some(fd) = md.get_field_by_name(name) {
        out.set_field(&fd, value);
    }
}

/// Narrows a CEL value to what the field's kind accepts. CEL has one integer type, so a
/// narrower field needs converting back rather than rejecting.
fn scalar(fd: &FieldDescriptor, value: &Value) -> Result<prost_reflect::Value, SerdeError> {
    let err = || SerdeError::Rule(format!("cannot write this value to field {}", fd.name()));
    Ok(match (fd.kind(), value) {
        (Kind::Bool, Value::Bool(b)) => prost_reflect::Value::Bool(*b),
        (Kind::String, Value::String(s)) => prost_reflect::Value::String(s.to_string()),
        (Kind::Bytes, Value::Bytes(b)) => {
            prost_reflect::Value::Bytes(b.to_vec().into())
        }
        (Kind::Float, v) => prost_reflect::Value::F32(as_f64(v).ok_or_else(err)? as f32),
        (Kind::Double, v) => prost_reflect::Value::F64(as_f64(v).ok_or_else(err)?),
        (Kind::Int32 | Kind::Sint32 | Kind::Sfixed32, v) => {
            prost_reflect::Value::I32(as_i64(v).ok_or_else(err)? as i32)
        }
        (Kind::Int64 | Kind::Sint64 | Kind::Sfixed64, v) => {
            prost_reflect::Value::I64(as_i64(v).ok_or_else(err)?)
        }
        (Kind::Uint32 | Kind::Fixed32, v) => {
            prost_reflect::Value::U32(as_i64(v).ok_or_else(err)? as u32)
        }
        (Kind::Uint64 | Kind::Fixed64, v) => {
            prost_reflect::Value::U64(as_i64(v).ok_or_else(err)? as u64)
        }
        (Kind::Enum(_), v) => {
            prost_reflect::Value::EnumNumber(as_i64(v).ok_or_else(err)? as i32)
        }
        _ => return Err(err()),
    })
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        Value::UInt(u) => Some(*u as i64),
        Value::Float(f) => Some(*f as i64),
        _ => None,
    }
}

fn as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Float(f) => Some(*f),
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        _ => None,
    }
}
