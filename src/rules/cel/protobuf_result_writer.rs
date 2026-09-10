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

use cel::Value;
use cel::objects::Key;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage};

use crate::rules::cel::decimal_funcs::{DECIMAL_TYPE_NAME, to_decimal};
use crate::rules::cel::variant_funcs::{VARIANT_TYPE_NAME, to_variant};
use crate::serdes::decimal_utils::decimal_parts;
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
    check_result_keys(desc, map)?;
    for (key, value) in map.map.iter() {
        let name = result_key_name(key);
        let Some(fd) = find_field(desc, &name) else {
            // The JVM client parses with a bare `JsonFormat.parser()` - `ignoringUnknownFields`
            // is off - so `mergeMessage` throws "Cannot find field: X in message Y". Dropping
            // the key instead would be worse than merely lenient: this writer has replace
            // semantics, so a mistyped name would silently delete the field it meant to set.
            return Err(SerdeError::Rule(format!(
                "result names field {name}, which {} does not declare",
                desc.full_name()
            )));
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

/// Rejects a result whose keys do not name a single field each.
///
/// Two problems, both of which would otherwise resolve by hash order and so vary between runs:
///
/// - **The same field twice.** [`find_field`] accepts a field's declared name *and* its JSON
///   name, so `total_amount` and `totalAmount` are the same field. Applying both keeps whichever
///   was visited last. `JsonFormat.mergeField` rejects this ("Field X has already been set"), and
///   its duplicate test precedes any null handling, so a `null` counts here.
/// - **Two members of one `oneof`.** Setting a member clears its siblings, so applying both keeps
///   whichever was visited last. `JsonFormat.mergeOneofField` rejects this too, but only after
///   returning early for a null - a null is "treated as absent" there - so a null does *not*
///   count, which also matches this writer's own rule that a null clears rather than sets.
///
/// A proto3 `optional` field sits in a synthetic oneof of exactly one member and so can never
/// collide with a sibling.
fn check_result_keys(desc: &MessageDescriptor, map: &cel::objects::Map) -> Result<(), SerdeError> {
    use std::collections::HashMap;
    let mut fields: HashMap<u32, String> = HashMap::new();
    let mut oneofs: HashMap<String, String> = HashMap::new();
    for (key, value) in map.map.iter() {
        let name = result_key_name(key);
        let Some(fd) = find_field(desc, &name) else {
            continue;
        };
        if let Some(first) = fields.insert(fd.number(), name.clone())
            && first != name
        {
            let (a, b) = ordered(first, name);
            return Err(SerdeError::Rule(format!(
                "result names field {} twice, as {a} and {b}",
                fd.full_name()
            )));
        }
        if matches!(value, Value::Null) {
            continue;
        }
        if let Some(oneof) = fd.containing_oneof()
            && let Some(first) = oneofs.insert(oneof.full_name().to_string(), fd.name().to_string())
            && first != *fd.name()
        {
            let (a, b) = ordered(first, fd.name().to_string());
            return Err(SerdeError::Rule(format!(
                "result sets more than one member of oneof {}: {a} and {b}",
                oneof.full_name()
            )));
        }
    }
    Ok(())
}

/// Two names in a stable order, so an error does not vary with hash iteration order.
fn ordered(x: String, y: String) -> (String, String) {
    if x <= y { (x, y) } else { (y, x) }
}

/// The field name a result key stands for.
///
/// A CEL map key may be an int, uint or bool as well as a string, and the JVM client reaches the
/// field the same way: `ProtobufResultWriter.convert` looks it up as `String.valueOf(key)`, and
/// Jackson then renders the key as that same text for the protobuf JSON parse. So a non-string
/// key is not skipped, it simply has to name a field like any other - and normally does not,
/// which the unknown-field check then reports. Skipping it instead would drop the entry silently,
/// and with replace semantics that deletes the field.
fn result_key_name(key: &Key) -> String {
    match key {
        Key::String(s) => s.to_string(),
        Key::Int(i) => i.to_string(),
        Key::Uint(u) => u.to_string(),
        Key::Bool(b) => b.to_string(),
    }
}

/// Resolves a result key by declared name, then by JSON name: a rule may legitimately return
/// either, so matching only the declared name would silently skip a field like `total_amount`.
fn find_field(desc: &MessageDescriptor, name: &str) -> Option<FieldDescriptor> {
    desc.get_field_by_name(name)
        .or_else(|| desc.get_field_by_json_name(name))
}

fn to_field_value(fd: &FieldDescriptor, value: &Value) -> Result<prost_reflect::Value, SerdeError> {
    if fd.is_list() {
        // A shape mismatch is a mistake in the rule, not an instruction to empty the field.
        // Coercing it to an empty list would let `{"items": 1}` silently delete every element.
        // `JsonFormat.mergeRepeatedField` rejects a non-array, so reject it here too.
        let Value::List(items) = value else {
            return Err(SerdeError::Rule(format!(
                "expected a list for repeated field {}",
                fd.full_name()
            )));
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items.iter() {
            // A repeated field cannot hold a null, and dropping one would silently shorten the
            // list. `JsonFormat.mergeRepeatedField`: "Repeated field elements cannot be null".
            if matches!(item, Value::Null) {
                return Err(SerdeError::Rule(format!(
                    "repeated field {} cannot contain a null element",
                    fd.full_name()
                )));
            }
            out.push(scalar_or_message(fd, item)?);
        }
        return Ok(prost_reflect::Value::List(out));
    }
    if fd.is_map() {
        // As for a repeated field: `JsonFormat.mergeMapField` rejects a non-object rather than
        // treating it as an instruction to clear the map.
        let Value::Map(entries) = value else {
            return Err(SerdeError::Rule(format!(
                "expected a map for field {}",
                fd.full_name()
            )));
        };
        let value_fd = fd
            .kind()
            .as_message()
            .and_then(|m| m.get_field_by_name("value"))
            .ok_or_else(|| SerdeError::Rule("map field has no value type".to_string()))?;
        let key_fd = fd
            .kind()
            .as_message()
            .and_then(|m| m.get_field_by_name("key"))
            .ok_or_else(|| SerdeError::Rule("map field has no key type".to_string()))?;
        let mut out = std::collections::HashMap::new();
        for (k, v) in entries.map.iter() {
            // `JsonFormat.mergeMapField`: "Map value cannot be null." Skipping the entry would
            // silently drop a key the rule named.
            if matches!(v, Value::Null) {
                return Err(SerdeError::Rule(format!(
                    "map field {} cannot contain a null value",
                    fd.full_name()
                )));
            }
            out.insert(map_key(&key_fd, k)?, scalar_or_message(&value_fd, v)?);
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
        // Through decimal_parts, the single definition, rather than repeating the arithmetic
        // here. This block had its own copy - so the coefficient-width guard lived on
        // `to_proto_decimal`, which nothing outside its unit tests calls, while the path a
        // *rule* takes had none. The reference's DecimalUtils.fromBigDecimal carries the digit
        // count, so leaving precision at its default would also have an identity transform
        // rewrite the field to 0. confluent.type.Decimal.precision is uint32.
        let parts = decimal_parts(&decimal)?;
        let mut out = DynamicMessage::new(md.clone());
        set_named(
            &mut out,
            md,
            "value",
            prost_reflect::Value::Bytes(parts.value.into()),
        );
        set_named(
            &mut out,
            md,
            "scale",
            prost_reflect::Value::I32(parts.scale),
        );
        set_named(
            &mut out,
            md,
            "precision",
            prost_reflect::Value::U32(parts.precision),
        );
        return Ok(out);
    }

    if full_name == VARIANT_TYPE_NAME {
        let variant = to_variant(value)
            .map_err(|e| SerdeError::Rule(e.to_string()))?
            .ok_or_else(|| {
                SerdeError::Rule(
                    "cannot write an absent variant; use null to clear the field".to_string(),
                )
            })?;
        let mut out = DynamicMessage::new(md.clone());
        set_named(
            &mut out,
            md,
            "metadata",
            prost_reflect::Value::Bytes(variant.metadata_bytes().to_vec().into()),
        );
        set_named(
            &mut out,
            md,
            "value",
            // Slice from this node's offset, not from 0. Trailing sibling bytes are kept so
            // the encoding matches the Java reference, which writes ByteBuffer
            // position..limit (see VariantFormat.slice and ProtobufResultWriter.toBytes).
            prost_reflect::Value::Bytes(variant.standalone_value_bytes().into()),
        );
        return Ok(out);
    }

    if full_name == TIMESTAMP_TYPE_NAME {
        let Value::Timestamp(ts) = value else {
            return Err(SerdeError::Rule(format!(
                "expected a timestamp for {full_name}"
            )));
        };
        let mut out = DynamicMessage::new(md.clone());
        set_named(
            &mut out,
            md,
            "seconds",
            prost_reflect::Value::I64(ts.timestamp()),
        );
        set_named(
            &mut out,
            md,
            "nanos",
            prost_reflect::Value::I32(ts.timestamp_subsec_nanos() as i32),
        );
        return Ok(out);
    }

    // The inverse of cel_executor::unwrap_well_known: those types were read as bare scalars,
    // so the rule hands back a scalar and there is no field map to rebuild from.
    if let Some(wrapped) = build_well_known(md, value)? {
        return Ok(wrapped);
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

/// Re-wraps a scalar CEL value into the well-known message it was unwrapped from. Returns
/// `Ok(None)` when the descriptor is not one of those types, so the caller falls through to
/// rebuilding a message from a field map.
fn build_well_known(
    md: &MessageDescriptor,
    value: &Value,
) -> Result<Option<DynamicMessage>, SerdeError> {
    let err = || SerdeError::Rule(format!("cannot write this value to {}", md.full_name()));
    let mut out = DynamicMessage::new(md.clone());
    match md.full_name() {
        "google.protobuf.BoolValue" => {
            let Value::Bool(b) = value else {
                return Err(err());
            };
            set_named(&mut out, md, "value", prost_reflect::Value::Bool(*b));
        }
        "google.protobuf.StringValue" => {
            let Value::String(v) = value else {
                return Err(err());
            };
            set_named(
                &mut out,
                md,
                "value",
                prost_reflect::Value::String(v.to_string()),
            );
        }
        "google.protobuf.BytesValue" => {
            let Value::Bytes(v) = value else {
                return Err(err());
            };
            set_named(
                &mut out,
                md,
                "value",
                prost_reflect::Value::Bytes(v.to_vec().into()),
            );
        }
        "google.protobuf.Int32Value" => {
            let v = as_i64(value).ok_or_else(err)?;
            set_named(
                &mut out,
                md,
                "value",
                prost_reflect::Value::I32(i32::try_from(v).map_err(|_| err())?),
            );
        }
        "google.protobuf.Int64Value" => {
            let v = as_i64(value).ok_or_else(err)?;
            set_named(&mut out, md, "value", prost_reflect::Value::I64(v));
        }
        "google.protobuf.UInt32Value" => {
            let v = as_u64(value).ok_or_else(err)?;
            set_named(
                &mut out,
                md,
                "value",
                prost_reflect::Value::U32(u32::try_from(v).map_err(|_| err())?),
            );
        }
        "google.protobuf.UInt64Value" => {
            let v = as_u64(value).ok_or_else(err)?;
            set_named(
                &mut out,
                md,
                "value",
                prost_reflect::Value::U64(u64::try_from(v).map_err(|_| err())?),
            );
        }
        "google.protobuf.FloatValue" => {
            // Same check as a plain float field: `mergeWrapper` sets the wrapper's `value`
            // through `parseFieldValue`, whose FLOAT case is `parseFloat` - so the wrapper gets
            // the range check too.
            let v = narrow_to_f32(as_f64(value).ok_or_else(err)?, md.full_name())?;
            set_named(&mut out, md, "value", prost_reflect::Value::F32(v));
        }
        "google.protobuf.DoubleValue" => {
            let v = as_f64(value).ok_or_else(err)?;
            set_named(&mut out, md, "value", prost_reflect::Value::F64(v));
        }
        "google.protobuf.Duration" => {
            let Value::Duration(d) = value else {
                return Err(err());
            };
            set_named(
                &mut out,
                md,
                "seconds",
                prost_reflect::Value::I64(d.num_seconds()),
            );
            // subsec_nanos, not num_nanoseconds: the latter is None beyond ~292 years, while a
            // google.protobuf.Duration validly spans ~10,000 years.
            set_named(
                &mut out,
                md,
                "nanos",
                prost_reflect::Value::I32(d.subsec_nanos()),
            );
        }
        _ => return Ok(None),
    }
    Ok(Some(out))
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

/// Converts a CEL map key to the type the map entry declares. Protobuf map keys are integral,
/// bool or string, and the CEL side keeps whichever it read, so writing every key as a string
/// would drop each non-string entry and silently shrink the map.
fn map_key(key_fd: &FieldDescriptor, key: &Key) -> Result<prost_reflect::MapKey, SerdeError> {
    let err = || {
        SerdeError::Rule(format!(
            "cannot write this map key to field {}",
            key_fd.name()
        ))
    };
    let as_int = |k: &Key| -> Option<i64> {
        match k {
            Key::Int(i) => Some(*i),
            Key::Uint(u) => i64::try_from(*u).ok(),
            _ => None,
        }
    };
    // Unsigned key kinds convert from Key::Uint directly: going via i64 first would reject
    // every key above i64::MAX, so an identity transform could not round-trip them.
    let as_uint = |k: &Key| -> Option<u64> {
        match k {
            Key::Uint(u) => Some(*u),
            Key::Int(i) => u64::try_from(*i).ok(),
            _ => None,
        }
    };
    Ok(match key_fd.kind() {
        Kind::Bool => match key {
            Key::Bool(b) => prost_reflect::MapKey::Bool(*b),
            _ => return Err(err()),
        },
        Kind::String => match key {
            Key::String(s) => prost_reflect::MapKey::String(s.to_string()),
            _ => return Err(err()),
        },
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => prost_reflect::MapKey::I32(
            i32::try_from(as_int(key).ok_or_else(err)?).map_err(|_| err())?,
        ),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => {
            prost_reflect::MapKey::I64(as_int(key).ok_or_else(err)?)
        }
        Kind::Uint32 | Kind::Fixed32 => prost_reflect::MapKey::U32(
            u32::try_from(as_uint(key).ok_or_else(err)?).map_err(|_| err())?,
        ),
        Kind::Uint64 | Kind::Fixed64 => prost_reflect::MapKey::U64(as_uint(key).ok_or_else(err)?),
        _ => return Err(err()),
    })
}

/// Narrows a CEL value to what the field's kind accepts. CEL has one integer type, so a
/// narrower field needs converting back rather than rejecting.
fn scalar(fd: &FieldDescriptor, value: &Value) -> Result<prost_reflect::Value, SerdeError> {
    let err = || SerdeError::Rule(format!("cannot write this value to field {}", fd.name()));
    Ok(match (fd.kind(), value) {
        (Kind::Bool, Value::Bool(b)) => prost_reflect::Value::Bool(*b),
        (Kind::String, Value::String(s)) => prost_reflect::Value::String(s.to_string()),
        (Kind::Bytes, Value::Bytes(b)) => prost_reflect::Value::Bytes(b.to_vec().into()),
        (Kind::Float, v) => {
            prost_reflect::Value::F32(narrow_to_f32(as_f64(v).ok_or_else(err)?, fd.name())?)
        }
        (Kind::Double, v) => prost_reflect::Value::F64(as_f64(v).ok_or_else(err)?),
        // CEL has one 64-bit integer type, so writing to a narrower field is a conversion that
        // can fail. Reject an out-of-range value rather than truncating it, which would write a
        // numerically different message (2147483648 -> -2147483648).
        (Kind::Int32 | Kind::Sint32 | Kind::Sfixed32, v) => {
            prost_reflect::Value::I32(i32::try_from(as_i64(v).ok_or_else(err)?).map_err(|_| err())?)
        }
        (Kind::Int64 | Kind::Sint64 | Kind::Sfixed64, v) => {
            prost_reflect::Value::I64(as_i64(v).ok_or_else(err)?)
        }
        (Kind::Uint32 | Kind::Fixed32, v) => {
            prost_reflect::Value::U32(u32::try_from(as_u64(v).ok_or_else(err)?).map_err(|_| err())?)
        }
        (Kind::Uint64 | Kind::Fixed64, v) => prost_reflect::Value::U64(as_u64(v).ok_or_else(err)?),
        (Kind::Enum(ed), v) => {
            // `JsonFormat.parseEnum` takes a value's name as well as its number, so a rule may
            // legitimately write "ACTIVE" rather than 1.
            if let Value::String(name) = v {
                let value = ed.get_value_by_name(name).ok_or_else(|| {
                    SerdeError::Rule(format!(
                        "invalid enum value {name} for enum type {}",
                        ed.full_name()
                    ))
                })?;
                return Ok(prost_reflect::Value::EnumNumber(value.number()));
            }
            let number = i32::try_from(as_i64(v).ok_or_else(err)?).map_err(|_| err())?;
            // An open (proto3) enum carries an unknown number through, which is what
            // findValueByNumberCreatingIfUnknown does; a closed (proto2) one rejects it, which
            // is findValueByNumber returning null and falling through to the throw.
            if ed.parent_file().syntax() == prost_reflect::Syntax::Proto2
                && ed.get_value(number).is_none()
            {
                return Err(SerdeError::Rule(format!(
                    "invalid enum value {number} for closed enum type {}",
                    ed.full_name()
                )));
            }
            prost_reflect::Value::EnumNumber(number)
        }
        _ => return Err(err()),
    })
}

/// Narrows a double to a float the way `JsonFormat.parseFloat` does: a finite value outside the
/// float range is an error rather than an infinity, with the same 1e-6 slack that method allows.
/// NaN and the infinities pass through - it accepts those explicitly.
///
/// Shared by the plain float field and the `FloatValue` wrapper because Java reaches both through
/// `parseFieldValue`; keeping one copy is what stops the two from drifting apart.
fn narrow_to_f32(d: f64, what: &str) -> Result<f32, SerdeError> {
    const EPSILON: f64 = 1e-6;
    let limit = f32::MAX as f64 * (1.0 + EPSILON);
    if d.is_finite() && (d > limit || d < -limit) {
        return Err(SerdeError::Rule(format!(
            "out of range float value for {what}: {d}"
        )));
    }
    Ok(d as f32)
}

fn as_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Int(i) => Some(*i),
        // A u64 above i64::MAX has no i64 form, and `as` would wrap it to a negative.
        Value::UInt(u) => i64::try_from(*u).ok(),
        Value::Float(f) => float_to_int(*f),
        _ => None,
    }
}

/// A CEL integer as a `u64`, for the unsigned field kinds. Kept separate from [`as_i64`] because
/// routing an unsigned value through `i64` would reject everything above `i64::MAX` - half of the
/// protobuf `uint64` domain, which an identity transform has to round-trip.
fn as_u64(value: &Value) -> Option<u64> {
    match value {
        Value::UInt(u) => Some(*u),
        Value::Int(i) => u64::try_from(*i).ok(),
        Value::Float(f) => u64::try_from(float_to_int(*f)?).ok(),
        _ => None,
    }
}

/// A float as an integer, only when it is exactly integral and inside the `i64` range.
///
/// The upper bound is exclusive of 2^63: `i64::MAX as f64` rounds *up* to 2^63, so comparing
/// against it would admit 2^63 itself, which `as i64` then saturates to `i64::MAX` - silently
/// changing the value. `-(i64::MIN as f64)` is exactly 2^63.
fn float_to_int(f: f64) -> Option<i64> {
    let truncated = f.trunc();
    #[allow(clippy::float_cmp)]
    if truncated == f && truncated >= i64::MIN as f64 && truncated < -(i64::MIN as f64) {
        Some(truncated as i64)
    } else {
        None
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

#[cfg(test)]
mod tests {
    use super::{as_i64, as_u64, check_result_keys, fill, float_to_int, to_field_value};
    use cel::Value;
    use cel::objects::{Key, Map};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn keys(pairs: Vec<(&str, Value)>) -> Map {
        Map {
            map: Arc::new(
                pairs
                    .into_iter()
                    .map(|(k, v)| (Key::String(Arc::new(k.to_string())), v))
                    .collect::<HashMap<Key, Value>>(),
            ),
        }
    }

    /// The coefficient-width guard has to sit on the path a *rule* takes.
    ///
    /// It was added to `serdes::decimal_utils::to_proto_decimal`, which turns out to have no
    /// caller outside its own unit tests - `build_message` here carried a second copy of the
    /// same arithmetic, unguarded. So a rule could emit a coefficient of any width and pay the
    /// quadratic base-256 conversion the guard exists to prevent. The two now share
    /// `decimal_parts`, and this exercises `write_back_value_type`, the production entry point.
    ///
    /// 4300 is CPython's `int_max_str_digits`, adopted across the family so every client agrees
    /// on which decimals can be written; CEL's documented decimal precision is 38 digits.
    #[test]
    fn the_coefficient_guard_covers_the_rule_write_back_path() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        let desc = crate::DESCRIPTOR_POOL
            .get_message_by_name("confluent.type.Decimal")
            .expect("decimal.proto is compiled into the descriptor pool");

        // Past the ceiling: refused, and the error names the limit.
        let wide = BigDecimal::from_str(&"9".repeat(5000)).unwrap();
        let err = super::write_back_value_type(
            &desc,
            &crate::rules::cel::decimal_funcs::decimal_value(wide),
        )
        .expect("a Decimal descriptor is handled")
        .unwrap_err();
        assert!(
            err.to_string().contains("4300"),
            "expected the 4300-digit limit in: {err}"
        );

        // Just inside it: written, with the digit count as precision.
        for digits in [1usize, 38, 4300] {
            let d = BigDecimal::from_str(&"9".repeat(digits)).unwrap();
            let out = super::write_back_value_type(
                &desc,
                &crate::rules::cel::decimal_funcs::decimal_value(d),
            )
            .expect("handled")
            .expect("should write");
            let precision = out
                .get_field_by_name("precision")
                .expect("precision field")
                .as_u32()
                .expect("uint32");
            assert_eq!(precision as usize, digits, "precision for {digits} digits");
        }
    }

    #[test]
    fn a_non_string_key_names_a_field_by_its_text() {
        // A CEL map key may be an int, uint or bool. The JVM client looks the field up as
        // String.valueOf(key) and Jackson renders the same text for the JSON parse, so such a
        // key is not skipped - it just normally names no field, which is then reported.
        // Skipping would drop the entry silently, deleting the field under replace semantics.
        let desc = crate::TEST_DESCRIPTOR_POOL
            .get_message_by_name("test.TestMessage")
            .unwrap();
        for key in [Key::Int(1), Key::Uint(1), Key::Bool(true)] {
            let mut out = prost_reflect::DynamicMessage::new(desc.clone());
            let map = Map {
                map: Arc::new(HashMap::from([(
                    key.clone(),
                    Value::String(Arc::new("x".to_string())),
                )])),
            };
            let err = fill(&mut out, &desc, &map).unwrap_err();
            let text = super::result_key_name(&key);
            assert!(
                err.to_string().contains(&text),
                "expected the key text {text} in: {err}"
            );
        }
    }

    #[test]
    fn rejects_a_key_the_schema_does_not_declare() {
        // The JVM client parses with a bare JsonFormat.parser(), so mergeMessage throws
        // "Cannot find field". Dropping the key would be worse than lenient here: with replace
        // semantics a mistyped name silently deletes the field it meant to set.
        let desc = crate::TEST_DESCRIPTOR_POOL
            .get_message_by_name("test.TestMessage")
            .unwrap();
        let mut out = prost_reflect::DynamicMessage::new(desc.clone());
        let err = fill(
            &mut out,
            &desc,
            &keys(vec![(
                "test_strng",
                Value::String(Arc::new("x".to_string())),
            )]),
        )
        .unwrap_err();
        assert!(err.to_string().contains("test_strng"), "{err}");

        // The correct spelling, and the JSON name, both work.
        for name in ["test_string", "testString"] {
            let mut out = prost_reflect::DynamicMessage::new(desc.clone());
            assert!(
                fill(
                    &mut out,
                    &desc,
                    &keys(vec![(name, Value::String(Arc::new("x".to_string())))])
                )
                .is_ok(),
                "{name} should resolve"
            );
        }
    }

    #[test]
    fn rejects_a_float_outside_the_float_range() {
        // JsonFormat.parseFloat throws "Out of range float value" rather than letting the cast
        // produce an infinity. NaN and the infinities are accepted explicitly there.
        let desc = crate::TEST_DESCRIPTOR_POOL
            .get_message_by_name("test.TestMessage")
            .unwrap();
        let f = desc.get_field_by_name("test_float").unwrap();

        let err = to_field_value(&f, &Value::Float(1e39)).unwrap_err();
        assert!(err.to_string().contains("out of range float"), "{err}");
        assert!(to_field_value(&f, &Value::Float(-1e39)).is_err());

        // In range, and the non-finite values, still pass.
        assert!(to_field_value(&f, &Value::Float(1.5)).is_ok());
        assert!(to_field_value(&f, &Value::Float(f64::INFINITY)).is_ok());
        assert!(to_field_value(&f, &Value::Float(f64::NAN)).is_ok());

        // A double field takes the full range.
        let d = desc.get_field_by_name("test_double").unwrap();
        assert!(to_field_value(&d, &Value::Float(1e39)).is_ok());
    }

    #[test]
    fn a_float_wrapper_gets_the_same_range_check_as_a_float_field() {
        // Java reaches both through parseFieldValue -> parseFloat, so a FloatValue must not
        // silently store infinity where a plain float field errors.
        let md = crate::TEST_DESCRIPTOR_POOL
            .get_message_by_name("google.protobuf.FloatValue")
            .unwrap();
        let err = super::build_well_known(&md, &Value::Float(1e39)).unwrap_err();
        assert!(err.to_string().contains("out of range float"), "{err}");

        // In range and non-finite still pass, as they do for a plain field.
        for v in [1.5f64, f64::INFINITY, f64::NAN] {
            assert!(
                super::build_well_known(&md, &Value::Float(v)).is_ok(),
                "{v} should be accepted"
            );
        }
    }

    #[test]
    fn rejects_a_wrong_shape_or_null_element_instead_of_clearing() {
        // Coercion here is destructive: an empty list or map replaces the field's contents, so a
        // mistyped result would silently delete data. Every case below is one the JVM client's
        // protobuf JSON rebuild rejects.
        let desc = crate::TEST_DESCRIPTOR_POOL
            .get_message_by_name("parity.ValueTypeContainers")
            .unwrap();
        let amounts = desc.get_field_by_name("amounts").unwrap();
        let amount_map = desc.get_field_by_name("amount_map").unwrap();

        // JsonFormat.mergeRepeatedField: "Expected an array for ..."
        let err = to_field_value(&amounts, &Value::Int(1)).unwrap_err();
        assert!(err.to_string().contains("expected a list"), "{err}");

        // JsonFormat.mergeRepeatedField: "Repeated field elements cannot be null"
        let err = to_field_value(&amounts, &Value::List(Arc::new(vec![Value::Null]))).unwrap_err();
        assert!(err.to_string().contains("null element"), "{err}");

        // JsonFormat.mergeMapField: "Expect a map object but found: ..."
        let err = to_field_value(&amount_map, &Value::Int(1)).unwrap_err();
        assert!(err.to_string().contains("expected a map"), "{err}");

        // JsonFormat.mergeMapField: "Map value cannot be null."
        let err =
            to_field_value(&amount_map, &Value::Map(keys(vec![("a", Value::Null)]))).unwrap_err();
        assert!(err.to_string().contains("null value"), "{err}");

        // An empty list or map is still a legitimate way to clear the field.
        assert!(to_field_value(&amounts, &Value::List(Arc::new(vec![]))).is_ok());
        assert!(to_field_value(&amount_map, &Value::Map(keys(vec![]))).is_ok());
    }

    #[test]
    fn rejects_a_result_naming_one_field_under_both_spellings() {
        // find_field accepts the declared name and the JSON name, so these are one field and
        // applying both would keep whichever the hash order visited last. JsonFormat.mergeField
        // rejects it ("Field X has already been set"), and its duplicate test runs before any
        // null handling - so unlike the oneof rule, a null still counts.
        let desc = crate::TEST_DESCRIPTOR_POOL
            .get_message_by_name("parity.ValueTypeContainers")
            .unwrap();

        let err = check_result_keys(
            &desc,
            &keys(vec![
                ("amount_map", Value::Map(keys(vec![]))),
                ("amountMap", Value::Map(keys(vec![]))),
            ]),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("amount_map") && msg.contains("amountMap"),
            "{msg}"
        );

        // A null under the other spelling is still a duplicate.
        assert!(
            check_result_keys(
                &desc,
                &keys(vec![
                    ("amount_map", Value::Map(keys(vec![]))),
                    ("amountMap", Value::Null),
                ])
            )
            .is_err(),
            "a null alias must still count as naming the field twice"
        );

        // One spelling alone is fine.
        assert!(
            check_result_keys(&desc, &keys(vec![("amountMap", Value::Map(keys(vec![])))])).is_ok()
        );
    }

    #[test]
    fn rejects_a_result_naming_two_members_of_one_oneof() {
        // Setting a oneof member clears its siblings, and the result is a hash map, so applying
        // both would keep whichever the iteration order visited last. The JVM client's protobuf
        // JSON rebuild refuses this outright, so it has to be an error here rather than a coin
        // flip. Reachable because the binding materialises every field, defaults included.
        let desc = crate::TEST_DESCRIPTOR_POOL
            .get_message_by_name("test.Author")
            .unwrap();
        let both: HashMap<Key, Value> = HashMap::from([
            (
                Key::String(Arc::new("oneof_string".to_string())),
                Value::String(Arc::new("x".to_string())),
            ),
            (
                Key::String(Arc::new("oneof_message".to_string())),
                Value::Map(Map {
                    map: Arc::new(HashMap::new()),
                }),
            ),
        ]);
        let err = check_result_keys(
            &desc,
            &Map {
                map: Arc::new(both),
            },
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("pii_oneof"), "{msg}");
        // Stable regardless of hash order: both members are named, sorted.
        assert!(
            msg.contains("oneof_message") && msg.contains("oneof_string"),
            "{msg}"
        );

        // One member alone is fine, and a null sibling clears rather than sets.
        let one: HashMap<Key, Value> = HashMap::from([
            (
                Key::String(Arc::new("oneof_string".to_string())),
                Value::String(Arc::new("x".to_string())),
            ),
            (
                Key::String(Arc::new("oneof_message".to_string())),
                Value::Null,
            ),
        ]);
        assert!(
            check_result_keys(&desc, &Map { map: Arc::new(one) }).is_ok(),
            "a null sibling must not count as a second member"
        );
    }

    #[test]
    fn unsigned_conversion_covers_the_whole_u64_domain() {
        // Above i64::MAX a uint64 has no i64 form, so routing it through as_i64 would reject
        // half the domain and an identity transform could not round-trip it.
        let big = u64::MAX;
        assert_eq!(as_u64(&Value::UInt(big)), Some(big));
        assert_eq!(as_u64(&Value::UInt(1u64 << 63)), Some(1u64 << 63));
        assert_eq!(as_i64(&Value::UInt(big)), None);

        // A non-negative signed value converts; a negative one does not.
        assert_eq!(as_u64(&Value::Int(7)), Some(7));
        assert_eq!(as_u64(&Value::Int(-1)), None);
    }

    #[test]
    fn float_to_int_excludes_two_to_the_63() {
        // `i64::MAX as f64` rounds up to 2^63, so a bound of `<= i64::MAX as f64` would admit
        // 2^63 and then saturate it to i64::MAX - silently changing the value.
        assert_eq!(float_to_int(9223372036854775808.0), None);
        assert_eq!(float_to_int(-9223372036854775808.0), Some(i64::MIN));
        assert_eq!(
            float_to_int(9223372036854774784.0),
            Some(9223372036854774784)
        );

        // Only exactly integral values convert.
        assert_eq!(float_to_int(1.5), None);
        assert_eq!(float_to_int(-3.0), Some(-3));
        assert_eq!(float_to_int(f64::NAN), None);
        assert_eq!(float_to_int(f64::INFINITY), None);
    }
}
