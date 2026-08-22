//! A self-contained codec for the Spark/Parquet Variant binary format (a metadata key-dictionary
//! plus a self-describing value stream). This is the Rust counterpart of the .NET
//! `Confluent.SchemaRegistry` `Variant`/`VariantBuilder`, the Go `serde/variant` package, the C++
//! `schemaregistry::serdes::Variant`, and Java's `io.confluent.kafka.schemaregistry.type.Variant`.
//!
//! [`Variant::to_json`] renders temporal types as ISO-8601 with the seconds field always present
//! (0/3/6/9-digit fractional grouping) and decimals in fixed-point - the cross-language contract.
//! [`Variant::parse_json`] follows Java number handling: a fractional JSON number becomes a DOUBLE,
//! an integer wider than 64 bits a scale-0 decimal.
//!
//! [`Variant`] also implements [`serde::Serialize`]/[`serde::Deserialize`] mapping to a 2-field
//! record `{ metadata: bytes, value: bytes }` so a struct with a `Variant` field round-trips through
//! apache-avro's serde against the `confluent.type.Variant` record schema, with no logical-type
//! machinery.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use bigdecimal::num_bigint::{BigInt, Sign};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// --- format constants (see VariantFormat / Variant.cs) ---

// Basic types (low 2 bits of the header byte).
const PRIMITIVE: u8 = 0;
const SHORT_STR: u8 = 1;
const OBJECT_TYPE: u8 = 2;
const ARRAY_TYPE: u8 = 3;

// Primitive type codes (upper 6 bits when basic type == Primitive).
const T_NULL: u8 = 0;
const T_TRUE: u8 = 1;
const T_FALSE: u8 = 2;
const T_INT1: u8 = 3;
const T_INT2: u8 = 4;
const T_INT4: u8 = 5;
const T_INT8: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_DECIMAL4: u8 = 8;
const T_DECIMAL8: u8 = 9;
const T_DECIMAL16: u8 = 10;
const T_DATE: u8 = 11;
const T_TIMESTAMP: u8 = 12;
const T_TIMESTAMP_NTZ: u8 = 13;
const T_FLOAT: u8 = 14;
const T_BINARY: u8 = 15;
const T_LONG_STR: u8 = 16;
const T_TIME: u8 = 17;
const T_TIMESTAMP_NANOS: u8 = 18;
const T_TIMESTAMP_NANOS_NTZ: u8 = 19;
const T_UUID: u8 = 20;

const BASIC_TYPE_MASK: u8 = 0x3;
const BASIC_TYPE_BITS: u8 = 2;
const TYPE_INFO_MASK: u8 = 0x3F;
const MAX_SHORT_STR_SIZE: usize = 0x3F;
const VERSION: u8 = 1;
const VERSION_MASK: u8 = 0x0F;
const U32_SIZE: usize = 4;
const BINARY_SEARCH_THRESHOLD: usize = 32;

/// The value type of a [`Variant`], mirroring Java's `Variant.Type`. Integer, decimal, and
/// timestamp widths are kept distinct here; the CEL layer collapses them. The declaration order
/// matches the C#/Go/C++/Python ports exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Object,
    Array,
    Null,
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    String,
    Double,
    Decimal4,
    Decimal8,
    Decimal16,
    Date,
    TimestampTz,
    TimestampNtz,
    Float,
    Binary,
    Time,
    TimestampNanosTz,
    TimestampNanosNtz,
    Uuid,
}

/// Raised for a malformed or unsupported Variant binary value, or for malformed JSON.
#[derive(Debug, thiserror::Error)]
pub enum VariantError {
    /// A byte offset fell outside the value or metadata buffer.
    #[error("malformed variant: index out of bounds")]
    IndexOutOfBounds,
    /// The metadata version byte was not the supported version.
    #[error("unsupported variant metadata version: {0}")]
    UnsupportedVersion(u8),
    /// The value at this position was not of the requested type.
    #[error("{0}")]
    TypeMismatch(String),
    /// The binary value was otherwise malformed.
    #[error("malformed variant: {0}")]
    Malformed(String),
    /// The input JSON could not be parsed (or exceeded decimal precision limits).
    #[error("invalid JSON: {0}")]
    Json(String),
}

/// A read-only view over a Variant value at a byte position. The value and metadata buffers are
/// held in [`Arc`]s so navigation ([`Variant::get_field_by_key`] / [`Variant::get_element_at_index`])
/// can return owned sub-`Variant`s that share the buffers cheaply with a different position -
/// mirroring the shared-buffer design of the C#/Go/C++ ports.
#[derive(Clone)]
pub struct Variant {
    value: Arc<Vec<u8>>,
    metadata: Arc<Vec<u8>>,
    pos: usize,
}

impl Variant {
    /// Construct a Variant from raw value + metadata byte buffers. Note the argument order is
    /// `(value, metadata)`, matching the cross-client contract.
    pub fn new(value: Vec<u8>, metadata: Vec<u8>) -> Variant {
        Variant {
            value: Arc::new(value),
            metadata: Arc::new(metadata),
            pos: 0,
        }
    }

    /// A sub-variant sharing this node's buffers, positioned at `pos`.
    fn at(&self, pos: usize) -> Variant {
        Variant {
            value: Arc::clone(&self.value),
            metadata: Arc::clone(&self.metadata),
            pos,
        }
    }

    /// Parse a JSON string into a Variant (matches Java `VariantUtils.fromJsonNode`).
    pub fn parse_json(json: &str) -> Result<Variant, VariantError> {
        let (value, metadata) = build_from_json(json)?;
        Ok(Variant::new(value, metadata))
    }

    /// The raw value bytes (the whole buffer, shared across sub-variants).
    pub fn value_bytes(&self) -> &[u8] {
        self.value.as_slice()
    }

    /// The raw metadata bytes (the key dictionary).
    pub fn metadata_bytes(&self) -> &[u8] {
        self.metadata.as_slice()
    }

    /// The value buffer sliced from this node's start offset - a self-contained value encoding for
    /// this node (trailing sibling bytes are harmless; the decoder reads only what it needs). A
    /// sub-variant re-encodes as `Variant::new(sub.standalone_value_bytes(), sub.metadata_bytes().to_vec())`.
    /// For a root Variant this equals [`Variant::value_bytes`].
    pub fn standalone_value_bytes(&self) -> Vec<u8> {
        if self.pos >= self.value.len() {
            Vec::new()
        } else {
            self.value[self.pos..].to_vec()
        }
    }

    // --- type ---

    /// The value type at this position. On malformed data returns [`Type::Null`] (a best-effort
    /// value); the errorable internal path is used for JSON serialization.
    pub fn get_type(&self) -> Type {
        self.variant_type().unwrap_or(Type::Null)
    }

    fn variant_type(&self) -> Result<Type, VariantError> {
        check_index(self.pos, self.value.len())?;
        let header = self.value[self.pos];
        let basic_type = header & BASIC_TYPE_MASK;
        let type_info = (header >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        match basic_type {
            SHORT_STR => return Ok(Type::String),
            OBJECT_TYPE => return Ok(Type::Object),
            ARRAY_TYPE => return Ok(Type::Array),
            _ => {}
        }
        Ok(match type_info {
            T_NULL => Type::Null,
            T_TRUE | T_FALSE => Type::Boolean,
            T_INT1 => Type::Byte,
            T_INT2 => Type::Short,
            T_INT4 => Type::Int,
            T_INT8 => Type::Long,
            T_DOUBLE => Type::Double,
            T_DECIMAL4 => Type::Decimal4,
            T_DECIMAL8 => Type::Decimal8,
            T_DECIMAL16 => Type::Decimal16,
            T_DATE => Type::Date,
            T_TIMESTAMP => Type::TimestampTz,
            T_TIMESTAMP_NTZ => Type::TimestampNtz,
            T_FLOAT => Type::Float,
            T_BINARY => Type::Binary,
            T_LONG_STR => Type::String,
            T_TIME => Type::Time,
            T_TIMESTAMP_NANOS => Type::TimestampNanosTz,
            T_TIMESTAMP_NANOS_NTZ => Type::TimestampNanosNtz,
            T_UUID => Type::Uuid,
            other => {
                return Err(VariantError::Malformed(format!(
                    "unknown variant primitive type: {other}"
                )));
            }
        })
    }

    // --- scalar getters ---

    fn primitive_info(&self) -> Result<u8, VariantError> {
        check_index(self.pos, self.value.len())?;
        let header = self.value[self.pos];
        if header & BASIC_TYPE_MASK != PRIMITIVE {
            return Err(VariantError::TypeMismatch(
                "expected a primitive variant value".to_string(),
            ));
        }
        Ok((header >> BASIC_TYPE_BITS) & TYPE_INFO_MASK)
    }

    /// The boolean value.
    pub fn get_boolean(&self) -> Result<bool, VariantError> {
        let ti = self.primitive_info()?;
        if ti != T_TRUE && ti != T_FALSE {
            return Err(VariantError::TypeMismatch("variant is not a boolean".to_string()));
        }
        Ok(ti == T_TRUE)
    }

    /// The value of an INT8-backed variant. This does not widen: only a byte (INT8) value is
    /// accepted.
    pub fn get_byte(&self) -> Result<i8, VariantError> {
        let ti = self.primitive_info()?;
        if ti != T_INT1 {
            return Err(VariantError::TypeMismatch("variant is not a byte".to_string()));
        }
        Ok(read_signed_long(&self.value, self.pos + 1, 1)? as i8)
    }

    /// The value of an integer-backed variant no wider than INT16 (byte/short), widening narrower
    /// widths.
    pub fn get_short(&self) -> Result<i16, VariantError> {
        let ti = self.primitive_info()?;
        match ti {
            T_INT1 => Ok(read_signed_long(&self.value, self.pos + 1, 1)? as i16),
            T_INT2 => Ok(read_signed_long(&self.value, self.pos + 1, 2)? as i16),
            _ => Err(VariantError::TypeMismatch("variant is not a short".to_string())),
        }
    }

    /// The value of an integer-backed variant no wider than INT32 (byte/short/int), widening
    /// narrower widths.
    pub fn get_int(&self) -> Result<i32, VariantError> {
        let ti = self.primitive_info()?;
        match ti {
            T_INT1 => Ok(read_signed_long(&self.value, self.pos + 1, 1)? as i32),
            T_INT2 => Ok(read_signed_long(&self.value, self.pos + 1, 2)? as i32),
            T_INT4 => Ok(read_signed_long(&self.value, self.pos + 1, 4)? as i32),
            _ => Err(VariantError::TypeMismatch("variant is not an int".to_string())),
        }
    }

    /// The raw integer for any integer-backed type (byte/short/int/long, date days, timestamp
    /// micros, time micros, timestamp-nanos) - mirrors Java `getLong`.
    pub fn get_long(&self) -> Result<i64, VariantError> {
        let ti = self.primitive_info()?;
        match ti {
            T_INT1 => read_signed_long(&self.value, self.pos + 1, 1),
            T_INT2 => read_signed_long(&self.value, self.pos + 1, 2),
            T_INT4 | T_DATE => read_signed_long(&self.value, self.pos + 1, 4),
            T_INT8 | T_TIMESTAMP | T_TIMESTAMP_NTZ | T_TIME | T_TIMESTAMP_NANOS
            | T_TIMESTAMP_NANOS_NTZ => read_signed_long(&self.value, self.pos + 1, 8),
            _ => Err(VariantError::TypeMismatch(
                "variant is not an integer-backed type".to_string(),
            )),
        }
    }

    /// The FLOAT value. Exact-typed: only a FLOAT value is accepted (a DOUBLE is not narrowed).
    pub fn get_float(&self) -> Result<f32, VariantError> {
        let ti = self.primitive_info()?;
        if ti != T_FLOAT {
            return Err(VariantError::TypeMismatch("variant is not a float".to_string()));
        }
        read_float_le(&self.value, self.pos + 1)
    }

    /// The DOUBLE value. Exact-typed: only a DOUBLE value is accepted (a FLOAT is not widened; use
    /// [`Variant::get_float`] for that).
    pub fn get_double(&self) -> Result<f64, VariantError> {
        let ti = self.primitive_info()?;
        if ti != T_DOUBLE {
            return Err(VariantError::TypeMismatch("variant is not a double".to_string()));
        }
        read_double_le(&self.value, self.pos + 1)
    }

    /// The unscaled integer (as big-endian two's-complement bytes) and scale of a decimal value
    /// (scale preserved).
    pub fn get_decimal_parts(&self) -> Result<(Vec<u8>, i32), VariantError> {
        let ti = self.primitive_info()?;
        check_index(self.pos + 1, self.value.len())?;
        let scale = self.value[self.pos + 1] as i32;
        let width = match ti {
            T_DECIMAL4 => 4usize,
            T_DECIMAL8 => 8,
            T_DECIMAL16 => 16,
            _ => return Err(VariantError::TypeMismatch("variant is not a decimal".to_string())),
        };
        check_index(self.pos + 2 + width - 1, self.value.len())?;
        // Value bytes are little-endian two's-complement; reverse for big-endian.
        let mut be = vec![0u8; width];
        for i in 0..width {
            be[i] = self.value[self.pos + 2 + (width - 1 - i)];
        }
        Ok((be, scale))
    }

    /// The plain decimal string (fixed-point, never scientific), i.e. Java `toPlainString`.
    pub fn get_decimal_string(&self) -> Result<String, VariantError> {
        let (be, scale) = self.get_decimal_parts()?;
        let n = BigInt::from_signed_bytes_be(&be);
        Ok(decimal_plain_string(&n, scale))
    }

    /// The binary value.
    pub fn get_binary(&self) -> Result<Vec<u8>, VariantError> {
        let ti = self.primitive_info()?;
        if ti != T_BINARY {
            return Err(VariantError::TypeMismatch("variant is not binary".to_string()));
        }
        let length = read_unsigned_le(&self.value, self.pos + 1, U32_SIZE)?;
        let start = self.pos + 1 + U32_SIZE;
        if length == 0 {
            return Ok(Vec::new());
        }
        check_index(start + length - 1, self.value.len())?;
        Ok(self.value[start..start + length].to_vec())
    }

    /// The UUID as its canonical big-endian hex string (e.g. "00112233-4455-6677-8899-aabbccddeeff").
    pub fn get_uuid(&self) -> Result<String, VariantError> {
        let ti = self.primitive_info()?;
        if ti != T_UUID {
            return Err(VariantError::TypeMismatch("variant is not a uuid".to_string()));
        }
        let start = self.pos + 1;
        check_index(start + 15, self.value.len())?;
        Ok(format_uuid(&self.value, start))
    }

    /// The string value.
    pub fn get_string(&self) -> Result<String, VariantError> {
        check_index(self.pos, self.value.len())?;
        let header = self.value[self.pos];
        let basic_type = header & BASIC_TYPE_MASK;
        let type_info = (header >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        let (start, length) = if basic_type == SHORT_STR {
            (self.pos + 1, type_info as usize)
        } else if basic_type == PRIMITIVE && type_info == T_LONG_STR {
            let length = read_unsigned_le(&self.value, self.pos + 1, U32_SIZE)?;
            (self.pos + 1 + U32_SIZE, length)
        } else {
            return Err(VariantError::TypeMismatch("variant is not a string".to_string()));
        };
        if length == 0 {
            return Ok(String::new());
        }
        check_index(start + length - 1, self.value.len())?;
        String::from_utf8(self.value[start..start + length].to_vec())
            .map_err(|_| VariantError::Malformed("invalid UTF-8 in string value".to_string()))
    }

    // --- object / array navigation ---

    fn object_info(&self) -> Result<ObjectInfo, VariantError> {
        check_index(self.pos, self.value.len())?;
        let header = self.value[self.pos];
        let basic_type = header & BASIC_TYPE_MASK;
        let type_info = (header >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if basic_type != OBJECT_TYPE {
            return Err(VariantError::TypeMismatch("variant is not an object".to_string()));
        }
        let large_size = ((type_info >> 4) & 0x1) != 0;
        let size_bytes = if large_size { U32_SIZE } else { 1 };
        let num_fields = read_unsigned_le(&self.value, self.pos + 1, size_bytes)?;
        let id_size = (((type_info >> 2) & 0x3) + 1) as usize;
        let offset_size = ((type_info & 0x3) + 1) as usize;
        let id_start = self.pos + 1 + size_bytes;
        let offset_start = id_start + num_fields * id_size;
        let data_start = offset_start + (num_fields + 1) * offset_size;
        Ok(ObjectInfo {
            num_fields,
            id_size,
            offset_size,
            id_start,
            offset_start,
            data_start,
        })
    }

    fn array_info(&self) -> Result<ArrayInfo, VariantError> {
        check_index(self.pos, self.value.len())?;
        let header = self.value[self.pos];
        let basic_type = header & BASIC_TYPE_MASK;
        let type_info = (header >> BASIC_TYPE_BITS) & TYPE_INFO_MASK;
        if basic_type != ARRAY_TYPE {
            return Err(VariantError::TypeMismatch("variant is not an array".to_string()));
        }
        let large_size = ((type_info >> 2) & 0x1) != 0;
        let size_bytes = if large_size { U32_SIZE } else { 1 };
        let num_fields = read_unsigned_le(&self.value, self.pos + 1, size_bytes)?;
        let offset_size = ((type_info & 0x3) + 1) as usize;
        let offset_start = self.pos + 1 + size_bytes;
        let data_start = offset_start + (num_fields + 1) * offset_size;
        Ok(ArrayInfo {
            num_fields,
            offset_size,
            offset_start,
            data_start,
        })
    }

    /// The number of fields in an object (0 on error).
    pub fn num_object_fields(&self) -> usize {
        self.object_info().map(|o| o.num_fields).unwrap_or(0)
    }

    /// The number of elements in an array (0 on error).
    pub fn num_array_elements(&self) -> usize {
        self.array_info().map(|a| a.num_fields).unwrap_or(0)
    }

    /// The object field with the given key, or `None` if absent (or if this is not an object /
    /// the data is malformed).
    pub fn get_field_by_key(&self, key: &str) -> Option<Variant> {
        let o = self.object_info().ok()?;
        if o.num_fields < BINARY_SEARCH_THRESHOLD {
            for i in 0..o.num_fields {
                let id = read_unsigned_le(&self.value, o.id_start + o.id_size * i, o.id_size).ok()?;
                let k = self.get_metadata_key(id).ok()?;
                if k == key {
                    let offset =
                        read_unsigned_le(&self.value, o.offset_start + o.offset_size * i, o.offset_size)
                            .ok()?;
                    return Some(self.at(o.data_start + offset));
                }
            }
            return None;
        }
        let mut low = 0isize;
        let mut high = o.num_fields as isize - 1;
        while low <= high {
            let mid = ((low + high) >> 1) as usize;
            let mid_id =
                read_unsigned_le(&self.value, o.id_start + o.id_size * mid, o.id_size).ok()?;
            let k = self.get_metadata_key(mid_id).ok()?;
            match k.as_str().cmp(key) {
                std::cmp::Ordering::Less => low = mid as isize + 1,
                std::cmp::Ordering::Greater => high = mid as isize - 1,
                std::cmp::Ordering::Equal => {
                    let offset = read_unsigned_le(
                        &self.value,
                        o.offset_start + o.offset_size * mid,
                        o.offset_size,
                    )
                    .ok()?;
                    return Some(self.at(o.data_start + offset));
                }
            }
        }
        None
    }

    /// The (key, value) of the field at `idx` (key-sorted). On malformed data returns
    /// `(String::new(), self.clone())`.
    pub fn get_field_at_index(&self, idx: usize) -> (String, Variant) {
        self.field_at_index(idx)
            .unwrap_or_else(|_| (String::new(), self.clone()))
    }

    fn field_at_index(&self, idx: usize) -> Result<(String, Variant), VariantError> {
        let o = self.object_info()?;
        let id = read_unsigned_le(&self.value, o.id_start + o.id_size * idx, o.id_size)?;
        let offset =
            read_unsigned_le(&self.value, o.offset_start + o.offset_size * idx, o.offset_size)?;
        let key = self.get_metadata_key(id)?;
        Ok((key, self.at(o.data_start + offset)))
    }

    /// The array element at `index`, or `None` if out of bounds (or if this is not an array /
    /// the data is malformed).
    pub fn get_element_at_index(&self, index: usize) -> Option<Variant> {
        let a = self.array_info().ok()?;
        if index >= a.num_fields {
            return None;
        }
        let offset =
            read_unsigned_le(&self.value, a.offset_start + a.offset_size * index, a.offset_size)
                .ok()?;
        Some(self.at(a.data_start + offset))
    }

    // --- metadata dictionary ---

    fn get_metadata_key(&self, id: usize) -> Result<String, VariantError> {
        check_index(0, self.metadata.len())?;
        if self.metadata[0] & VERSION_MASK != VERSION {
            return Err(VariantError::UnsupportedVersion(self.metadata[0] & VERSION_MASK));
        }
        let offset_size = (((self.metadata[0] >> 6) & 0x3) + 1) as usize;
        let dict_size = read_unsigned_le(&self.metadata, 1, offset_size)?;
        if id >= dict_size {
            return Err(VariantError::Malformed("field id out of range".to_string()));
        }
        let string_start = 1 + (dict_size + 2) * offset_size;
        let offset = read_unsigned_le(&self.metadata, 1 + (id + 1) * offset_size, offset_size)?;
        let next_offset =
            read_unsigned_le(&self.metadata, 1 + (id + 2) * offset_size, offset_size)?;
        if offset > next_offset {
            return Err(VariantError::Malformed("non-monotonic metadata offsets".to_string()));
        }
        if next_offset == offset {
            return Ok(String::new());
        }
        check_index(string_start + next_offset - 1, self.metadata.len())?;
        String::from_utf8(self.metadata[string_start + offset..string_start + next_offset].to_vec())
            .map_err(|_| VariantError::Malformed("invalid UTF-8 in metadata key".to_string()))
    }

    // --- JSON serialization ---

    /// Serialize to a JSON string, matching the cross-language contract.
    pub fn to_json(&self) -> Result<String, VariantError> {
        let mut out = String::new();
        self.write_json(&mut out)?;
        Ok(out)
    }

    fn write_json(&self, out: &mut String) -> Result<(), VariantError> {
        match self.variant_type()? {
            Type::Object => {
                out.push('{');
                let n = self.object_info()?.num_fields;
                for i in 0..n {
                    if i > 0 {
                        out.push(',');
                    }
                    let (key, field) = self.field_at_index(i)?;
                    out.push_str(&json_quote(&key));
                    out.push(':');
                    field.write_json(out)?;
                }
                out.push('}');
            }
            Type::Array => {
                out.push('[');
                let n = self.array_info()?.num_fields;
                for i in 0..n {
                    if i > 0 {
                        out.push(',');
                    }
                    let el = self
                        .get_element_at_index(i)
                        .ok_or_else(|| VariantError::Malformed("array element out of range".to_string()))?;
                    el.write_json(out)?;
                }
                out.push(']');
            }
            Type::Null => out.push_str("null"),
            Type::Boolean => out.push_str(if self.get_boolean()? { "true" } else { "false" }),
            Type::String => out.push_str(&json_quote(&self.get_string()?)),
            Type::Byte | Type::Short | Type::Int | Type::Long => {
                out.push_str(&self.get_long()?.to_string());
            }
            Type::Float => out.push_str(&format_double(self.get_float()? as f64)?),
            Type::Double => out.push_str(&format_double(self.get_double()?)?),
            Type::Decimal4 | Type::Decimal8 | Type::Decimal16 => {
                out.push_str(&self.get_decimal_string()?);
            }
            Type::Date => {
                out.push('"');
                out.push_str(&format_date(self.get_long()?));
                out.push('"');
            }
            Type::TimestampTz => {
                out.push('"');
                out.push_str(&format_instant(self.get_long()? * 1000));
                out.push('"');
            }
            Type::TimestampNtz => {
                out.push('"');
                out.push_str(&format_local_date_time(self.get_long()? * 1000));
                out.push('"');
            }
            Type::TimestampNanosTz => {
                out.push('"');
                out.push_str(&format_instant(self.get_long()?));
                out.push('"');
            }
            Type::TimestampNanosNtz => {
                out.push('"');
                out.push_str(&format_local_date_time(self.get_long()?));
                out.push('"');
            }
            Type::Time => {
                out.push('"');
                out.push_str(&format_local_time(self.get_long()?));
                out.push('"');
            }
            Type::Binary => {
                use base64::Engine;
                out.push('"');
                out.push_str(&base64::engine::general_purpose::STANDARD.encode(self.get_binary()?));
                out.push('"');
            }
            Type::Uuid => {
                out.push('"');
                out.push_str(&self.get_uuid()?);
                out.push('"');
            }
        }
        Ok(())
    }
}

impl fmt::Debug for Variant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.to_json() {
            Ok(s) => write!(f, "Variant({s})"),
            Err(_) => f
                .debug_struct("Variant")
                .field("value_len", &self.value.len())
                .field("metadata_len", &self.metadata.len())
                .field("pos", &self.pos)
                .finish(),
        }
    }
}

struct ObjectInfo {
    num_fields: usize,
    id_size: usize,
    offset_size: usize,
    id_start: usize,
    offset_start: usize,
    data_start: usize,
}

struct ArrayInfo {
    num_fields: usize,
    offset_size: usize,
    offset_start: usize,
    data_start: usize,
}

// --- low-level byte helpers ---

fn check_index(pos: usize, length: usize) -> Result<(), VariantError> {
    if pos >= length {
        Err(VariantError::IndexOutOfBounds)
    } else {
        Ok(())
    }
}

fn read_unsigned_le(data: &[u8], pos: usize, num_bytes: usize) -> Result<usize, VariantError> {
    check_index(pos, data.len())?;
    check_index(pos + num_bytes - 1, data.len())?;
    let mut result: u64 = 0;
    for i in (0..num_bytes).rev() {
        result = (result << 8) | data[pos + i] as u64;
    }
    Ok(result as usize)
}

fn read_signed_long(data: &[u8], pos: usize, num_bytes: usize) -> Result<i64, VariantError> {
    check_index(pos, data.len())?;
    check_index(pos + num_bytes - 1, data.len())?;
    let mut result: u64 = 0;
    for i in (0..num_bytes).rev() {
        result = (result << 8) | data[pos + i] as u64;
    }
    if num_bytes < 8 {
        let sign_bit = 1u64 << (num_bytes * 8 - 1);
        if result & sign_bit != 0 {
            result |= u64::MAX << (num_bytes * 8);
        }
    }
    Ok(result as i64)
}

fn read_float_le(data: &[u8], pos: usize) -> Result<f32, VariantError> {
    check_index(pos + 3, data.len())?;
    let bits = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
    Ok(f32::from_bits(bits))
}

fn read_double_le(data: &[u8], pos: usize) -> Result<f64, VariantError> {
    check_index(pos + 7, data.len())?;
    let bits = u64::from_le_bytes([
        data[pos],
        data[pos + 1],
        data[pos + 2],
        data[pos + 3],
        data[pos + 4],
        data[pos + 5],
        data[pos + 6],
        data[pos + 7],
    ]);
    Ok(f64::from_bits(bits))
}

// --- calendar / temporal formatting (cross-language contract) ---
//
// Uses Howard Hinnant's civil_from_days (ported from the C++ Variant.cpp) rather than `chrono`,
// because `chrono` is an optional dependency in this crate (enabled only via `rules-cel`) and this
// module must build in the default (no-feature) configuration. The algorithm is proleptic
// Gregorian and correct for negative epochs.

/// Days since 1970-01-01 -> (year, month, day).
fn civil_from_days(z0: i64) -> (i64, u32, u32) {
    let z = z0 + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = y + if m <= 2 { 1 } else { 0 };
    (year, m as u32, d as u32)
}

fn floor_div(a: i64, b: i64) -> i64 {
    let mut q = a / b;
    if a % b != 0 && (a < 0) != (b < 0) {
        q -= 1;
    }
    q
}

fn floor_mod(a: i64, b: i64) -> i64 {
    let mut r = a % b;
    if r != 0 && (r < 0) != (b < 0) {
        r += b;
    }
    r
}

fn frac(nano: i64) -> String {
    if nano == 0 {
        String::new()
    } else if nano % 1_000_000 == 0 {
        format!(".{:03}", nano / 1_000_000)
    } else if nano % 1_000 == 0 {
        format!(".{:06}", nano / 1_000)
    } else {
        format!(".{:09}", nano)
    }
}

fn format_instant(total_nanos: i64) -> String {
    let sec = floor_div(total_nanos, 1_000_000_000);
    let nano = floor_mod(total_nanos, 1_000_000_000);
    let days = floor_div(sec, 86400);
    let sod = floor_mod(sec, 86400);
    let (y, mo, da) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}Z",
        y,
        mo,
        da,
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60,
        frac(nano)
    )
}

fn format_local_date_time(total_nanos: i64) -> String {
    let sec = floor_div(total_nanos, 1_000_000_000);
    let nano = floor_mod(total_nanos, 1_000_000_000);
    let days = floor_div(sec, 86400);
    let sod = floor_mod(sec, 86400);
    let (y, mo, da) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}",
        y,
        mo,
        da,
        sod / 3600,
        (sod % 3600) / 60,
        sod % 60,
        frac(nano)
    )
}

fn format_local_time(micros: i64) -> String {
    let nano_of_day = micros * 1000;
    let secs = floor_div(nano_of_day, 1_000_000_000);
    let nano = floor_mod(nano_of_day, 1_000_000_000);
    let hour = secs / 3600;
    let rem = secs % 3600;
    format!("{:02}:{:02}:{:02}{}", hour, rem / 60, rem % 60, frac(nano))
}

fn format_date(days: i64) -> String {
    let (y, mo, da) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}", y, mo, da)
}

/// Integral doubles render as N.0; other values use Rust's shortest round-trip decimal
/// representation. (Rust's `Display` never uses scientific notation, so very large/small magnitudes
/// stay in plain decimal - a minor divergence from Go's/C++'s `%g` and Java's `Double.toString`,
/// which is the documented cross-language edge case for doubles.)
fn format_double(d: f64) -> Result<String, VariantError> {
    if !d.is_finite() {
        return Err(VariantError::Malformed(
            "cannot render non-finite double as JSON".to_string(),
        ));
    }
    if d == d.floor() && d.abs() < 1e16 {
        return Ok(format!("{}.0", d as i64));
    }
    Ok(format!("{d}"))
}

fn format_uuid(data: &[u8], start: usize) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(36);
    for i in 0..16 {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out.push('-');
        }
        let b = data[start + i];
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xF) as usize] as char);
    }
    out
}

/// Render `s` as a JSON string literal, without HTML-escaping (matching Newtonsoft
/// `JsonConvert.ToString` / nlohmann `dump` / Go's `SetEscapeHTML(false)`).
fn json_quote(s: &str) -> String {
    // serde_json does not HTML-escape (`<`, `>`, `&`); it only escapes what JSON requires.
    serde_json::to_string(s).expect("string serialization cannot fail")
}

/// Render `unscaled * 10^-scale` as an exact fixed-point string (never scientific), matching
/// Java `BigDecimal.toPlainString`.
fn decimal_plain_string(unscaled: &BigInt, scale: i32) -> String {
    let negative = unscaled.sign() == Sign::Minus;
    let digits = unscaled.magnitude().to_string(); // abs value, "0" if zero
    let sign = if negative { "-" } else { "" };
    if scale <= 0 {
        return format!("{sign}{digits}");
    }
    let scale = scale as usize;
    let digits = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale - digits.len() + 1), digits)
    } else {
        digits
    };
    let point = digits.len() - scale;
    format!("{sign}{}.{}", &digits[..point], &digits[point..])
}

// --- builder (JSON -> value + metadata bytes) ---

struct FieldEntry {
    key: String,
    id: usize,
    offset: usize,
}

#[derive(Default)]
struct Builder {
    value: Vec<u8>,
    dictionary: HashMap<String, usize>,
    dictionary_keys: Vec<Vec<u8>>,
}

impl Builder {
    fn add_key(&mut self, key: &str) -> usize {
        if let Some(&existing) = self.dictionary.get(key) {
            return existing;
        }
        let id = self.dictionary_keys.len();
        self.dictionary.insert(key.to_string(), id);
        self.dictionary_keys.push(key.as_bytes().to_vec());
        id
    }

    fn append_null(&mut self) {
        self.value.push(primitive_header(T_NULL));
    }

    fn append_boolean(&mut self, b: bool) {
        self.value
            .push(primitive_header(if b { T_TRUE } else { T_FALSE }));
    }

    fn append_string(&mut self, s: &str) {
        let text = s.as_bytes();
        if text.len() > MAX_SHORT_STR_SIZE {
            self.value.push(primitive_header(T_LONG_STR));
            append_uint_le(&mut self.value, text.len(), U32_SIZE);
        } else {
            self.value
                .push(((text.len() as u8) << BASIC_TYPE_BITS) | SHORT_STR);
        }
        self.value.extend_from_slice(text);
    }

    fn append_int(&mut self, i: i64) {
        if (i8::MIN as i64..=i8::MAX as i64).contains(&i) {
            self.value.push(primitive_header(T_INT1));
            append_long_le(&mut self.value, i, 1);
        } else if (i16::MIN as i64..=i16::MAX as i64).contains(&i) {
            self.value.push(primitive_header(T_INT2));
            append_long_le(&mut self.value, i, 2);
        } else if (i32::MIN as i64..=i32::MAX as i64).contains(&i) {
            self.value.push(primitive_header(T_INT4));
            append_long_le(&mut self.value, i, 4);
        } else {
            self.value.push(primitive_header(T_INT8));
            append_long_le(&mut self.value, i, 8);
        }
    }

    fn append_decimal(&mut self, unscaled: &BigInt, scale: i32) -> Result<(), VariantError> {
        let num_digits = unscaled.magnitude().to_string().len();
        let (code, width) = if scale <= 9 && num_digits <= 9 {
            (T_DECIMAL4, 4usize)
        } else if scale <= 18 && num_digits <= 18 {
            (T_DECIMAL8, 8)
        } else if scale <= 38 && num_digits <= 38 {
            (T_DECIMAL16, 16)
        } else {
            return Err(VariantError::Json(
                "decimal exceeds maximum precision (38)".to_string(),
            ));
        };
        self.value.push(primitive_header(code));
        self.value.push(scale as u8);
        append_bigint_le(&mut self.value, unscaled, width);
        Ok(())
    }

    fn append_double(&mut self, d: f64) {
        self.value.push(primitive_header(T_DOUBLE));
        self.value.extend_from_slice(&d.to_bits().to_le_bytes());
    }

    // Fixed-width scalar appends (used by the public VariantBuilder). These write a
    // specific primitive width (symmetric with the reader's granular getters), unlike
    // `append_int`, which auto-selects the smallest int width for `parse_json`. The byte
    // layout matches what `parse_json` produces for a value of the same width.

    fn append_byte(&mut self, v: i8) {
        self.value.push(primitive_header(T_INT1));
        append_long_le(&mut self.value, v as i64, 1);
    }

    fn append_short(&mut self, v: i16) {
        self.value.push(primitive_header(T_INT2));
        append_long_le(&mut self.value, v as i64, 2);
    }

    fn append_int32(&mut self, v: i32) {
        self.value.push(primitive_header(T_INT4));
        append_long_le(&mut self.value, v as i64, 4);
    }

    fn append_long(&mut self, v: i64) {
        self.value.push(primitive_header(T_INT8));
        append_long_le(&mut self.value, v, 8);
    }

    fn append_float(&mut self, v: f32) {
        self.value.push(primitive_header(T_FLOAT));
        self.value.extend_from_slice(&v.to_bits().to_le_bytes());
    }

    fn append_binary(&mut self, data: &[u8]) {
        self.value.push(primitive_header(T_BINARY));
        append_uint_le(&mut self.value, data.len(), U32_SIZE);
        self.value.extend_from_slice(data);
    }

    fn append_uuid(&mut self, u: &[u8; 16]) {
        self.value.push(primitive_header(T_UUID));
        self.value.extend_from_slice(u);
    }

    fn append_temporal(&mut self, code: u8, width: usize, v: i64) {
        self.value.push(primitive_header(code));
        append_long_le(&mut self.value, v, width);
    }

    fn finish_writing_array(&mut self, start: usize, offsets: &[usize]) {
        let data_size = self.value.len() - start;
        let num_offsets = offsets.len();
        let large_size = num_offsets > 0xFF;
        let size_bytes = if large_size { U32_SIZE } else { 1 };
        let offset_size = integer_size(data_size);
        let mut header = Vec::new();
        header.push(
            ((large_size as u8) << (BASIC_TYPE_BITS + 2))
                | (((offset_size - 1) as u8) << BASIC_TYPE_BITS)
                | ARRAY_TYPE,
        );
        append_uint_le(&mut header, num_offsets, size_bytes);
        for &offset in offsets {
            append_uint_le(&mut header, offset, offset_size);
        }
        append_uint_le(&mut header, data_size, offset_size);
        self.value.splice(start..start, header);
    }

    fn finish_writing_object(&mut self, start: usize, mut fields: Vec<FieldEntry>) {
        let num_fields = fields.len();
        // Sort by key using ordinal byte order (Rust `str` Ord compares by bytes = code-point
        // order), matching Go/C++.
        fields.sort_by(|a, b| a.key.cmp(&b.key));
        let max_id = fields.iter().map(|f| f.id).max().unwrap_or(0);
        let data_size = self.value.len() - start;
        let large_size = num_fields > 0xFF;
        let size_bytes = if large_size { U32_SIZE } else { 1 };
        let id_size = integer_size(max_id);
        let offset_size = integer_size(data_size);
        let mut header = Vec::new();
        header.push(
            ((large_size as u8) << (BASIC_TYPE_BITS + 4))
                | (((id_size - 1) as u8) << (BASIC_TYPE_BITS + 2))
                | (((offset_size - 1) as u8) << BASIC_TYPE_BITS)
                | OBJECT_TYPE,
        );
        append_uint_le(&mut header, num_fields, size_bytes);
        for f in &fields {
            append_uint_le(&mut header, f.id, id_size);
        }
        for f in &fields {
            append_uint_le(&mut header, f.offset, offset_size);
        }
        append_uint_le(&mut header, data_size, offset_size);
        self.value.splice(start..start, header);
    }

    fn finish(self) -> (Vec<u8>, Vec<u8>) {
        let num_keys = self.dictionary_keys.len();
        let dict_string_size: usize = self.dictionary_keys.iter().map(|k| k.len()).sum();
        let offset_size = integer_size(dict_string_size.max(num_keys));

        let mut metadata = Vec::new();
        metadata.push(VERSION | (((offset_size - 1) as u8) << 6));
        append_uint_le(&mut metadata, num_keys, offset_size);
        let mut current_offset = 0usize;
        for k in &self.dictionary_keys {
            append_uint_le(&mut metadata, current_offset, offset_size);
            current_offset += k.len();
        }
        append_uint_le(&mut metadata, current_offset, offset_size);
        for k in &self.dictionary_keys {
            metadata.extend_from_slice(k);
        }
        (self.value, metadata)
    }
}

fn primitive_header(type_code: u8) -> u8 {
    (type_code << BASIC_TYPE_BITS) | PRIMITIVE
}

// --- public flat streaming VariantBuilder ---

/// A single frame of the builder's nesting stack.
enum BuilderFrame {
    Object {
        start: usize,
        fields: Vec<FieldEntry>,
        /// The key (and its dictionary id) set by `append_key`, awaiting its value.
        pending: Option<(String, usize)>,
    },
    Array {
        start: usize,
        offsets: Vec<usize>,
    },
}

/// Programmatically constructs a [`Variant`] using a flat streaming-writer model with an internal
/// nesting stack (the arrow-dotnet `VariantValueWriter` shape). A single builder emits scalars and
/// opens/closes containers; appends target the current slot (the root, the next array element, or
/// the current object field once its key has been set via [`VariantBuilder::append_key`]). Object
/// fields are sorted by key at [`VariantBuilder::end_object`] (canonical form) and the metadata
/// dictionary accumulates keys in append order.
///
/// The output is byte-identical to [`Variant::parse_json`] of the equivalent JSON document.
///
/// ```
/// # use schema_registry_client::serdes::variant::VariantBuilder;
/// let mut b = VariantBuilder::new();
/// b.start_object().unwrap();
/// b.append_key("id").unwrap();
/// b.append_long(42).unwrap();
/// b.append_key("tags").unwrap();
/// b.start_array().unwrap();
/// b.append_string("x").unwrap();
/// b.append_string("y").unwrap();
/// b.end_array().unwrap();
/// b.end_object().unwrap();
/// let v = b.build().unwrap();
/// assert_eq!(v.to_json().unwrap(), r#"{"id":42,"tags":["x","y"]}"#);
/// ```
pub struct VariantBuilder {
    builder: Builder,
    stack: Vec<BuilderFrame>,
    root_written: bool,
}

impl Default for VariantBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl VariantBuilder {
    /// Create a new, empty builder.
    pub fn new() -> Self {
        VariantBuilder {
            builder: Builder::default(),
            stack: Vec::new(),
            root_written: false,
        }
    }

    /// Records the current write position in the enclosing container (if any) so the value about to
    /// be written is addressable, and enforces the slot rules (a lone root value; a preceding
    /// `append_key` inside an object). Must be called immediately before any value bytes are written.
    fn prepare_slot(&mut self) -> Result<(), VariantError> {
        let len = self.builder.value.len();
        if self.stack.is_empty() {
            if self.root_written {
                return Err(VariantError::Json("builder already has a root value".to_string()));
            }
            self.root_written = true;
            return Ok(());
        }
        match self.stack.last_mut().unwrap() {
            BuilderFrame::Array { start, offsets } => offsets.push(len - *start),
            BuilderFrame::Object { start, fields, pending } => {
                let (key, id) = pending.take().ok_or_else(|| {
                    VariantError::Json(
                        "value appended to object without a preceding append_key".to_string(),
                    )
                })?;
                fields.push(FieldEntry {
                    key,
                    id,
                    offset: len - *start,
                });
            }
        }
        Ok(())
    }

    /// Append a null value to the current slot.
    pub fn append_null(&mut self) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_null();
        Ok(())
    }

    /// Append a boolean value.
    pub fn append_boolean(&mut self, v: bool) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_boolean(v);
        Ok(())
    }

    /// Append an INT8 value.
    pub fn append_byte(&mut self, v: i8) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_byte(v);
        Ok(())
    }

    /// Append an INT16 value.
    pub fn append_short(&mut self, v: i16) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_short(v);
        Ok(())
    }

    /// Append an INT32 value.
    pub fn append_int(&mut self, v: i32) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_int32(v);
        Ok(())
    }

    /// Append an INT64 value.
    pub fn append_long(&mut self, v: i64) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_long(v);
        Ok(())
    }

    /// Append a FLOAT (32-bit) value.
    pub fn append_float(&mut self, v: f32) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_float(v);
        Ok(())
    }

    /// Append a DOUBLE (64-bit) value.
    pub fn append_double(&mut self, v: f64) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_double(v);
        Ok(())
    }

    /// Append a decimal value from its unscaled integer (big-endian two's-complement bytes) and
    /// scale. The width (Decimal4/8/16) is selected from the digit count and scale, matching
    /// `parse_json`.
    pub fn append_decimal(
        &mut self,
        unscaled_big_endian: &[u8],
        scale: i32,
    ) -> Result<(), VariantError> {
        self.prepare_slot()?;
        let n = BigInt::from_signed_bytes_be(unscaled_big_endian);
        self.builder.append_decimal(&n, scale)
    }

    /// Append a string, auto-selecting the short-string (<=63 bytes) or long-string encoding.
    pub fn append_string(&mut self, s: &str) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_string(s);
        Ok(())
    }

    /// Append a binary (byte-string) value.
    pub fn append_binary(&mut self, data: &[u8]) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_binary(data);
        Ok(())
    }

    /// Append a UUID value (16 raw big-endian bytes).
    pub fn append_uuid(&mut self, uuid: &[u8; 16]) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_uuid(uuid);
        Ok(())
    }

    /// Append a DATE value (days since the Unix epoch).
    pub fn append_date(&mut self, days_since_epoch: i32) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_temporal(T_DATE, 4, days_since_epoch as i64);
        Ok(())
    }

    /// Append a TIME_NTZ value (microseconds since midnight).
    pub fn append_time(&mut self, micros_since_midnight: i64) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_temporal(T_TIME, 8, micros_since_midnight);
        Ok(())
    }

    /// Append a TIMESTAMP (with time zone) value in microseconds.
    pub fn append_timestamp_tz(&mut self, micros: i64) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_temporal(T_TIMESTAMP, 8, micros);
        Ok(())
    }

    /// Append a TIMESTAMP_NTZ value in microseconds.
    pub fn append_timestamp_ntz(&mut self, micros: i64) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_temporal(T_TIMESTAMP_NTZ, 8, micros);
        Ok(())
    }

    /// Append a TIMESTAMP_NANOS (with time zone) value in nanoseconds.
    pub fn append_timestamp_nanos_tz(&mut self, nanos: i64) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_temporal(T_TIMESTAMP_NANOS, 8, nanos);
        Ok(())
    }

    /// Append a TIMESTAMP_NANOS_NTZ value in nanoseconds.
    pub fn append_timestamp_nanos_ntz(&mut self, nanos: i64) -> Result<(), VariantError> {
        self.prepare_slot()?;
        self.builder.append_temporal(T_TIMESTAMP_NANOS_NTZ, 8, nanos);
        Ok(())
    }

    /// Open a new object. Subsequent `append_key`/value pairs populate it until the matching
    /// `end_object`.
    pub fn start_object(&mut self) -> Result<(), VariantError> {
        self.prepare_slot()?;
        let start = self.builder.value.len();
        self.stack.push(BuilderFrame::Object {
            start,
            fields: Vec::new(),
            pending: None,
        });
        Ok(())
    }

    /// Set the key for the next appended value. Valid only directly inside an object and only once
    /// per value.
    pub fn append_key(&mut self, key: &str) -> Result<(), VariantError> {
        match self.stack.last() {
            Some(BuilderFrame::Object { pending, .. }) => {
                if pending.is_some() {
                    return Err(VariantError::Json(
                        "append_key called twice without an intervening value".to_string(),
                    ));
                }
            }
            _ => {
                return Err(VariantError::Json("append_key called outside an object".to_string()));
            }
        }
        let id = self.builder.add_key(key);
        if let Some(BuilderFrame::Object { pending, .. }) = self.stack.last_mut() {
            *pending = Some((key.to_string(), id));
        }
        Ok(())
    }

    /// Close the current object, sorting its fields by key.
    pub fn end_object(&mut self) -> Result<(), VariantError> {
        match self.stack.last() {
            Some(BuilderFrame::Object { pending, .. }) => {
                if pending.is_some() {
                    return Err(VariantError::Json(
                        "end_object called with a pending key and no value".to_string(),
                    ));
                }
            }
            _ => {
                return Err(VariantError::Json(
                    "end_object called without a matching start_object".to_string(),
                ));
            }
        }
        if let Some(BuilderFrame::Object { start, fields, .. }) = self.stack.pop() {
            self.builder.finish_writing_object(start, fields);
        }
        Ok(())
    }

    /// Open a new array. Subsequent value appends become its elements until the matching `end_array`.
    pub fn start_array(&mut self) -> Result<(), VariantError> {
        self.prepare_slot()?;
        let start = self.builder.value.len();
        self.stack.push(BuilderFrame::Array {
            start,
            offsets: Vec::new(),
        });
        Ok(())
    }

    /// Close the current array.
    pub fn end_array(&mut self) -> Result<(), VariantError> {
        match self.stack.last() {
            Some(BuilderFrame::Array { .. }) => {}
            _ => {
                return Err(VariantError::Json(
                    "end_array called without a matching start_array".to_string(),
                ));
            }
        }
        if let Some(BuilderFrame::Array { start, offsets }) = self.stack.pop() {
            self.builder.finish_writing_array(start, &offsets);
        }
        Ok(())
    }

    /// Finalize the builder and return the constructed [`Variant`]. Errors if a container is still
    /// open or no value has been appended.
    pub fn build(self) -> Result<Variant, VariantError> {
        if !self.stack.is_empty() {
            return Err(VariantError::Json("build called with an open container".to_string()));
        }
        if !self.root_written {
            return Err(VariantError::Json("build called with no value appended".to_string()));
        }
        let (value, metadata) = self.builder.finish();
        Ok(Variant::new(value, metadata))
    }
}

fn integer_size(v: usize) -> usize {
    if v <= 0xFF {
        1
    } else if v <= 0xFFFF {
        2
    } else if v <= 0xFFFFFF {
        3
    } else {
        4
    }
}

fn append_uint_le(out: &mut Vec<u8>, v: usize, num_bytes: usize) {
    for i in 0..num_bytes {
        out.push(((v >> (8 * i)) & 0xFF) as u8);
    }
}

fn append_long_le(out: &mut Vec<u8>, mut v: i64, width: usize) {
    for _ in 0..width {
        out.push((v & 0xFF) as u8);
        v >>= 8;
    }
}

/// Append `width` bytes of little-endian two's-complement encoding of `n` (matching .NET
/// `BigInteger.ToByteArray` padded to a fixed width).
fn append_bigint_le(out: &mut Vec<u8>, n: &BigInt, width: usize) {
    let le = n.to_signed_bytes_le(); // minimal little-endian two's-complement
    let pad = if n.sign() == Sign::Minus { 0xFFu8 } else { 0x00 };
    for i in 0..width {
        out.push(if i < le.len() { le[i] } else { pad });
    }
}

/// Build value + metadata bytes from a JSON string. Number handling follows Java
/// `VariantUtils.fromJsonNode`: a fractional JSON number (containing '.', 'e', or 'E') becomes a
/// DOUBLE; an integer literal becomes the smallest int1/2/4/8 that fits, or a scale-0 decimal when
/// wider than 64 bits. Object key order in the metadata dictionary follows JSON document order; the
/// object header itself is key-sorted (matching VariantBuilder.cs).
///
/// This uses a small hand-rolled JSON reader rather than `serde_json::Value` because the latter is
/// lossy for integer literals wider than i64/u64 (it would coerce them to f64), which would break
/// the "wider than 64 bits -> scale-0 decimal" rule. Keeping the parse confined here avoids
/// enabling `serde_json`'s crate-wide `arbitrary_precision` feature.
fn build_from_json(json: &str) -> Result<(Vec<u8>, Vec<u8>), VariantError> {
    let mut parser = JsonReader {
        bytes: json.as_bytes(),
        i: 0,
    };
    let mut builder = Builder::default();
    parser.skip_ws();
    parser.parse_value(&mut builder)?;
    parser.skip_ws();
    if parser.i != parser.bytes.len() {
        return Err(VariantError::Json("trailing content after JSON value".to_string()));
    }
    Ok(builder.finish())
}

struct JsonReader<'a> {
    bytes: &'a [u8],
    i: usize,
}

impl<'a> JsonReader<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.i).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.bytes.get(self.i).copied();
        if c.is_some() {
            self.i += 1;
        }
        c
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c == b' ' || c == b'\t' || c == b'\n' || c == b'\r' {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    fn expect_literal(&mut self, lit: &[u8]) -> Result<(), VariantError> {
        if self.i + lit.len() <= self.bytes.len() && &self.bytes[self.i..self.i + lit.len()] == lit {
            self.i += lit.len();
            Ok(())
        } else {
            Err(VariantError::Json(format!(
                "invalid literal, expected {}",
                String::from_utf8_lossy(lit)
            )))
        }
    }

    fn parse_value(&mut self, out: &mut Builder) -> Result<(), VariantError> {
        self.skip_ws();
        match self.peek() {
            Some(b'{') => {
                self.i += 1;
                self.parse_object(out)
            }
            Some(b'[') => {
                self.i += 1;
                self.parse_array(out)
            }
            Some(b'"') => {
                let s = self.parse_string()?;
                out.append_string(&s);
                Ok(())
            }
            Some(b't') => {
                self.expect_literal(b"true")?;
                out.append_boolean(true);
                Ok(())
            }
            Some(b'f') => {
                self.expect_literal(b"false")?;
                out.append_boolean(false);
                Ok(())
            }
            Some(b'n') => {
                self.expect_literal(b"null")?;
                out.append_null();
                Ok(())
            }
            Some(c) if c == b'-' || c.is_ascii_digit() => self.parse_number(out),
            Some(c) => Err(VariantError::Json(format!(
                "unexpected character '{}'",
                c as char
            ))),
            None => Err(VariantError::Json("unexpected end of input".to_string())),
        }
    }

    fn parse_object(&mut self, out: &mut Builder) -> Result<(), VariantError> {
        let start = out.value.len();
        let mut fields = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            out.finish_writing_object(start, fields);
            return Ok(());
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err(VariantError::Json("expected object key string".to_string()));
            }
            let key = self.parse_string()?;
            self.skip_ws();
            if self.next() != Some(b':') {
                return Err(VariantError::Json("expected ':' after object key".to_string()));
            }
            let id = out.add_key(&key);
            let offset = out.value.len() - start;
            fields.push(FieldEntry { key, id, offset });
            self.parse_value(out)?;
            self.skip_ws();
            match self.next() {
                Some(b',') => continue,
                Some(b'}') => break,
                _ => return Err(VariantError::Json("expected ',' or '}' in object".to_string())),
            }
        }
        out.finish_writing_object(start, fields);
        Ok(())
    }

    fn parse_array(&mut self, out: &mut Builder) -> Result<(), VariantError> {
        let start = out.value.len();
        let mut offsets = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            out.finish_writing_array(start, &offsets);
            return Ok(());
        }
        loop {
            offsets.push(out.value.len() - start);
            self.parse_value(out)?;
            self.skip_ws();
            match self.next() {
                Some(b',') => continue,
                Some(b']') => break,
                _ => return Err(VariantError::Json("expected ',' or ']' in array".to_string())),
            }
        }
        out.finish_writing_array(start, &offsets);
        Ok(())
    }

    /// Parse a JSON string literal (current position must be the opening quote), decoding escapes.
    fn parse_string(&mut self) -> Result<String, VariantError> {
        // Consume the opening quote.
        self.i += 1;
        let mut out: Vec<u8> = Vec::new();
        loop {
            let c = self
                .next()
                .ok_or_else(|| VariantError::Json("unterminated string".to_string()))?;
            match c {
                b'"' => break,
                b'\\' => {
                    let e = self
                        .next()
                        .ok_or_else(|| VariantError::Json("unterminated escape".to_string()))?;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'b' => out.push(0x08),
                        b'f' => out.push(0x0C),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'u' => {
                            let cp = self.read_hex4()?;
                            if (0xD800..=0xDBFF).contains(&cp) {
                                if self.next() != Some(b'\\') || self.next() != Some(b'u') {
                                    return Err(VariantError::Json(
                                        "expected low surrogate escape".to_string(),
                                    ));
                                }
                                let lo = self.read_hex4()?;
                                if !(0xDC00..=0xDFFF).contains(&lo) {
                                    return Err(VariantError::Json(
                                        "invalid low surrogate".to_string(),
                                    ));
                                }
                                let scalar = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                push_code_point(&mut out, scalar)?;
                            } else if (0xDC00..=0xDFFF).contains(&cp) {
                                return Err(VariantError::Json(
                                    "unexpected low surrogate".to_string(),
                                ));
                            } else {
                                push_code_point(&mut out, cp)?;
                            }
                        }
                        _ => return Err(VariantError::Json("invalid escape".to_string())),
                    }
                }
                _ => out.push(c),
            }
        }
        String::from_utf8(out).map_err(|_| VariantError::Json("invalid UTF-8 in string".to_string()))
    }

    fn read_hex4(&mut self) -> Result<u32, VariantError> {
        let mut v = 0u32;
        for _ in 0..4 {
            let c = self
                .next()
                .ok_or_else(|| VariantError::Json("truncated \\u escape".to_string()))?;
            let d = match c {
                b'0'..=b'9' => (c - b'0') as u32,
                b'a'..=b'f' => (c - b'a' + 10) as u32,
                b'A'..=b'F' => (c - b'A' + 10) as u32,
                _ => return Err(VariantError::Json("invalid hex digit".to_string())),
            };
            v = (v << 4) | d;
        }
        Ok(v)
    }

    fn parse_number(&mut self, out: &mut Builder) -> Result<(), VariantError> {
        let start = self.i;
        while let Some(c) = self.peek() {
            if c == b'-' || c == b'+' || c == b'.' || c == b'e' || c == b'E' || c.is_ascii_digit() {
                self.i += 1;
            } else {
                break;
            }
        }
        let token = std::str::from_utf8(&self.bytes[start..self.i])
            .map_err(|_| VariantError::Json("invalid number token".to_string()))?;
        let fractional = token.contains('.') || token.contains('e') || token.contains('E');
        if !fractional {
            if let Ok(i) = token.parse::<i64>() {
                out.append_int(i);
                return Ok(());
            }
            let bi = token
                .parse::<BigInt>()
                .map_err(|_| VariantError::Json(format!("invalid integer literal {token:?}")))?;
            return out.append_decimal(&bi, 0);
        }
        let d = token
            .parse::<f64>()
            .map_err(|_| VariantError::Json(format!("invalid number literal {token:?}")))?;
        out.append_double(d);
        Ok(())
    }
}

fn push_code_point(out: &mut Vec<u8>, cp: u32) -> Result<(), VariantError> {
    let ch = char::from_u32(cp).ok_or_else(|| VariantError::Json("invalid code point".to_string()))?;
    let mut buf = [0u8; 4];
    out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
    Ok(())
}

// --- serde surfacing ---
//
// Maps to a 2-field record `{ metadata: bytes, value: bytes }` (fields serialized in the order
// metadata, then value), so a user struct with a `Variant` field round-trips through apache-avro's
// serde against the `confluent.type.Variant` record schema, without any avro logical-type
// machinery.

/// Serializes a `&[u8]` via `serialize_bytes` (serde's default `&[u8]` impl would emit a sequence
/// of `u8`, which avro would not treat as `bytes`).
struct BytesField<'a>(&'a [u8]);

impl Serialize for BytesField<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

impl Serialize for Variant {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut st = serializer.serialize_struct("Variant", 2)?;
        st.serialize_field("metadata", &BytesField(self.metadata_bytes()))?;
        let value = self.standalone_value_bytes();
        st.serialize_field("value", &BytesField(&value))?;
        st.end()
    }
}

/// Deserializes a byte field, accepting avro `bytes`/`fixed` (via `visit_byte_buf`/`visit_bytes`)
/// as well as a plain `u8` sequence.
struct ByteField(Vec<u8>);

impl<'de> Deserialize<'de> for ByteField {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Vec<u8>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a byte buffer")
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
                Ok(v.to_vec())
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
                Ok(v)
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Vec<u8>, E> {
                Ok(v.as_bytes().to_vec())
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
                let mut out = Vec::new();
                while let Some(b) = seq.next_element::<u8>()? {
                    out.push(b);
                }
                Ok(out)
            }
        }
        deserializer.deserialize_byte_buf(V).map(ByteField)
    }
}

impl<'de> Deserialize<'de> for Variant {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct VariantVisitor;
        impl<'de> Visitor<'de> for VariantVisitor {
            type Value = Variant;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a Variant record with `metadata` and `value` byte fields")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Variant, A::Error> {
                let mut metadata: Option<Vec<u8>> = None;
                let mut value: Option<Vec<u8>> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "metadata" => metadata = Some(map.next_value::<ByteField>()?.0),
                        "value" => value = Some(map.next_value::<ByteField>()?.0),
                        _ => {
                            let _ = map.next_value::<de::IgnoredAny>()?;
                        }
                    }
                }
                let metadata = metadata.ok_or_else(|| de::Error::missing_field("metadata"))?;
                let value = value.ok_or_else(|| de::Error::missing_field("value"))?;
                Ok(Variant::new(value, metadata))
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Variant, A::Error> {
                // Field order: metadata, then value.
                let metadata: ByteField = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let value: ByteField = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(Variant::new(value.0, metadata.0))
            }
        }
        deserializer.deserialize_struct("Variant", &["metadata", "value"], VariantVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[test]
    fn builder_nested_matches_parse_json() {
        // Build a nested document with the flat streaming API. The int widths are chosen so each
        // value's encoding matches parse_json of the equivalent JSON.
        let mut b = VariantBuilder::new();
        b.start_object().unwrap();
        b.append_key("id").unwrap();
        b.append_long(10_000_000_000).unwrap(); // INT64 == "10000000000"
        b.append_key("count").unwrap();
        b.append_int(100_000).unwrap(); // INT32 == "100000"
        b.append_key("tags").unwrap();
        b.start_array().unwrap();
        b.append_string("x").unwrap();
        b.append_string("y").unwrap();
        b.end_array().unwrap();
        b.append_key("nested").unwrap();
        b.start_object().unwrap();
        b.append_key("flag").unwrap();
        b.append_boolean(true).unwrap();
        b.append_key("pi").unwrap();
        b.append_double(3.14).unwrap();
        b.end_object().unwrap();
        b.end_object().unwrap();
        let built = b.build().unwrap();

        // Key order is chosen so both the builder (append order) and parse_json (document order)
        // assign the same metadata dictionary IDs.
        let equivalent =
            r#"{"id":10000000000,"count":100000,"tags":["x","y"],"nested":{"flag":true,"pi":3.14}}"#;
        let parsed = Variant::parse_json(equivalent).unwrap();

        assert_eq!(built.to_json().unwrap(), parsed.to_json().unwrap());
        assert_eq!(built.value_bytes(), parsed.value_bytes(), "value bytes differ");
        assert_eq!(
            built.metadata_bytes(),
            parsed.metadata_bytes(),
            "metadata bytes differ"
        );
    }

    #[test]
    fn builder_root_scalar() {
        let mut b = VariantBuilder::new();
        b.append_byte(42).unwrap();
        let built = b.build().unwrap();
        let parsed = Variant::parse_json("42").unwrap();
        assert_eq!(built.value_bytes(), parsed.value_bytes());
        assert_eq!(built.get_byte().unwrap(), 42);
    }

    #[test]
    fn builder_errors() {
        // append_key outside an object.
        assert!(VariantBuilder::new().append_key("k").is_err());

        // Value appended to an object without a preceding append_key.
        let mut b = VariantBuilder::new();
        b.start_object().unwrap();
        assert!(b.append_long(1).is_err());

        // build with an open container.
        let mut b2 = VariantBuilder::new();
        b2.start_array().unwrap();
        assert!(b2.build().is_err());

        // build with nothing appended.
        assert!(VariantBuilder::new().build().is_err());
    }

    #[test]
    fn large_data_region_uses_4_byte_offsets() {
        // Regression test for Bug #1: `integer_size` capped at 3 bytes and never returned 4, so a
        // container whose data/offset region exceeds 0xFFFFFF (16777215) bytes produced a corrupt
        // Variant. Build an array holding a single string of 16777216 bytes so the data region
        // exceeds 16 MiB, forcing the 4-byte offset-size path, then verify it round-trips.
        const SIZE: usize = 16_777_216; // 0x1000000, one byte past the 3-byte offset limit
        let big = "a".repeat(SIZE);

        let mut b = VariantBuilder::new();
        b.start_array().unwrap();
        b.append_string(&big).unwrap();
        b.end_array().unwrap();
        let built = b.build().unwrap();

        assert_eq!(built.get_type(), Type::Array);
        assert_eq!(built.num_array_elements(), 1);
        let el = built.get_element_at_index(0).unwrap();
        assert_eq!(el.get_type(), Type::String);
        assert_eq!(el.get_string().unwrap().len(), SIZE);
    }

    #[test]
    fn parse_json_round_trip_object_array_scalars_null() {
        for json in [
            "{\"x\":1}",
            "{\"a\":1,\"b\":2,\"c\":3}",
            "[1,2,3]",
            "[true,false,null]",
            "{\"k\":\"v\",\"nested\":{\"arr\":[1,\"two\",3.5,null,true]}}",
            "\"hello\"",
            "42",
            "3.14",
            "true",
            "false",
            "null",
            "{}",
            "[]",
        ] {
            let v = Variant::parse_json(json).unwrap();
            assert_eq!(v.to_json().unwrap(), json, "round-trip mismatch for {json}");
        }
    }

    #[test]
    fn simple_object_round_trip() {
        assert_eq!(
            Variant::parse_json("{\"x\":1}").unwrap().to_json().unwrap(),
            "{\"x\":1}"
        );
    }

    #[test]
    fn object_keys_are_sorted_in_output() {
        // Document order c,a,b -> output sorted a,b,c.
        let v = Variant::parse_json("{\"c\":3,\"a\":1,\"b\":2}").unwrap();
        assert_eq!(v.to_json().unwrap(), "{\"a\":1,\"b\":2,\"c\":3}");
    }

    #[test]
    fn get_type_per_type() {
        assert_eq!(Variant::parse_json("{}").unwrap().get_type(), Type::Object);
        assert_eq!(Variant::parse_json("[]").unwrap().get_type(), Type::Array);
        assert_eq!(Variant::parse_json("null").unwrap().get_type(), Type::Null);
        assert_eq!(Variant::parse_json("true").unwrap().get_type(), Type::Boolean);
        assert_eq!(Variant::parse_json("\"s\"").unwrap().get_type(), Type::String);
        // Integer widths.
        assert_eq!(Variant::parse_json("1").unwrap().get_type(), Type::Byte);
        assert_eq!(Variant::parse_json("300").unwrap().get_type(), Type::Short);
        assert_eq!(Variant::parse_json("70000").unwrap().get_type(), Type::Int);
        assert_eq!(
            Variant::parse_json("5000000000").unwrap().get_type(),
            Type::Long
        );
        assert_eq!(Variant::parse_json("1.5").unwrap().get_type(), Type::Double);
        // Integer wider than 64 bits -> scale-0 decimal.
        assert_eq!(
            Variant::parse_json("123456789012345678901234567890")
                .unwrap()
                .get_type(),
            Type::Decimal16
        );
    }

    #[test]
    fn scalar_getters() {
        assert!(Variant::parse_json("true").unwrap().get_boolean().unwrap());
        assert!(!Variant::parse_json("false").unwrap().get_boolean().unwrap());
        assert_eq!(Variant::parse_json("-42").unwrap().get_long().unwrap(), -42);
        assert_eq!(
            Variant::parse_json("5000000000").unwrap().get_long().unwrap(),
            5_000_000_000
        );
        assert_eq!(
            Variant::parse_json("3.25").unwrap().get_double().unwrap(),
            3.25
        );
        assert_eq!(
            Variant::parse_json("\"héllo\"").unwrap().get_string().unwrap(),
            "héllo"
        );
    }

    #[test]
    fn decimal_get_decimal_string() {
        // 3.14 -> unscaled 314 scale 2.
        let v = Variant::parse_json("3.14").unwrap();
        // 3.14 is fractional -> double, not decimal. Use a wide integer for a real decimal.
        assert_eq!(v.get_type(), Type::Double);

        let big = Variant::parse_json("123456789012345678901234567890").unwrap();
        assert_eq!(big.get_type(), Type::Decimal16);
        assert_eq!(
            big.get_decimal_string().unwrap(),
            "123456789012345678901234567890"
        );
        let (be, scale) = big.get_decimal_parts().unwrap();
        assert_eq!(scale, 0);
        assert_eq!(BigInt::from_signed_bytes_be(&be).to_string(), big.to_json().unwrap());

        // Negative wide integer.
        let neg = Variant::parse_json("-98765432109876543210").unwrap();
        assert_eq!(neg.get_decimal_string().unwrap(), "-98765432109876543210");
    }

    #[test]
    fn navigation_field_and_element() {
        let v = Variant::parse_json("{\"a\":10,\"b\":[100,200,300]}").unwrap();
        assert_eq!(v.num_object_fields(), 2);
        let a = v.get_field_by_key("a").unwrap();
        assert_eq!(a.get_long().unwrap(), 10);
        let b = v.get_field_by_key("b").unwrap();
        assert_eq!(b.get_type(), Type::Array);
        assert_eq!(b.num_array_elements(), 3);
        assert_eq!(
            b.get_element_at_index(1).unwrap().get_long().unwrap(),
            200
        );
        // Miss / OOB -> None.
        assert!(v.get_field_by_key("missing").is_none());
        assert!(b.get_element_at_index(5).is_none());
        // Wrong-type navigation -> None.
        assert!(a.get_field_by_key("x").is_none());
        assert!(a.get_element_at_index(0).is_none());

        // get_field_at_index returns key-sorted entries.
        let (k0, _) = v.get_field_at_index(0);
        let (k1, _) = v.get_field_at_index(1);
        assert_eq!(k0, "a");
        assert_eq!(k1, "b");
    }

    #[test]
    fn standalone_value_bytes_reencode_round_trip() {
        let v = Variant::parse_json("{\"outer\":{\"inner\":[1,2,3],\"k\":\"v\"}}").unwrap();
        let sub = v.get_field_by_key("outer").unwrap();
        // Re-encode the sub-variant as a standalone Variant sharing the same metadata.
        let reencoded = Variant::new(sub.standalone_value_bytes(), sub.metadata_bytes().to_vec());
        assert_eq!(reencoded.to_json().unwrap(), sub.to_json().unwrap());
        assert_eq!(reencoded.to_json().unwrap(), "{\"inner\":[1,2,3],\"k\":\"v\"}");
    }

    #[test]
    fn pre_1970_date_and_timestamps() {
        // Directly construct temporals since JSON only produces number/string types.
        // Date: days since epoch; -1 = 1969-12-31.
        let value = vec![primitive_header(T_DATE), 0xFF, 0xFF, 0xFF, 0xFF]; // -1 LE i32
        let metadata = vec![VERSION, 0x00, 0x00];
        let v = Variant::new(value, metadata);
        assert_eq!(v.get_type(), Type::Date);
        assert_eq!(v.to_json().unwrap(), "\"1969-12-31\"");

        // A well-before-1970 date: -719162 days ~ year 0001-01-01 area; just check it renders.
        assert_eq!(format_date(-1), "1969-12-31");
        assert_eq!(format_date(0), "1970-01-01");
        // Negative timestamp micros -> pre-epoch instant.
        assert_eq!(format_instant(-1_000_000_000), "1969-12-31T23:59:59Z");
    }

    #[test]
    fn double_formatting() {
        assert_eq!(format_double(1.0).unwrap(), "1.0");
        assert_eq!(format_double(-3.0).unwrap(), "-3.0");
        assert_eq!(format_double(1.5).unwrap(), "1.5");
        assert_eq!(format_double(0.1).unwrap(), "0.1");
    }

    #[test]
    fn malformed_json_errors() {
        assert!(Variant::parse_json("{").is_err());
        assert!(Variant::parse_json("{\"a\":}").is_err());
        assert!(Variant::parse_json("[1,2").is_err());
        assert!(Variant::parse_json("nul").is_err());
        assert!(Variant::parse_json("1 2").is_err()); // trailing content
        assert!(Variant::parse_json("").is_err());
    }

    #[test]
    fn binary_and_uuid_render() {
        // Binary: header, u32 length=3 (LE), then bytes 0x01 0x02 0x03 -> base64 "AQID".
        let value = vec![primitive_header(T_BINARY), 0x03, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03];
        let metadata = vec![VERSION, 0x00, 0x00];
        let v = Variant::new(value, metadata.clone());
        assert_eq!(v.get_type(), Type::Binary);
        assert_eq!(v.get_binary().unwrap(), vec![1, 2, 3]);
        assert_eq!(v.to_json().unwrap(), "\"AQID\"");

        // UUID: 16 bytes.
        let mut uv = vec![primitive_header(T_UUID)];
        uv.extend_from_slice(&[
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]);
        let u = Variant::new(uv, metadata);
        assert_eq!(u.get_type(), Type::Uuid);
        assert_eq!(
            u.get_uuid().unwrap(),
            "00112233-4455-6677-8899-aabbccddeeff"
        );
    }

    fn float_variant(f: f32) -> Variant {
        let mut value = vec![primitive_header(T_FLOAT)];
        value.extend_from_slice(&f.to_le_bytes());
        Variant::new(value, vec![VERSION, 0x00, 0x00])
    }

    fn double_variant(d: f64) -> Variant {
        let mut value = vec![primitive_header(T_DOUBLE)];
        value.extend_from_slice(&d.to_le_bytes());
        Variant::new(value, vec![VERSION, 0x00, 0x00])
    }

    #[test]
    fn int_getters_width_and_widening() {
        // get_byte: INT8 only.
        assert_eq!(Variant::parse_json("1").unwrap().get_byte().unwrap(), 1);
        assert!(Variant::parse_json("300").unwrap().get_byte().is_err());
        // get_short: <=INT16, widens.
        assert_eq!(Variant::parse_json("1").unwrap().get_short().unwrap(), 1);
        assert_eq!(Variant::parse_json("300").unwrap().get_short().unwrap(), 300);
        assert!(Variant::parse_json("100000").unwrap().get_short().is_err());
        // get_int: <=INT32, widens.
        assert_eq!(Variant::parse_json("1").unwrap().get_int().unwrap(), 1);
        assert_eq!(Variant::parse_json("300").unwrap().get_int().unwrap(), 300);
        assert_eq!(Variant::parse_json("100000").unwrap().get_int().unwrap(), 100000);
        assert!(Variant::parse_json("10000000000").unwrap().get_int().is_err());
        // get_long: widens any int width.
        assert_eq!(
            Variant::parse_json("10000000000").unwrap().get_long().unwrap(),
            10_000_000_000
        );
    }

    #[test]
    fn float_and_double_are_exact() {
        // get_float accepts FLOAT exactly.
        assert_eq!(float_variant(1.5).get_float().unwrap(), 1.5f32);
        // get_float rejects DOUBLE.
        assert!(double_variant(1.5).get_float().is_err());
        // get_double accepts DOUBLE exactly.
        assert_eq!(double_variant(3.5).get_double().unwrap(), 3.5f64);
        // get_double rejects FLOAT (no longer widens).
        assert!(float_variant(1.5).get_double().is_err());
        // A FLOAT renders through get_float in to_json.
        assert_eq!(float_variant(1.5).get_type(), Type::Float);
        assert_eq!(float_variant(1.5).to_json().unwrap(), "1.5");
    }

    fn variant_avro_schema() -> apache_avro::Schema {
        apache_avro::Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Variant",
                "namespace": "confluent.type",
                "fields": [
                    {"name": "metadata", "type": "bytes"},
                    {"name": "value", "type": "bytes"}
                ]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn avro_serde_round_trip_direct() {
        let schema = variant_avro_schema();
        let v = Variant::parse_json("{\"a\":1,\"b\":[true,\"x\"],\"c\":3.5}").unwrap();

        let value = apache_avro::to_value(&v).unwrap();
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();
        let decoded = apache_avro::from_avro_datum(&schema, &mut &bytes[..], None).unwrap();
        let recovered: Variant = apache_avro::from_value(&decoded).unwrap();

        assert_eq!(recovered.to_json().unwrap(), v.to_json().unwrap());
    }

    #[test]
    fn avro_serde_round_trip_wrapper_struct() {
        #[derive(Serialize, Deserialize)]
        struct Holder {
            data: Variant,
        }

        let schema = apache_avro::Schema::parse_str(
            r#"{
                "type": "record",
                "name": "Holder",
                "fields": [
                    {"name": "data", "type": {
                        "type": "record",
                        "name": "Variant",
                        "namespace": "confluent.type",
                        "fields": [
                            {"name": "metadata", "type": "bytes"},
                            {"name": "value", "type": "bytes"}
                        ]
                    }}
                ]
            }"#,
        )
        .unwrap();

        let v = Variant::parse_json("{\"nested\":{\"n\":42},\"list\":[1,2,3]}").unwrap();
        let holder = Holder { data: v.clone() };

        let value = apache_avro::to_value(&holder).unwrap();
        let bytes = apache_avro::to_avro_datum(&schema, value).unwrap();
        let decoded = apache_avro::from_avro_datum(&schema, &mut &bytes[..], None).unwrap();
        let recovered: Holder = apache_avro::from_value(&decoded).unwrap();

        assert_eq!(recovered.data.to_json().unwrap(), v.to_json().unwrap());
    }
}
