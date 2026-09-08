//! Conversions between [`bigdecimal::BigDecimal`] and the `confluent.type.Decimal` proto message -
//! the Rust counterpart of Java's `io.confluent.protobuf.type.utils.DecimalUtils` (BigDecimal) and
//! C#'s `DecimalExtensions` (System.Decimal). Independent of CEL: available to the Protobuf serde
//! for `confluent.type.Decimal` fields, and the CEL layer's decimal handling shares the same
//! unscaled-bytes/scale encoding.

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;

use crate::serdes::protobuf::confluent::r#type::Decimal as ProtoDecimal;
use crate::serdes::serde::SerdeError;

/// Converts a `confluent.type.Decimal` message to a [`BigDecimal`].
///
/// `value` is the unscaled integer as big-endian two's-complement bytes; `scale` is the number of
/// fractional digits (the value is `unscaled * 10^-scale`).
pub fn from_proto_decimal(d: &ProtoDecimal) -> BigDecimal {
    BigDecimal::new(BigInt::from_signed_bytes_be(&d.value), d.scale as i64)
}

/// Converts a [`BigDecimal`] to a `confluent.type.Decimal` message.
///
/// Mirrors Java `DecimalUtils.fromBigDecimal`: the scale is the number of fractional digits, the
/// precision is the unscaled value's digit count, and the value is the unscaled integer as
/// big-endian two's-complement bytes.
///
/// Fails when the scale does not fit the message's `int32` field. Java cannot reach that case at
/// all - `BigDecimal.scale()` is an `int` there - whereas `bigdecimal` keeps an `i64` exponent, so
/// the conversion is checked rather than truncating to a different number.
pub fn to_proto_decimal(d: &BigDecimal) -> Result<ProtoDecimal, SerdeError> {
    let (unscaled, scale) = d.clone().into_bigint_and_exponent();
    let scale = i32::try_from(scale)
        .map_err(|_| SerdeError::Rule(format!("decimal scale out of int range: {scale}")))?;
    let precision = u32::try_from(unscaled.magnitude().to_string().len()).unwrap_or(u32::MAX);
    Ok(ProtoDecimal {
        value: unscaled.to_signed_bytes_be(),
        precision,
        scale,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn round_trips_through_confluent_decimal() {
        for s in [
            "12.34",
            "0",
            "-7.5",
            "100",
            "1.50",
            "0.001",
            "-0.0000000001",
        ] {
            let d = BigDecimal::from_str(s).unwrap();
            let back = from_proto_decimal(&to_proto_decimal(&d).unwrap());
            assert_eq!(back, d, "round-trip mismatch for {s}");
        }
    }

    #[test]
    fn matches_the_known_wire_form() {
        // 12.34 = unscaled 1234 (0x04D2) at scale 2.
        let proto = to_proto_decimal(&BigDecimal::from_str("12.34").unwrap()).unwrap();
        assert_eq!(proto.value, vec![0x04, 0xd2]);
        assert_eq!(proto.scale, 2);
        assert_eq!(
            from_proto_decimal(&ProtoDecimal {
                value: vec![0x04, 0xd2],
                precision: 4,
                scale: 2,
            }),
            BigDecimal::from_str("12.34").unwrap()
        );
    }
}
