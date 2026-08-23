//! Conversions between [`bigdecimal::BigDecimal`] and the `confluent.type.Decimal` proto message -
//! the Rust counterpart of Java's `io.confluent.protobuf.type.utils.DecimalUtils` (BigDecimal) and
//! C#'s `DecimalExtensions` (System.Decimal). Independent of CEL: available to the Protobuf serde
//! for `confluent.type.Decimal` fields, and the CEL layer's decimal handling shares the same
//! unscaled-bytes/scale encoding.

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;

use crate::serdes::protobuf::confluent::r#type::Decimal as ProtoDecimal;

/// Converts a `confluent.type.Decimal` message to a [`BigDecimal`].
///
/// `value` is the unscaled integer as big-endian two's-complement bytes; `scale` is the number of
/// fractional digits (the value is `unscaled * 10^-scale`).
pub fn from_proto_decimal(d: &ProtoDecimal) -> BigDecimal {
    BigDecimal::new(BigInt::from_signed_bytes_be(&d.value), d.scale as i64)
}

/// Converts a [`BigDecimal`] to a `confluent.type.Decimal` message.
///
/// Mirrors Java `BigDecimal.unscaledValue()`/`scale()`: the scale is the number of fractional
/// digits and the value is the unscaled integer as big-endian two's-complement bytes.
pub fn to_proto_decimal(d: &BigDecimal) -> ProtoDecimal {
    let (unscaled, scale) = d.clone().into_bigint_and_exponent();
    ProtoDecimal {
        value: unscaled.to_signed_bytes_be(),
        precision: 0,
        scale: scale as i32,
    }
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
            let back = from_proto_decimal(&to_proto_decimal(&d));
            assert_eq!(back, d, "round-trip mismatch for {s}");
        }
    }

    #[test]
    fn matches_the_known_wire_form() {
        // 12.34 = unscaled 1234 (0x04D2) at scale 2.
        let proto = to_proto_decimal(&BigDecimal::from_str("12.34").unwrap());
        assert_eq!(proto.value, vec![0x04, 0xd2]);
        assert_eq!(proto.scale, 2);
        assert_eq!(
            from_proto_decimal(&ProtoDecimal {
                value: vec![0x04, 0xd2],
                precision: 0,
                scale: 2,
            }),
            BigDecimal::from_str("12.34").unwrap()
        );
    }
}
