//! CEL bindings for the `decimal` constructor and `decimals.*` operators.
//!
//! A Decimal flows through CEL as an opaque value (`confluent.type.Decimal`) backed by
//! [`bigdecimal::BigDecimal`], matching the cross-platform design (Java `BigDecimal`,
//! Python `decimal.Decimal`, JS `decimal.js`). Division uses a 38-digit HALF_UP context to
//! match Flink / Java `BigDecimal`; add/sub/mul are exact.

use std::cmp::Ordering;
use std::num::NonZeroU64;
use std::str::FromStr;
use std::sync::Arc;

use bigdecimal::num_bigint::{BigInt, Sign};
use bigdecimal::{BigDecimal, RoundingMode, ToPrimitive};
use cel::extractors::Arguments;
use cel::objects::Opaque;
use cel::{Context, ExecutionError, Value};

/// The opaque-type label shared across all Schema Registry CEL clients.
pub const DECIMAL_TYPE_NAME: &str = "confluent.type.Decimal";

/// Digits of precision for division, matching Flink / Java `BigDecimal` (HALF_UP).
const DIV_PRECISION: u64 = 38;

/// A CEL Decimal value: an opaque wrapper over [`BigDecimal`].
#[derive(Debug, Clone)]
pub struct CelDecimal(pub BigDecimal);

impl PartialEq for CelDecimal {
    fn eq(&self, other: &Self) -> bool {
        // Numeric equality (1.0 == 1.00), so the `==` operator agrees with `decimals.eq`.
        self.0.cmp(&other.0) == Ordering::Equal
    }
}
impl Eq for CelDecimal {}

impl Opaque for CelDecimal {
    fn runtime_type_name(&self) -> &str {
        DECIMAL_TYPE_NAME
    }
}

/// Wraps a [`BigDecimal`] as a CEL opaque value.
pub fn decimal_value(d: BigDecimal) -> Value {
    Value::Opaque(Arc::new(CelDecimal(d)))
}

/// Builds a [`BigDecimal`] from big-endian two's-complement unscaled bytes and a scale.
pub fn from_bytes_scale(bytes: &[u8], scale: i64) -> BigDecimal {
    BigDecimal::new(BigInt::from_signed_bytes_be(bytes), scale)
}

fn err(msg: impl Into<String>) -> ExecutionError {
    ExecutionError::FunctionError {
        function: "decimals".to_string(),
        message: msg.into(),
    }
}

/// Coerces a CEL value to a [`BigDecimal`] (the `decimal(dyn)` dispatch).
pub fn to_decimal(v: &Value) -> Result<BigDecimal, ExecutionError> {
    match v {
        Value::Opaque(o) if o.runtime_type_name() == DECIMAL_TYPE_NAME => o
            .downcast_ref::<CelDecimal>()
            .map(|d| d.0.clone())
            .ok_or_else(|| err("decimal: opaque value is not a Decimal")),
        Value::String(s) => {
            BigDecimal::from_str(s).map_err(|e| err(format!("decimal: invalid string: {e}")))
        }
        Value::Int(i) => Ok(BigDecimal::from(*i)),
        Value::UInt(u) => Ok(BigDecimal::from(*u)),
        // Convert through the double's shortest decimal string (`0.1`), not its exact binary
        // expansion (`0.1000...0555`), to match Python `Decimal(str(v))` and JS `new Decimal(v)`.
        Value::Float(f) => {
            BigDecimal::from_str(&f.to_string()).map_err(|e| err(format!("decimal: {e}")))
        }
        Value::Null => Err(err("decimal: cannot convert null to Decimal")),
        _ => Err(err("decimal: cannot convert value to Decimal")),
    }
}

fn is_zero(d: &BigDecimal) -> bool {
    d.sign() == Sign::NoSign
}

/// Whether a CEL value is a Decimal opaque.
fn is_decimal(v: &Value) -> bool {
    matches!(v, Value::Opaque(o) if o.runtime_type_name() == DECIMAL_TYPE_NAME)
}

/// The `decimal(...)` constructor: `decimal(dyn)` or `decimal(bytes, int)`.
fn decimal(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    match args.as_slice() {
        [v] => Ok(decimal_value(to_decimal(v)?)),
        [Value::Bytes(bytes), Value::Int(scale)] => {
            let scale = require_int_scale(*scale, "decimal(bytes, scale)")?;
            Ok(decimal_value(from_bytes_scale(bytes, scale)))
        }
        _ => Err(err("decimal: expected (dyn) or (bytes, int)")),
    }
}

// ---- comparison (no `.ne` - rules use `!decimals.eq(...)`) ----
fn decimals_eq(a: Value, b: Value) -> Result<bool, ExecutionError> {
    Ok(to_decimal(&a)?.cmp(&to_decimal(&b)?) == Ordering::Equal)
}
fn decimals_lt(a: Value, b: Value) -> Result<bool, ExecutionError> {
    Ok(to_decimal(&a)? < to_decimal(&b)?)
}
fn decimals_le(a: Value, b: Value) -> Result<bool, ExecutionError> {
    Ok(to_decimal(&a)? <= to_decimal(&b)?)
}
fn decimals_gt(a: Value, b: Value) -> Result<bool, ExecutionError> {
    Ok(to_decimal(&a)? > to_decimal(&b)?)
}
fn decimals_ge(a: Value, b: Value) -> Result<bool, ExecutionError> {
    Ok(to_decimal(&a)? >= to_decimal(&b)?)
}

// ---- arithmetic ----
fn decimals_add(a: Value, b: Value) -> Result<Value, ExecutionError> {
    Ok(decimal_value(to_decimal(&a)? + to_decimal(&b)?))
}
fn decimals_sub(a: Value, b: Value) -> Result<Value, ExecutionError> {
    Ok(decimal_value(to_decimal(&a)? - to_decimal(&b)?))
}
fn decimals_mul(a: Value, b: Value) -> Result<Value, ExecutionError> {
    Ok(decimal_value(to_decimal(&a)? * to_decimal(&b)?))
}
fn decimals_div(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let (a, b) = (to_decimal(&a)?, to_decimal(&b)?);
    if is_zero(&b) {
        return Err(err("decimals.div: division by zero"));
    }
    let prec = NonZeroU64::new(DIV_PRECISION).unwrap();
    let quotient = (&a / &b).with_precision_round(prec, RoundingMode::HalfUp);
    Ok(decimal_value(strip_if_exact(quotient, |q| {
        (q * &b).cmp(&a)
    })))
}

/// `with_precision_round` pads an exact/short result out to 38 significant digits, but Java's
/// `divide`/`sqrt` with a `MathContext` (and Python/JS) return the natural value (`1/8` -> `0.125`,
/// `sqrt(144)` -> `12`). Strip that padding only when the result is exact: an *inexact* 38-digit
/// result can legitimately end in a significant `0` (e.g. `1/99`) that must be kept. `is_exact`
/// reconstructs the input from the rounded result and reports whether it matches numerically.
fn strip_if_exact(value: BigDecimal, is_exact: impl Fn(&BigDecimal) -> Ordering) -> BigDecimal {
    if is_exact(&value) == Ordering::Equal {
        value.normalized()
    } else {
        value
    }
}
fn decimals_mod(a: Value, b: Value) -> Result<Value, ExecutionError> {
    // Java BigDecimal.remainder / SQL MOD: a - trunc(a / b) * b.
    let (a, b) = (to_decimal(&a)?, to_decimal(&b)?);
    if is_zero(&b) {
        return Err(err("decimals.mod: division by zero"));
    }
    let q = (&a / &b).with_scale_round(0, RoundingMode::Down);
    Ok(decimal_value(a - q * b))
}

// ---- min / max ----
fn decimals_greatest(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let (a, b) = (to_decimal(&a)?, to_decimal(&b)?);
    Ok(decimal_value(if a >= b { a } else { b }))
}
fn decimals_least(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let (a, b) = (to_decimal(&a)?, to_decimal(&b)?);
    Ok(decimal_value(if a <= b { a } else { b }))
}

// ---- unary ----
fn decimals_neg(a: Value) -> Result<Value, ExecutionError> {
    Ok(decimal_value(-to_decimal(&a)?))
}
fn decimals_abs(a: Value) -> Result<Value, ExecutionError> {
    Ok(decimal_value(to_decimal(&a)?.abs()))
}
fn decimals_sign(a: Value) -> Result<i64, ExecutionError> {
    Ok(match to_decimal(&a)?.sign() {
        Sign::Minus => -1,
        Sign::NoSign => 0,
        Sign::Plus => 1,
    })
}
fn decimals_sqrt(a: Value) -> Result<Value, ExecutionError> {
    let d = to_decimal(&a)?;
    // Same 38-digit HALF_UP context as division; bigdecimal's bare `sqrt` would otherwise use a
    // 100-digit default and diverge from Python/JS on `string(sqrt(x))`.
    let prec = NonZeroU64::new(DIV_PRECISION).unwrap();
    // As in `div`, strip padding only for a perfect square (`sqrt(144)` -> `12`, not `12.000...`).
    d.sqrt()
        .map(|r| {
            let rounded = r.with_precision_round(prec, RoundingMode::HalfUp);
            decimal_value(strip_if_exact(rounded, |root| (root * root).cmp(&d)))
        })
        .ok_or_else(|| err("decimals.sqrt: square root of negative number"))
}

// ---- rounding (round/trunc take an optional scale; floor/ceil are scale 0) ----
fn scale_arg(args: &[Value]) -> Result<i64, ExecutionError> {
    match args {
        [_] => Ok(0),
        [_, Value::Int(s)] => Ok(*s),
        [_, _] => Err(err("expected an int scale")),
        _ => Err(err("expected 1 or 2 arguments")),
    }
}

/// Narrow a CEL int (i64) scale into the i32 range a `BigDecimal` scale occupies elsewhere,
/// erroring on out-of-range values instead of silently honoring them. CEL int is i64, but
/// Java/Python/JS all back the scale with a 32-bit int, so a value like `3_000_000_000` is
/// rejected there. bigdecimal accepts an i64 scale, so without this check Rust would diverge
/// and honor it. Mirrors Java's `requireIntScale` (`Math.toIntExact`), same error text.
fn require_int_scale(scale: i64, function_name: &str) -> Result<i64, ExecutionError> {
    i32::try_from(scale)
        .map(i64::from)
        .map_err(|_| err(format!("{function_name}: scale out of int range: {scale}")))
}

fn decimals_round(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    let d = to_decimal(
        args.first()
            .ok_or_else(|| err("decimals.round: missing argument"))?,
    )?;
    let scale = require_int_scale(scale_arg(&args)?, "decimals.round")?;
    Ok(decimal_value(
        d.with_scale_round(scale, RoundingMode::HalfUp),
    ))
}
fn decimals_trunc(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    let d = to_decimal(
        args.first()
            .ok_or_else(|| err("decimals.trunc: missing argument"))?,
    )?;
    let scale = require_int_scale(scale_arg(&args)?, "decimals.trunc")?;
    // Flink's TRUNCATE early-returns when the target scale is at-or-finer than the current one:
    // there is nothing to drop, so the input is returned unchanged. Without this guard
    // `with_scale_round` would zero-pad and `string(trunc(d, n >= cur))` would diverge from
    // Flink and the Python/JS clients (numerically identical, but a different string).
    if scale >= d.fractional_digit_count() {
        return Ok(decimal_value(d));
    }
    Ok(decimal_value(d.with_scale_round(scale, RoundingMode::Down)))
}
fn decimals_floor(a: Value) -> Result<Value, ExecutionError> {
    Ok(decimal_value(
        to_decimal(&a)?.with_scale_round(0, RoundingMode::Floor),
    ))
}
fn decimals_ceil(a: Value) -> Result<Value, ExecutionError> {
    Ok(decimal_value(
        to_decimal(&a)?.with_scale_round(0, RoundingMode::Ceiling),
    ))
}

// ---- stdlib conversions extended to Decimal ----
//
// cel-rust resolves `string(x)` / `double(x)` against its stdlib env overloads *before* it
// consults context functions, so these only run for arguments no stdlib overload matched - i.e.
// our Decimal opaque. That extends the built-ins to Decimal without shadowing them for any other
// type (mirroring the `string(Decimal)` / `double(Decimal)` extensions in the other clients).
fn decimal_to_string(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    match args.as_slice() {
        [v] if is_decimal(v) => Ok(Value::String(Arc::new(plain_decimal_string(&to_decimal(
            v,
        )?)?))),
        _ => Err(err("string: no matching overload")),
    }
}

/// Formats a Decimal in plain notation (Java `toPlainString`, Python `format(d, 'f')`,
/// JS `toFixed`) rather than the scientific form bigdecimal's `Display` would use for extreme
/// magnitudes. `to_plain_string` expands the full scale into digits, so a pathological scale -
/// `decimal(b"\x01", 1_000_000_000)` or `decimal("1e-1000000000")` - would allocate gigabytes;
/// bound the length first and error instead. (Java's `toPlainString` has the same blow-up.)
fn plain_decimal_string(d: &BigDecimal) -> Result<String, ExecutionError> {
    const MAX_LEN: u64 = 1 << 20; // 1 MiB of digits is already absurd for a rule value
    let length = d
        .digits()
        .saturating_add(d.fractional_digit_count().unsigned_abs());
    if length > MAX_LEN {
        return Err(err("string: decimal is too large to format"));
    }
    Ok(d.to_plain_string())
}
fn decimal_to_double(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    match args.as_slice() {
        [v] if is_decimal(v) => to_decimal(v)?
            .to_f64()
            .map(Value::Float)
            .ok_or_else(|| err("double: cannot convert Decimal to double")),
        _ => Err(err("double: no matching overload")),
    }
}

/// Extends the stdlib `string`/`double` conversions to Decimal. See [`decimal_to_string`].
pub fn add_string_overloads(ctx: &mut Context) {
    ctx.add_function("string", decimal_to_string);
    ctx.add_function("double", decimal_to_double);
}

/// Registers the decimal constructor and `decimals.*` operators on `ctx`. cel-rust dispatches the
/// namespaced `decimals.op(a, b)` calls to these registered `"decimals.op"` functions directly
/// (it qualifies a member call whose target is a bare identifier before resolving the target).
pub fn add_decimal_functions(ctx: &mut Context) {
    ctx.add_function("decimal", decimal);
    ctx.add_function("decimals.eq", decimals_eq);
    ctx.add_function("decimals.lt", decimals_lt);
    ctx.add_function("decimals.le", decimals_le);
    ctx.add_function("decimals.gt", decimals_gt);
    ctx.add_function("decimals.ge", decimals_ge);
    ctx.add_function("decimals.add", decimals_add);
    ctx.add_function("decimals.sub", decimals_sub);
    ctx.add_function("decimals.mul", decimals_mul);
    ctx.add_function("decimals.div", decimals_div);
    ctx.add_function("decimals.mod", decimals_mod);
    ctx.add_function("decimals.greatest", decimals_greatest);
    ctx.add_function("decimals.least", decimals_least);
    ctx.add_function("decimals.neg", decimals_neg);
    ctx.add_function("decimals.abs", decimals_abs);
    ctx.add_function("decimals.sign", decimals_sign);
    ctx.add_function("decimals.sqrt", decimals_sqrt);
    ctx.add_function("decimals.round", decimals_round);
    ctx.add_function("decimals.trunc", decimals_trunc);
    ctx.add_function("decimals.floor", decimals_floor);
    ctx.add_function("decimals.ceil", decimals_ceil);
}

#[cfg(test)]
mod tests {
    use crate::rules::cel::cel_lib::default_context;
    use cel::{Program, Value};

    fn eval(expr: &str) -> Value {
        let program = Program::compile(expr).expect("compile");
        program.execute(&default_context()).expect("execute")
    }

    fn eval_bool(expr: &str) -> bool {
        matches!(eval(expr), Value::Bool(true))
    }

    #[test]
    fn namespaced_comparison_dispatches() {
        assert!(eval_bool(
            "decimals.gt(decimal(\"12.34\"), decimal(\"10.00\"))"
        ));
        assert!(eval_bool(
            "decimals.lt(decimal(\"9.99\"), decimal(\"10.00\"))"
        ));
        assert!(!eval_bool(
            "decimals.ge(decimal(\"9.99\"), decimal(\"10.00\"))"
        ));
    }

    /// The CEL `==` / `!=` operators on two Decimal opaques are NUMERIC (scale-insensitive),
    /// agreeing with `decimals.eq`. This exercises the `CelDecimal::eq` path (via cel's
    /// `PartialEq` dispatch on `Value::Opaque`), which must use `cmp(...) == Equal` and NOT
    /// bigdecimal's own scale-sensitive `BigDecimal::eq` (where `2.0 != 2.00`).
    #[test]
    fn equality_operator_is_numeric() {
        assert!(eval_bool("decimal(\"2.0\") == decimal(\"2.00\")"));
        assert!(eval_bool("decimal(\"2.0\") == decimal(\"2.0\")"));
        assert!(!eval_bool("decimal(\"2.0\") == decimal(\"2.1\")"));
        // `!=` negates.
        assert!(!eval_bool("decimal(\"2.0\") != decimal(\"2.00\")"));
        assert!(eval_bool("decimal(\"2.0\") != decimal(\"2.1\")"));
    }

    #[test]
    fn arithmetic_is_exact() {
        assert!(eval_bool(
            "decimals.eq(decimals.add(decimal(\"12.34\"), decimal(\"1.66\")), decimal(\"14.00\"))"
        ));
        assert!(eval_bool(
            "decimals.eq(decimals.mul(decimal(\"2.5\"), decimal(\"4\")), decimal(\"10.0\"))"
        ));
    }

    #[test]
    fn division_uses_half_up() {
        // 10 / 3 rounded to 38 digits, HALF_UP.
        assert!(eval_bool(
            "decimals.gt(decimals.div(decimal(\"10\"), decimal(\"3\")), decimal(\"3.33\"))"
        ));
    }

    #[test]
    fn string_overload_extends_stdlib() {
        assert!(eval_bool("string(decimal(\"12.34\")) == \"12.34\""));
        // stdlib string() still works for other types.
        assert!(eval_bool("string(42) == \"42\""));
    }

    #[test]
    fn bytes_scale_constructor() {
        // unscaled 1234 with scale 2 == 12.34
        assert!(eval_bool(
            "decimals.eq(decimal(b\"\\x04\\xd2\", 2), decimal(\"12.34\"))"
        ));
    }

    #[test]
    fn sign_and_abs() {
        assert!(eval_bool("decimals.sign(decimal(\"-5\")) == -1"));
        assert!(eval_bool(
            "decimals.eq(decimals.abs(decimal(\"-5\")), decimal(\"5\"))"
        ));
    }

    /// String output has to match the Python and JS clients (and Flink) exactly, since a rule
    /// can serialize a Decimal back through `string(...)`.
    #[test]
    fn string_output_matches_other_clients() {
        // A double converts through its shortest decimal form, not its binary expansion.
        assert_eq!(eval_str("string(decimal(0.1))"), "0.1");
        // sqrt uses the 38-digit division context, not bigdecimal's 100-digit default.
        assert_eq!(
            eval_str("string(decimals.sqrt(decimal(\"2\")))"),
            "1.4142135623730950488016887242096980786"
        );
        // trunc early-returns (no zero-padding) when the target scale is at-or-finer than
        // the current scale.
        assert_eq!(
            eval_str("string(decimals.trunc(decimal(\"12.34\"), 5))"),
            "12.34"
        );
        assert_eq!(
            eval_str("string(decimals.trunc(decimal(\"12.349\"), 2))"),
            "12.34"
        );
        assert_eq!(eval_str("string(decimals.trunc(decimal(\"12\")))"), "12");
        // div is 38-digit HALF_UP.
        assert_eq!(
            eval_str("string(decimals.div(decimal(\"10\"), decimal(\"3\")))"),
            "3.3333333333333333333333333333333333333"
        );
        // An exact div/sqrt is the natural value, not padded to 38 digits (Java `divide`/`sqrt`
        // with a MathContext, and Python/JS, all leave `1/8` as `0.125` and `sqrt(144)` as `12`).
        assert_eq!(
            eval_str("string(decimals.div(decimal(\"1\"), decimal(\"8\")))"),
            "0.125"
        );
        assert_eq!(
            eval_str("string(decimals.div(decimal(\"100\"), decimal(\"1\")))"),
            "100"
        );
        assert_eq!(eval_str("string(decimals.sqrt(decimal(\"144\")))"), "12");
        // An *inexact* result keeps a significant trailing zero at the 38th digit (only exact
        // results are stripped): `1/99` is `0.0101...010`, a full 38 digits, like Python/Java.
        assert_eq!(
            eval_str("string(decimals.div(decimal(\"1\"), decimal(\"99\")))"),
            "0.010101010101010101010101010101010101010"
        );
        // string() is plain notation (Java `toPlainString`), never scientific.
        assert_eq!(
            eval_str("string(decimals.div(decimal(\"1\"), decimal(\"100000000000\")))"),
            "0.00000000001"
        );
    }

    #[test]
    fn oversized_decimal_string_errors_instead_of_allocating() {
        // A pathological scale would expand to gigabytes under plain formatting; it must error.
        assert!(
            Program::compile("string(decimal(b\"\\x01\", 1000000000))")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
    }

    /// A scale outside i32 range must error rather than silently narrow. CEL int is i64, but
    /// the scale is a 32-bit int in Java/Python/JS (Java's `requireIntScale`), so all clients
    /// reject the same inputs; bigdecimal would otherwise honor an i64 scale here.
    #[test]
    fn out_of_int32_scale_errors_instead_of_narrowing() {
        // decimals.round / decimals.trunc with a scale beyond i32::MAX.
        assert!(
            Program::compile("decimals.round(decimal(\"1.5\"), 3000000000)")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
        assert!(
            Program::compile("decimals.trunc(decimal(\"1.5\"), 3000000000)")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
        // Below i32::MIN as well.
        assert!(
            Program::compile("decimals.round(decimal(\"1.5\"), -3000000000)")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
        // The decimal(bytes, scale) constructor guards its scale the same way.
        assert!(
            Program::compile("decimal(b\"\\x01\", 9223372036854775807)")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
    }

    /// The bounds check accepts the full i32 range and rejects anything past it (matching Java's
    /// `Math.toIntExact`). Tested on the helper directly so the "accepted" cases don't zero-pad a
    /// BigDecimal out to billions of digits the way a real `round` at i32::MAX would.
    #[test]
    fn require_int_scale_boundaries() {
        use super::require_int_scale;
        assert_eq!(
            require_int_scale(i32::MAX as i64, "f").unwrap(),
            i32::MAX as i64
        );
        assert_eq!(
            require_int_scale(i32::MIN as i64, "f").unwrap(),
            i32::MIN as i64
        );
        assert_eq!(require_int_scale(0, "f").unwrap(), 0);
        assert!(require_int_scale(i32::MAX as i64 + 1, "f").is_err());
        assert!(require_int_scale(i32::MIN as i64 - 1, "f").is_err());
        assert!(require_int_scale(i64::MAX, "f").is_err());
        assert!(require_int_scale(i64::MIN, "f").is_err());
    }

    fn eval_str(expr: &str) -> String {
        match eval(expr) {
            Value::String(s) => s.to_string(),
            other => panic!("expected string, got {other:?}"),
        }
    }
}
