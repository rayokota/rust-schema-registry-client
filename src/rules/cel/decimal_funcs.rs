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

/// `|a| < |b|`, without aligning the operands.
///
/// `BigDecimal`'s own comparison short-circuits on magnitude (measured: comparing 1e-2000000000
/// with 1e2000000000 is free), so this is just `abs` on both sides - stated as a helper because
/// the *reason* it is cheap is what makes the remainder's zero-quotient case cheap.
fn magnitude_lt(a: &BigDecimal, b: &BigDecimal) -> bool {
    a.abs() < b.abs()
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
//
// `add`/`sub` align their operands on the finer scale before computing a digit, so the aligned
// frame is what has to be built - and `bigdecimal` has no cap of its own, so nothing else
// bounds it. `mul` does not align: it adds the exponents and multiplies the coefficients, so
// its result is as compact as its operands, and it is deliberately left unguarded. Measured on
// libmpdec in the Python sibling, peak RSS on operands 1e2147483647 and 3: `mul`, `div`,
// comparison, negation and `abs` all 13 MB; `add` 1738 MB, `sub` 1738 MB, `remainder` 1733 MB.
fn decimals_add(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let (a, b) = (to_decimal(&a)?, to_decimal(&b)?);
    check_alignment_width(&a, &b, "decimals.add")?;
    Ok(decimal_value(a + b))
}
fn decimals_sub(a: Value, b: Value) -> Result<Value, ExecutionError> {
    let (a, b) = (to_decimal(&a)?, to_decimal(&b)?);
    check_alignment_width(&a, &b, "decimals.sub")?;
    Ok(decimal_value(a - b))
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
    // Bounded by the *integral quotient*, which is what has to be produced first - not by the
    // aligned frame add/sub use. Calling `check_alignment_width` here was wrong: the quotient
    // is narrow whenever the operands' magnitudes are close or the dividend is the smaller,
    // and the frame then refuses values the reference accepts. `1e-2147483647 mod
    // 1e2147483647` is the dividend itself at precision 1, scale 2147483647 on the JVM, and
    // 68us here - against an aligned frame of 4.3e9 digits.
    //
    // Measured on `bigdecimal`, and the estimate below tracks it closely (predicted/actual
    // quotient digits in brackets):
    //
    //   1e-2147483647 mod 1e2147483647    68us     [1 / 1]
    //   1e2147483647  mod 1e2147483000    35us     [648 / 648]
    //   1E40          mod 3               58us     [41 / 40]
    //   1e10000       mod 3              1.0ms     [10001 / 10000]
    //   1e100000      mod 3             29.5ms     [100001 / 100000]
    //   1.5           mod 1e-100000     27.7ms     [100001 / 100001]
    //
    // so the cost is (slightly super-)linear in the quotient width, and 2**31 quotient digits
    // is the case worth refusing. No `|a| < |b|` short-circuit is needed: the estimate already
    // yields 1 there, and `a - q*b` with a zero quotient already returns the dividend at its
    // own scale.
    // A zero dividend has a quotient of zero whatever the scales, and its adjusted exponent
    // says nothing useful - a zero keeps the scale it was built with, so `0E+2e9 mod 1E-2e9`
    // estimated 4e9 digits for a result that is just zero. The JDK returns 0 at precision 1.
    // Saturating, because `bigdecimal` accepts a scale this arithmetic cannot hold: the JVM caps
    // a scale at int32 ("Too many nonzero exponent digits" past that), while `10e9223372036854775807`
    // parses here with scale -i64::MAX, and subtracting it overflowed - a debug panic, or a
    // wrapped estimate in release. Saturating leaves the decision to the width logic below, which
    // is where it belongs: `1 mod 10e9223372036854775807` then estimates 1 digit and the
    // magnitude shortcut returns the dividend, while the reverse refuses on quotient width.
    let adjusted = |d: &BigDecimal| {
        i64::try_from(d.digits())
            .unwrap_or(i64::MAX)
            .saturating_sub(1)
            .saturating_sub(d.fractional_digit_count())
    };
    let quotient_digits = if is_zero(&a) {
        1
    } else {
        let span = adjusted(&a).saturating_sub(adjusted(&b)).max(0);
        u64::try_from(span).unwrap_or(u64::MAX).saturating_add(1)
    };
    if quotient_digits > MAX_DECIMAL_DIGITS {
        return Err(err(format!(
            "decimals.mod: the integral quotient would need {quotient_digits} digits"
        )));
    }
    // Computed on the unscaled integers, not through `&a / &b`. `bigdecimal`'s division
    // rounds to its default 100-digit precision, so truncating that quotient gave a *wrong
    // remainder* past 100 digits - silently:
    //
    //   1e99   mod 3 -> 1                          (correct)
    //   1e100  mod 3 -> 1                          (correct)
    //   1e101  mod 3 -> 10                         WRONG
    //   1e200  mod 3 -> 1 followed by 99 zeros     WRONG
    //   1e10000 mod 3 -> a 10000-digit number      WRONG
    //
    // The reference gives 1 for every one of them (10^k mod 3 is 1 for all k), and so do the
    // other clients, whose remainders are exact: libmpdec's `mpd_qrem` in Python and C++,
    // apd's `Rem` in Go, decimal.js's `mod` in the unbounded context in JS, and `BigInteger %`
    // in C#. The earlier test only reached 1E40 - 41 digits - so it never crossed the
    // threshold.
    //
    // Integer arithmetic makes it exact: align both coefficients on the finer scale and take
    // `A % B`, which truncates toward zero and so carries the dividend's sign, exactly as
    // BigDecimal.remainder and SQL MOD do.
    if magnitude_lt(&a, &b) {
        // |a| < |b| means an integral quotient of zero and a remainder of `a` itself. Settled
        // on magnitudes, so no power of ten is built - which is what keeps
        // `1e-2147483647 mod 1e2147483647` free, the case the aligned frame would refuse.
        return Ok(decimal_value(a));
    }
    let (a_int, a_scale) = a.clone().into_bigint_and_exponent();
    let (b_int, b_scale) = b.clone().into_bigint_and_exponent();
    let scale = a_scale.max(b_scale);
    // Aligning builds 10^|a_scale - b_scale|, so that gap has to be bounded too - the quotient
    // estimate above does not imply it. The |a| < |b| shortcut above has already taken the
    // far-apart cases that matter in practice.
    let gap = a_scale.abs_diff(b_scale);
    if gap > MAX_DECIMAL_DIGITS {
        return Err(err(format!(
            "decimals.mod: aligning the operands would need {gap} digits"
        )));
    }
    let pow = |n: u64| BigInt::from(10u8).pow(u32::try_from(n).unwrap_or(u32::MAX));
    let a_aligned = if scale > a_scale {
        a_int * pow((scale - a_scale) as u64)
    } else {
        a_int
    };
    let b_aligned = if scale > b_scale {
        b_int * pow((scale - b_scale) as u64)
    } else {
        b_int
    };
    Ok(decimal_value(BigDecimal::new(a_aligned % b_aligned, scale)))
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
    check_scale_width(&d, scale, "decimals.round")?;
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
// floor/ceil target scale 0 without going through `decimals_round`, so they carry the width
// bound separately. Scale 0 is a *widening* whenever the value's own scale is negative - a
// large positive exponent - and `floor(decimal("1e20000000"))` is then a 20000001-digit
// coefficient, reachable from a rule that names no scale at all.
fn decimals_floor(a: Value) -> Result<Value, ExecutionError> {
    let d = to_decimal(&a)?;
    check_scale_width(&d, 0, "decimals.floor")?;
    Ok(decimal_value(d.with_scale_round(0, RoundingMode::Floor)))
}
fn decimals_ceil(a: Value) -> Result<Value, ExecutionError> {
    let d = to_decimal(&a)?;
    check_scale_width(&d, 0, "decimals.ceil")?;
    Ok(decimal_value(d.with_scale_round(0, RoundingMode::Ceiling)))
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
/// A practical ceiling on the digits a decimal result may carry. 1 MiB of them is already absurd
/// for a rule value.
///
/// The JVM reference needs no such constant: `BigInteger` caps its own magnitude, so an
/// over-large `setScale` raises an `ArithmeticException` that surfaces as a failed rule.
/// `bigdecimal` has no cap, and an allocation failure in Rust aborts the process instead of
/// unwinding - so the bound is what keeps the caller-visible behaviour the same.
const MAX_DECIMAL_DIGITS: u64 = 1 << 20;

fn plain_decimal_string(d: &BigDecimal) -> Result<String, ExecutionError> {
    // A zero at a negative scale is "0", not "0" followed by that many zeros.
    // `to_plain_string` pads a zero out like any other coefficient, so `string(decimal("0E+3"))`
    // gave "0000". `BigDecimal.toPlainString` has this case in exactly this branch
    // ("if (this.scale < 0) { if (signum() == 0) return "0"; ... }") and only here: measured on
    // the JVM, a zero at a *positive* scale keeps its fractional zeros ("0.00" stays "0.00")
    // and a non-zero coefficient still pads (123 at scale -1 is "1230"). Checked before the
    // width bound below, since such a zero is cheap to render however extreme its scale.
    if is_zero(d) && d.fractional_digit_count() < 0 {
        return Ok("0".to_string());
    }
    let length = d
        .digits()
        .saturating_add(d.fractional_digit_count().unsigned_abs());
    if length > MAX_DECIMAL_DIGITS {
        return Err(err("string: decimal is too large to format"));
    }
    Ok(d.to_plain_string())
}

/// Rejects two operands whose aligned frame would exceed [`MAX_DECIMAL_DIGITS`].
///
/// Addition and subtraction expand the narrower operand into the wider one's frame, so the
/// frame carries the smaller scale's exponent and spans both magnitudes. No exemption for a
/// zero operand: aligning a zero at an extreme scale with `1` still expands the *one* into the
/// zero's scale.
fn check_alignment_width(
    a: &BigDecimal,
    b: &BigDecimal,
    function_name: &str,
) -> Result<(), ExecutionError> {
    let scale = a.fractional_digit_count().max(b.fractional_digit_count());
    // A zero operand contributes one digit whatever the distance: expanding a zero appends
    // none. That decides several cases outright, because alignment expands only the operand
    // whose scale is coarser. Measured on libmpdec, with the JDK agreeing on every row:
    // `0E+2e9 + 0E-2e9`, `0E+2e9 + 1` and `0E+2e9 mod 1E-2e9` are free at one digit, while
    // `0E-2e9 + 1` is 1601 MB and 2e9+1 digits (`ArithmeticException` there). Only the last
    // must be refused, and the difference is purely which operand expands.
    let widest = [a, b]
        .iter()
        .map(|d| {
            if is_zero(d) {
                return 1;
            }
            d.digits().saturating_add(
                scale
                    .saturating_sub(d.fractional_digit_count())
                    .unsigned_abs(),
            )
        })
        .max()
        .unwrap_or(0);
    let length = widest.saturating_add(1);
    if length > MAX_DECIMAL_DIGITS {
        return Err(err(format!(
            "{function_name}: aligning the operands would need {length} digits"
        )));
    }
    Ok(())
}

/// Rejects a target scale whose zero-padded result would exceed [`MAX_DECIMAL_DIGITS`].
///
/// Only widening needs checking: rounding to a coarser scale drops digits. `decimals.trunc`
/// never reaches a widening `with_scale_round` because it early-returns first, so only
/// `decimals.round` needs this.
fn check_scale_width(
    d: &BigDecimal,
    scale: i64,
    function_name: &str,
) -> Result<(), ExecutionError> {
    // A zero is one digit at any scale, so rescaling it expands nothing - and `bigdecimal`
    // agrees in fact, not just in principle: measured, `zero.with_scale_round(2147483647)`
    // takes 84 ns and yields one digit. Without this the estimate read the target scale and
    // refused `decimals.round(decimal("0"), 2147483647)`, which the reference holds at
    // precision 1 (`new BigDecimal(BigInteger.ZERO, 2147483647)`) and every other client in
    // the family accepts.
    if is_zero(d) {
        return Ok(());
    }
    let current = d.fractional_digit_count();
    if scale <= current {
        return Ok(());
    }
    let length = d
        .digits()
        .saturating_add(scale.saturating_sub(current).unsigned_abs());
    if length > MAX_DECIMAL_DIGITS {
        return Err(err(format!(
            "{function_name}: scale {scale} would produce {length} digits"
        )));
    }
    Ok(())
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

    /// The error text, for the cases where refusing is the behaviour under test.
    fn eval_err(expr: &str) -> String {
        let program = Program::compile(expr).expect("compile");
        match program.execute(&default_context()) {
            Ok(v) => panic!("expected an error, got {v:?}"),
            Err(e) => e.to_string(),
        }
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

    /// Alignment is the arithmetic width risk, and the only one: `add`/`sub` expand the
    /// narrower operand into the wider one's frame, `mod` has to produce the integral quotient
    /// first, and `mul`/`div`/comparison do neither. Measured on libmpdec in the Python
    /// sibling, peak RSS on operands 1e2147483647 and 3: `mul`, `div`, `<`, `==`, `min`, `neg`,
    /// `abs` all 13 MB; `add` 1738 MB, `sub` 1738 MB, `remainder` 1733 MB. `bigdecimal` has no
    /// cap of its own, and an allocation failure in Rust aborts the process rather than
    /// unwinding, so the bound is what keeps the caller-visible behaviour a failed rule.
    #[test]
    fn alignment_width_errors_instead_of_allocating() {
        for expr in [
            "decimals.add(decimal(\"1e2000000000\"), decimal(\"1\"))",
            "decimals.sub(decimal(\"1e2000000000\"), decimal(\"1\"))",
            "decimals.add(decimal(\"1e-2000000000\"), decimal(\"1\"))",
            "decimals.add(decimal(\"1e2000000000\"), decimal(\"1e-2000000000\"))",
            "decimals.mod(decimal(\"1e2000000000\"), decimal(\"3\"))",
            // The other direction: a tiny dividend against a divisor so fine that the integral
            // quotient spans the whole gap.
            "decimals.mod(decimal(\"1.5\"), decimal(\"1e-2000000000\"))",
        ] {
            assert!(
                Program::compile(expr)
                    .unwrap()
                    .execute(&default_context())
                    .is_err(),
                "{expr} must be refused on width"
            );
        }
    }

    /// `decimals.mod` must be **exact**, at any width.
    ///
    /// It was computed as `(&a / &b).with_scale_round(0, Down)` and `bigdecimal`'s division
    /// rounds to its default 100-digit precision, so past 100 digits the integral quotient was
    /// approximate and the remainder silently wrong:
    ///
    ///   1e99    mod 3 -> 1                       (correct)
    ///   1e100   mod 3 -> 1                       (correct)
    ///   1e101   mod 3 -> 10                      WRONG
    ///   1e200   mod 3 -> 1 and 99 zeros          WRONG
    ///   1e10000 mod 3 -> a 10000-digit number    WRONG
    ///
    /// Measured on the JDK, `new BigDecimal("1e" + k).remainder(new BigDecimal("3"))` is 1 for
    /// every k - 10^k mod 3 is 1 for all k - and the other five clients are exact too. The
    /// previous test stopped at 1E40, 41 digits, so it never crossed the threshold; these
    /// straddle it deliberately.
    /// `bigdecimal` accepts a scale the quotient-width estimate cannot hold. The JVM caps a
    /// scale at int32 - `new BigDecimal("10e9223372036854775807")` raises "Too many nonzero
    /// exponent digits" - while it parses here with scale -i64::MAX, and the adjusted-exponent
    /// subtraction overflowed: a panic in debug, a wrapped estimate in release. Saturating
    /// hands the case to the width logic, which answers both directions correctly.
    #[test]
    fn an_extreme_parsed_exponent_does_not_overflow_the_quotient_estimate() {
        let huge = "10e9223372036854775807";
        // |a| < |b|, so the remainder is the dividend - the magnitude shortcut reaches it only
        // because the estimate no longer overflows on the way there.
        assert_eq!(
            eval_str(&format!(
                "string(decimals.mod(decimal(\"1\"), decimal(\"{huge}\")))"
            )),
            "1"
        );
        // The reverse genuinely needs an astronomical quotient, and is refused as one.
        let err = eval_err(&format!(
            "decimals.mod(decimal(\"{huge}\"), decimal(\"1\"))"
        ));
        assert!(
            err.contains("integral quotient would need"),
            "unexpected error: {err}"
        );
        // The mirrored sign of the exponent does not overflow either.
        let tiny = "10e-9223372036854775807";
        let err = eval_err(&format!(
            "decimals.mod(decimal(\"1\"), decimal(\"{tiny}\"))"
        ));
        assert!(err.contains("would need"), "unexpected error: {err}");
    }

    /// A zero at a negative scale renders as "0", not as "0" followed by that many zeros.
    /// `to_plain_string` pads a zero out like any other coefficient, so `string(decimal("0E+3"))`
    /// answered "0000" where the reference, Python, JS and C++ all give "0" - measured. Go and
    /// C# had the same defect. `BigDecimal.toPlainString` special-cases zero in the
    /// negative-scale branch and only there, so the neighbours must keep their zeros.
    #[test]
    fn a_zero_at_a_negative_scale_renders_as_zero() {
        for (expr, want) in [
            // The fix.
            (r#"string(decimal("0E+3"))"#, "0"),
            (r#"string(decimal("0E+1"))"#, "0"),
            (r#"string(decimals.round(decimal("1.23"), -3))"#, "0"),
            // The neighbours, which must not change: a zero at a *positive* scale keeps its
            // fractional zeros, and a non-zero coefficient still pads.
            (r#"string(decimal("0.00"))"#, "0.00"),
            (r#"string(decimal("0"))"#, "0"),
            (r#"string(decimal("1E+1"))"#, "10"),
            (r#"string(decimals.round(decimal("1.23"), 2))"#, "1.23"),
        ] {
            assert_eq!(eval_str(expr), want, "{expr}");
        }
    }

    #[test]
    fn mod_is_exact_past_the_division_precision() {
        for k in [1u32, 10, 50, 99, 100, 101, 200, 1000, 10000] {
            let expr = format!("string(decimals.mod(decimal(\"1e{k}\"), decimal(\"3\"))) == \"1\"");
            assert_eq!(eval_bool(&expr), true, "1e{k} mod 3 must be exactly 1");
        }
        // A few more where the quotient is wide and the answer is not 1.
        for (expr, want) in [
            (
                "string(decimals.mod(decimal(\"1e200\"), decimal(\"7\")))",
                "2",
            ),
            (
                "string(decimals.mod(decimal(\"1e500\"), decimal(\"9\")))",
                "1",
            ),
            (
                "string(decimals.mod(decimal(\"12.34\"), decimal(\"1.5\")))",
                "0.34",
            ),
            (
                "string(decimals.mod(decimal(\"-1e101\"), decimal(\"3\")))",
                "-1",
            ),
        ] {
            assert_eq!(eval_str(expr), want, "{expr}");
        }
    }

    /// Expanding a *zero* is free, so the aligned frame is set by the operands that actually
    /// have digits - several of these turn on which operand expands rather than on how far
    /// apart the scales are. A zero also keeps whatever scale it was built with, so its
    /// adjusted exponent says nothing about the cost, which is what an earlier estimate got
    /// wrong. Every row measured on libmpdec and on the JDK, which agree:
    ///
    ///   0E+2e9 + 0E-2e9      free, 1 digit           precision 1
    ///   0E+2e9 + 1           free, 1 digit           precision 1, scale 0 (the zero expands)
    ///   0E+2e9 mod 1E-2e9    free, 1 digit           precision 1
    ///   1 + 0E-2e9           1601 MB, 2e9+1 digits   ArithmeticException (the *one* expands)

    #[test]
    fn expanding_a_zero_operand_is_free() {
        for expr in [
            "decimals.eq(decimals.add(decimal(\"0E+2000000000\"), decimal(\"0E-2000000000\")), \
             decimal(\"0\"))",
            "decimals.eq(decimals.sub(decimal(\"0E+2000000000\"), decimal(\"0E-2000000000\")), \
             decimal(\"0\"))",
            "decimals.eq(decimals.add(decimal(\"0E+2000000000\"), decimal(\"1\")), decimal(\"1\"))",
            "decimals.eq(decimals.mod(decimal(\"0E+2000000000\"), decimal(\"1E-2000000000\")), \
             decimal(\"0\"))",
            "decimals.eq(decimals.mod(decimal(\"0E-2000000000\"), decimal(\"1E+2000000000\")), \
             decimal(\"0\"))",
            "decimals.eq(decimals.mod(decimal(\"0\"), decimal(\"3\")), decimal(\"0\"))",
        ] {
            assert_eq!(eval_bool(expr), true, "{expr} must be free and answer");
        }
        // The row that must still be refused: here the *one* expands into the zero's scale, so
        // a blanket zero exemption would have let it through.
        assert!(
            Program::compile("decimals.add(decimal(\"1\"), decimal(\"0E-2000000000\"))")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
    }

    /// The must-fail twin. `mul` is unguarded at any width, comparison never aligns, and
    /// alignment that stays narrow is accepted however extreme both operands are.
    #[test]
    fn the_cheap_operations_stay_unguarded() {
        for expr in [
            "decimals.eq(decimals.mul(decimal(\"1e2000000000\"), decimal(\"1e-2000000000\")), decimal(\"1\"))",
            "decimals.lt(decimal(\"1e-2000000000\"), decimal(\"1e2000000000\"))",
            "decimals.eq(decimals.sub(decimal(\"1e2000000000\"), decimal(\"1e2000000000\")), decimal(\"0\"))",
            "decimals.eq(decimals.add(decimal(\"12.34\"), decimal(\"1.5\")), decimal(\"13.84\"))",
            "decimals.eq(decimals.mod(decimal(\"1E40\"), decimal(\"3\")), decimal(\"1\"))",
            // `mod` is bounded by its integral quotient, not by the aligned frame - so a
            // dividend smaller than the divisor is free however far apart they are, and the
            // result is the dividend itself. This is what a frame-based guard refused: the
            // JVM gives precision 1 at scale 2000000000, and `bigdecimal` takes 68us.
            "decimals.eq(decimals.mod(decimal(\"1e-2000000000\"), decimal(\"1e2000000000\")), \
             decimal(\"1e-2000000000\"))",
            // And operands whose magnitudes are close, however extreme both are: the quotient
            // spans only the difference.
            "decimals.eq(decimals.mod(decimal(\"1e2000000000\"), decimal(\"1e1999999999\")), \
             decimal(\"0\"))",
        ] {
            assert_eq!(
                eval_bool(expr),
                true,
                "{expr} must stay unbounded and answer"
            );
        }
    }

    /// `floor`/`ceil` target scale 0 without going through `decimals_round`, so they carry the
    /// bound separately - and scale 0 is a *widening* whenever the value's own scale is
    /// negative. Coarsening stays free: rounding to a coarser scale drops digits rather than
    /// adding them, which is why `check_scale_width` early-returns there.
    #[test]
    fn the_one_argument_rounding_family_is_bounded_too() {
        for expr in [
            "decimals.round(decimal(\"1e20000000\"))",
            "decimals.floor(decimal(\"1e20000000\"))",
            "decimals.ceil(decimal(\"1e20000000\"))",
        ] {
            assert!(
                Program::compile(expr)
                    .unwrap()
                    .execute(&default_context())
                    .is_err(),
                "{expr} must be refused on width"
            );
        }
        // Coarsening is free at any distance, so these answer.
        for expr in [
            "decimals.eq(decimals.round(decimal(\"1e-20000000\")), decimal(\"0\"))",
            "decimals.eq(decimals.floor(decimal(\"1e-20000000\")), decimal(\"0\"))",
            "decimals.eq(decimals.ceil(decimal(\"1e-20000000\")), decimal(\"1\"))",
            "decimals.eq(decimals.trunc(decimal(\"1e-20000000\")), decimal(\"0\"))",
            "decimals.eq(decimals.round(decimal(\"2.5\")), decimal(\"3\"))",
            // A zero rescales to any target for free - measured 84 ns at 2^31 in `bigdecimal`,
            // and the reference holds it at precision 1. The width estimate read the target
            // scale and refused these.
            "decimals.eq(decimals.round(decimal(\"0\"), 2147483647), decimal(\"0\"))",
            "decimals.eq(decimals.round(decimal(\"0\"), 20000000), decimal(\"0\"))",
            "decimals.eq(decimals.trunc(decimal(\"0\"), 2147483647), decimal(\"0\"))",
            "decimals.eq(decimals.floor(decimal(b\"\", 2147483647)), decimal(\"0\"))",
            "decimals.eq(decimals.ceil(decimal(b\"\", 2147483647)), decimal(\"0\"))",
            "decimals.eq(decimals.floor(decimal(\"-1.5\")), decimal(\"-2\"))",
        ] {
            assert_eq!(eval_bool(expr), true, "{expr} must answer");
        }
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
    fn decimals_round_rejects_an_absurd_scale() {
        // i32::MAX passes require_int_scale, and with_scale_round would then zero-pad to that
        // many digits - gigabytes, and an allocation failure in Rust aborts the process rather
        // than unwinding. The JVM reference gets an ArithmeticException from BigInteger's own
        // magnitude cap, so a rule error is the matching outcome. Reaching the assertion at all
        // is most of the point: it means nothing tried to allocate.
        assert!(
            Program::compile("decimals.round(decimal('1.5'), 2147483647)")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
    }

    #[test]
    fn decimals_round_still_widens_reasonably() {
        assert_eq!(
            eval_str("string(decimals.round(decimal('1.5'), 10))"),
            "1.5000000000"
        );
        // Coarsening is unaffected by the bound.
        assert_eq!(
            eval_str("string(decimals.round(decimal('1.55'), 1))"),
            "1.6"
        );
        // A hugely negative scale drops digits rather than padding, so it must not be rejected.
        assert!(
            Program::compile("decimals.round(decimal('1.5'), -2147483648)")
                .unwrap()
                .execute(&default_context())
                .is_ok()
        );
    }

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

    /// Cross-client parity: a bare `confluent.type.Decimal` field is usable with `decimals.*`,
    /// `==`, `string()` and `double()` with **no `decimal(...)` call** on it. The discriminating
    /// case is the scale-differing equality: a client comparing decimals by their protobuf
    /// encoding (unscaled bytes plus scale, field by field) answers false for
    /// `decimal("12.340")`, because 12.34 and 12.340 are the same number in two encodings.
    #[test]
    fn proto_decimal_needs_no_constructor() {
        use crate::rules::cel::cel_executor::from_protobuf_value_for_test;
        use prost_reflect::{DynamicMessage, Value as ProtoValue};

        let desc = crate::DESCRIPTOR_POOL
            .get_message_by_name(super::DECIMAL_TYPE_NAME)
            .expect("decimal.proto is compiled into the descriptor pool");
        let mut msg = DynamicMessage::new(desc);
        // 12.34 = unscaled 1234 (0x04D2) at scale 2.
        msg.set_field_by_name("value", ProtoValue::Bytes(vec![0x04, 0xd2].into()));
        msg.set_field_by_name("scale", ProtoValue::I32(2));
        let this = from_protobuf_value_for_test(&ProtoValue::Message(msg));

        for expr in [
            // Bare: no constructor call on the field.
            "decimals.eq(this, decimal(\"12.34\"))",
            "decimals.gt(this, decimal(\"10.00\"))",
            // The wrapped form must keep working (decimal(...) re-entry).
            "decimals.eq(decimal(this), decimal(\"12.34\"))",
            // `==` is numeric on it: 12.34 equals 12.340 despite the differing scale.
            "this == decimal(\"12.340\")",
            "decimals.lt(this, decimal(\"100\"))",
            "string(this) == \"12.34\"",
            "double(this) == 12.34",
        ] {
            assert!(eval_bool_with(expr, this.clone()), "{expr}");
        }
        // Negative controls.
        assert!(!eval_bool_with("this != decimal(\"12.340\")", this.clone()));
        assert!(!eval_bool_with(
            "decimals.gt(this, decimal(\"100\"))",
            this.clone()
        ));
    }

    fn eval_bool_with(expr: &str, this: Value) -> bool {
        let program = Program::compile(expr).expect("compile");
        let mut ctx = default_context();
        ctx.add_variable_from_value("this", this);
        matches!(program.execute(&ctx).expect("execute"), Value::Bool(true))
    }
}
