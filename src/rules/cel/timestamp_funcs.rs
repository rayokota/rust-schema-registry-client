//! CEL bindings for the `timestamp` constructor.
//!
//! cel-rust already provides a stdlib `timestamp(string)` (RFC 3339 parsing),
//! `timestamp(timestamp)` (identity) and the standard timestamp operators (`<`, `>`, `==`,
//! `- duration`, `+ duration`, etc.). Two overloads are added here, both on the standard name —
//! there is no `timestamp.of` namespace:
//!
//!   * `timestamp(int) -> timestamp` — epoch **seconds**, the one stdlib overload cel-rust is
//!     missing, which cel-java/go/cpp/csharp all declare.
//!   * `timestamp(int, int) -> timestamp` — an epoch value at a Flink-style decimal precision:
//!     0 seconds, 3 millis, 6 micros, 9 nanos.
//!
//! Nothing extra is needed for the one-argument non-int cases: an Avro timestamp field is
//! converted to a `Value::Timestamp` at the boundary, which stdlib's identity overload already
//! accepts, so it needs no wrapper at all.

use cel::extractors::Arguments;
use cel::{Context, ExecutionError, Value};
use chrono::{DateTime, FixedOffset, Utc};

const UNIT_SECONDS: &str = "seconds";
const UNIT_MILLIS: &str = "millis";
const UNIT_MICROS: &str = "micros";
const UNIT_NANOS: &str = "nanos";

fn err(msg: impl Into<String>) -> ExecutionError {
    ExecutionError::FunctionError {
        function: "timestamp".to_string(),
        message: msg.into(),
    }
}

/// CEL's timestamp range: `0001-01-01T00:00:00Z` through `9999-12-31T23:59:59.999999999Z`, the
/// `google.protobuf.Timestamp` contract the CEL specification adopts wholesale.
///
/// chrono's own range is far wider (roughly year -262143 through 262142), so `from_timestamp`
/// accepts instants no other client will: `timestamp(253402300800)` built a year-10000 value that
/// merely compared unequal, where cel-java, cel-go, cel-cpp and cel-python all raise.
const MIN_TIMESTAMP_SECONDS: i64 = -62_135_596_800;
const MAX_TIMESTAMP_SECONDS: i64 = 253_402_300_799;

fn check_range(utc: DateTime<Utc>) -> Result<DateTime<Utc>, ExecutionError> {
    let seconds = utc.timestamp();
    if !(MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS).contains(&seconds) {
        return Err(err(format!(
            "timestamp: seconds ({seconds}) must be in range \
             [{MIN_TIMESTAMP_SECONDS}, {MAX_TIMESTAMP_SECONDS}]"
        )));
    }
    Ok(utc)
}

/// Builds a CEL timestamp from an epoch numeric value plus a unit string.
pub fn from_epoch(value: i64, unit: &str) -> Result<DateTime<FixedOffset>, ExecutionError> {
    let utc: DateTime<Utc> = match unit {
        UNIT_SECONDS => DateTime::from_timestamp(value, 0)
            .ok_or_else(|| err("timestamp: seconds value out of range"))?,
        UNIT_MILLIS => DateTime::from_timestamp_millis(value)
            .ok_or_else(|| err("timestamp: millis value out of range"))?,
        UNIT_MICROS => DateTime::from_timestamp_micros(value)
            .ok_or_else(|| err("timestamp: micros value out of range"))?,
        // chrono supports nanosecond precision; nanos never overflows an i64 timestamp.
        UNIT_NANOS => DateTime::from_timestamp_nanos(value),
        _ => {
            return Err(err(format!(
                "timestamp: unknown unit '{unit}'; expected one of seconds, millis, micros, nanos"
            )));
        }
    };
    Ok(check_range(utc)?.fixed_offset())
}

/// The unit a Flink-style decimal precision names. Precisions outside {0, 3, 6, 9} are rejected
/// rather than generalized to "any p means 10^-p": with the unit a number rather than a name,
/// that check is the only thing between a typo and a silently wrong instant.
fn unit_for_precision(precision: i64) -> Result<&'static str, ExecutionError> {
    match precision {
        0 => Ok(UNIT_SECONDS),
        3 => Ok(UNIT_MILLIS),
        6 => Ok(UNIT_MICROS),
        9 => Ok(UNIT_NANOS),
        _ => Err(err(format!(
            "timestamp: unknown precision {precision}; expected 0 (seconds), 3 (millis), \
             6 (micros) or 9 (nanos)"
        ))),
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Int(i) => Some(*i),
        Value::UInt(u) => i64::try_from(*u).ok(),
        _ => None,
    }
}

/// Backs the `timestamp` overloads cel-rust's stdlib does not declare (it registers only
/// `string_to_timestamp` and `timestamp_to_timestamp`): `(int)` as epoch **seconds**, matching
/// cel-java's `int64_to_timestamp` and Go/C++/C#, and `(int, int)` as an epoch value at a
/// decimal precision.
///
/// cel-rust resolves a call against the `Env` overloads first and only falls back to the
/// `Context::add_function` registry when no `Env` overload matches, so `timestamp(string)` and
/// `timestamp(timestamp)` keep hitting the stdlib and never reach this function. Anything else is
/// reported as the crate's normal no-such-overload error.
fn timestamp_fn(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    match args.as_slice() {
        [value, precision] => {
            let value =
                as_i64(value).ok_or_else(|| err("timestamp: the epoch value must be an int"))?;
            let precision =
                as_i64(precision).ok_or_else(|| err("timestamp: the precision must be an int"))?;
            Ok(Value::Timestamp(from_epoch(
                value,
                unit_for_precision(precision)?,
            )?))
        }
        [Value::Int(seconds)] => Ok(Value::Timestamp(from_epoch(*seconds, UNIT_SECONDS)?)),
        _ => Err(ExecutionError::NoSuchOverload),
    }
}

/// Registers the epoch-seconds `timestamp(int)` and precision `timestamp(int, int)` overloads.
pub fn add_timestamp_functions(ctx: &mut Context) {
    ctx.add_function("timestamp", timestamp_fn);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::cel::cel_lib::default_context;
    use cel::{Program, Value};

    #[test]
    fn from_epoch_units() {
        // 1_500_000_000 seconds since the epoch, expressed in each unit, is the same instant.
        assert_eq!(
            from_epoch(1_500_000_000, "seconds").unwrap().timestamp(),
            1_500_000_000
        );
        assert_eq!(
            from_epoch(1_500_000_000_000, "millis").unwrap().timestamp(),
            1_500_000_000
        );
        assert_eq!(
            from_epoch(1_500_000_000_000_000, "micros")
                .unwrap()
                .timestamp(),
            1_500_000_000
        );
        let nanos = from_epoch(1_500_000_000_123_456_789, "nanos").unwrap();
        assert_eq!(nanos.timestamp(), 1_500_000_000);
        assert_eq!(nanos.timestamp_subsec_nanos(), 123_456_789);
    }

    #[test]
    fn from_epoch_unknown_unit_errors() {
        assert!(from_epoch(0, "weeks").is_err());
    }

    #[test]
    fn from_epoch_out_of_range_errors() {
        // i64::MAX seconds is far past chrono's supported range.
        assert!(from_epoch(i64::MAX, "seconds").is_err());
    }

    fn eval(expr: &str) -> Value {
        Program::compile(expr)
            .expect("compile")
            .execute(&default_context())
            .expect("execute")
    }

    #[test]
    fn two_arg_precision_dispatch_through_cel() {
        // Every precision names the same instant, and precision 0 equals the 1-arg form.
        for expr in [
            "timestamp(1500000000, 0) == timestamp(1500000000)",
            "timestamp(1500000000000, 3) == timestamp(1500000000)",
            "timestamp(1500000000000000, 6) == timestamp(1500000000)",
            "timestamp(1500000000000000000, 9) == timestamp(1500000000)",
            // Sub-second precision survives, and the two arities differ for the same int.
            "timestamp(1700000000123, 3) == timestamp(\"2023-11-14T22:13:20.123Z\")",
            "timestamp(1700000000, 3) != timestamp(1700000000)",
        ] {
            assert!(matches!(eval(expr), Value::Bool(true)), "{expr}");
        }
    }

    #[test]
    fn precision_outside_the_set_errors() {
        for precision in [1, 2, 4, 5, 7, 8, 10, -3] {
            match try_eval(&format!("timestamp(1700000000, {precision})")) {
                Err(cel::ExecutionError::FunctionError { function, message }) => {
                    assert_eq!(function, "timestamp");
                    assert!(
                        message.contains("unknown precision"),
                        "precision {precision}: {message}"
                    );
                }
                other => panic!("precision {precision}: expected a FunctionError, got {other:?}"),
            }
        }
    }

    fn try_eval(expr: &str) -> Result<Value, cel::ExecutionError> {
        Program::compile(expr)
            .expect("compile")
            .execute(&default_context())
    }

    #[test]
    fn bare_int_is_epoch_seconds() {
        // cel-java's int64_to_timestamp: a bare int is seconds since the epoch.
        assert!(matches!(
            eval("timestamp(1700000000) == timestamp(\"2023-11-14T22:13:20Z\")"),
            Value::Bool(true)
        ));
        assert!(matches!(
            eval("timestamp(1700000000).getFullYear()"),
            Value::Int(2023)
        ));
    }

    #[test]
    fn bare_int_accepts_pre_epoch() {
        assert!(matches!(
            eval("timestamp(-1) == timestamp(\"1969-12-31T23:59:59Z\")"),
            Value::Bool(true)
        ));
    }

    #[test]
    fn cel_timestamp_range_is_enforced() {
        // CEL's range is google.protobuf.Timestamp's: 0001-01-01T00:00:00Z through
        // 9999-12-31T23:59:59.999999999Z. chrono accepts far wider (roughly year -262143 to
        // 262142), so it has to be checked explicitly — cel-java raises on each of these, and
        // before this check the first one built a year-10000 instant that merely compared unequal.
        for expr in [
            "timestamp(253402300800)",
            "timestamp(-62135596801)",
            "timestamp(253402300800000, 3)",
        ] {
            match try_eval(expr) {
                Err(cel::ExecutionError::FunctionError { function, message }) => {
                    assert_eq!(function, "timestamp");
                    assert!(message.contains("must be in range"), "{expr}: {message}");
                }
                other => panic!("{expr}: expected a FunctionError, got {other:?}"),
            }
        }
        // Both boundaries are themselves valid.
        for expr in [
            "timestamp(253402300799).getFullYear() == 9999",
            "timestamp(-62135596800).getFullYear() == 1",
        ] {
            assert!(matches!(eval(expr), Value::Bool(true)), "{expr}");
        }
    }

    #[test]
    fn bare_int_out_of_range_errors() {
        // Must be a clean FunctionError, not a panic (and not an UndeclaredReference, which would
        // mean the overload never reached us).
        for expr in [
            "timestamp(9223372036854775807)",
            "timestamp(-9223372036854775807)",
        ] {
            match try_eval(expr) {
                Err(cel::ExecutionError::FunctionError { function, .. }) => {
                    assert_eq!(function, "timestamp")
                }
                other => panic!("{expr}: expected a timestamp FunctionError, got {other:?}"),
            }
        }
    }

    #[test]
    fn stdlib_string_overload_is_not_shadowed() {
        // `Context::add_function` is a single impl per name, so verify the Env overloads still win.
        assert!(matches!(
            eval("timestamp(\"2023-11-14T22:13:20Z\").getFullYear()"),
            Value::Int(2023)
        ));
        // An unparseable string still reaches the stdlib parse error rather than our int impl.
        assert!(try_eval("timestamp(\"not-a-timestamp\")").is_err());
    }

    #[test]
    fn stdlib_timestamp_identity_is_not_shadowed() {
        assert!(matches!(
            eval("timestamp(timestamp(\"2023-11-14T22:13:20Z\")) == timestamp(1700000000)"),
            Value::Bool(true)
        ));
        assert!(matches!(
            eval("timestamp(timestamp(1700000000, 0)) == timestamp(1700000000)"),
            Value::Bool(true)
        ));
    }

    #[test]
    fn timestamp_of_namespace_is_gone() {
        // The namespaced form is no longer registered; `timestamp.of` must not resolve.
        assert!(try_eval("timestamp.of(1700000000000, 3)").is_err());
    }

    #[test]
    fn unhandled_timestamp_arg_shapes_error() {
        // Not swallowed: no matching Env overload and no matching arm here.
        assert!(try_eval("timestamp(1.5)").is_err());
        assert!(try_eval("timestamp(true)").is_err());
        // Two args now means (value, precision), so this fails on the precision, not the arity.
        assert!(try_eval("timestamp(1, 2)").is_err());
    }
}
