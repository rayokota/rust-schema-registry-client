//! CEL binding for the `timestamp.of` constructor.
//!
//! cel-rust already provides a stdlib `timestamp(string)` (RFC 3339 parsing) plus the standard
//! timestamp operators (`<`, `>`, `==`, `- duration`, `+ duration`, etc.). The extension we add
//! here is the namespaced `timestamp.of(...)` constructor, mirroring the other clients:
//!
//!   * `timestamp.of(dyn) -> timestamp` — runtime-dispatches on the value (already-decoded
//!     timestamp, RFC 3339 string, etc.).
//!   * `timestamp.of(int, string) -> timestamp` — epoch numeric + unit
//!     (`seconds` | `millis` | `micros` | `nanos`).
//!
//! We use the namespaced form (rather than extending stdlib `timestamp(...)` with a `(dyn)`
//! overload) because `(dyn)` and `(string)` would overlap per the CEL signature-overlap rule on
//! conformant impls (cel-java/go/cpp); the namespaced form keeps cross-client parity.

use cel::extractors::Arguments;
use cel::{Context, ExecutionError, Value};
use chrono::{DateTime, FixedOffset, Utc};

const UNIT_SECONDS: &str = "seconds";
const UNIT_MILLIS: &str = "millis";
const UNIT_MICROS: &str = "micros";
const UNIT_NANOS: &str = "nanos";

fn err(msg: impl Into<String>) -> ExecutionError {
    ExecutionError::FunctionError {
        function: "timestamp.of".to_string(),
        message: msg.into(),
    }
}

/// Builds a CEL timestamp from an epoch numeric value plus a unit string.
pub fn from_epoch(value: i64, unit: &str) -> Result<DateTime<FixedOffset>, ExecutionError> {
    let utc: DateTime<Utc> = match unit {
        UNIT_SECONDS => DateTime::from_timestamp(value, 0)
            .ok_or_else(|| err("timestamp.of: seconds value out of range"))?,
        UNIT_MILLIS => DateTime::from_timestamp_millis(value)
            .ok_or_else(|| err("timestamp.of: millis value out of range"))?,
        UNIT_MICROS => DateTime::from_timestamp_micros(value)
            .ok_or_else(|| err("timestamp.of: micros value out of range"))?,
        // chrono supports nanosecond precision; nanos never overflows an i64 timestamp.
        UNIT_NANOS => DateTime::from_timestamp_nanos(value),
        _ => {
            return Err(err(format!(
                "timestamp.of: unknown unit '{unit}'; expected one of seconds, millis, micros, nanos"
            )));
        }
    };
    Ok(utc.fixed_offset())
}

/// Runtime dispatch backing `timestamp.of(...)`: `(dyn)` or `(int, string)`.
fn timestamp_of(Arguments(args): Arguments) -> Result<Value, ExecutionError> {
    match args.as_slice() {
        [Value::Int(value), Value::String(unit)] => Ok(Value::Timestamp(from_epoch(*value, unit)?)),
        [Value::UInt(value), Value::String(unit)] => Ok(Value::Timestamp(from_epoch(
            i64::try_from(*value).map_err(|_| err("timestamp.of: epoch value out of range"))?,
            unit,
        )?)),
        [_, _] => Err(err(
            "timestamp.of: expected (int, string) for the 2-arg form",
        )),
        [v] => timestamp_of_dyn(v),
        _ => Err(err("timestamp.of: expected 1 or 2 args")),
    }
}

fn timestamp_of_dyn(v: &Value) -> Result<Value, ExecutionError> {
    match v {
        // Already a timestamp (e.g. from an Avro timestamp-* logical field): pass through.
        Value::Timestamp(_) => Ok(v.clone()),
        // Delegate RFC 3339 strings to the same parse the stdlib timestamp() uses.
        Value::String(s) => DateTime::parse_from_rfc3339(s)
            .map(Value::Timestamp)
            .map_err(|e| err(format!("timestamp.of: invalid RFC 3339 string: {e}"))),
        Value::Null => Err(err("timestamp.of: cannot convert null to Timestamp")),
        Value::Bool(_) => Err(err("timestamp.of: cannot convert bool to Timestamp")),
        // A raw int carries no unit — force the caller to the 2-arg form.
        Value::Int(_) | Value::UInt(_) => Err(err(
            "timestamp.of: raw int has no unit; use timestamp.of(value, \
             \"seconds\"|\"millis\"|\"micros\"|\"nanos\")",
        )),
        _ => Err(err("timestamp.of: cannot convert value to Timestamp")),
    }
}

/// Registers `timestamp.of` on `ctx`. The namespaced name is dispatched by the executor's AST
/// rewrite (cel-rust resolves member calls by bare name).
pub fn add_timestamp_functions(ctx: &mut Context) {
    ctx.add_function("timestamp.of", timestamp_of);
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
    fn two_arg_dispatch_through_cel() {
        // timestamp.of(value, unit) constructs, and the result compares as a timestamp.
        assert!(matches!(
            eval(
                "timestamp.of(1500000000000, \"millis\") == timestamp.of(1500000000, \"seconds\")"
            ),
            Value::Bool(true)
        ));
    }

    #[test]
    fn raw_int_without_unit_errors() {
        assert!(
            Program::compile("timestamp.of(1500000000)")
                .unwrap()
                .execute(&default_context())
                .is_err()
        );
    }
}
