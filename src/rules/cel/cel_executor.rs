use crate::rules::cel::cel_lib::default_context;
use crate::serdes::serde::{RuleBase, RuleContext, RuleExecutor, SerdeError, SerdeValue};
use async_trait::async_trait;
use cel_interpreter::objects::{Key, Map};
use cel_interpreter::{Context, ExecutionError, ParseErrors, Program, Value};
use dashmap::DashMap;
use prost::bytes::Bytes;
use prost_reflect::{MapKey, ReflectMessage};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct CelExecutor {
    cache: DashMap<String, Program>,
    presence_cache: DashMap<(String, String), Arc<PresencePaths>>,
}

impl RuleBase for CelExecutor {
    fn get_type(&self) -> &'static str {
        "CEL"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Default for CelExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CelExecutor {
    pub fn new() -> Self {
        CelExecutor {
            cache: DashMap::new(),
            presence_cache: DashMap::new(),
        }
    }

    /// The `message` binding for a rule: the message, with the keys of unset fields the rule
    /// tests with `has()` dropped.
    ///
    /// The JVM client hands the message itself to its engine, which answers `has()` from
    /// protobuf presence and a plain read from the field's default. A message bound as a map
    /// cannot do both from one key, so the keys a rule actually tests are the ones dropped -
    /// the same resolution [`crate::rules::cel::cel_validator::CelValidator`] applies to
    /// `this`, and applied here so that a rule reads the same either way.
    pub(crate) fn message_binding(&self, ctx: &RuleContext, msg: &SerdeValue) -> Value {
        match ctx.rule.expr.as_deref() {
            Some(expr) => {
                from_serde_value_with_presence(msg, &self.presence_paths(expr, "message"))
            }
            None => from_serde_value(msg),
        }
    }

    /// The paths `expr` tests `binding` for presence on, compiled once per expression.
    ///
    /// A rule's expression can be a `guard ; body` pair, which is not itself a CEL
    /// expression. The parts are scanned separately and their paths pooled: both are
    /// evaluated against the same bindings, so a `has()` in either one has to drop the key.
    fn presence_paths(&self, expr: &str, binding: &str) -> Arc<PresencePaths> {
        let key = (expr.to_string(), binding.to_string());
        if let Some(cached) = self.presence_cache.get(&key) {
            return cached.value().clone();
        }
        let mut paths = PresencePaths::new();
        for part in expr.split(';') {
            paths.extend(collect_has_paths(part, binding));
        }
        let paths = Arc::new(paths);
        self.presence_cache.insert(key, paths.clone());
        paths
    }

    pub(crate) fn execute(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
        args: &HashMap<String, Value>,
    ) -> Result<SerdeValue, SerdeError> {
        let mut expr = ctx.rule.expr.clone().ok_or(SerdeError::Rule(
            "rule does not contain an expression".to_string(),
        ))?;
        let parts: Vec<&str> = expr.split(";").collect();
        if parts.len() > 1 {
            let guard = parts[0];
            if !guard.is_empty() {
                let guard_result = self.execute_rule(ctx, msg, guard, args)?;
                if !guard_result.as_bool() {
                    return Ok(msg.clone());
                }
            }
            expr = parts[1].to_string();
        }
        self.execute_rule(ctx, msg, &expr, args)
    }

    fn execute_rule(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
        expr: &str,
        args: &HashMap<String, Value>,
    ) -> Result<SerdeValue, SerdeError> {
        let mut prog = self.cache.get(expr);
        if prog.is_none() {
            let program = Program::compile(expr)?;
            self.cache.insert(expr.to_string(), program);
        }
        prog = self.cache.get(expr);
        let prog = prog.ok_or(SerdeError::Rule("failed to compile program".to_string()))?;
        let mut context = default_context();
        for (k, v) in args {
            context.add_variable_from_value(k.clone(), v.clone());
        }
        let result = prog.value().execute(&context)?;
        Ok(to_serde_value(msg, &result))
    }

    pub fn register() {
        crate::serdes::rule_registry::register_rule_executor(CelExecutor::new());
    }
}

#[async_trait]
impl RuleExecutor for CelExecutor {
    async fn transform(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        let mut args = HashMap::new();
        args.insert("message".to_string(), self.message_binding(ctx, msg));
        self.execute(ctx, msg, &args)
    }
}

pub fn from_serde_value(value: &SerdeValue) -> Value {
    // No paths, so every field keeps its key. That is the right conversion for a caller with
    // no rule to inspect; a caller that has one binds through
    // [`CelExecutor::message_binding`] instead, so that `has()` reports presence.
    from_serde_value_with_presence(value, &PresencePaths::new())
}

/// As [`from_serde_value`], but omitting the keys of unset fields that the rule tests with
/// `has()`. See [`collect_has_paths`].
pub(crate) fn from_serde_value_with_presence(
    value: &SerdeValue,
    presence: &PresencePaths,
) -> Value {
    match value {
        SerdeValue::Avro(v) => from_avro_value(v),
        SerdeValue::Protobuf(v) => from_protobuf_value_with_presence(v, presence, &[], &[]),
        SerdeValue::Json(v) => from_json_value(v),
    }
}

fn from_avro_value(value: &apache_avro::types::Value) -> Value {
    match value {
        apache_avro::types::Value::Boolean(v) => Value::Bool(*v),
        apache_avro::types::Value::Int(v) => Value::Int(*v as i64),
        apache_avro::types::Value::Long(v) => Value::Int(*v),
        apache_avro::types::Value::Float(v) => Value::Float(*v as f64),
        apache_avro::types::Value::Double(v) => Value::Float(*v),
        apache_avro::types::Value::String(v) => Value::String(Arc::new((*v).clone())),
        apache_avro::types::Value::Bytes(v) => Value::Bytes(Arc::new(v.to_vec())),
        apache_avro::types::Value::Fixed(_, v) => Value::Bytes(Arc::new(v.to_vec())),
        apache_avro::types::Value::Enum(_, v) => Value::String(Arc::new((*v).clone())),
        apache_avro::types::Value::Array(v) => {
            Value::List(Arc::new(v.iter().map(from_avro_value).collect()))
        }
        apache_avro::types::Value::Map(v) => Value::Map(Map {
            map: Arc::new(
                v.iter()
                    .map(|(k, v)| (Key::String(Arc::new(k.clone())), from_avro_value(v)))
                    .collect(),
            ),
        }),
        apache_avro::types::Value::Record(v) => {
            let mut map: HashMap<Key, Value> = HashMap::with_capacity(v.len());
            for (k, v) in v {
                map.insert(Key::String(Arc::new(k.clone())), from_avro_value(v));
            }
            Value::Map(Map { map: Arc::new(map) })
        }
        apache_avro::types::Value::Null => Value::Null,
        _ => Value::Null,
    }
}

/// The field paths a rule applies `has()` to, relative to one binding.
///
/// A path is the chain of field names under the binding, so `has(this.a.b)` collects
/// `["a", "b"]` for the binding `this`.
pub(crate) type PresencePaths = HashSet<Vec<String>>;

/// Collects every path a rule tests with `has()` under `binding`.
///
/// A message is bound to CEL as a map, and `has()` on a map is a key-presence check, so an
/// unset field has to be missing from the map for `has()` to answer `false`. But the key
/// also has to be there for a plain read like `this.count == 0` to resolve at all. The two
/// cannot both hold, so the key is omitted only for the paths a rule actually tests - which
/// is what this finds. prost-protovalidate resolves it the same way, for the same reason,
/// though it stops at the binding rather than following comprehension variables.
///
/// The expression is parsed a second time here: `Program` keeps its AST private, and
/// re-parsing once per distinct rule is cheap next to evaluating it per message.
///
/// A `has()` inside a comprehension is rooted at the comprehension's own variable rather
/// than at the binding, so the variables are followed back to the path they stand for - see
/// [`Roots`]. What remains untracked is a root that only exists at run time, such as the
/// element of an indexed read; those fields keep their key, which is the safe direction:
/// `has()` over-reports rather than a plain read failing.
pub(crate) fn collect_has_paths(expr: &str, binding: &str) -> PresencePaths {
    let mut paths = PresencePaths::new();
    if let Ok(parsed) = cel_parser::Parser::default().parse(expr) {
        let roots = Roots::from([(binding.to_string(), Vec::new())]);
        walk_for_has(&parsed, &roots, &mut paths);
    }
    paths
}

/// The path each identifier in scope stands for, relative to the binding.
///
/// The binding itself stands for the empty path. A comprehension variable stands for the
/// path of the collection it iterates: an element of a list, or a value of a map, is reached
/// by the same path as the collection that holds it, because `has()` cannot address an index
/// or a key and the conversion gives elements their parent's path to match.
type Roots = HashMap<String, Vec<String>>;

fn walk_for_has(ided: &cel_parser::ast::IdedExpr, roots: &Roots, paths: &mut PresencePaths) {
    use cel_parser::ast::Expr;
    match &ided.expr {
        Expr::Select(select) => {
            walk_for_has(&select.operand, roots, paths);
            // A "test-only" select is how the parser records `has(operand.field)`.
            if select.test
                && let Some(path) = select_path(&select.operand, &select.field, roots)
            {
                paths.insert(path);
            }
        }
        Expr::Call(call) => {
            if let Some(target) = &call.target {
                walk_for_has(target, roots, paths);
            }
            for arg in &call.args {
                walk_for_has(arg, roots, paths);
            }
        }
        Expr::Comprehension(comp) => {
            // The range and the accumulator's initial value are evaluated outside the loop,
            // where the iteration variables do not yet exist.
            walk_for_has(&comp.iter_range, roots, paths);
            walk_for_has(&comp.accu_init, roots, paths);

            let mut inner = roots.clone();
            // The two-variable form binds the key or index first and the element second;
            // the one-variable form binds the element alone. Only the element stands for a
            // path - a key is not part of the message, and neither is the accumulator - and
            // a variable that stands for nothing shadows whatever its name meant outside.
            let element = comp.iter_var2.as_ref().unwrap_or(&comp.iter_var);
            match path_under_binding(&comp.iter_range, roots) {
                Some(path) => {
                    inner.insert(element.clone(), path);
                }
                None => {
                    inner.remove(element);
                }
            }
            if comp.iter_var2.is_some() {
                inner.remove(&comp.iter_var);
            }
            inner.remove(&comp.accu_var);

            for part in [&comp.loop_cond, &comp.loop_step, &comp.result] {
                walk_for_has(part, &inner, paths);
            }
        }
        Expr::List(list) => {
            for element in &list.elements {
                walk_for_has(element, roots, paths);
            }
        }
        Expr::Struct(structure) => {
            for entry in &structure.entries {
                if let cel_parser::ast::EntryExpr::StructField(field) = &entry.expr {
                    walk_for_has(&field.value, roots, paths);
                }
            }
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                if let cel_parser::ast::EntryExpr::MapEntry(pair) = &entry.expr {
                    walk_for_has(&pair.key, roots, paths);
                    walk_for_has(&pair.value, roots, paths);
                }
            }
        }
        Expr::Ident(_) | Expr::Literal(_) | Expr::Unspecified => {}
    }
}

/// The dotted path of `operand.field` when its root is one of `roots`, else None.
fn select_path(
    operand: &cel_parser::ast::IdedExpr,
    field: &str,
    roots: &Roots,
) -> Option<Vec<String>> {
    let mut path = path_under_binding(operand, roots)?;
    path.push(field.to_string());
    Some(path)
}

/// The path an expression names: the path its root identifier stands for, plus one entry per
/// field selected from it. None when the expression is rooted at an identifier not in scope,
/// or at anything that is not a chain of selects.
fn path_under_binding(ided: &cel_parser::ast::IdedExpr, roots: &Roots) -> Option<Vec<String>> {
    use cel_parser::ast::Expr;
    match &ided.expr {
        Expr::Ident(name) => roots.get(name).cloned(),
        Expr::Select(select) if !select.test => {
            let mut path = path_under_binding(&select.operand, roots)?;
            path.push(select.field.clone());
            Some(path)
        }
        // An index reads an element of a list or a value of a map, and both are reached by
        // the path of the collection that holds them - `has()` cannot address an index or a
        // key, so the conversion gives them their parent's path. `this.children[0]` therefore
        // names what `this.children` names, the same as a comprehension variable over it.
        Expr::Call(call)
            if call.func_name == cel_parser::ast::operators::INDEX
                || call.func_name == cel_parser::ast::operators::OPT_INDEX =>
        {
            path_under_binding(call.args.first()?, roots)
        }
        _ => None,
    }
}

/// Test hook for the protobuf conversion, which is otherwise private to this module.
#[cfg(test)]
pub(crate) fn from_protobuf_value_for_test(value: &prost_reflect::Value) -> Value {
    from_protobuf_value_with_presence(value, &PresencePaths::new(), &[], &[])
}

/// Converts a protobuf value for CEL.
///
/// `path` is where the value sits under the binding, for matching against `presence`.
/// `expanding` is the chain of message types enclosing it, which bounds the expansion of
/// absent messages - see the Message arm.
fn from_protobuf_value_with_presence(
    value: &prost_reflect::Value,
    presence: &PresencePaths,
    path: &[String],
    expanding: &[String],
) -> Value {
    match value {
        prost_reflect::Value::Bool(v) => Value::Bool(*v),
        prost_reflect::Value::I32(v) => Value::Int(*v as i64),
        prost_reflect::Value::I64(v) => Value::Int(*v),
        // CEL has a distinct unsigned type; mapping these to Int would wrap any u64
        // above i64::MAX to a negative number, so `this > 0` would reject valid values.
        prost_reflect::Value::U32(v) => Value::UInt(*v as u64),
        prost_reflect::Value::U64(v) => Value::UInt(*v),
        prost_reflect::Value::F32(v) => Value::Float(*v as f64),
        prost_reflect::Value::F64(v) => Value::Float(*v),
        prost_reflect::Value::String(v) => Value::String(Arc::new(v.clone())),
        prost_reflect::Value::Bytes(v) => Value::Bytes(Arc::new(v.to_vec())),
        prost_reflect::Value::EnumNumber(v) => Value::Int(*v as i64),
        prost_reflect::Value::Message(msg) => {
            // A well-known type is the thing it wraps, not a message with a `value` field:
            // a Timestamp is a CEL timestamp, a StringValue is a string. Every other client
            // does this - the Go, C++, Java and JS engines natively, and the Python client
            // through a table of its own - so without it a rule on a Timestamp field would
            // have to read `this.seconds` here and `this` everywhere else.
            if let Some(unwrapped) = unwrap_well_known(msg) {
                return unwrapped;
            }
            // Every field gets a key, so a plain read like `msg.count == 0` resolves even
            // when the producer never wrote the field - protobuf calls a proto3 scalar at
            // its default unset, but CEL still has to be able to read it.
            //
            // The exception is a field the rule tests with `has()`: `has()` on a map is a
            // key-presence check, so an unset field has to be missing for it to answer
            // false. Only those keys are dropped, which is the narrowest way to satisfy both
            // and is how prost-protovalidate resolves the same conflict.
            //
            // has_field() is protobuf's own presence rule - explicit presence when the field
            // tracks it, difference from the default otherwise, non-empty for a repeated or
            // map field - so it needs no help per field kind.
            let descriptor = msg.descriptor();
            let mut enclosing = Vec::with_capacity(expanding.len() + 1);
            enclosing.extend_from_slice(expanding);
            enclosing.push(descriptor.full_name().to_string());

            let mut map: HashMap<Key, Value> = HashMap::with_capacity(descriptor.fields().len());
            for fd in descriptor.fields() {
                let mut field_path = Vec::with_capacity(path.len() + 1);
                field_path.extend_from_slice(path);
                field_path.push(fd.name().to_string());

                let key = Key::String(Arc::new(fd.name().to_string()));
                if !msg.has_field(&fd) {
                    if presence.contains(&field_path) {
                        continue;
                    }
                    // A wrapper carries null-or-value rather than the zero value an ordinary
                    // message carries, which is the whole reason to declare a field as one:
                    // it is how a producer says "unset" as opposed to "empty". Expanding it
                    // like any other message would unwrap the default to "" or 0, and the
                    // distinction the field exists for would be gone.
                    if let prost_reflect::Kind::Message(field_md) = fd.kind()
                        && is_wrapper_message(field_md.full_name())
                    {
                        map.insert(key, Value::Null);
                        continue;
                    }
                    // An absent message is expanded from its default, which has every field
                    // of its own - including, in a recursive schema like
                    // `message Node { Node child = 1; }`, another absent message of the same
                    // type. Expanding that has no end, so a type already being expanded
                    // stops here as an empty map. An engine that reads fields on demand,
                    // which is every other client's, never materializes the chain at all and
                    // so needs no such bound; only a message built as a map does.
                    if let prost_reflect::Kind::Message(field_md) = fd.kind()
                        && enclosing.iter().any(|name| name == field_md.full_name())
                    {
                        map.insert(
                            key,
                            Value::Map(Map {
                                map: Arc::new(HashMap::new()),
                            }),
                        );
                        continue;
                    }
                }
                // get_field yields the default for an unset field - the zero scalar, or an
                // empty message so that `msg.sub.field` still resolves.
                map.insert(
                    key,
                    from_protobuf_value_with_presence(
                        &msg.get_field(&fd),
                        presence,
                        &field_path,
                        &enclosing,
                    ),
                );
            }
            Value::Map(Map { map: Arc::new(map) })
        }
        prost_reflect::Value::List(v) => {
            // List elements share the parent's path: `has()` cannot address an index, so a
            // rule reaches an element only through a comprehension variable, which
            // [`collect_has_paths`] resolves to this same path.
            Value::List(Arc::new(
                v.iter()
                    .map(|item| from_protobuf_value_with_presence(item, presence, path, expanding))
                    .collect(),
            ))
        }
        prost_reflect::Value::Map(v) => {
            let map = v
                .iter()
                .map(|(k, v)| {
                    (
                        from_protobuf_map_key(k),
                        from_protobuf_value_with_presence(v, presence, path, expanding),
                    )
                })
                .collect();
            Value::Map(Map { map: Arc::new(map) })
        }
    }
}

/// Whether `full_name` names one of the wrapper messages, which stand for null when unset
/// and for the value they hold when set. cel-go singles out the same nine, and
/// protovalidate-cc asks cel-cpp for the behaviour with
/// `enable_empty_wrapper_null_unboxing`; our C++ client sets that too.
fn is_wrapper_message(full_name: &str) -> bool {
    matches!(
        full_name,
        "google.protobuf.BoolValue"
            | "google.protobuf.BytesValue"
            | "google.protobuf.DoubleValue"
            | "google.protobuf.FloatValue"
            | "google.protobuf.Int32Value"
            | "google.protobuf.Int64Value"
            | "google.protobuf.StringValue"
            | "google.protobuf.UInt32Value"
            | "google.protobuf.UInt64Value"
    )
}

/// The CEL value a well-known message stands for, or None when it is an ordinary message.
///
/// Mirrors prost-protovalidate's `try_unwrap_well_known_message`. A Timestamp or Duration
/// whose fields are out of range falls through to the map representation rather than being
/// clamped, so the rule can still inspect the raw seconds and nanos.
fn unwrap_well_known(msg: &prost_reflect::DynamicMessage) -> Option<Value> {
    let field = |name: &str| msg.get_field_by_name(name);
    match msg.descriptor().full_name() {
        "google.protobuf.BoolValue" => Some(Value::Bool(
            field("value").and_then(|v| v.as_bool()).unwrap_or(false),
        )),
        "google.protobuf.Int32Value" => Some(Value::Int(i64::from(
            field("value").and_then(|v| v.as_i32()).unwrap_or(0),
        ))),
        "google.protobuf.Int64Value" => Some(Value::Int(
            field("value").and_then(|v| v.as_i64()).unwrap_or(0),
        )),
        "google.protobuf.UInt32Value" => Some(Value::UInt(u64::from(
            field("value").and_then(|v| v.as_u32()).unwrap_or(0),
        ))),
        "google.protobuf.UInt64Value" => Some(Value::UInt(
            field("value").and_then(|v| v.as_u64()).unwrap_or(0),
        )),
        "google.protobuf.FloatValue" => Some(Value::Float(f64::from(
            field("value").and_then(|v| v.as_f32()).unwrap_or(0.0),
        ))),
        "google.protobuf.DoubleValue" => Some(Value::Float(
            field("value").and_then(|v| v.as_f64()).unwrap_or(0.0),
        )),
        "google.protobuf.StringValue" => Some(Value::String(Arc::new(
            field("value")
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_default(),
        ))),
        "google.protobuf.BytesValue" => Some(Value::Bytes(Arc::new(
            field("value")
                .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
                .unwrap_or_default(),
        ))),
        "google.protobuf.Duration" => {
            let seconds = field("seconds").and_then(|v| v.as_i64()).unwrap_or(0);
            let nanos = field("nanos").and_then(|v| v.as_i32()).unwrap_or(0);
            let duration = chrono::Duration::try_seconds(seconds)?
                .checked_add(&chrono::Duration::nanoseconds(i64::from(nanos)))?;
            Some(Value::Duration(duration))
        }
        "google.protobuf.Timestamp" => {
            let seconds = field("seconds").and_then(|v| v.as_i64()).unwrap_or(0);
            let nanos = field("nanos").and_then(|v| v.as_i32()).unwrap_or(0);
            let utc = chrono::DateTime::from_timestamp(seconds, nanos.max(0) as u32)?;
            let offset = chrono::FixedOffset::east_opt(0)?;
            Some(Value::Timestamp(utc.with_timezone(&offset)))
        }
        _ => None,
    }
}

fn from_protobuf_map_key(value: &MapKey) -> Key {
    match value {
        MapKey::Bool(v) => Key::Bool(*v),
        MapKey::I32(v) => Key::Int(*v as i64),
        MapKey::I64(v) => Key::Int(*v),
        MapKey::U32(v) => Key::Uint(*v as u64),
        MapKey::U64(v) => Key::Uint(*v),
        MapKey::String(v) => Key::String(Arc::new(v.clone())),
    }
}

fn from_json_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Bool(v) => Value::Bool(*v),
        serde_json::Value::Number(v) => {
            if let Some(i) = v.as_i64() {
                Value::Int(i)
            } else if let Some(f) = v.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(v) => Value::String(Arc::new(v.clone())),
        serde_json::Value::Array(v) => {
            Value::List(Arc::new(v.iter().map(from_json_value).collect()))
        }
        serde_json::Value::Object(v) => {
            let map = v
                .iter()
                .map(|(k, v)| (Key::String(Arc::new(k.clone())), from_json_value(v)))
                .collect();
            Value::Map(Map { map: Arc::new(map) })
        }
        serde_json::Value::Null => Value::Null,
    }
}

pub fn to_serde_value(input: &SerdeValue, value: &Value) -> SerdeValue {
    match input {
        SerdeValue::Avro(v) => SerdeValue::Avro(to_avro_value(v, value)),
        SerdeValue::Protobuf(v) => SerdeValue::Protobuf(to_protobuf_value(v, value)),
        SerdeValue::Json(v) => SerdeValue::Json(to_json_value(v, value)),
    }
}

fn to_avro_value(input: &apache_avro::types::Value, value: &Value) -> apache_avro::types::Value {
    match value {
        Value::Bool(v) => apache_avro::types::Value::Boolean(*v),
        Value::Int(v) => {
            if let apache_avro::types::Value::Int(_) = input {
                apache_avro::types::Value::Int(*v as i32)
            } else {
                apache_avro::types::Value::Long(*v)
            }
        }
        Value::UInt(v) => {
            if let apache_avro::types::Value::Int(_) = input {
                apache_avro::types::Value::Int(*v as i32)
            } else {
                apache_avro::types::Value::Long(*v as i64)
            }
        }
        Value::Float(v) => {
            if let apache_avro::types::Value::Float(_) = input {
                apache_avro::types::Value::Float(*v as f32)
            } else {
                apache_avro::types::Value::Double(*v)
            }
        }
        Value::String(v) => {
            if let apache_avro::types::Value::Enum(s, _) = input {
                apache_avro::types::Value::Enum(*s, v.to_string())
            } else {
                apache_avro::types::Value::String(v.to_string())
            }
        }
        Value::Bytes(v) => {
            if let apache_avro::types::Value::Fixed(s, _) = input {
                apache_avro::types::Value::Fixed(*s, (**v).clone())
            } else {
                apache_avro::types::Value::Bytes((**v).clone())
            }
        }
        Value::List(v) => apache_avro::types::Value::Array(
            (**v)
                .clone()
                .into_iter()
                .map(|x| to_avro_value(input, &x))
                .collect(),
        ),
        Value::Map(v) => {
            let iter = (*v.map).clone().into_iter().map(|(k, v)| {
                let key = if let Key::String(s) = k {
                    s.to_string()
                } else {
                    k.to_string()
                };
                (key, to_avro_value(input, &v))
            });
            if let apache_avro::types::Value::Record(_) = input {
                apache_avro::types::Value::Record(iter.collect())
            } else {
                apache_avro::types::Value::Map(iter.collect())
            }
        }
        Value::Null => apache_avro::types::Value::Null,
        _ => apache_avro::types::Value::Null,
    }
}

fn to_protobuf_value(input: &prost_reflect::Value, value: &Value) -> prost_reflect::Value {
    match value {
        Value::Bool(v) => prost_reflect::Value::Bool(*v),
        Value::Int(v) => match input {
            prost_reflect::Value::I32(_) => prost_reflect::Value::I32(*v as i32),
            prost_reflect::Value::I64(_) => prost_reflect::Value::I64(*v),
            prost_reflect::Value::U32(_) => prost_reflect::Value::U32(*v as u32),
            prost_reflect::Value::U64(_) => prost_reflect::Value::U64(*v as u64),
            prost_reflect::Value::EnumNumber(_) => prost_reflect::Value::EnumNumber(*v as i32),
            _ => prost_reflect::Value::I64(*v),
        },
        Value::UInt(v) => match input {
            prost_reflect::Value::I32(_) => prost_reflect::Value::I32(*v as i32),
            prost_reflect::Value::I64(_) => prost_reflect::Value::I64(*v as i64),
            prost_reflect::Value::U32(_) => prost_reflect::Value::U32(*v as u32),
            prost_reflect::Value::U64(_) => prost_reflect::Value::U64(*v),
            prost_reflect::Value::EnumNumber(_) => prost_reflect::Value::EnumNumber(*v as i32),
            _ => prost_reflect::Value::U64(*v),
        },
        Value::Float(v) => {
            if let prost_reflect::Value::F32(_) = input {
                prost_reflect::Value::F32(*v as f32)
            } else {
                prost_reflect::Value::F64(*v)
            }
        }
        Value::String(v) => prost_reflect::Value::String(v.to_string()),
        Value::Bytes(v) => prost_reflect::Value::Bytes(Bytes::from((**v).clone())),
        Value::List(v) => prost_reflect::Value::List(
            (**v)
                .clone()
                .into_iter()
                .map(|x| to_protobuf_value(input, &x))
                .collect(),
        ),
        Value::Map(v) => {
            let iter = (*v.map).clone().into_iter().map(|(k, v)| {
                let key = to_protobuf_map_key(&k);
                (key, to_protobuf_value(input, &v))
            });
            prost_reflect::Value::Map(iter.collect())
        }
        Value::Null => prost_reflect::Value::Bytes(Bytes::from(Vec::new())),
        _ => prost_reflect::Value::Bytes(Bytes::from(Vec::new())),
    }
}

fn to_protobuf_map_key(value: &Key) -> MapKey {
    match value {
        Key::Bool(v) => MapKey::Bool(*v),
        Key::Int(v) => MapKey::I64(*v),
        Key::Uint(v) => MapKey::U64(*v),
        Key::String(v) => MapKey::String(v.to_string()),
    }
}

fn to_json_value(input: &serde_json::Value, value: &Value) -> serde_json::Value {
    match value {
        Value::Bool(v) => serde_json::Value::Bool(*v),
        Value::Int(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        Value::UInt(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        Value::Float(v) => serde_json::Value::Number(serde_json::Number::from_f64(*v).unwrap()),
        Value::String(v) => serde_json::Value::String(v.to_string()),
        Value::List(v) => serde_json::Value::Array(
            (**v)
                .clone()
                .into_iter()
                .map(|x| to_json_value(input, &x))
                .collect(),
        ),
        Value::Map(v) => {
            let iter = (*v.map).clone().into_iter().map(|(k, v)| {
                let key = if let Key::String(s) = k {
                    s.to_string()
                } else {
                    k.to_string()
                };
                (key, to_json_value(input, &v))
            });
            serde_json::Value::Object(iter.collect())
        }
        Value::Null => serde_json::Value::Null,
        _ => serde_json::Value::Null,
    }
}

impl From<ExecutionError> for SerdeError {
    fn from(value: ExecutionError) -> Self {
        SerdeError::Rule(format!("CEL execution error: {value}"))
    }
}

impl From<ParseErrors> for SerdeError {
    fn from(value: ParseErrors) -> Self {
        SerdeError::Rule(format!("CEL parse error: {value}"))
    }
}
