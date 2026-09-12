use crate::rules::cel::cel_lib::default_context;
use crate::rules::cel::decimal_funcs::{
    DECIMAL_TYPE_NAME, decimal_value, from_bytes_scale, to_decimal,
};
use crate::rules::cel::protobuf_result_writer::{write_back_protobuf, write_back_value_type};
use crate::rules::cel::variant_funcs::{VARIANT_TYPE_NAME, to_variant};
use crate::serdes::avro::collect_named_schemas;
use crate::serdes::serde::{
    RuleBase, RuleContext, RuleExecutor, SerdeError, SerdeSchema, SerdeValue,
};
use apache_avro::Schema as AvroSchema;
use apache_avro::schema::Name as AvroName;
use async_trait::async_trait;
use bigdecimal::BigDecimal;
use bigdecimal::RoundingMode;
use bigdecimal::num_bigint::BigInt;
use cel::objects::{Key, Map};
use cel::{ExecutionError, ParseErrors, Program, Value};
use chrono::Utc;
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
        // Avro decimal values are unscaled on their own (the scale lives in the schema) and
        // logical timestamps need their unit, so the Avro conversion is walked against the
        // schema the message conforms to. `has()`-presence dropping is a protobuf concern, so
        // Avro never needed it.
        if let SerdeValue::Avro(v) = msg {
            // The record this value actually is, which for a nested one is not the target
            // schema; only the field context knows it. Falling back to the target keeps a
            // message-level rule, which has no field context, converting against the root.
            let containing = ctx
                .current_field()
                .and_then(|f| f.containing_schema.clone());
            let schema = containing.as_ref().or(ctx.parsed_target.as_ref());
            return match schema {
                Some(SerdeSchema::Avro((schema, named))) => {
                    from_avro_value_with_schema(v, schema, &avro_definitions(schema, named))
                }
                _ => from_avro_value(v),
            };
        }
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
        // `now` is available to every rule, so a condition like `timestamp(this.ts) < now`
        // resolves. It is read fresh per evaluation, matching the other clients.
        context.add_variable_from_value("now", Value::Timestamp(Utc::now().into()));
        for (k, v) in args {
            context.add_variable_from_value(k.clone(), v.clone());
        }
        let result = prog.value().execute(&context)?;
        // Write the result back against the schema `msg` conforms to, so a Decimal is
        // re-quantized to the field's scale and a timestamp keeps the field's unit (a bare
        // `to_avro_value` has no scale/unit to target). In a field rule `msg` is the field value,
        // so its own schema (from the field context) applies; otherwise it is the whole message
        // and the target schema applies. Non-Avro, or Avro without a schema, converts schemaless.
        let schema = ctx
            .current_field()
            .and_then(|f| f.field_schema.as_ref())
            .or(ctx.parsed_target.as_ref());
        match (msg, schema) {
            (SerdeValue::Avro(input), Some(SerdeSchema::Avro((schema, named)))) => {
                let defs = avro_definitions(schema, named);
                Ok(SerdeValue::Avro(to_avro_value_with_schema(
                    input, &result, schema, &defs,
                )?))
            }
            // A message-level transform returns a map that is the whole new message; rebuild
            // it. Without this the protobuf arm of to_serde_value turns the map into a
            // prost_reflect Map and its catch-all turns an unrecognised value into empty bytes,
            // so decimal and timestamp were replaced with b"" rather than merely unwritten.
            (SerdeValue::Protobuf(prost_reflect::Value::Message(m)), _) => {
                // A field rule over a decimal or timestamp is handed the whole message and
                // hands back a CEL value; encode it against that message's own descriptor.
                // The generic conversion below has no arm for a decimal opaque and would turn
                // it into empty bytes.
                // A condition answers with a bool, which is a verdict on the value rather
                // than a replacement for it and must not be encoded as one.
                if ctx.rule.kind != Some(crate::rest::models::Kind::Condition)
                    && let Some(encoded) = write_back_value_type(&m.descriptor(), &result)
                {
                    return Ok(SerdeValue::Protobuf(prost_reflect::Value::Message(
                        encoded?,
                    )));
                }
                match write_back_protobuf(m, &result)? {
                    Some(rebuilt) => {
                        Ok(SerdeValue::Protobuf(prost_reflect::Value::Message(rebuilt)))
                    }
                    None => to_serde_value(msg, &result),
                }
            }
            _ => to_serde_value(msg, &result),
        }
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
        // A logical timestamp carries its unit in the variant, so no schema is needed to
        // convert it; local (timezone-naive) timestamps are left as their raw epoch value.
        apache_avro::types::Value::TimestampMillis(v) => avro_timestamp(*v, "millis"),
        apache_avro::types::Value::TimestampMicros(v) => avro_timestamp(*v, "micros"),
        apache_avro::types::Value::TimestampNanos(v) => avro_timestamp(*v, "nanos"),
        // Already-scaled (the non-standard `big-decimal` type) - use it directly.
        apache_avro::types::Value::BigDecimal(d) => decimal_value(d.clone()),
        // A bare decimal is unscaled without its schema, and this path has none. The message
        // binding uses [`from_avro_value_with_schema`], which applies the scale; a CEL_FIELD
        // rule's `value` binding reaches a decimal here and sees it unscaled, so such a rule
        // should read the field through the (scaled) `message` binding instead.
        apache_avro::types::Value::Decimal(d) => {
            decimal_value(BigDecimal::new(BigInt::from(d.clone()), 0))
        }
        apache_avro::types::Value::Union(_, inner) => from_avro_value(inner),
        apache_avro::types::Value::Null => Value::Null,
        _ => Value::Null,
    }
}

/// Converts an Avro logical timestamp (`millis`/`micros`/`nanos` since the epoch, UTC) to a CEL
/// timestamp, falling back to the raw epoch integer if it is out of range.
fn avro_timestamp(value: i64, unit: &str) -> Value {
    match crate::rules::cel::timestamp_funcs::from_epoch(value, unit) {
        Ok(ts) => Value::Timestamp(ts),
        Err(_) => Value::Int(value),
    }
}

/// Indexes every named definition reachable from the root schema and its referenced schemas, so
/// a [`AvroSchema::Ref`] - whether to an inline definition or an imported one - resolves.
fn avro_definitions<'a>(
    schema: &'a AvroSchema,
    named: &'a [AvroSchema],
) -> HashMap<AvroName, &'a AvroSchema> {
    let mut defs = HashMap::new();
    collect_named_schemas(schema, &mut defs);
    for n in named {
        collect_named_schemas(n, &mut defs);
    }
    defs
}

/// Converts an Avro value for CEL, walking it against its schema so that decimal fields get
/// their scale and logical types are recognised. Anything the schema does not add information to
/// falls back to the schemaless [`from_avro_value`].
pub(crate) fn from_avro_value_with_schema(
    value: &apache_avro::types::Value,
    schema: &AvroSchema,
    defs: &HashMap<AvroName, &AvroSchema>,
) -> Value {
    use apache_avro::types::Value as AV;
    let schema = resolve_avro_ref(schema, defs);
    match (value, schema) {
        (AV::Record(fields), AvroSchema::Record(rs)) => {
            let mut map: HashMap<Key, Value> = HashMap::with_capacity(fields.len());
            for (k, v) in fields {
                let cv = match rs.fields.iter().find(|f| &f.name == k) {
                    Some(field) => from_avro_value_with_schema(v, &field.schema, defs),
                    None => from_avro_value(v),
                };
                map.insert(Key::String(Arc::new(k.clone())), cv);
            }
            Value::Map(Map { map: Arc::new(map) })
        }
        (AV::Array(items), AvroSchema::Array(a)) => Value::List(Arc::new(
            items
                .iter()
                .map(|it| from_avro_value_with_schema(it, &a.items, defs))
                .collect(),
        )),
        (AV::Map(entries), AvroSchema::Map(m)) => Value::Map(Map {
            map: Arc::new(
                entries
                    .iter()
                    .map(|(k, v)| {
                        (
                            Key::String(Arc::new(k.clone())),
                            from_avro_value_with_schema(v, &m.types, defs),
                        )
                    })
                    .collect(),
            ),
        }),
        // A union value names the variant it took; recurse into that branch's schema.
        (AV::Union(idx, inner), AvroSchema::Union(u)) => match u.variants().get(*idx as usize) {
            Some(variant) => from_avro_value_with_schema(inner, variant, defs),
            None => from_avro_value(inner),
        },
        // The only place the schema is load-bearing: the scale that a bare decimal lacks.
        (AV::Decimal(d), AvroSchema::Decimal(ds)) => {
            decimal_value(BigDecimal::new(BigInt::from(d.clone()), ds.scale as i64))
        }
        _ => from_avro_value(value),
    }
}

/// The fully qualified name of an Avro record schema.
fn avro_record_full_name(rs: &apache_avro::schema::RecordSchema) -> String {
    match rs.name.namespace() {
        Some(ns) => format!("{ns}.{}", rs.name.name()),
        None => rs.name.name().to_string(),
    }
}

/// Resolves a named `Schema::Ref` to its definition in `defs`; any other schema is returned as-is.
fn resolve_avro_ref<'a>(
    schema: &'a AvroSchema,
    defs: &HashMap<AvroName, &'a AvroSchema>,
) -> &'a AvroSchema {
    if let AvroSchema::Ref { name } = schema
        && let Some(def) = defs.get(name)
    {
        return def;
    }
    schema
}

/// Converts one Avro field value for CEL against the field's own schema, so a field rule's
/// `value` binding sees a decimal at its scale and a timestamp in its unit rather than raw bytes.
pub(crate) fn from_avro_field_value(
    value: &apache_avro::types::Value,
    schema: &AvroSchema,
    named: &[AvroSchema],
) -> Value {
    from_avro_value_with_schema(value, schema, &avro_definitions(schema, named))
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
    if let Ok(parsed) = cel::parser::Parser::default().parse(expr) {
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

fn walk_for_has(ided: &cel::common::ast::IdedExpr, roots: &Roots, paths: &mut PresencePaths) {
    use cel::common::ast::Expr;
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
                if let cel::common::ast::EntryExpr::StructField(field) = &entry.expr {
                    walk_for_has(&field.value, roots, paths);
                }
            }
        }
        Expr::Map(map) => {
            for entry in &map.entries {
                if let cel::common::ast::EntryExpr::MapEntry(pair) = &entry.expr {
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
    operand: &cel::common::ast::IdedExpr,
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
fn path_under_binding(ided: &cel::common::ast::IdedExpr, roots: &Roots) -> Option<Vec<String>> {
    use cel::common::ast::Expr;
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
            if call.func_name == cel::common::ast::operators::INDEX
                || call.func_name == cel::common::ast::operators::OPT_INDEX =>
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
        // A Decimal message carries the scale the Avro bytes lack, so it converts to a CEL
        // Decimal directly rather than being read field-by-field.
        "confluent.type.Decimal" => {
            let bytes = field("value")
                .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
                .unwrap_or_default();
            let scale = field("scale").and_then(|v| v.as_i32()).unwrap_or(0);
            Some(decimal_value(from_bytes_scale(&bytes, i64::from(scale))))
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

pub fn to_serde_value(input: &SerdeValue, value: &Value) -> Result<SerdeValue, SerdeError> {
    Ok(match input {
        SerdeValue::Avro(v) => SerdeValue::Avro(to_avro_value(v, value)),
        SerdeValue::Protobuf(v) => SerdeValue::Protobuf(to_protobuf_value(v, value)?),
        SerdeValue::Json(v) => SerdeValue::Json(to_json_value(v, value)),
    })
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
        // A Decimal carries its own scale, so writing it back at that scale round-trips a value
        // that was read at the schema's scale. (A decimal *computed* by a rule keeps its computed
        // scale; the reverse path has no schema to re-quantize against.)
        Value::Opaque(o) if o.runtime_type_name() == DECIMAL_TYPE_NAME => match to_decimal(value) {
            Ok(d) => {
                let (unscaled, _scale) = d.into_bigint_and_exponent();
                apache_avro::types::Value::Decimal(apache_avro::Decimal::from(
                    unscaled.to_signed_bytes_be(),
                ))
            }
            Err(_) => apache_avro::types::Value::Null,
        },
        // Write a timestamp back in whatever unit the field held.
        Value::Timestamp(ts) => match input {
            apache_avro::types::Value::TimestampMicros(_) => {
                apache_avro::types::Value::TimestampMicros(ts.timestamp_micros())
            }
            apache_avro::types::Value::TimestampNanos(_) => {
                apache_avro::types::Value::TimestampNanos(ts.timestamp_nanos_opt().unwrap_or(0))
            }
            _ => apache_avro::types::Value::TimestampMillis(ts.timestamp_millis()),
        },
        Value::Null => apache_avro::types::Value::Null,
        _ => apache_avro::types::Value::Null,
    }
}

/// Writes a CEL result back to Avro against `schema`, so a Decimal is re-quantized to the field's
/// scale and a timestamp is emitted in the field's unit. Recurses through records/arrays/maps/
/// unions, pairing each result child with its input child and field schema; leaves the schema
/// does not inform fall back to the schemaless [`to_avro_value`]. Fallible because a Decimal that
/// does not fit the schema scale, or a timestamp past the nanosecond range, is a rule error rather
/// than a silently-wrong value.
fn to_avro_value_with_schema(
    input: &apache_avro::types::Value,
    value: &Value,
    schema: &AvroSchema,
    defs: &HashMap<AvroName, &AvroSchema>,
) -> Result<apache_avro::types::Value, SerdeError> {
    use apache_avro::types::Value as AV;
    let schema = resolve_avro_ref(schema, defs);
    match (schema, value) {
        // Re-quantize to the field's scale before encoding the unscaled bytes; without this a
        // result whose scale differs from the schema (e.g. after a multiply) would be mis-scaled.
        (AvroSchema::Decimal(ds), Value::Opaque(o))
            if o.runtime_type_name() == DECIMAL_TYPE_NAME =>
        {
            let d = to_decimal(value)?.with_scale_round(ds.scale as i64, RoundingMode::HalfUp);
            let (unscaled, _) = d.into_bigint_and_exponent();
            Ok(AV::Decimal(apache_avro::Decimal::from(
                unscaled.to_signed_bytes_be(),
            )))
        }
        // A computed variant is a CEL opaque, not a map of the record's fields, so the
        // Record/Map arm below never sees it. Without this it fell through to the loose
        // conversion and was written back as Avro null - the counterpart of the decimal arm
        // above, and the one shape that arm did not cover.
        // The record has to be the variant shape, not merely any record: the Java reference
        // gates this on the branch carrying a logical type (AvroResultWriter, RECORD case), so
        // without a schema-side check an unrelated record would silently accept a Variant.
        (AvroSchema::Record(rs), Value::Opaque(o))
            if o.runtime_type_name() == VARIANT_TYPE_NAME
                && avro_record_full_name(rs) == VARIANT_TYPE_NAME =>
        {
            let variant = to_variant(value)
                .map_err(|e| SerdeError::Rule(e.to_string()))?
                .ok_or_else(|| {
                    SerdeError::Rule(
                        "cannot write an absent variant; use null to clear the field".to_string(),
                    )
                })?;
            let mut out = Vec::with_capacity(rs.fields.len());
            for field in &rs.fields {
                let bytes = match field.name.as_str() {
                    "metadata" => variant.metadata_bytes().to_vec(),
                    // Slice from this node's offset, not from 0: a Variant from
                    // variants.field/path/index is a view, and value_bytes() would write the
                    // entire source variant. Trailing sibling bytes are kept deliberately -
                    // the Java reference emits ByteBuffer position..limit (VariantFormat.slice
                    // sets only the position), so this matches it byte for byte.
                    "value" => variant.standalone_value_bytes(),
                    // A variant record carries exactly these two fields; anything else is
                    // not part of the shape and has no value to write.
                    _ => continue,
                };
                out.push((field.name.clone(), AV::Bytes(bytes)));
            }
            Ok(AV::Record(out))
        }
        (AvroSchema::TimestampMillis, Value::Timestamp(ts)) => {
            Ok(AV::TimestampMillis(ts.timestamp_millis()))
        }
        (AvroSchema::TimestampMicros, Value::Timestamp(ts)) => {
            Ok(AV::TimestampMicros(ts.timestamp_micros()))
        }
        (AvroSchema::TimestampNanos, Value::Timestamp(ts)) => ts
            .timestamp_nanos_opt()
            .map(AV::TimestampNanos)
            .ok_or_else(|| {
                SerdeError::Rule("CEL result timestamp is outside the nanosecond range".to_string())
            }),
        (AvroSchema::Record(rs), Value::Map(m)) => {
            let mut out = Vec::with_capacity(rs.fields.len());
            for field in &rs.fields {
                let Some(cel_v) = m.map.get(&Key::String(Arc::new(field.name.clone()))) else {
                    // Replace, not merge: the map is the whole new record, so a field the rule
                    // does not name is not carried over from the input. Avro has no absent
                    // field, so it takes the schema's declared default - or is an error, which
                    // is what GenericRecordBuilder.build() does on the JVM. Omitting it instead
                    // produced a record the writer rejected with "Value does not match schema",
                    // naming nothing.
                    let Some(default) = field.default.as_ref() else {
                        return Err(SerdeError::Rule(format!(
                            "CEL transform result does not set field '{}' of '{}', which has \
                             no default value",
                            field.name, rs.name
                        )));
                    };
                    // The default is stored as the raw JSON it was written as; `resolve` is how
                    // apache-avro itself reads one, so a union default lands on the right branch
                    // and a logical type is lifted rather than left as its underlying value.
                    out.push((
                        field.name.clone(),
                        AV::try_from(default.clone())?.resolve(&field.schema)?,
                    ));
                    continue;
                };
                let child_input = match input {
                    AV::Record(fields) => fields
                        .iter()
                        .find(|(n, _)| n == &field.name)
                        .map(|(_, v)| v),
                    _ => None,
                }
                .unwrap_or(input);
                out.push((
                    field.name.clone(),
                    to_avro_value_with_schema(child_input, cel_v, &field.schema, defs)?,
                ));
            }
            Ok(AV::Record(out))
        }
        (AvroSchema::Array(a), Value::List(items)) => {
            let child_input = match input {
                AV::Array(xs) => xs.first(),
                _ => None,
            }
            .unwrap_or(input);
            let mut out = Vec::with_capacity(items.len());
            for it in items.iter() {
                out.push(to_avro_value_with_schema(child_input, it, &a.items, defs)?);
            }
            Ok(AV::Array(out))
        }
        (AvroSchema::Map(mp), Value::Map(m)) => {
            let child_input = match input {
                AV::Map(xs) => xs.values().next(),
                _ => None,
            }
            .unwrap_or(input);
            let mut out = HashMap::with_capacity(m.map.len());
            for (k, v) in m.map.iter() {
                let key = match k {
                    Key::String(s) => s.to_string(),
                    other => other.to_string(),
                };
                out.insert(
                    key,
                    to_avro_value_with_schema(child_input, v, &mp.types, defs)?,
                );
            }
            Ok(AV::Map(out))
        }
        // Scalars the *schema* decides, not the input. The fallback below shapes a number from
        // whatever the field already held, which is right while the shape still applies and
        // wrong the moment a union resolves to a different branch: an int result routed to an
        // `int` branch came out as a Long and the writer refused the record. These mirror
        // `branch_accepts` one for one, which is what keeps accept and produce in step.
        (AvroSchema::Int, Value::Int(v)) => Ok(AV::Int(*v as i32)),
        (AvroSchema::Int, Value::UInt(v)) => Ok(AV::Int(*v as i32)),
        (AvroSchema::Long, Value::Int(v)) => Ok(AV::Long(*v)),
        (AvroSchema::Long, Value::UInt(v)) => Ok(AV::Long(*v as i64)),
        (AvroSchema::Float, Value::Float(v)) => Ok(AV::Float(*v as f32)),
        (AvroSchema::Float, Value::Int(v)) => Ok(AV::Float(*v as f32)),
        (AvroSchema::Double, Value::Float(v)) => Ok(AV::Double(*v)),
        (AvroSchema::Double, Value::Int(v)) => Ok(AV::Double(*v as f64)),
        (AvroSchema::Boolean, Value::Bool(v)) => Ok(AV::Boolean(*v)),
        (AvroSchema::String | AvroSchema::Uuid(_), Value::String(v)) => {
            Ok(AV::String(v.to_string()))
        }
        (AvroSchema::Enum(e), Value::String(v)) => {
            match e.symbols.iter().position(|sym| sym == v.as_str()) {
                Some(i) => Ok(AV::Enum(i as u32, v.to_string())),
                None => Ok(AV::String(v.to_string())),
            }
        }
        (AvroSchema::Bytes, Value::Bytes(v)) => Ok(AV::Bytes((**v).clone())),
        (AvroSchema::Fixed(f), Value::Bytes(v)) if v.len() == f.size => {
            Ok(AV::Fixed(f.size, (**v).clone()))
        }
        // Unions are transparent in CEL, so pick the variant matching the result's kind and
        // recurse into it (handles the common `[null, T]` nullable field).
        (AvroSchema::Union(u), _) => {
            let variants = u.variants();
            match union_variant_index(variants, value, defs) {
                Some(i) => {
                    let inner_input = match input {
                        AV::Union(_, boxed) => boxed.as_ref(),
                        other => other,
                    };
                    Ok(AV::Union(
                        i as u32,
                        Box::new(to_avro_value_with_schema(
                            inner_input,
                            value,
                            &variants[i],
                            defs,
                        )?),
                    ))
                }
                None => Ok(to_avro_value(input, value)),
            }
        }
        // Primitives (and anything the schema does not inform) use the loose conversion.
        _ => Ok(to_avro_value(input, value)),
    }
}

/// Picks the union variant a CEL result belongs to: the `null` branch for null, then the branch
/// that accepts the value, and only failing that the first non-null branch.
///
/// The last step was all this did, which is the right answer for the `[null, T]` nullable shape
/// and wrong for any union offering a real choice: an int result for `["string", "int"]` took the
/// string branch and the writer then refused the record with "Value does not match schema" - a
/// transform the reference performs happily. The reference resolves by value
/// (`AvroResultWriter.branchAccepts`), so this does too, keeping the old behaviour as the
/// fallback for a value kind `branch_accepts` does not enumerate.
fn union_variant_index(
    variants: &[AvroSchema],
    value: &Value,
    defs: &HashMap<AvroName, &AvroSchema>,
) -> Option<usize> {
    if matches!(value, Value::Null) {
        return variants
            .iter()
            .position(|v| matches!(resolve_avro_ref(v, defs), AvroSchema::Null));
    }
    variants
        .iter()
        .position(|v| branch_accepts(v, value, defs))
        .or_else(|| {
            variants
                .iter()
                .position(|v| !matches!(resolve_avro_ref(v, defs), AvroSchema::Null))
        })
}

/// Whether `value` can be written as `schema`.
///
/// The pairs here are exactly the ones [`to_avro_value_with_schema`] knows how to write, and they
/// have to stay that way: an asymmetry between what a branch *accepts* and what the writer can
/// *produce* is the one thing union resolution must not have. The CEL wrapper types are tested
/// first because a Variant is a record and would otherwise match any record branch.
fn branch_accepts(
    schema: &AvroSchema,
    value: &Value,
    defs: &HashMap<AvroName, &AvroSchema>,
) -> bool {
    let schema = resolve_avro_ref(schema, defs);
    match (schema, value) {
        (AvroSchema::Null, Value::Null) => true,
        (_, Value::Null) => false,
        (AvroSchema::Decimal(_), Value::Opaque(o)) => o.runtime_type_name() == DECIMAL_TYPE_NAME,
        (AvroSchema::Record(rs), Value::Opaque(o)) => {
            o.runtime_type_name() == VARIANT_TYPE_NAME
                && avro_record_full_name(rs) == VARIANT_TYPE_NAME
        }
        (_, Value::Opaque(_)) => false,
        (
            AvroSchema::TimestampMillis | AvroSchema::TimestampMicros | AvroSchema::TimestampNanos,
            Value::Timestamp(_),
        ) => true,
        (_, Value::Timestamp(_)) => false,
        (AvroSchema::Boolean, Value::Bool(_)) => true,
        (AvroSchema::Int, Value::Int(v)) => i32::try_from(*v).is_ok(),
        (AvroSchema::Int, Value::UInt(v)) => i32::try_from(*v).is_ok(),
        (AvroSchema::Long, Value::Int(_) | Value::UInt(_)) => true,
        // The reference's FLOAT/DOUBLE case is `value instanceof Number`, which an integer
        // satisfies too, so a widened CEL int resolves to a float branch declared ahead of a long.
        (
            AvroSchema::Float | AvroSchema::Double,
            Value::Float(_) | Value::Int(_) | Value::UInt(_),
        ) => true,
        (AvroSchema::String | AvroSchema::Uuid(_), Value::String(_)) => true,
        (AvroSchema::Enum(e), Value::String(s)) => e.symbols.iter().any(|sym| sym == s.as_str()),
        (AvroSchema::Bytes, Value::Bytes(_)) => true,
        (AvroSchema::Fixed(f), Value::Bytes(b)) => b.len() == f.size,
        (AvroSchema::Array(_), Value::List(_)) => true,
        (AvroSchema::Map(_) | AvroSchema::Record(_), Value::Map(_)) => true,
        _ => false,
    }
}

/// Converts a CEL result back to a protobuf value, shaped by the value the field already held.
///
/// **Narrowing is deliberately unchecked, and must stay that way**, however wrong it looks. CEL
/// has one integer type, so writing to a narrower field truncates - 2^32 into an int32 becomes 0.
/// That is what the JVM reference does: `CelFieldExecutor` ends with a narrowing chain of
/// `num.intValue()` / `num.longValue()` / `num.floatValue()` / `num.doubleValue()`, all of which
/// truncate or saturate silently.
///
/// This is *not* the message-level path. A message-level transform returns a map that is rebuilt
/// through `protobuf_result_writer`, where the JVM client goes via protobuf JSON and `JsonFormat`
/// rejects an out-of-range value - so that path checks every conversion. The two paths differ in
/// the reference, so they differ here.
///
/// A checked conversion was tried here and reverted: it made Rust reject values every other
/// client accepts. If this should change, it is a cross-client contract decision and the JVM
/// narrowing chain has to change with it.
fn to_protobuf_value(
    input: &prost_reflect::Value,
    value: &Value,
) -> Result<prost_reflect::Value, SerdeError> {
    Ok(match value {
        Value::Bool(v) => prost_reflect::Value::Bool(*v),
        // `as`, not `try_from`: see the note above - the JVM field path narrows with
        // `Number.intValue()`, which truncates the same way.
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
        Value::List(v) => {
            let mut out = Vec::with_capacity(v.len());
            for x in (**v).iter() {
                out.push(to_protobuf_value(input, x)?);
            }
            prost_reflect::Value::List(out)
        }
        Value::Map(v) => {
            let mut out = std::collections::HashMap::with_capacity(v.map.len());
            for (k, val) in v.map.iter() {
                out.insert(to_protobuf_map_key(k), to_protobuf_value(input, val)?);
            }
            prost_reflect::Value::Map(out)
        }
        // Neither a null nor an unrecognised CEL value has a protobuf form for an arbitrary
        // field. Writing empty bytes instead stored a wrong value, and for any non-bytes field
        // it panics inside `set_field`, which validates the value against the field. The JVM
        // reference hands the result straight to `Builder.setField`, which rejects both (a
        // ClassCastException, or a NullPointerException for a null), so failing is what matches
        // - and an error beats a panic either way.
        Value::Null => {
            return Err(SerdeError::Rule(
                "cannot write a null to this protobuf field; a rule clears a field by returning \
                 null only where the field is a value-type message"
                    .to_string(),
            ));
        }
        other => {
            return Err(SerdeError::Rule(format!(
                "cannot write {other:?} to this protobuf field"
            )));
        }
    })
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
