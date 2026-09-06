use crate::rules::cel::cel_executor::{
    PresencePaths, collect_has_paths, from_avro_value_with_schema, from_serde_value_with_presence,
};
use crate::rules::cel::cel_lib::default_context;
use crate::serdes::serde::{SerdeError, SerdeValue};
use crate::serdes::validation_rule::{
    ValidationRule, ValidationRuleExecutor, ValidationRuleResult, ValidationSchema,
};
use cel::{Program, Value};
use chrono::Utc;
use dashmap::DashMap;

/// Validation-rule executor backed by CEL. The rule expression is evaluated with `this`
/// bound to the value being validated and `now` bound to the current time, and must
/// resolve to a bool (false meaning the rule failed) or a string (non-empty meaning the
/// rule failed, with that string as the message).
pub struct CelValidator {
    /// Compiled rule, with the field paths it tests with `has()`. Both are derived
    /// from the expression alone, so they are cached together.
    cache: DashMap<String, (Program, PresencePaths)>,
}

impl Default for CelValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl CelValidator {
    pub fn new() -> Self {
        CelValidator {
            cache: DashMap::new(),
        }
    }

    pub fn register() {
        crate::serdes::rule_registry::register_validation_rule_executor(CelValidator::new());
    }
}

impl ValidationRuleExecutor for CelValidator {
    fn get_type(&self) -> &'static str {
        "CEL"
    }

    fn execute(
        &self,
        rule: &ValidationRule,
        schema: Option<ValidationSchema<'_>>,
        value: &SerdeValue,
    ) -> Result<ValidationRuleResult, SerdeError> {
        let name = rule_name(rule);
        if rule.expr.is_empty() {
            return Err(SerdeError::Rule(format!(
                "validation rule '{name}' has no expression"
            )));
        }
        if !self.cache.contains_key(&rule.expr) {
            self.cache.insert(
                rule.expr.clone(),
                (
                    Program::compile(&rule.expr)?,
                    collect_has_paths(&rule.expr, "this"),
                ),
            );
        }
        let cached = self.cache.get(&rule.expr).ok_or_else(|| {
            SerdeError::Rule(format!("could not compile validation rule '{name}'"))
        })?;
        let (program, presence) = cached.value();

        // Bind `this` against the schema when the walk supplied one. An Avro decimal is
        // unscaled bytes with the scale held in the schema, so without this a rule reads
        // 12.34 as 1234 and answers wrongly with no error. This is the same resolution
        // CelExecutor::message_binding applies to `message` for domain rules; presence
        // dropping is a protobuf concern, so the Avro arm does not need it.
        let this = match (&schema, value) {
            (Some(ValidationSchema::Avro(s, defs)), SerdeValue::Avro(v)) => {
                from_avro_value_with_schema(v, s, defs)
            }
            _ => from_serde_value_with_presence(value, presence),
        };

        let mut context = default_context();
        context.add_variable_from_value("this", this);
        context.add_variable_from_value("now", Value::Timestamp(Utc::now().into()));

        match program.execute(&context)? {
            Value::Bool(b) => Ok(ValidationRuleResult::Bool(b)),
            Value::String(s) => Ok(ValidationRuleResult::Message(s.to_string())),
            _ => Err(SerdeError::Rule(format!(
                "validation rule '{name}' must return bool or string"
            ))),
        }
    }
}

fn rule_name(rule: &ValidationRule) -> &str {
    if rule.name.is_empty() {
        "unnamed"
    } else {
        &rule.name
    }
}
