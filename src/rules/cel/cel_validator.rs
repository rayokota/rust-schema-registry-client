use crate::rules::cel::cel_executor::from_serde_value;
use crate::rules::cel::cel_lib::default_context;
use crate::serdes::serde::{SerdeError, SerdeValue};
use crate::serdes::validation_rule::{
    ValidationRule, ValidationRuleExecutor, ValidationRuleResult,
};
use cel_interpreter::{Program, Value};
use chrono::Utc;
use dashmap::DashMap;

/// Validation-rule executor backed by CEL. The rule expression is evaluated with `this`
/// bound to the value being validated and `now` bound to the current time, and must
/// resolve to a bool (false meaning the rule failed) or a string (non-empty meaning the
/// rule failed, with that string as the message).
pub struct CelValidator {
    cache: DashMap<String, Program>,
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
        value: &SerdeValue,
    ) -> Result<ValidationRuleResult, SerdeError> {
        let name = rule_name(rule);
        if rule.expr.is_empty() {
            return Err(SerdeError::Rule(format!(
                "validation rule '{name}' has no expression"
            )));
        }
        if !self.cache.contains_key(&rule.expr) {
            self.cache
                .insert(rule.expr.clone(), Program::compile(&rule.expr)?);
        }
        let program = self.cache.get(&rule.expr).ok_or_else(|| {
            SerdeError::Rule(format!("could not compile validation rule '{name}'"))
        })?;

        let mut context = default_context();
        context.add_variable_from_value("this", from_serde_value(value));
        context.add_variable_from_value("now", Value::Timestamp(Utc::now().into()));

        match program.value().execute(&context)? {
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
