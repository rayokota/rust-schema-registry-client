use crate::serdes::serde::{SerdeError, SerdeValue};
use std::fmt;
use std::fmt::{Display, Formatter};

/// The schema property (Avro) / keyword (JSON Schema) that holds inline validation rules.
pub const VALIDATION_RULES_PROP: &str = "confluent:rules";

/// Determines when inline validation rules run, relative to domain rule transformations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ValidationRulesExecution {
    /// Inline validation rules are not evaluated.
    #[default]
    Disabled,
    /// Evaluate inline validation rules on the original message, before domain rule
    /// transformations.
    BeforeDomainRules,
    /// Evaluate inline validation rules on the transformed message, after domain rules.
    AfterDomainRules,
}

impl ValidationRulesExecution {
    /// Parses the string form used by the other Schema Registry clients.
    pub fn parse(value: &str) -> Option<ValidationRulesExecution> {
        match value {
            "DISABLED" => Some(ValidationRulesExecution::Disabled),
            "BEFORE_DOMAIN_RULES" => Some(ValidationRulesExecution::BeforeDomainRules),
            "AFTER_DOMAIN_RULES" => Some(ValidationRulesExecution::AfterDomainRules),
            _ => None,
        }
    }
}

/// An inline validation rule (a CHECK constraint) declared on a schema, either on a
/// record/message/object or on one of its fields.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ValidationRule {
    pub name: String,
    pub doc: String,
    pub expr: String,
    pub sql: String,
}

/// The outcome of evaluating a validation rule: either a bool (false meaning the rule
/// failed) or a string (non-empty meaning the rule failed, with that string as the
/// failure message).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationRuleResult {
    Bool(bool),
    Message(String),
}

/// Evaluates a single inline validation rule against a value.
pub trait ValidationRuleExecutor: Send + Sync {
    /// The type identifier for this executor.
    fn get_type(&self) -> &'static str;

    /// Evaluates the rule against `value`. Returns an error when the rule cannot be
    /// compiled or evaluated, or when it resolves to something other than a bool or a
    /// string.
    fn execute(
        &self,
        rule: &ValidationRule,
        value: &SerdeValue,
    ) -> Result<ValidationRuleResult, SerdeError>;
}

/// A single inline validation rule failure, located at `field_path` within the message
/// that was validated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationRuleError {
    pub rule: ValidationRule,
    pub field_path: String,
    /// An optional dynamic error message returned by the rule itself — set when the rule
    /// expression returned a non-empty string explaining the failure (e.g.
    /// `x > 0 ? '' : 'x must be positive'`). Empty when the failure was a plain `false`
    /// or an evaluation error.
    pub message: String,
    /// Executor failure text, when the rule could not be evaluated at all.
    pub cause: String,
}

impl Display for ValidationRuleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let path = if self.field_path.is_empty() {
            "<root>"
        } else {
            &self.field_path
        };
        let name = if self.rule.name.is_empty() {
            "unnamed"
        } else {
            &self.rule.name
        };
        // Prefer the dynamic message returned by the rule itself; fall back to the rule's
        // authored doc / SQL / CEL expression in that order.
        let detail = if !self.message.is_empty() {
            &self.message
        } else if !self.rule.doc.is_empty() {
            &self.rule.doc
        } else if !self.rule.sql.is_empty() {
            &self.rule.sql
        } else {
            &self.rule.expr
        };
        write!(f, "{path}: {name}: {detail}")?;
        if !self.cause.is_empty() {
            write!(f, " (caused by: {})", self.cause)?;
        }
        Ok(())
    }
}

/// Aggregates every inline validation rule failure found while walking a message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationRulesFailed {
    pub violations: Vec<ValidationRuleError>,
}

impl Display for ValidationRulesFailed {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let count = self.violations.len();
        if count == 0 {
            return write!(f, "Validation rule failed (no detail)");
        }
        let plural = if count == 1 { "" } else { "s" };
        write!(f, "Validation rule failed ({count} violation{plural}):")?;
        for violation in &self.violations {
            write!(f, "\n  - {violation}")?;
        }
        Ok(())
    }
}

/// Parses a `confluent:rules` property value — a list of objects with name/doc/expr/sql
/// keys. Anything that is not such a list yields no rules; malformed entries within the
/// list are skipped.
pub fn parse_validation_rules(prop: Option<&serde_json::Value>) -> Vec<ValidationRule> {
    let Some(serde_json::Value::Array(entries)) = prop else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            let serde_json::Value::Object(entry) = entry else {
                return None;
            };
            let string_prop = |key: &str| {
                entry
                    .get(key)
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            };
            Some(ValidationRule {
                name: string_prop("name"),
                doc: string_prop("doc"),
                expr: string_prop("expr"),
                sql: string_prop("sql"),
            })
        })
        .collect()
}

/// Evaluates one inline validation rule, pushing a [`ValidationRuleError`] onto
/// `violations` when it fails. A rule that cannot be evaluated is itself recorded as a
/// violation so the walk can continue.
///
/// Returns whether a violation was recorded.
pub fn evaluate_validation_rule(
    executor: &dyn ValidationRuleExecutor,
    rule: &ValidationRule,
    value: &SerdeValue,
    path: &str,
    violations: &mut Vec<ValidationRuleError>,
) -> bool {
    match executor.execute(rule, value) {
        Err(e) => {
            violations.push(ValidationRuleError {
                rule: rule.clone(),
                field_path: path.to_string(),
                message: String::new(),
                cause: e.to_string(),
            });
            true
        }
        Ok(ValidationRuleResult::Bool(true)) => false,
        Ok(ValidationRuleResult::Bool(false)) => {
            violations.push(ValidationRuleError {
                rule: rule.clone(),
                field_path: path.to_string(),
                message: String::new(),
                cause: String::new(),
            });
            true
        }
        Ok(ValidationRuleResult::Message(message)) => {
            if message.is_empty() {
                return false;
            }
            violations.push(ValidationRuleError {
                rule: rule.clone(),
                field_path: path.to_string(),
                message,
                cause: String::new(),
            });
            true
        }
    }
}

/// Turns the collected violations into a single error, or `Ok(())` when there are none.
pub fn raise_validation_violations(violations: Vec<ValidationRuleError>) -> Result<(), SerdeError> {
    if violations.is_empty() {
        return Ok(());
    }
    Err(SerdeError::ValidationRules(Box::new(
        ValidationRulesFailed { violations },
    )))
}

/// Appends a field name to a dotted validation path.
pub fn append_validation_path(path: &str, name: &str) -> String {
    if path.is_empty() {
        name.to_string()
    } else {
        format!("{path}.{name}")
    }
}

#[cfg(test)]
#[cfg(feature = "rules-cel")]
mod tests {
    use super::*;
    use crate::rules::cel::cel_validator::CelValidator;
    use serde_json::json;

    fn rule(name: &str, expr: &str) -> ValidationRule {
        ValidationRule {
            name: name.to_string(),
            doc: String::new(),
            expr: expr.to_string(),
            sql: String::new(),
        }
    }

    #[test]
    fn test_cel_validator_returns_bool() {
        let validator = CelValidator::new();
        let value = SerdeValue::Json(json!({"name": "alice"}));

        assert_eq!(
            validator
                .execute(&rule("n", "this.name == 'alice'"), &value)
                .unwrap(),
            ValidationRuleResult::Bool(true)
        );
        assert_eq!(
            validator
                .execute(&rule("n", "this.name == 'bob'"), &value)
                .unwrap(),
            ValidationRuleResult::Bool(false)
        );
    }

    #[test]
    fn test_cel_validator_returns_message() {
        let validator = CelValidator::new();
        let value = SerdeValue::Json(json!({"age": 3}));

        assert_eq!(
            validator
                .execute(
                    &rule("n", "this.age >= 18 ? '' : 'must be an adult'"),
                    &value
                )
                .unwrap(),
            ValidationRuleResult::Message("must be an adult".to_string())
        );
    }

    #[test]
    fn test_cel_validator_rejects_non_boolean_result() {
        let validator = CelValidator::new();
        let value = SerdeValue::Json(json!({"age": 3}));
        assert!(validator.execute(&rule("n", "this.age"), &value).is_err());
    }

    #[test]
    fn test_cel_validator_rejects_empty_expression() {
        let validator = CelValidator::new();
        let value = SerdeValue::Json(json!({"age": 3}));
        assert!(validator.execute(&rule("n", ""), &value).is_err());
    }

    #[test]
    fn test_failed_rule_becomes_a_violation_with_its_cause() {
        let validator = CelValidator::new();
        let value = SerdeValue::Json(json!({"age": 3}));
        let mut violations = Vec::new();

        // A rule that cannot be evaluated is recorded as a violation rather than aborting
        // the walk.
        assert!(evaluate_validation_rule(
            &validator,
            &rule("bad", "this.missing"),
            &value,
            "$",
            &mut violations
        ));
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].field_path, "$");
        assert!(!violations[0].cause.is_empty());
    }

    #[test]
    fn test_violation_message_prefers_dynamic_then_doc_then_expr() {
        let with_doc = ValidationRule {
            name: "r".to_string(),
            doc: "doc".to_string(),
            expr: "expr".to_string(),
            sql: String::new(),
        };
        assert_eq!(
            ValidationRuleError {
                rule: with_doc.clone(),
                field_path: "a.b".to_string(),
                message: "dynamic".to_string(),
                cause: String::new(),
            }
            .to_string(),
            "a.b: r: dynamic"
        );
        assert_eq!(
            ValidationRuleError {
                rule: with_doc,
                field_path: "a.b".to_string(),
                message: String::new(),
                cause: String::new(),
            }
            .to_string(),
            "a.b: r: doc"
        );
        assert_eq!(
            ValidationRuleError {
                rule: rule("r", "expr"),
                field_path: String::new(),
                message: String::new(),
                cause: String::new(),
            }
            .to_string(),
            "<root>: r: expr"
        );
        assert_eq!(
            ValidationRuleError {
                rule: rule("", "expr"),
                field_path: String::new(),
                message: String::new(),
                cause: "boom".to_string(),
            }
            .to_string(),
            "<root>: unnamed: expr (caused by: boom)"
        );
    }

    #[test]
    fn test_aggregated_error_lists_every_violation() {
        let violations = vec![
            ValidationRuleError {
                rule: rule("a", "e1"),
                field_path: "x".to_string(),
                message: String::new(),
                cause: String::new(),
            },
            ValidationRuleError {
                rule: rule("b", "e2"),
                field_path: "y".to_string(),
                message: String::new(),
                cause: String::new(),
            },
        ];
        let err = raise_validation_violations(violations).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("2 violations"), "{message}");
        assert!(message.contains("x: a: e1"), "{message}");
        assert!(message.contains("y: b: e2"), "{message}");
    }

    #[test]
    fn test_no_violations_raises_nothing() {
        assert!(raise_validation_violations(Vec::new()).is_ok());
    }

    #[test]
    fn test_parses_rules_from_schema_property() {
        let prop = json!([
            {"name": "a", "doc": "d", "expr": "e", "sql": "s"},
            {"name": "b"},
            "not an object"
        ]);
        let rules = parse_validation_rules(Some(&prop));
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].name, "a");
        assert_eq!(rules[0].doc, "d");
        assert_eq!(rules[0].expr, "e");
        assert_eq!(rules[0].sql, "s");
        assert_eq!(rules[1].name, "b");
        assert!(rules[1].expr.is_empty());

        assert!(parse_validation_rules(Some(&json!({}))).is_empty());
        assert!(parse_validation_rules(None).is_empty());
    }

    #[test]
    fn test_parses_execution_mode() {
        assert_eq!(
            ValidationRulesExecution::parse("DISABLED"),
            Some(ValidationRulesExecution::Disabled)
        );
        assert_eq!(
            ValidationRulesExecution::parse("BEFORE_DOMAIN_RULES"),
            Some(ValidationRulesExecution::BeforeDomainRules)
        );
        assert_eq!(
            ValidationRulesExecution::parse("AFTER_DOMAIN_RULES"),
            Some(ValidationRulesExecution::AfterDomainRules)
        );
        assert_eq!(ValidationRulesExecution::parse("nonsense"), None);
    }

    #[test]
    fn test_validation_is_disabled_by_default() {
        let config = crate::serdes::config::SerializerConfig::default();
        assert_eq!(
            config.validation_rules_execution,
            ValidationRulesExecution::Disabled
        );
        assert!(!config.validation_rules_fail_fast);
    }
}
