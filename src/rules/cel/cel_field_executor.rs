use crate::rules::cel::cel_executor::{CelExecutor, from_serde_value};
use crate::serdes::serde::{FieldRuleExecutor, RuleBase, RuleContext, SerdeError, SerdeValue};
use async_trait::async_trait;
use cel_interpreter::Value;
use std::collections::HashMap;
use std::sync::Arc;

pub struct CelFieldExecutor {
    executor: CelExecutor,
}

impl RuleBase for CelFieldExecutor {
    fn get_type(&self) -> &'static str {
        "CEL_FIELD"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Default for CelFieldExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl CelFieldExecutor {
    pub fn new() -> Self {
        CelFieldExecutor {
            executor: CelExecutor::new(),
        }
    }

    pub fn register() {
        crate::serdes::rule_registry::register_rule_executor(CelFieldExecutor::new());
    }
}

#[async_trait]
impl FieldRuleExecutor for CelFieldExecutor {
    async fn transform_field(
        &self,
        ctx: &mut RuleContext,
        field_value: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        let field_ctx = ctx.current_field().expect("no field context");
        if !field_ctx.is_primitive() {
            return Ok(field_value.clone());
        }
        let mut args = HashMap::new();
        args.insert("value".to_string(), from_serde_value(field_value));
        args.insert(
            "fullName".to_string(),
            Value::String(Arc::new(field_ctx.full_name.clone())),
        );
        args.insert(
            "name".to_string(),
            Value::String(Arc::new(field_ctx.name.clone())),
        );
        args.insert(
            "typeName".to_string(),
            Value::String(Arc::new(field_ctx.type_name())),
        );
        args.insert(
            "tags".to_string(),
            Value::List(Arc::new(
                field_ctx
                    .tags
                    .iter()
                    .map(|v| Value::String(Arc::new(v.clone())))
                    .collect(),
            )),
        );
        args.insert(
            "message".to_string(),
            from_serde_value(&field_ctx.containing_message),
        );
        self.executor.execute(ctx, field_value, &args)
    }
}
