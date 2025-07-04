use crate::serdes::serde::{RuleBase, RuleContext, RuleExecutor, SerdeError, SerdeValue};
use async_trait::async_trait;
use bumpalo::Bump;
use jsonata_rs::{Error, JsonAta};
use serde_json::Value;

pub struct JsonataExecutor {}

impl RuleBase for JsonataExecutor {
    fn get_type(&self) -> &'static str {
        "JSONATA"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl Default for JsonataExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonataExecutor {
    pub fn new() -> Self {
        JsonataExecutor {}
    }
}

#[async_trait]
impl RuleExecutor for JsonataExecutor {
    async fn transform(
        &self,
        ctx: &mut RuleContext,
        msg: &SerdeValue,
    ) -> Result<SerdeValue, SerdeError> {
        let expr = ctx.rule.expr.clone().ok_or(SerdeError::Rule(
            "rule does not contain an expression".to_string(),
        ))?;
        let arena = Bump::new();
        let jsonata = JsonAta::new(&expr, &arena)?;
        let value = match msg {
            SerdeValue::Json(v) => serde_json::to_string(v)?,
            _ => {
                return Err(SerdeError::Rule(
                    "unsupported message type for jsonata rule".to_string(),
                ));
            }
        };
        let result = jsonata.evaluate(Some(&value), None)?;
        let json = Value::from(result.serialize(false));
        Ok(SerdeValue::Json(json))
    }
}

impl From<Error> for SerdeError {
    fn from(value: Error) -> Self {
        SerdeError::Rule(format!("JSONata error: {value}"))
    }
}
