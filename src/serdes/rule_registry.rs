use crate::serdes::serde::{RuleAction, RuleExecutor};
use dashmap::DashMap;
use lazy_static::lazy_static;
use std::sync::{Arc, RwLock};

lazy_static! {
    /// Global rule registry
    static ref GLOBAL_RULE_REGISTRY: RwLock<RuleRegistry> = RwLock::new(RuleRegistry::new());
}

#[derive(Clone, Debug)]
pub struct RuleOverride {
    pub r#type: String,
    pub on_success: Option<String>,
    pub on_failure: Option<String>,
    pub disabled: Option<bool>,
}

#[derive(Clone)]
pub struct RuleRegistry {
    rule_executors: Arc<DashMap<String, Arc<dyn RuleExecutor>>>,
    rule_actions: Arc<DashMap<String, Arc<dyn RuleAction>>>,
    rule_overrides: Arc<DashMap<String, RuleOverride>>,
}

impl Default for RuleRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl RuleRegistry {
    pub fn new() -> RuleRegistry {
        RuleRegistry {
            rule_executors: Arc::new(DashMap::new()),
            rule_actions: Arc::new(DashMap::new()),
            rule_overrides: Arc::new(DashMap::new()),
        }
    }

    pub fn register_executor<T: RuleExecutor + 'static>(&self, executor: T) {
        let rule_executors = &self.rule_executors;
        rule_executors.insert(executor.get_type().to_string(), Arc::new(executor));
    }

    pub fn get_executor(&self, r#type: &str) -> Option<Arc<dyn RuleExecutor>> {
        let rule_executors = &self.rule_executors;
        rule_executors.get(r#type).map(|e| e.clone())
    }

    pub fn get_executors(&self) -> Vec<Arc<dyn RuleExecutor>> {
        let rule_executors = &self.rule_executors;
        rule_executors.iter().map(|e| e.value().clone()).collect()
    }

    pub fn register_action<T: RuleAction + 'static>(&self, action: T) {
        let rule_actions = &self.rule_actions;
        rule_actions.insert(action.get_type().to_string(), Arc::new(action));
    }

    pub fn get_action(&self, r#type: &str) -> Option<Arc<dyn RuleAction>> {
        let rule_actions = &self.rule_actions;
        rule_actions.get(r#type).map(|a| a.clone())
    }

    pub fn get_actions(&self) -> Vec<Arc<dyn RuleAction>> {
        let rule_actions = &self.rule_actions;
        rule_actions.iter().map(|a| a.value().clone()).collect()
    }

    pub fn register_override(&self, rule_override: RuleOverride) {
        let rule_overrides = &self.rule_overrides;
        rule_overrides.insert(rule_override.r#type.to_string(), rule_override);
    }

    pub fn get_override(&self, r#type: &str) -> Option<RuleOverride> {
        let rule_overrides = &self.rule_overrides;
        rule_overrides.get(r#type).map(|o| o.clone())
    }

    pub fn get_overrides(&self) -> Vec<RuleOverride> {
        let rule_overrides = &self.rule_overrides;
        rule_overrides.iter().map(|o| o.value().clone()).collect()
    }
}

pub fn register_rule_executor<T: RuleExecutor + 'static>(executor: T) {
    let registry = GLOBAL_RULE_REGISTRY.write().unwrap();
    registry.register_executor(executor);
}

pub fn get_rule_executor(r#type: &str) -> Option<Arc<dyn RuleExecutor>> {
    let registry = GLOBAL_RULE_REGISTRY.read().unwrap();
    registry.get_executor(r#type)
}

pub fn get_rule_executors() -> Vec<Arc<dyn RuleExecutor>> {
    let registry = GLOBAL_RULE_REGISTRY.read().unwrap();
    registry.get_executors()
}

pub fn register_rule_action<T: RuleAction + 'static>(action: T) {
    let registry = GLOBAL_RULE_REGISTRY.write().unwrap();
    registry.register_action(action);
}

pub fn get_rule_action(r#type: &str) -> Option<Arc<dyn RuleAction>> {
    let registry = GLOBAL_RULE_REGISTRY.read().unwrap();
    registry.get_action(r#type)
}

pub fn get_rule_actions() -> Vec<Arc<dyn RuleAction>> {
    let registry = GLOBAL_RULE_REGISTRY.read().unwrap();
    registry.get_actions()
}

pub fn register_rule_override(rule_override: RuleOverride) {
    let registry = GLOBAL_RULE_REGISTRY.write().unwrap();
    registry.register_override(rule_override);
}

pub fn get_rule_override(r#type: &str) -> Option<RuleOverride> {
    let registry = GLOBAL_RULE_REGISTRY.read().unwrap();
    registry.get_override(r#type)
}

pub fn get_rule_overrides() -> Vec<RuleOverride> {
    let registry = GLOBAL_RULE_REGISTRY.read().unwrap();
    registry.get_overrides()
}
