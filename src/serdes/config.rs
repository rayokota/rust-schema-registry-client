use crate::serdes::serde::{
    SchemaIdDeserializer, SchemaIdSerializer, SubjectNameStrategy, dual_schema_id_deserializer,
    prefix_schema_id_serializer, topic_name_strategy,
};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub enum SchemaSelector {
    SchemaId(i32),
    LatestVersion,
    LatestWithMetadata(HashMap<String, String>),
}

#[derive(Clone, Debug)]
pub struct SerializerConfig {
    pub auto_register_schemas: bool,
    pub use_schema: Option<SchemaSelector>,
    pub normalize_schemas: bool,
    pub validate: bool,
    pub rule_config: HashMap<String, String>,
    pub subject_name_strategy: SubjectNameStrategy,
    pub schema_id_serializer: SchemaIdSerializer,
}

impl SerializerConfig {
    pub fn new(
        auto_register_schemas: bool,
        use_schema: Option<SchemaSelector>,
        normalize_schemas: bool,
        validate: bool,
        rule_config: HashMap<String, String>,
    ) -> SerializerConfig {
        SerializerConfig {
            auto_register_schemas,
            use_schema,
            normalize_schemas,
            validate,
            rule_config,
            subject_name_strategy: topic_name_strategy,
            schema_id_serializer: prefix_schema_id_serializer,
        }
    }
}

impl Default for SerializerConfig {
    fn default() -> SerializerConfig {
        SerializerConfig {
            auto_register_schemas: true,
            use_schema: None,
            normalize_schemas: false,
            validate: false,
            rule_config: HashMap::new(),
            subject_name_strategy: topic_name_strategy,
            schema_id_serializer: prefix_schema_id_serializer,
        }
    }
}

#[derive(Clone, Debug)]
pub struct DeserializerConfig {
    pub use_schema: Option<SchemaSelector>,
    pub validate: bool,
    pub rule_config: HashMap<String, String>,
    pub subject_name_strategy: SubjectNameStrategy,
    pub schema_id_deserializer: SchemaIdDeserializer,
}

impl DeserializerConfig {
    pub fn new(
        use_schema: Option<SchemaSelector>,
        validate: bool,
        rule_config: HashMap<String, String>,
    ) -> DeserializerConfig {
        DeserializerConfig {
            use_schema,
            validate,
            rule_config,
            subject_name_strategy: topic_name_strategy,
            schema_id_deserializer: dual_schema_id_deserializer,
        }
    }
}

impl Default for DeserializerConfig {
    fn default() -> DeserializerConfig {
        DeserializerConfig {
            use_schema: None,
            validate: false,
            rule_config: HashMap::new(),
            subject_name_strategy: topic_name_strategy,
            schema_id_deserializer: dual_schema_id_deserializer,
        }
    }
}
