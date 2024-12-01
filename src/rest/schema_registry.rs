use crate::rest::apis::Error;

pub trait SchemaRegistry {
    fn get_subjects(&self) -> Result<Vec<String>, Error<>>;

    /*
    fn get_schema_by_id(&self, id: i32) -> Result<models::Schema, Error>;
    fn get_schema_by_subject_version(&self, subject: &str, version: i32) -> Result<models::Schema, Error>;
    fn get_subject_versions(&self, subject: &str) -> Result<Vec<i32>, Error>;
    fn get_version(&self, subject: &str, schema: &models::Schema) -> Result<i32, Error>;
    fn get_version_id(&self, subject: &str, version: i32) -> Result<i32, Error>;
    fn get_version_schema(&self, subject: &str, version: i32) -> Result<models::Schema, Error>;
    fn is_compatible(&self, subject: &str, version: i32, schema: &models::Schema) -> Result<models::CompatibilityCheckResponse, Error>;
    fn is_global_compatible(&self, schema: &models::Schema) -> Result<models::CompatibilityCheckResponse, Error>;
    fn register_schema(&self, subject: &str, schema: &models::Schema) -> Result<models::Schema, Error>;
    fn register_schema_version(&self, subject: &str, schema: &models::Schema) -> Result<models::Schema, Error>;
    fn set_global_compatibility(&self, schema: &models::Schema) -> Result<models::Schema, Error>;
    fn set_subject_compatibility(&self, subject: &str, schema: &models::Schema) -> Result<models::Schema, Error>;
    fn set_version_compatibility(&self, subject: &str, version: i32, schema: &models::Schema) -> Result<models::Schema, Error>;
    fn test_compatibility(&self, schema: &models::Schema) -> Result<models::CompatibilityCheckResponse, Error>;
    fn test_global_compatibility(&self, schema: &models::Schema) -> Result<models::CompatibilityCheckResponse, Error>;
    fn test_subject_compatibility(&self, subject: &str, schema: &models::Schema) -> Result<models::CompatibilityCheckResponse, Error>;
    fn test_version_compatibility(&self, subject: &str, version: i32, schema: &models::Schema) -> Result<models::CompatibilityCheckResponse, Error>;
    */
}