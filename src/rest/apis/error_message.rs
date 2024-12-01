use serde::{Deserialize, Serialize};

/// ErrorMessage : Error message
#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ErrorMessage {
    /// Error code
    #[serde(rename = "error_code", skip_serializing_if = "Option::is_none")]
    pub error_code: Option<i32>,
    /// Detailed error message
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ErrorMessage {
    /// Error message
    pub fn new() -> ErrorMessage {
        ErrorMessage {
            error_code: None,
            message: None,
        }
    }
}
