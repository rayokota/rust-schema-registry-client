use crate::rules::encryption::awskms::aws_aead::AwsAead;
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_kms::config::Region;
use regex::Regex;
use tink_core::{TinkError, utils::wrap_err};

/// Prefix for any AWS-KMS key URIs.
pub const AWS_PREFIX: &str = "aws-kms://";

/// `AwsClient` represents a client that connects to the AWS KMS backend.
pub struct AwsClient {
    key_uri_prefix: String,
    kms: aws_sdk_kms::Client,
}

impl std::fmt::Debug for AwsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsClient")
            .field("key_uri_prefix", &self.key_uri_prefix)
            .finish()
    }
}

impl AwsClient {
    /// Return a new AWS KMS client which will use default credentials to handle keys with
    /// `uri_prefix` prefix. `uri_prefix` must have the following format:
    /// `aws-kms://arn:<partition>:kms:<region>:[:path]`
    /// See <http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>.
    pub fn new(
        uri_prefix: &str,
        creds: impl ProvideCredentials + 'static,
    ) -> Result<AwsClient, TinkError> {
        let r = get_region(uri_prefix)?;

        let config = aws_sdk_kms::config::Builder::new()
            .region(r)
            .credentials_provider(creds)
            .build();

        let kms = aws_sdk_kms::Client::from_conf(config);
        Self::new_with_kms(uri_prefix, kms)
    }

    /// Return a new AWS KMS client with user created KMS client.  Client is responsible for keeping
    /// the region consistency between key URI and KMS client.  `uri_prefix` must have the
    /// following format: `aws-kms://arn:<partition>:kms:<region>:[:path]`
    /// See <http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>.
    pub fn new_with_kms(
        uri_prefix: &str,
        kms: aws_sdk_kms::Client,
    ) -> Result<AwsClient, TinkError> {
        if !uri_prefix.to_lowercase().starts_with(AWS_PREFIX) {
            return Err(
                format!("uri_prefix must start with {AWS_PREFIX}, but got {uri_prefix}").into(),
            );
        }

        Ok(AwsClient {
            key_uri_prefix: uri_prefix.to_string(),
            kms,
        })
    }
}

impl tink_core::registry::KmsClient for AwsClient {
    fn supported(&self, key_uri: &str) -> bool {
        key_uri.starts_with(&self.key_uri_prefix)
    }

    /// Get an AEAD backed by `key_uri`.
    /// `key_uri` must have the following format: `aws-kms://arn:<partition>:kms:<region>:[:path]`.
    /// See <http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html>.
    fn get_aead(&self, key_uri: &str) -> Result<Box<dyn tink_core::Aead>, TinkError> {
        if !self.supported(key_uri) {
            return Err(format!(
                "key_uri must start with prefix {}, but got {}",
                self.key_uri_prefix, key_uri
            )
            .into());
        }

        let uri = if let Some(stripped) = key_uri.strip_prefix(AWS_PREFIX) {
            stripped
        } else {
            key_uri
        };
        Ok(Box::new(AwsAead::new(uri, self.kms.clone())?))
    }
}

fn get_region(key_uri: &str) -> Result<Region, TinkError> {
    // key_uri must have the following format: 'aws-kms://arn:<partition>:kms:<region>:[:path]'.
    // See http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html.
    let re1 = Regex::new(r"aws-kms://arn:(aws[a-zA-Z0-9-_]*):kms:([a-z0-9-]+):")
        .map_err(|e| wrap_err("failed to compile regex", e))?;
    let r = re1
        .captures(key_uri)
        .ok_or_else(|| TinkError::new("extracting region from URI failed"))?;
    if r.len() != 3 {
        return Err("extracting region from URI failed".into());
    }
    Ok(Region::new(r[2].to_string()))
}
