use crate::rules::encryption::awskms::aws_client::AwsClient;
use crate::rules::encryption::kms_driver::KmsDriver;
use crate::serdes::serde::SerdeError;
use aws_config::default_provider::credentials::DefaultCredentialsChain;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::Credentials;
use aws_credential_types::provider::ProvideCredentials;
use aws_sdk_kms::config::Region;
use log::error;
use std::collections::HashMap;
use std::env;
use std::sync::mpsc::{Sender, SyncSender};
use std::sync::{Arc, mpsc};
use tink_core::TinkError;
use tink_core::registry::KmsClient;

const PREFIX: &str = "aws-kms://";
const ACCESS_KEY_ID: &str = "access.key.id";
const SECRET_ACCESS_KEY: &str = "secret.access.key";
const PROFILE: &str = "profile";
const ROLE_ARN: &str = "role.arn";
const ROLE_SESSION_NAME: &str = "role.session.name";
const ROLE_EXTERNAL_ID: &str = "role.external.id";

pub struct AwsKmsDriver {}

impl Default for AwsKmsDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsKmsDriver {
    pub fn new() -> AwsKmsDriver {
        AwsKmsDriver {}
    }

    pub fn register() {
        crate::rules::encryption::register_kms_driver(AwsKmsDriver::new());
    }
}

impl KmsDriver for AwsKmsDriver {
    fn get_key_url_prefix(&self) -> &'static str {
        PREFIX
    }

    fn new_kms_client(
        &self,
        conf: &HashMap<String, String>,
        key_url: &str,
    ) -> Result<Arc<dyn KmsClient>, SerdeError> {
        let creds = get_creds(conf, key_url)?;
        Ok(Arc::new(AwsClient::new(key_url, creds)?))
    }
}

fn get_creds(
    conf: &HashMap<String, String>,
    key_url: &str,
) -> Result<Arc<dyn ProvideCredentials>, TinkError> {
    let (sender, receiver) = mpsc::sync_channel(1);
    tokio::spawn(get_creds_async(conf.clone(), key_url.to_string(), sender));
    receiver
        .recv()
        .map_err(|e| TinkError::new("failed to receive"))?
}

async fn get_creds_async(
    conf: HashMap<String, String>,
    key_url: String,
    sender: SyncSender<Result<Arc<dyn ProvideCredentials>, TinkError>>,
) {
    let creds = build_creds(conf, key_url).await;
    if creds.is_err() {
        error!("failed to get creds: {creds:?}");
    }
    if sender.send(creds).is_err() {
        error!("failed to send result");
    }
}

async fn build_creds(
    conf: HashMap<String, String>,
    key_url: String,
) -> Result<Arc<dyn ProvideCredentials>, TinkError> {
    let mut role_arn = conf.get(ROLE_ARN).cloned();
    if role_arn.is_none() {
        role_arn = env::var("AWS_ROLE_ARN").ok();
    }
    let mut role_session_name = conf.get(ROLE_SESSION_NAME).cloned();
    if role_session_name.is_none() {
        role_session_name = env::var("AWS_ROLE_SESSION_NAME").ok();
    }
    let mut role_external_id = conf.get(ROLE_EXTERNAL_ID).cloned();
    if role_external_id.is_none() {
        role_external_id = env::var("AWS_ROLE_EXTERNAL_ID").ok();
    }
    let role_web_identity_token_file = env::var("AWS_WEB_IDENTITY_TOKEN_FILE").ok();
    let key = conf.get(ACCESS_KEY_ID).cloned();
    let secret = conf.get(SECRET_ACCESS_KEY).cloned();
    let profile = conf.get(PROFILE).cloned();

    let key_arn = key_uri_to_key_arn(key_url.as_str())?;
    let region = Region::new(get_region_from_key_arn(&key_arn)?);
    let mut creds: Arc<dyn ProvideCredentials>;
    if let Some(key) = key
        && let Some(secret) = secret
    {
        creds = Arc::new(Credentials::from_keys(key, secret, None));
    } else if let Some(profile) = profile {
        creds = Arc::new(
            ProfileFileCredentialsProvider::builder()
                .profile_name(profile)
                .build(),
        );
    } else {
        let builder = DefaultCredentialsChain::builder();
        let c = builder.build().await;
        creds = Arc::new(c);
    }
    // If roleWebIdentityTokenFile is set, use the DefaultCredentialsProvider
    if let Some(role_arn) = role_arn
        && role_web_identity_token_file.is_none()
    {
        let mut builder = AssumeRoleProvider::builder(role_arn).region(region.clone());
        if let Some(role_session_name) = role_session_name {
            builder = builder.session_name(role_session_name);
        }
        if let Some(role_external_id) = role_external_id {
            builder = builder.external_id(role_external_id);
        }
        let c = builder.build_from_provider(creds).await;
        creds = Arc::new(c);
    }
    Ok(creds)
}

fn key_uri_to_key_arn(key_uri: &str) -> Result<String, TinkError> {
    if !key_uri.starts_with(PREFIX) {
        return Err(TinkError::new("invalid key uri"));
    }
    Ok(key_uri.replace(PREFIX, ""))
}

fn get_region_from_key_arn(key_arn: &str) -> Result<String, TinkError> {
    // An AWS key ARN is of the form
    // arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab.
    let parts: Vec<&str> = key_arn.split(':').collect();
    if parts.len() < 6 {
        return Err(TinkError::new("invalid key arn"));
    }
    Ok(parts[3].to_string())
}
