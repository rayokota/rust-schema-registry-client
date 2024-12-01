#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub base_urls: Vec<String>,

    pub basic_auth: Option<BasicAuth>,
    pub bearer_access_token: Option<String>,

    pub max_retries: u32,
    pub retries_wait_ms: u32,
    pub retries_max_wait_ms: u32,

    pub client: reqwest::Client,
}

pub type BasicAuth = (String, Option<String>);

impl ClientConfig {
    pub fn new() -> ClientConfig {
        ClientConfig::default()
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            base_urls: vec!["http://localhost:8081".to_owned()],
            basic_auth: None,
            bearer_access_token: None,
            max_retries: 2,
            retries_wait_ms: 1000,
            retries_max_wait_ms: 20000,
            client: reqwest::Client::new(),
        }
    }
}
