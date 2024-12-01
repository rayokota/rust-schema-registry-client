#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub base_urls: Vec<String>,

    pub basic_auth: Option<BasicAuth>,
    pub bearer_access_token: Option<String>,

    pub cache_capacity: u64,
    pub cache_latest_ttl_sec: u64,

    pub max_retries: u32,
    pub retries_wait_ms: u32,
    pub retries_max_wait_ms: u32,

    pub client: reqwest::Client,
}

pub type BasicAuth = (String, Option<String>);

impl ClientConfig {
    pub fn new(base_urls: Vec<String>) -> Self {
        ClientConfig {
            base_urls,
            basic_auth: None,
            bearer_access_token: None,
            cache_capacity: 1000,
            cache_latest_ttl_sec: 60,
            max_retries: 2,
            retries_wait_ms: 1000,
            retries_max_wait_ms: 20000,
            client: reqwest::Client::new(),
        }
    }
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            base_urls: vec!["http://localhost:8081".to_string()],
            basic_auth: None,
            bearer_access_token: None,
            cache_capacity: 1000,
            cache_latest_ttl_sec: 60,
            max_retries: 2,
            retries_wait_ms: 1000,
            retries_max_wait_ms: 20000,
            client: reqwest::Client::new(),
        }
    }
}

impl PartialEq for ClientConfig {
    fn eq(&self, other: &Self) -> bool {
        self.base_urls == other.base_urls
            && self.basic_auth == other.basic_auth
            && self.bearer_access_token == other.bearer_access_token
            && self.max_retries == other.max_retries
            && self.retries_wait_ms == other.retries_wait_ms
            && self.retries_max_wait_ms == other.retries_max_wait_ms
    }
}
