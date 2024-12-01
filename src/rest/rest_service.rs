use reqwest::StatusCode;
use crate::rest::client_config;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RestService {
    config: client_config::ClientConfig,
}

impl RestService {
    pub fn new(config: client_config::ClientConfig) -> Self {
        RestService {
            config: config,
        }
    }

    pub fn get_config(&self) -> &client_config::ClientConfig {
        &self.config
    }

    pub async fn list_subjects(&self) -> Result<reqwest::Response, reqwest::Error> {
        let client = &self.config.client;
        let url = "/subjects";
        self.send_request_urls(&url, reqwest::Method::GET, None)
    }

    async fn send_request_urls(&self, url: &str, method: reqwest::Method, body: Option<String>)
        -> Result<reqwest::Response, reqwest::Error> {
        let base_urls  = &self.config.base_urls;
        for (i, base_url) in base_urls.enumerate() {
            let new_url = format!("{}/{}", base_url, url);
            match self.try_send_request(new_url, method.clone(), body.clone()).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    if !is_retriable(&e) || i == base_urls.len() - 1 {
                        return Err(e);
                    }
                }
            }
        }
    }

    async fn try_send_request(&self, url: &str, method: reqwest::Method, body: Option<String>)
        -> Result<reqwest::Response, reqwest::Error> {
        let mut retries = 0;
        loop {
            match self.send_request(url, method.clone(), body.clone()).await {
                Ok(response) => return Ok(response),
                Err(e) => {
                    if !is_retriable(&e) || retries >= self.config.max_retries {
                        return Err(e);
                    }
                    let backoff = calculate_exponential_backoff(
                        self.config.retries_wait_ms,
                        retries,
                        Duration::from_millis(self.config.retries_max_wait_ms as u64),
                    );
                    // TODO use async runtime
                    tokio::time::sleep(backoff).await;
                    retries += 1;
                }
            }
        }
    }

    async fn send_request(&self, url: &str, method: reqwest::Method, body: Option<String>)
        -> Result<reqwest::Response, reqwest::Error> {
        let client = &self.config.client;
        let request = client.request(method, url);
        let request = match body {
            Some(body) => request.body(body),
            None => request,
        };
        request.send()
    }
}

fn calculate_exponential_backoff(
    initial_backoff: u32,
    retry_attempts: u32,
    max_backoff: Duration) -> Duration {
    let result = match 2_u32
        .checked_pow(retry_attempts)
        .map(|power| power * initial_backoff)
    {
        Some(backoff) => Duration::from_millis(backoff as u64),
        None => max_backoff,
    };

    // Apply jitter to `result`, and note that it can be applied to `max_backoff`.
    // Won't panic because `base` is in range 0..1
    let base = fastrand::f64();
    result.mul_f64(base)
}

fn is_retriable(e: &reqwest::Error) -> bool {
    match e.status() {
        Some(status) => status == StatusCode::REQUEST_TIMEOUT
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::INTERNAL_SERVER_ERROR
            || status == StatusCode::BAD_GATEWAY
            || status == StatusCode::SERVICE_UNAVAILABLE
            || status == StatusCode::GATEWAY_TIMEOUT,
        None => true,
    }
}
