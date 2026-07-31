use crate::rest::client_config;
use reqwest::StatusCode;
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) struct RestService {
    config: client_config::ClientConfig,
}

impl RestService {
    pub fn new(config: client_config::ClientConfig) -> Self {
        RestService { config }
    }

    pub fn config(&self) -> &client_config::ClientConfig {
        &self.config
    }

    pub async fn send_request_urls(
        &self,
        url: &str,
        method: reqwest::Method,
        query: Option<&[(String, String)]>,
        body: Option<&str>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let base_urls = &self.config.base_urls;
        for (i, base_url) in base_urls.iter().enumerate() {
            let base_url = base_url.trim_end_matches('/');
            let new_url = base_url.to_string() + url;
            let is_last_url = i == base_urls.len() - 1;
            match self.try_send_request(&new_url, &method, query, body).await {
                Ok(response) => {
                    // A retriable status means this URL is (at least for now)
                    // unable to serve the request, so fail over to the next one.
                    // The response from the last URL is returned as-is, for the
                    // caller to turn into an error.
                    if is_last_url || !is_retriable_status(response.status()) {
                        return Ok(response);
                    }
                }
                Err(e) => {
                    if is_last_url || !is_retriable_error(&e) {
                        return Err(e);
                    }
                }
            }
        }
        unreachable!()
    }

    async fn try_send_request(
        &self,
        url: &str,
        method: &reqwest::Method,
        query: Option<&[(String, String)]>,
        body: Option<&str>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let mut retries = 0;
        loop {
            let result = self.send_request(url, method, query, body).await;
            let retriable = match &result {
                // An error status is not reported by reqwest as an error, so it
                // has to be checked on the response itself. Without this, only
                // failures that occur before a response is received are ever
                // retried, and a retriable status such as 503 is returned on
                // the first attempt.
                Ok(response) => is_retriable_status(response.status()),
                Err(e) => is_retriable_error(e),
            };
            if !retriable || retries >= self.config.max_retries {
                return result;
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

    async fn send_request(
        &self,
        url: &str,
        method: &reqwest::Method,
        query: Option<&[(String, String)]>,
        body: Option<&str>,
    ) -> Result<reqwest::Response, reqwest::Error> {
        let client = &self.config.client;
        let mut request = client.request(method.clone(), url);
        request = request.header(
            reqwest::header::CONTENT_TYPE,
            "application/vnd.schemaregistry.v1+json",
        );
        request = request.header("Confluent-Accept-Unknown-Properties", "true");
        if let Some((username, password)) = &self.config.basic_auth {
            request = request.basic_auth(username, password.as_deref());
        } else if let Some(token) = &self.config.bearer_access_token {
            request = request.bearer_auth(token);
        }
        if let Some(query) = query
            && !query.is_empty()
        {
            request = request.query(query);
        }
        if let Some(body) = body {
            request = request.body(body.to_string());
        }
        request.send().await
    }
}

fn calculate_exponential_backoff(
    initial_backoff: u32,
    retry_attempts: u32,
    max_backoff: Duration,
) -> Duration {
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

/// Statuses for which the request may succeed if it is sent again, either to the
/// same URL after a delay or to another URL.
fn is_retriable_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::TOO_MANY_REQUESTS
        || status == StatusCode::INTERNAL_SERVER_ERROR
        || status == StatusCode::BAD_GATEWAY
        || status == StatusCode::SERVICE_UNAVAILABLE
        || status == StatusCode::GATEWAY_TIMEOUT
}

fn is_retriable_error(e: &reqwest::Error) -> bool {
    match e.status() {
        Some(status) => is_retriable_status(status),
        // No status means the request failed before a response was received
        // (connection failure, timeout, TLS error), which is retriable.
        None => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// An HTTP server that answers every request with the same status line, and
    /// counts the requests it received.
    struct StubServer {
        url: String,
        requests: Arc<AtomicUsize>,
    }

    impl StubServer {
        async fn start(status_line: &'static str) -> StubServer {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(AtomicUsize::new(0));

            let counter = requests.clone();
            tokio::spawn(async move {
                while let Ok((mut socket, _)) = listener.accept().await {
                    counter.fetch_add(1, Ordering::SeqCst);

                    // Consume the request up to the end of its headers, so that
                    // the client isn't answered before it has finished sending.
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                        match socket.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => request.extend_from_slice(&buf[..n]),
                        }
                    }

                    let response = format!(
                        "HTTP/1.1 {status_line}\r\n\
                         Content-Type: application/vnd.schemaregistry.v1+json\r\n\
                         Content-Length: 2\r\n\
                         Connection: close\r\n\r\n[]"
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.flush().await;
                }
            });

            StubServer { url, requests }
        }

        fn request_count(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
    }

    fn config(base_urls: Vec<String>) -> client_config::ClientConfig {
        let mut config = client_config::ClientConfig::new(base_urls);
        config.max_retries = 2;
        config.retries_wait_ms = 1;
        config.retries_max_wait_ms = 2;
        config
    }

    async fn get_subjects(service: &RestService) -> reqwest::Response {
        service
            .send_request_urls("/subjects", reqwest::Method::GET, None, None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn retries_retriable_status() {
        // An error status is returned by reqwest as a response rather than as an
        // error, so a retriable status used to be returned on the first attempt
        // without the retry loop ever engaging.
        let server = StubServer::start("503 Service Unavailable").await;
        let service = RestService::new(config(vec![server.url.clone()]));

        let response = get_subjects(&service).await;

        assert_eq!(StatusCode::SERVICE_UNAVAILABLE, response.status());
        // The initial attempt plus max_retries.
        assert_eq!(3, server.request_count());
    }

    #[tokio::test]
    async fn fails_over_to_next_url_on_retriable_status() {
        let unavailable = StubServer::start("503 Service Unavailable").await;
        let available = StubServer::start("200 OK").await;
        let service = RestService::new(config(vec![
            unavailable.url.clone(),
            available.url.clone(),
        ]));

        let response = get_subjects(&service).await;

        assert_eq!(StatusCode::OK, response.status());
        assert_eq!(3, unavailable.request_count());
        assert_eq!(1, available.request_count());
    }

    #[tokio::test]
    async fn does_not_retry_non_retriable_status() {
        let server = StubServer::start("404 Not Found").await;
        let service = RestService::new(config(vec![server.url.clone()]));

        let response = get_subjects(&service).await;

        assert_eq!(StatusCode::NOT_FOUND, response.status());
        assert_eq!(1, server.request_count());
    }

    #[test]
    fn retriable_statuses() {
        for status in [
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
        ] {
            assert!(is_retriable_status(status), "{status} should be retriable");
        }
    }

    #[test]
    fn non_retriable_statuses() {
        for status in [
            StatusCode::OK,
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::METHOD_NOT_ALLOWED,
            StatusCode::CONFLICT,
            StatusCode::UNPROCESSABLE_ENTITY,
            StatusCode::NOT_IMPLEMENTED,
        ] {
            assert!(
                !is_retriable_status(status),
                "{status} should not be retriable"
            );
        }
    }
}
