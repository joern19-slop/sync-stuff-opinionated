//! Small shared utilities for the hub and clients: a robust HTTP client and
//! strict environment parsing.

pub mod env;

use std::time::Duration;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const USER_AGENT: &str = concat!("filesync/", env!("CARGO_PKG_VERSION"));

/// A [`reqwest::Client`] with a connect timeout and a default request timeout,
/// so a black-holed peer can't hang a request forever. Callers that
/// legitimately hold a request open (long-poll) should override the timeout
/// per request with [`reqwest::RequestBuilder::timeout`].
pub fn http_client() -> reqwest::Client {
  reqwest::Client::builder()
    .connect_timeout(CONNECT_TIMEOUT)
    .timeout(REQUEST_TIMEOUT)
    .user_agent(USER_AGENT)
    .build()
    .expect("build http client")
}
