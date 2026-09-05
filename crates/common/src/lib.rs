//! Small shared utilities for the hub and clients: a robust HTTP client and
//! strict environment parsing.

pub mod env;

use std::time::Duration;

/// Default total request timeout. On native this is baked into the client; on
/// wasm (where `ClientBuilder` has no timeout) callers apply it per request.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[cfg(not(target_arch = "wasm32"))]
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(not(target_arch = "wasm32"))]
const USER_AGENT: &str = concat!("filesync/", env!("CARGO_PKG_VERSION"));

/// A [`reqwest::Client`] with a connect timeout and default request timeout,
/// so a black-holed peer can't hang a request forever. Callers that hold a
/// request open (long-poll) override the timeout per request.
///
/// On wasm, browser `fetch` provides neither timeout (and forbids
/// `User-Agent`), so those are native-only; wasm callers set the request
/// timeout per request.
pub fn http_client() -> reqwest::Client {
  #[cfg(not(target_arch = "wasm32"))]
  {
    reqwest::Client::builder()
      .connect_timeout(CONNECT_TIMEOUT)
      .user_agent(USER_AGENT)
      .timeout(REQUEST_TIMEOUT)
      .build()
      .expect("build http client")
  }
  #[cfg(target_arch = "wasm32")]
  {
    reqwest::Client::builder().build().expect("build http client")
  }
}
