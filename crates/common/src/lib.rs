pub mod env;

use std::time::Duration;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Overwrite request timeout for long-polling
pub fn http_client() -> reqwest::Client {
  reqwest::Client::builder()
    .connect_timeout(CONNECT_TIMEOUT)
    .timeout(REQUEST_TIMEOUT)
    .build()
    .expect("build http client")
}
