use std::time::Duration;

use reqwest::{StatusCode, Url};
use thiserror::Error;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum HttpClientError {
  #[error("failed to build url, is base_url valid?")]
  UrlBuild,
  #[error("unexpected response shape: {0}")]
  Decode(String),
  #[error("hub returned {status}: {body}")]
  Api { status: StatusCode, body: String },
  #[error("http transport error: {0}")]
  Transport(#[from] reqwest::Error),
}

#[derive(Clone)]
pub struct HttpClient {
  pub reqwest_client: reqwest::Client,
  base_url: Url,
}

pub struct UrlWrapper {
  pub url: Url,
}

impl UrlWrapper {
  pub fn add_query_param(&mut self, key: &str, value: &str) {
    let mut pairs = self.url.query_pairs_mut();
    pairs.append_pair(key, value);
  }
}

impl HttpClient {
  pub fn new(
    reqwest_client: reqwest::Client,
    base_url: Url,
  ) -> Result<HttpClient, HttpClientError> {
    let mut url = base_url.clone();
    let mut path_segements = url
      .path_segments_mut()
      .map_err(|()| HttpClientError::UrlBuild)?;
    path_segements.push("/"); // Make sure the last segement is not replaced.
    drop(path_segements);

    Ok(Self {
      reqwest_client,
      base_url: url,
    })
  }

  pub fn build_url(&self, sub_path: &str) -> Result<UrlWrapper, HttpClientError> {
    let mut url = self.base_url.clone();
    let mut path_segements = url
      .path_segments_mut()
      .map_err(|()| HttpClientError::UrlBuild)?;
    path_segements.push(sub_path);
    drop(path_segements);
    Ok(UrlWrapper { url })
  }

  pub async fn json_or_err<T: for<'de> serde::Deserialize<'de>>(
    response: reqwest::Response,
  ) -> Result<T, HttpClientError> {
    if response.status().is_success() {
      response
        .json()
        .await
        .map_err(|err| HttpClientError::Decode(format!("{:?}", err)))
    } else {
      let status = response.status();
      let body = response
        .text()
        .await
        .unwrap_or("<failed to read body>".to_string());
      Err(HttpClientError::Api { status, body })
    }
  }
}

/// Overwrite request timeout for long-polling
pub fn http_client() -> reqwest::Client {
  reqwest::Client::builder()
    .connect_timeout(CONNECT_TIMEOUT)
    .timeout(REQUEST_TIMEOUT)
    .build()
    .expect("build http client")
}
