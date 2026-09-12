//! HTTP client for the Hub Sync API - the client-side counterpart to the
//! CouchDB client the hub uses internally. A client speaks only this
//! contract, never CouchDB directly.
//!
//! Each `HubClient` targets one hub; failover is layered on by the engine,
//! which owns per-hub checkpoints (CouchDB `seq` is node-local).

use std::time::Duration;

use protocol_types::{ChangesResponse, PushChange, PushResult};
use reqwest::{RequestBuilder, StatusCode, Url};
use thiserror::Error;

/// Must outlast the hub's long-poll hold (up to 60s), with margin.
const LONGPOLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(65);

#[derive(Debug, Error)]
pub enum HubError {
  #[error("http transport error: {0}")]
  Transport(#[from] reqwest::Error),
  #[error("hub returned {status}: {body}")]
  Api { status: StatusCode, body: String },
  #[error("unexpected response shape: {0}")]
  Decode(String),
  #[error("failed to build url, is base_url valid?")]
  UrlBuild,
}

/// A file's content plus the metadata the hub attached to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileContent {
  pub rev: String,
  pub mtime: i64,
  pub content_type: String,
  pub content: Vec<u8>,
}

#[derive(Clone)]
pub struct HubClient {
  http: reqwest::Client,
  base_url: Url,
  device_token: String,
}

impl HubClient {
  fn build_url(&self, sub_path: &str) -> Result<Url, HubError> {
    let mut url = self.base_url.clone();
    let mut path_segements = url.path_segments_mut().map_err(|()| HubError::UrlBuild)?;
    path_segements.push("/"); // Make sure the last segement is not replaced.
    path_segements.push(sub_path);
    drop(path_segements);
    Ok(url)
  }

  fn add_query_param(&self, url: &mut Url, key: &str, value: &str) {
    let mut pairs = url.query_pairs_mut();
    pairs.append_pair(key, value);
  }

  async fn send_request(&self, builder: RequestBuilder) -> Result<reqwest::Response, reqwest::Error> {
    builder.bearer_auth(&self.device_token).send().await?.error_for_status()
  }

  async fn send_and_parse<T: for<'de> serde::Deserialize<'de>>(&self, builder: RequestBuilder) -> Result<T, HubError> {
      Self::json_or_err(self.send_request(builder).await?).await
  }

  fn header(resp: &reqwest::Response, name: &str) -> Result<String, HubError> {
    let value = resp
        .headers().get(name).ok_or_else(|| HubError::Decode(format!("Header {name} is missing.")))?
        .to_str().map_err(|_| HubError::Decode(format!("Header {name} has an invalid value.")))?
        .to_string();
    Ok(value)
  }

  pub fn new(base_url: Url, device_token: impl Into<String>) -> Self {
    Self {
      http: common::http_client(),
      base_url,
      device_token: device_token.into(),
    }
  }

  pub async fn metadata(&self) -> Result<String, HubError> {
    let url = self.build_url("metadata")?;
    self.send_and_parse(self.http.get(url)).await
  }

  /// `GET /changes`; `None` means "from the beginning".
  pub async fn changes(&self, since: Option<&str>) -> Result<ChangesResponse, HubError> {
    let mut url = self.build_url("changes")?;
    if let Some(s) = since {
      self.add_query_param(&mut url, "since", s);
    }
    self.send_and_parse(self.http.get(url)).await
  }

  /// `GET /changes/longpoll` - blocks up to `timeout_secs`, or returns
  /// immediately when a change lands. The desktop "wake" path.
  pub async fn longpoll(
    &self,
    since: Option<&str>,
    timeout_secs: u64,
  ) -> Result<ChangesResponse, HubError> {
    let mut url = self.build_url("changes/longpoll")?;
    self.add_query_param(&mut url, "timeout", &format!("{}", timeout_secs));
    if let Some(since) = since {
        self.add_query_param(&mut url, "since", since);
    }
    self.send_and_parse(self.http.get(url).timeout(LONGPOLL_REQUEST_TIMEOUT)).await
  }

  /// `GET /file/{path}`. `Ok(None)` when the hub has no such file.
  pub async fn get_file(&self, path: &str) -> Result<Option<FileContent>, HubError> {
    let url = self.build_url(&format!("file/{}", encode_path(path)))?;
    let response = match self.send_request(self.http.get(url)).await {
        Err(err) => {
            if err.status() == Some(reqwest::StatusCode::NOT_FOUND) {
                return Ok(None)
            }
            return Err(err.into());
        },
        Ok(response) => response,
    };

    let rev = Self::header(&response, "x-file-rev")?;
    let mtime = Self::header(&response, "x-file-mtime")?
      .parse::<i64>()
      .map_err(|_| HubError::Decode("non-numeric x-file-mtime".into()))?;
    let content_type = Self::header(&response, "content-type")
      .unwrap_or_else(|_| "application/octet-stream".to_string());
    let content = response.bytes().await?.to_vec();

    Ok(Some(FileContent {
      rev,
      mtime,
      content_type,
      content,
    }))
  }

  /// `POST /changes` - returns one result per input, in order.
  pub async fn push(&self, changes: &[PushChange]) -> Result<Vec<PushResult>, HubError> {
    let url = self.build_url("changes")?;
    self.send_and_parse(self.http.post(url).json(changes)).await
  }

  async fn json_or_err<T: for<'de> serde::Deserialize<'de>>(response: reqwest::Response) -> Result<T, HubError> {
    if response.status().is_success() {
      response.json().await.map_err(|err| HubError::Decode(format!("{:?}", err)))
    } else {
      let status = response.status();
      let body = response.text().await.unwrap_or("<failed to read body>".to_string());
      Err(HubError::Api { status, body })
    }
  }
}

/// Encodes each path segment but keeps `/` separators, so `notes/a.txt`
/// stays a sub-path while spaces/special chars are encoded.
fn encode_path(path: &str) -> String {
  path
    .split('/')
    .map(urlencoding::encode)
    .collect::<Vec<_>>()
    .join("/")
}

#[cfg(test)]
mod tests {
  use super::*;
  use wiremock::matchers::{bearer_token, method, path, query_param};
  use wiremock::{Mock, MockServer, ResponseTemplate};

  fn client(server: &MockServer) -> HubClient {
    HubClient::new(Url::parse(&server.uri()).unwrap(), "dev-token")
  }

  #[test]
  fn encode_path_preserves_slashes_and_encodes_segments() {
    assert_eq!(encode_path("notes/a.txt"), "notes/a.txt");
    assert_eq!(encode_path("my file.txt"), "my%20file.txt");
    assert_eq!(encode_path("a/b c/d"), "a/b%20c/d");
  }

  #[tokio::test]
  async fn changes_sends_bearer_auth_and_passes_since() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/changes"))
      .and(query_param("since", "42-abc"))
      .and(bearer_token("dev-token"))
      .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
          "changes": [{"path": "a.txt", "deleted": false, "rev": "1-x"}],
          "checkpoint": "43-def"
      })))
      .mount(&server)
      .await;

    let resp = client(&server).changes(Some("42-abc")).await.unwrap();
    assert_eq!(resp.checkpoint, "43-def");
    assert_eq!(resp.changes.len(), 1);
  }

  #[tokio::test]
  async fn longpoll_sends_timeout_and_since() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/changes/longpoll"))
      .and(query_param("timeout", "25"))
      .and(query_param("since", "42-abc"))
      .and(bearer_token("dev-token"))
      .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
          "changes": [{"path": "a.txt", "deleted": false, "rev": "1-x"}],
          "checkpoint": "43-def"
      })))
      .mount(&server)
      .await;

    let resp = client(&server).longpoll(Some("42-abc"), 25).await.unwrap();
    assert_eq!(resp.checkpoint, "43-def");
    assert_eq!(resp.changes.len(), 1);
  }

  #[tokio::test]
  async fn get_file_parses_headers_and_returns_none_on_404() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/file/a.txt"))
      .respond_with(
        ResponseTemplate::new(200)
          .insert_header("x-file-rev", "1-abc")
          .insert_header("x-file-mtime", "1700000000")
          .insert_header("content-type", "text/plain")
          .set_body_bytes(b"hello".to_vec()),
      )
      .mount(&server)
      .await;
    Mock::given(method("GET"))
      .and(path("/file/missing.txt"))
      .respond_with(ResponseTemplate::new(404))
      .mount(&server)
      .await;

    let f = client(&server).get_file("a.txt").await.unwrap().unwrap();
    assert_eq!(f.rev, "1-abc");
    assert_eq!(f.mtime, 1_700_000_000);
    assert_eq!(f.content_type, "text/plain");
    assert_eq!(f.content, b"hello");

    assert!(client(&server)
      .get_file("missing.txt")
      .await
      .unwrap()
      .is_none());
  }

  #[tokio::test]
  async fn push_posts_json_and_decodes_results() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
      .and(path("/changes"))
      .and(bearer_token("dev-token"))
      .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
          { "path": "a.txt", "rev": "2-b" },
          { "path": "b.txt", "rev": "3-c" }
      ])))
      .mount(&server)
      .await;

    let changes = vec![protocol_types::PushChange {
      path: "a.txt".into(),
      deleted: false,
      base_rev: None,
      mtime: 1,
      content_type: Some("text/plain".into()),
      content_base64: Some("aGVsbG8=".into()),
    }];
    let results = client(&server).push(&changes).await.unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].path, "a.txt");
    assert_eq!(results[0].rev, "2-b");
    assert_eq!(results[1].rev, "3-c");
  }
}
