//! HTTP client for the Hub Sync API, the client-side counterpart to the
//! CouchDB client the *hub* uses internally (`hub-api::couch`). A client
//! speaks only this contract - it never talks CouchDB directly.
//!
//! Each `HubClient` targets a single hub; multi-hub failover is layered on
//! top by the sync engine, which owns per-hub checkpoints (a CouchDB `seq`
//! is node-local, so it must be scoped to the hub that issued it).

use std::time::Duration;

use protocol_types::{ChangesResponse, PushChange, PushResult};
use thiserror::Error;

/// The HTTP request timeout for a long-poll: it must outlast the hub's hold
/// (up to 60s), with margin.
const LONGPOLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(65);

#[derive(Debug, Error)]
pub enum HubError {
  #[error("http transport error: {0}")]
  Transport(#[from] reqwest::Error),
  #[error("hub returned {status}: {body}")]
  Api { status: u16, body: String },
  #[error("unexpected response shape: {0}")]
  Decode(String),
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
  base_url: String,
  device_token: String,
}

impl HubClient {
  pub fn new(base_url: impl Into<String>, device_token: impl Into<String>) -> Self {
    Self {
      http: common::http_client(),
      base_url: base_url.into(),
      device_token: device_token.into(),
    }
  }

  /// Stable identifier for this hub, used to key its checkpoint. For now
  /// that's just the base URL.
  pub fn id(&self) -> &str {
    &self.base_url
  }

  /// `GET /changes?since=<checkpoint>`. `None` means "from the beginning".
  pub async fn changes(&self, since: Option<&str>) -> Result<ChangesResponse, HubError> {
    let mut url = format!("{}/changes", self.base_url.trim_end_matches('/'));
    if let Some(s) = since {
      url.push_str("?since=");
      url.push_str(&urlencoding::encode(s));
    }
    let resp = self
      .http
      .get(&url)
      .bearer_auth(&self.device_token)
      .send()
      .await?;
    Self::json_or_err(resp).await
  }

  /// `GET /changes/longpoll` - blocks up to `timeout_secs` waiting for a
  /// change, returning immediately when one lands (or empty on timeout).
  /// The hub-side, FCM-free "wake" path for desktop clients.
  pub async fn longpoll(
    &self,
    since: Option<&str>,
    timeout_secs: u64,
  ) -> Result<ChangesResponse, HubError> {
    let mut url = format!(
      "{}/changes/longpoll?timeout={timeout_secs}",
      self.base_url.trim_end_matches('/')
    );
    if let Some(s) = since {
      url.push_str("&since=");
      url.push_str(&urlencoding::encode(s));
    }
    let resp = self
      .http
      .get(&url)
      .bearer_auth(&self.device_token)
      .timeout(LONGPOLL_REQUEST_TIMEOUT)
      .send()
      .await?;
    Self::json_or_err(resp).await
  }

  /// `GET /file/{path}`. `Ok(None)` when the hub has no such file (the
  /// path was deleted, or never existed).
  pub async fn get_file(&self, path: &str) -> Result<Option<FileContent>, HubError> {
    let url = format!(
      "{}/file/{}",
      self.base_url.trim_end_matches('/'),
      encode_path(path)
    );
    let resp = self
      .http
      .get(&url)
      .bearer_auth(&self.device_token)
      .send()
      .await?;

    if resp.status() == reqwest::StatusCode::NOT_FOUND {
      return Ok(None);
    }
    if !resp.status().is_success() {
      return Err(Self::api_err(resp).await);
    }

    let rev = Self::header(&resp, "x-file-rev")?;
    let mtime = Self::header(&resp, "x-file-mtime")?
      .parse::<i64>()
      .map_err(|_| HubError::Decode("non-numeric x-file-mtime".into()))?;
    let content_type = Self::header(&resp, "content-type")
      .unwrap_or_else(|_| "application/octet-stream".to_string());
    let content = resp.bytes().await?.to_vec();

    Ok(Some(FileContent {
      rev,
      mtime,
      content_type,
      content,
    }))
  }

  /// `POST /changes` - push a batch of local changes. Returns one result
  /// per input, in order.
  pub async fn push(&self, changes: &[PushChange]) -> Result<Vec<PushResult>, HubError> {
    let url = format!("{}/changes", self.base_url.trim_end_matches('/'));
    let resp = self
      .http
      .post(&url)
      .bearer_auth(&self.device_token)
      .json(changes)
      .send()
      .await?;
    Self::json_or_err(resp).await
  }

  fn header(resp: &reqwest::Response, name: &str) -> Result<String, HubError> {
    resp
      .headers()
      .get(name)
      .and_then(|v| v.to_str().ok())
      .map(String::from)
      .ok_or_else(|| HubError::Decode(format!("missing {name} header")))
  }

  async fn api_err(resp: reqwest::Response) -> HubError {
    let status = resp.status().as_u16();
    let body = resp.text().await.unwrap_or_default();
    HubError::Api { status, body }
  }

  async fn json_or_err<T: for<'de> serde::Deserialize<'de>>(
    resp: reqwest::Response,
  ) -> Result<T, HubError> {
    if !resp.status().is_success() {
      return Err(Self::api_err(resp).await);
    }
    let text = resp.text().await?;
    serde_json::from_str(&text).map_err(|e| HubError::Decode(format!("{e}: {text}")))
  }
}

/// Percent-encodes each path segment but preserves `/` separators, so a file
/// path like `notes/a.txt` stays a sub-path while spaces/special chars are
/// encoded. (The hub's wildcard route then decodes each segment back.)
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
    HubClient::new(server.uri(), "dev-token")
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
