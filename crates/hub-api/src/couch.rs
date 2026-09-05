//! Minimal CouchDB HTTP client: only what the hub and conflict resolver
//! need. Takes arbitrary JSON/bytes; file-path <-> doc-id mapping lives in
//! `hub-api`.

use crate::error::CouchError;
use bytes::Bytes;
use reqwest::{Client, Method, Response, StatusCode};
use serde::Deserialize;
use serde_json::json;
use url::Url;

#[derive(Clone)]
pub struct CouchClient {
  http: Client,
  base: Url,
  db: String,
  user: String,
  pass: String,
}

#[derive(Debug, Deserialize)]
pub struct PutResult {
  pub id: String,
  pub rev: String,
}

#[derive(Debug, Deserialize)]
pub struct RawChangesResponse {
  pub results: Vec<RawChangeRow>,
  /// CouchDB's `seq` shape varies by version/config - opaque, passed through.
  pub last_seq: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct RawChangeRow {
  pub id: String,
  #[serde(default)]
  pub deleted: bool,
  pub changes: Vec<RawRev>,
  /// The winning revision's body (`include_docs`/`conflicts` as requested).
  #[serde(default)]
  pub doc: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct RawRev {
  pub rev: String,
}

/// CouchDB `_revisions` (`revs=true`), newest first; `ids[i]` is generation
/// `start - i`.
#[derive(Debug, Clone, Deserialize)]
pub struct Revisions {
  pub start: u64,
  pub ids: Vec<String>,
}

/// One node-level `/_scheduler/jobs` entry - e.g. a continuous hub-to-hub
/// replication the health checker watches.
#[derive(Debug, Deserialize)]
pub struct SchedulerJob {
  pub id: String,
  pub source: String,
  pub target: String,
  #[serde(default)]
  pub info: SchedulerJobInfo,
}

#[derive(Debug, Default, Deserialize)]
pub struct SchedulerJobInfo {
  #[serde(default)]
  pub error: Option<String>,
  #[serde(default)]
  pub last_updated: Option<String>,
}

impl CouchClient {
  pub fn new(
    base_url: impl Into<String>,
    db: impl Into<String>,
    user: impl Into<String>,
    pass: impl Into<String>,
  ) -> Result<Self, CouchError> {
    let base = Url::parse(&base_url.into()).map_err(|e| CouchError::BadUrl(e.to_string()))?;
    Ok(Self {
      http: common::http_client(),
      base,
      db: db.into(),
      user: user.into(),
      pass: pass.into(),
    })
  }

  fn db_url(&self) -> Result<Url, CouchError> {
    self.append(&self.base, &[&self.db])
  }

  /// Doc id (may contain `/`) as one percent-encoded segment, so a path
  /// like `notes/a.txt` is one doc, not a sub-path.
  fn doc_url(&self, id: &str) -> Result<Url, CouchError> {
    self.append(&self.db_url()?, &[id])
  }

  fn append(&self, url: &Url, segments: &[&str]) -> Result<Url, CouchError> {
    let mut url = url.clone();
    {
      let mut path = url
        .path_segments_mut()
        .map_err(|_| CouchError::BadUrl("url cannot be a base".to_string()))?;
      for segment in segments {
        path.push(segment);
      }
    }
    Ok(url)
  }

  fn req(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
    self
      .http
      .request(method, url)
      .basic_auth(&self.user, Some(&self.pass))
  }

  /// Idempotent.
  pub async fn ensure_db(&self) -> Result<(), CouchError> {
    let resp = self.req(Method::PUT, self.db_url()?).send().await?;
    match resp.status() {
      StatusCode::CREATED | StatusCode::PRECONDITION_FAILED => Ok(()),
      status => Err(Self::api_err(status, resp).await),
    }
  }

  /// Node-wide (`/_scheduler/jobs`), not scoped to `self.db`.
  pub async fn replication_jobs(&self) -> Result<Vec<SchedulerJob>, CouchError> {
    let url = self.append(&self.base, &["_scheduler", "jobs"])?;
    let resp = self.req(Method::GET, url).send().await?;
    #[derive(Deserialize)]
    struct JobsResponse {
      #[serde(default)]
      jobs: Vec<SchedulerJob>,
    }
    let body: JobsResponse = Self::json_or_err(resp).await?;
    Ok(body.jobs)
  }

  pub async fn changes(&self, since: Option<&str>) -> Result<RawChangesResponse, CouchError> {
    self.changes_inner(since, false).await
  }

  pub async fn changes_longpoll(
    &self,
    since: Option<&str>,
    timeout_secs: u64,
  ) -> Result<RawChangesResponse, CouchError> {
    let mut url = self.append(&self.db_url()?, &["_changes"])?;
    {
      let mut query = url.query_pairs_mut();
      query.append_pair("style", "all_docs");
      query.append_pair("feed", "longpoll");
      query.append_pair("timeout", &timeout_secs.to_string());
      if let Some(s) = since {
        query.append_pair("since", s);
      }
    }
    let resp = self.req(Method::GET, url).send().await?;
    Self::json_or_err(resp).await
  }

  /// With `include_docs=true&conflicts=true` so rows carry `_conflicts`.
  pub async fn changes_with_docs(
    &self,
    since: Option<&str>,
  ) -> Result<RawChangesResponse, CouchError> {
    self.changes_inner(since, true).await
  }

  async fn changes_inner(
    &self,
    since: Option<&str>,
    with_docs: bool,
  ) -> Result<RawChangesResponse, CouchError> {
    let mut url = self.append(&self.db_url()?, &["_changes"])?;
    {
      let mut query = url.query_pairs_mut();
      query.append_pair("style", "all_docs");
      if with_docs {
        query.append_pair("include_docs", "true");
        query.append_pair("conflicts", "true");
      }
      if let Some(s) = since {
        query.append_pair("since", s);
      }
    }
    let resp = self.req(Method::GET, url).send().await?;
    Self::json_or_err(resp).await
  }

  /// The winning revision's body, or `None` if the doc is absent or a
  /// tombstone.
  pub async fn get_doc(&self, id: &str) -> Result<Option<serde_json::Value>, CouchError> {
    let mut url = self.doc_url(id)?;
    url.query_pairs_mut().append_pair("conflicts", "true");
    let resp = self.req(Method::GET, url).send().await?;
    if resp.status() == StatusCode::NOT_FOUND {
      return Ok(None);
    }
    Ok(Some(Self::json_or_err(resp).await?))
  }

  /// A specific revision's body with `_revisions` history; `None` if absent.
  /// Used to read losing leaves and their shared ancestor.
  pub async fn get_doc_at_rev(
    &self,
    id: &str,
    rev: &str,
  ) -> Result<Option<serde_json::Value>, CouchError> {
    let mut url = self.doc_url(id)?;
    {
      let mut query = url.query_pairs_mut();
      query.append_pair("rev", rev);
      query.append_pair("revs", "true");
    }
    let resp = self.req(Method::GET, url).send().await?;
    if resp.status() == StatusCode::NOT_FOUND {
      return Ok(None);
    }
    Ok(Some(Self::json_or_err(resp).await?))
  }

  pub async fn get_attachment(&self, id: &str, attachment: &str) -> Result<Bytes, CouchError> {
    self.get_attachment_at_rev(id, attachment, None).await
  }

  /// `rev` reads a losing leaf's or the shared ancestor's content (the
  /// resolver's need); without it, the winning revision's.
  pub async fn get_attachment_at_rev(
    &self,
    id: &str,
    attachment: &str,
    rev: Option<&str>,
  ) -> Result<Bytes, CouchError> {
    let mut url = self.append(&self.doc_url(id)?, &[attachment])?;
    if let Some(r) = rev {
      url.query_pairs_mut().append_pair("rev", r);
    }
    let resp = self.req(Method::GET, url).send().await?;
    if !resp.status().is_success() {
      return Err(Self::api_err(resp.status(), resp).await);
    }
    Ok(resp.bytes().await?)
  }

  /// All leaf revisions (`open_revs=all`). The only way to see a losing
  /// edit leaf when a deletion won - `get_doc` 404s and `_changes` reports
  /// it as a plain `deleted`.
  pub async fn get_doc_leaves(&self, id: &str) -> Result<Vec<serde_json::Value>, CouchError> {
    let mut url = self.doc_url(id)?;
    {
      let mut query = url.query_pairs_mut();
      query.append_pair("open_revs", "all");
      query.append_pair("revs", "true");
    }
    let resp = self.req(Method::GET, url).send().await?;
    if resp.status() == StatusCode::NOT_FOUND {
      return Ok(vec![]);
    }
    if !resp.status().is_success() {
      return Err(Self::api_err(resp.status(), resp).await);
    }

    let content_type = resp
      .headers()
      .get("content-type")
      .and_then(|v| v.to_str().ok())
      .unwrap_or_default()
      .to_string();
    let body = resp.bytes().await?;
    Ok(parse_open_revs(&content_type, &body))
  }

  /// CAS write: `rev` is the revision the write is conditioned on; `None` =
  /// must not already exist. A 409 surfaces as `RevConflict` for the caller
  /// to resolve; this client never retries.
  pub async fn put_doc(
    &self,
    id: &str,
    rev: Option<&str>,
    mut body: serde_json::Value,
  ) -> Result<PutResult, CouchError> {
    if let Some(r) = rev {
      body["_rev"] = json!(r);
    }
    let resp = self
      .req(Method::PUT, self.doc_url(id)?)
      .json(&body)
      .send()
      .await?;
    Self::put_result(resp, id).await
  }

  /// A second call after `put_doc` (not one multipart write). Both are
  /// CAS-conditioned, so a crash between them just looks like a stale
  /// attachment a retry with the current rev fixes.
  pub async fn put_attachment(
    &self,
    id: &str,
    rev: &str,
    content_type: &str,
    bytes: Bytes,
  ) -> Result<PutResult, CouchError> {
    let mut url = self.append(&self.doc_url(id)?, &["content"])?;
    url.query_pairs_mut().append_pair("rev", rev);
    let resp = self
      .req(Method::PUT, url)
      .header("Content-Type", content_type)
      .body(bytes)
      .send()
      .await?;
    Self::put_result(resp, id).await
  }

  pub async fn delete_doc(&self, id: &str, rev: &str) -> Result<PutResult, CouchError> {
    let mut url = self.doc_url(id)?;
    url.query_pairs_mut().append_pair("rev", rev);
    let resp = self.req(Method::DELETE, url).send().await?;
    Self::put_result(resp, id).await
  }

  /// Inserts a revision verbatim (`_bulk_docs`, `new_edits:false`) - CouchDB
  /// trusts the carried `_id`/`_rev`/`_revisions` instead of deriving them -
  /// to *branch* the tree on a stale `base_rev` push. A per-doc `conflict`
  /// means the branch is already present (idempotent).
  pub async fn put_revision(&self, doc: serde_json::Value) -> Result<(), CouchError> {
    let url = self.append(&self.db_url()?, &["_bulk_docs"])?;
    let resp = self
      .req(Method::POST, url)
      .json(&json!({ "new_edits": false, "docs": [doc] }))
      .send()
      .await?;

    #[derive(Deserialize)]
    struct BulkDocResult {
      #[serde(default)]
      error: Option<String>,
      #[serde(default)]
      reason: Option<String>,
    }

    if !resp.status().is_success() {
      return Err(Self::api_err(resp.status(), resp).await);
    }
    let results: Vec<BulkDocResult> = Self::json_or_err(resp).await?;
    for r in results {
      match r.error.as_deref() {
        None | Some("conflict") => {}
        Some(other) => {
          return Err(CouchError::Api {
            status: 409,
            body: r.reason.unwrap_or_else(|| other.to_string()),
          });
        }
      }
    }
    Ok(())
  }

  async fn put_result(resp: Response, id: &str) -> Result<PutResult, CouchError> {
    match resp.status() {
      StatusCode::OK | StatusCode::CREATED => Self::json_or_err(resp).await,
      StatusCode::CONFLICT => Err(CouchError::RevConflict(id.to_string())),
      status => Err(Self::api_err(status, resp).await),
    }
  }

  async fn api_err(status: StatusCode, resp: Response) -> CouchError {
    let body = resp.text().await.unwrap_or_default();
    CouchError::Api {
      status: status.as_u16(),
      body,
    }
  }

  async fn json_or_err<T: for<'de> Deserialize<'de>>(resp: Response) -> Result<T, CouchError> {
    let status = resp.status();
    if !status.is_success() {
      return Err(Self::api_err(status, resp).await);
    }
    let text = resp.text().await?;
    serde_json::from_str(&text).map_err(|e| CouchError::Decode(format!("{e}: {text}")))
  }
}

/// Parses `open_revs=all`: CouchDB emits `multipart/mixed` (a JSON body per
/// leaf); some setups return a plain JSON array instead.
fn parse_open_revs(content_type: &str, body: &[u8]) -> Vec<serde_json::Value> {
  if content_type.contains("multipart") {
    let mut out = Vec::new();
    // A leaf's JSON body arrives as a single line starting with `{`.
    for line in body.split(|&b| b == b'\n') {
      let line = std::str::from_utf8(line).unwrap_or("");
      let line = line.trim();
      if line.starts_with('{') {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(line) {
          out.push(v);
        }
      }
    }
    out
  } else {
    serde_json::from_slice::<Vec<serde_json::Value>>(body).unwrap_or_default()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use wiremock::matchers::{basic_auth, method, path, query_param};
  use wiremock::{Mock, MockServer, ResponseTemplate};

  async fn client(server: &MockServer) -> CouchClient {
    CouchClient::new(server.uri(), "filesync", "hub", "hub-pass").unwrap()
  }

  #[tokio::test]
  async fn ensure_db_treats_already_exists_as_success() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
      .and(path("/filesync"))
      .and(basic_auth("hub", "hub-pass"))
      .respond_with(ResponseTemplate::new(412))
      .mount(&server)
      .await;

    client(&server).await.ensure_db().await.unwrap();
  }

  #[tokio::test]
  async fn doc_url_percent_encodes_slashes_in_path_ids() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/filesync/notes%2Fa.txt"))
      .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
          "_id": "notes/a.txt",
          "_rev": "1-abc"
      })))
      .mount(&server)
      .await;

    let doc = client(&server)
      .await
      .get_doc("notes/a.txt")
      .await
      .unwrap()
      .expect("doc present");
    assert_eq!(doc["_rev"], "1-abc");
  }

  #[tokio::test]
  async fn get_doc_returns_none_on_404() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/filesync/missing.txt"))
      .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
          "error": "not_found", "reason": "missing"
      })))
      .mount(&server)
      .await;

    assert!(client(&server)
      .await
      .get_doc("missing.txt")
      .await
      .unwrap()
      .is_none());
  }

  #[tokio::test]
  async fn put_doc_maps_409_to_rev_conflict() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
      .and(path("/filesync/a.txt"))
      .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
          "error": "conflict", "reason": "Document update conflict."
      })))
      .mount(&server)
      .await;

    let err = client(&server)
      .await
      .put_doc("a.txt", Some("1-stale"), serde_json::json!({}))
      .await
      .unwrap_err();
    assert!(matches!(err, CouchError::RevConflict(id) if id == "a.txt"));
  }

  #[tokio::test]
  async fn replication_jobs_parses_scheduler_payload() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/_scheduler/jobs"))
      .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
          "jobs": [
              { "id": "a-to-b", "source": "filesync", "target": "http://node-b:5984/filesync",
                "info": { "error": "timeout", "last_updated": "2026-09-03T10:00:00Z" } },
              { "id": "b-to-a", "source": "filesync", "target": "http://node-a:5984/filesync",
                "info": { "last_updated": "2026-09-03T10:00:01Z" } }
          ],
          "total_rows": 2
      })))
      .mount(&server)
      .await;

    let jobs = client(&server).await.replication_jobs().await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].info.error.as_deref(), Some("timeout"));
    assert!(jobs[1].info.error.is_none());
  }

  #[tokio::test]
  async fn changes_passes_since_through_as_query_param() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/filesync/_changes"))
      .and(query_param("since", "42-abc"))
      .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
          "results": [{"id": "a.txt", "deleted": false, "changes": [{"rev": "1-x"}]}],
          "last_seq": "43-def"
      })))
      .mount(&server)
      .await;

    let resp = client(&server).await.changes(Some("42-abc")).await.unwrap();
    assert_eq!(resp.results.len(), 1);
    assert_eq!(resp.results[0].id, "a.txt");
    assert_eq!(resp.last_seq, serde_json::json!("43-def"));
  }

  #[tokio::test]
  async fn changes_longpoll_sends_feed_and_timeout() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
      .and(path("/filesync/_changes"))
      .and(query_param("feed", "longpoll"))
      .and(query_param("timeout", "25"))
      .and(query_param("since", "42-abc"))
      .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
          "results": [],
          "last_seq": "42-abc"
      })))
      .mount(&server)
      .await;

    let resp = client(&server)
      .await
      .changes_longpoll(Some("42-abc"), 25)
      .await
      .unwrap();
    assert!(resp.results.is_empty());
    assert_eq!(resp.last_seq, serde_json::json!("42-abc"));
  }

  #[test]
  fn parse_open_revs_extracts_multipart_leaf_docs() {
    let content_type = "multipart/mixed; boundary=\"abc123\"";
    let body = b"--abc123\r\nContent-Type: application/json\r\n\r\n{\"_id\":\"n\",\"_rev\":\"2-b\"}\r\n--abc123\r\nContent-Type: application/json\r\n\r\n{\"_id\":\"n\",\"_rev\":\"2-a\"}\r\n--abc123--\r\n";
    let leaves = parse_open_revs(content_type, body);
    assert_eq!(leaves.len(), 2);
    assert_eq!(leaves[0]["_rev"], "2-b");
    assert_eq!(leaves[1]["_rev"], "2-a");
  }

  #[test]
  fn parse_open_revs_accepts_json_array_fallback() {
    let content_type = "application/json";
    let body = b"[{\"_rev\":\"1-a\"},{\"_rev\":\"2-b\"}]";
    let leaves = parse_open_revs(content_type, body);
    assert_eq!(leaves.len(), 2);
  }
}
