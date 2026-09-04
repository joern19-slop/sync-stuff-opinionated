//! Minimal CouchDB HTTP client.
//!
//! This is intentionally narrow: only what the hub API and (eventually) the
//! conflict resolver need. It knows nothing about "files" - `put_doc` /
//! `put_attachment` take arbitrary JSON and bytes. Mapping "file path" <->
//! "CouchDB doc id" happens one layer up, in `hub-api`.

use crate::error::CouchError;
use bytes::Bytes;
use reqwest::{Client, Method, Response, StatusCode};
use serde::Deserialize;
use serde_json::json;

#[derive(Clone)]
pub struct CouchClient {
    http: Client,
    base_url: String,
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
    /// CouchDB's `seq` shape varies by version/config (string, or
    /// `[number, string]`); treat it as opaque JSON and pass it straight
    /// through as the API checkpoint.
    pub last_seq: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct RawChangeRow {
    pub id: String,
    #[serde(default)]
    pub deleted: bool,
    pub changes: Vec<RawRev>,
    /// Present when the feed was requested with `include_docs=true`. The
    /// winning revision's body (with `_conflicts` when the doc is conflicted
    /// and `conflicts=true` was requested).
    #[serde(default)]
    pub doc: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct RawRev {
    pub rev: String,
}

/// CouchDB's `_revisions` shape (`GET /db/doc?revs=true`): the winning (or
/// requested) branch's history, newest first. `ids[i]` is generation
/// `start - i`.
#[derive(Debug, Clone, Deserialize)]
pub struct Revisions {
    pub start: u64,
    pub ids: Vec<String>,
}

/// One entry from CouchDB's node-level `GET /_scheduler/jobs`, describing a
/// replication the node is currently managing (including continuous
/// hub-to-hub replications). Used by the hub's replication-health checker.
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
    /// Non-empty when the replication last errored.
    #[serde(default)]
    pub error: Option<String>,
    /// RFC3339 timestamp of the last replication activity, if any.
    #[serde(default)]
    pub last_updated: Option<String>,
}

impl CouchClient {
    pub fn new(
        base_url: impl Into<String>,
        db: impl Into<String>,
        user: impl Into<String>,
        pass: impl Into<String>,
    ) -> Self {
        Self {
            http: Client::new(),
            base_url: base_url.into(),
            db: db.into(),
            user: user.into(),
            pass: pass.into(),
        }
    }

    fn db_url(&self) -> String {
        format!("{}/{}", self.base_url.trim_end_matches('/'), self.db)
    }

    /// Percent-encode the whole document id (including any `/`) so a file
    /// path like `notes/a.txt` addresses one CouchDB doc, not a sub-path.
    fn doc_url(&self, id: &str) -> String {
        format!("{}/{}", self.db_url(), urlencoding::encode(id))
    }

    fn req(&self, method: Method, url: &str) -> reqwest::RequestBuilder {
        self.http
            .request(method, url)
            .basic_auth(&self.user, Some(&self.pass))
    }

    /// Idempotent: succeeds whether or not the db already existed.
    pub async fn ensure_db(&self) -> Result<(), CouchError> {
        let resp = self.req(Method::PUT, &self.db_url()).send().await?;
        match resp.status() {
            StatusCode::CREATED | StatusCode::PRECONDITION_FAILED => Ok(()),
            status => Err(Self::api_err(status, resp).await),
        }
    }

    /// Node-level replication jobs via `GET /_scheduler/jobs`. Note this is
    /// *not* scoped to `self.db` - the scheduler is a node-wide concept.
    pub async fn replication_jobs(&self) -> Result<Vec<SchedulerJob>, CouchError> {
        let url = format!("{}/_scheduler/jobs", self.base_url.trim_end_matches('/'));
        let resp = self.req(Method::GET, &url).send().await?;
        #[derive(Deserialize)]
        struct JobsResponse {
            #[serde(default)]
            jobs: Vec<SchedulerJob>,
        }
        let body: JobsResponse = Self::json_or_err(resp).await?;
        Ok(body.jobs)
    }

    /// Wraps `GET /{db}/_changes`.
    pub async fn changes(&self, since: Option<&str>) -> Result<RawChangesResponse, CouchError> {
        self.changes_inner(since, false).await
    }

    /// Like `changes`, but with `include_docs=true&conflicts=true` so each
    /// row carries the winning doc body (and `_conflicts` when present).
    /// Used by the conflict-detecting change watcher.
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
        let mut url = format!("{}/_changes?style=all_docs", self.db_url());
        if with_docs {
            url.push_str("&include_docs=true&conflicts=true");
        }
        if let Some(s) = since {
            url.push_str("&since=");
            url.push_str(&urlencoding::encode(s));
        }
        let resp = self.req(Method::GET, &url).send().await?;
        Self::json_or_err(resp).await
    }

    /// Current winning revision's JSON body, or `None` if the doc doesn't
    /// exist (never existed, or its winning revision is a tombstone).
    /// Requested with `conflicts=true` so a conflicted doc carries its
    /// `_conflicts` list; callers that don't care can ignore it.
    pub async fn get_doc(&self, id: &str) -> Result<Option<serde_json::Value>, CouchError> {
        let url = format!("{}?conflicts=true", self.doc_url(id));
        let resp = self.req(Method::GET, &url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::json_or_err(resp).await?))
    }

    /// A specific revision's body (including `_revisions` history and
    /// `_deleted` flag) via `GET /db/doc?rev=..&revs=true`. `None` if that
    /// revision (or the doc) doesn't exist. Used by the conflict resolver to
    /// read losing leaves and their shared ancestor.
    pub async fn get_doc_at_rev(
        &self,
        id: &str,
        rev: &str,
    ) -> Result<Option<serde_json::Value>, CouchError> {
        let url = format!(
            "{}?rev={}&revs=true",
            self.doc_url(id),
            urlencoding::encode(rev)
        );
        let resp = self.req(Method::GET, &url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::json_or_err(resp).await?))
    }

    pub async fn get_attachment(&self, id: &str, attachment: &str) -> Result<Bytes, CouchError> {
        self.get_attachment_at_rev(id, attachment, None).await
    }

    /// Fetches an attachment, optionally at a specific revision. The
    /// conflict resolver needs `rev` to read a losing leaf's or the common
    /// ancestor's content; the plain `get_attachment` call reads the winning
    /// revision's content.
    pub async fn get_attachment_at_rev(
        &self,
        id: &str,
        attachment: &str,
        rev: Option<&str>,
    ) -> Result<Bytes, CouchError> {
        let mut url = format!("{}/{}", self.doc_url(id), attachment);
        if let Some(r) = rev {
            url.push_str("?rev=");
            url.push_str(&urlencoding::encode(r));
        }
        let resp = self.req(Method::GET, &url).send().await?;
        if !resp.status().is_success() {
            return Err(Self::api_err(resp.status(), resp).await);
        }
        Ok(resp.bytes().await?)
    }

    /// All leaf revisions via `GET /db/doc?open_revs=all&revs=true`, returned
    /// as individual docs (with `_revisions` history and `_deleted` flag).
    /// This is the only way to see a *losing* edit leaf when a deletion is
    /// the winning revision - the normal `get_doc` returns 404 in that case
    /// and the `_changes` feed reports it as a plain `deleted`.
    pub async fn get_doc_leaves(&self, id: &str) -> Result<Vec<serde_json::Value>, CouchError> {
        let url = format!("{}?open_revs=all&revs=true", self.doc_url(id));
        let resp = self.req(Method::GET, &url).send().await?;
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

    /// Create/update a doc's JSON body. `rev` is the revision this write is
    /// conditioned on; `None` means "must not already exist". Returns
    /// `Err(CouchError::RevConflict)` on a 409, which the caller (hub-api /
    /// conflict resolver) turns into a retry-or-report decision - this
    /// client never retries on its own.
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
            .req(Method::PUT, &self.doc_url(id))
            .json(&body)
            .send()
            .await?;
        Self::put_result(resp, id).await
    }

    /// Upload the file's bytes as the doc's `content` attachment. This is a
    /// second HTTP call after `put_doc`, not a single multipart write - kept
    /// simple for Stage 2. Both calls are CAS-conditioned so a crash between
    /// them just looks like "doc updated, attachment stale", which a client
    /// retry (with the doc's now-current rev) fixes.
    pub async fn put_attachment(
        &self,
        id: &str,
        rev: &str,
        content_type: &str,
        bytes: Bytes,
    ) -> Result<PutResult, CouchError> {
        let url = format!(
            "{}/content?rev={}",
            self.doc_url(id),
            urlencoding::encode(rev)
        );
        let resp = self
            .req(Method::PUT, &url)
            .header("Content-Type", content_type)
            .body(bytes)
            .send()
            .await?;
        Self::put_result(resp, id).await
    }

    pub async fn delete_doc(&self, id: &str, rev: &str) -> Result<PutResult, CouchError> {
        let url = format!("{}?rev={}", self.doc_url(id), urlencoding::encode(rev));
        let resp = self.req(Method::DELETE, &url).send().await?;
        Self::put_result(resp, id).await
    }

    /// Inserts an arbitrary revision into the tree via `_bulk_docs` with
    /// `new_edits:false`. The doc must already carry `_id`, `_rev`,
    /// `_revisions`, and (for tombstones) `_deleted`; CouchDB trusts those
    /// fields verbatim instead of deriving them from the body. Used by the
    /// hub to *branch* the tree when a client pushes a stale `base_rev`.
    ///
    /// A per-doc `conflict` result means that exact revision already exists,
    /// which the caller treats as "branch already present" (idempotent retry).
    pub async fn put_revision(&self, doc: serde_json::Value) -> Result<(), CouchError> {
        let url = format!("{}/_bulk_docs", self.db_url());
        let resp = self
            .req(Method::POST, &url)
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

/// Parses an `open_revs=all` response into individual leaf docs. CouchDB
/// returns these as `multipart/mixed` (one JSON body per leaf); older/other
/// setups may return a plain JSON array instead.
fn parse_open_revs(content_type: &str, body: &[u8]) -> Vec<serde_json::Value> {
    if content_type.contains("multipart") {
        let mut out = Vec::new();
        // Each part's JSON body is emitted by CouchDB as a single line that
        // starts with `{`; boundary (`--...`) and header (`Key: value`) lines
        // never do.
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
        CouchClient::new(server.uri(), "filesync", "hub", "hub-pass")
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
