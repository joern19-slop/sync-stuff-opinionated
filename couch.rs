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
}

#[derive(Debug, Deserialize)]
pub struct RawRev {
    pub rev: String,
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

    /// Wraps `GET /{db}/_changes`.
    pub async fn changes(&self, since: Option<&str>) -> Result<RawChangesResponse, CouchError> {
        let mut url = format!("{}/_changes?style=all_docs", self.db_url());
        if let Some(s) = since {
            url.push_str("&since=");
            url.push_str(&urlencoding::encode(s));
        }
        let resp = self.req(Method::GET, &url).send().await?;
        Self::json_or_err(resp).await
    }

    /// Current winning revision's JSON body, or `None` if the doc doesn't
    /// exist (never existed, or its winning revision is a tombstone).
    pub async fn get_doc(&self, id: &str) -> Result<Option<serde_json::Value>, CouchError> {
        let resp = self.req(Method::GET, &self.doc_url(id)).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(Self::json_or_err(resp).await?))
    }

    /// All conflicting leaf revisions via `open_revs=all`. Empty if the doc
    /// doesn't exist. Used by the (future) conflict resolver; exposed here
    /// now so Stage 5 doesn't need further changes to this client.
    pub async fn get_doc_all_revs(&self, id: &str) -> Result<Vec<serde_json::Value>, CouchError> {
        let url = format!("{}?open_revs=all", self.doc_url(id));
        let resp = self.req(Method::GET, &url).send().await?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(vec![]);
        }
        let raw: Vec<serde_json::Value> = Self::json_or_err(resp).await?;
        Ok(raw.into_iter().filter_map(|v| v.get("ok").cloned()).collect())
    }

    pub async fn get_attachment(&self, id: &str, attachment: &str) -> Result<Bytes, CouchError> {
        let url = format!("{}/{}", self.doc_url(id), attachment);
        let resp = self.req(Method::GET, &url).send().await?;
        if !resp.status().is_success() {
            return Err(Self::api_err(resp.status(), resp).await);
        }
        Ok(resp.bytes().await?)
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

    async fn put_result(resp: Response, id: &str) -> Result<PutResult, CouchError> {
        match resp.status() {
            StatusCode::OK | StatusCode::CREATED => Self::json_or_err(resp).await,
            StatusCode::CONFLICT => Err(CouchError::RevConflict(id.to_string())),
            status => Err(Self::api_err(status, resp).await),
        }
    }

    async fn api_err(status: StatusCode, resp: Response) -> CouchError {
        let body = resp.text().await.unwrap_or_default();
        CouchError::Api { status: status.as_u16(), body }
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

        assert!(client(&server).await.get_doc("missing.txt").await.unwrap().is_none());
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
}
