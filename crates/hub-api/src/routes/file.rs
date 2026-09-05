use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use std::sync::Arc;

use crate::{error::ApiError, state::AppState};

/// `GET /file/{path}` - streams the file's CouchDB attachment; `X-File-Rev`
/// / `X-File-Mtime` carry its revision and edit time without a second trip.
pub async fn get_file(
  State(state): State<Arc<AppState>>,
  Path(path): Path<String>,
) -> Result<Response, ApiError> {
  let doc = state
    .couch
    .get_doc(&path)
    .await?
    .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("no such file: {path}")))?;

  let rev = doc["_rev"].as_str().unwrap_or_default().to_string();
  let mtime = doc["mtime"].as_i64().unwrap_or_default();
  let content_type = doc["content_type"]
    .as_str()
    .unwrap_or("application/octet-stream")
    .to_string();

  let bytes: bytes::Bytes = state.couch.get_attachment(&path, "content").await?;

  let mut headers = HeaderMap::new();
  headers.insert(
    header::CONTENT_TYPE,
    HeaderValue::from_str(&content_type)
      .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
  );
  headers.insert(
    "x-file-rev",
    HeaderValue::from_str(&rev).unwrap_or_else(|_| HeaderValue::from_static("")),
  );
  headers.insert(
    "x-file-mtime",
    HeaderValue::from_str(&mtime.to_string()).unwrap_or_else(|_| HeaderValue::from_static("0")),
  );

  Ok((headers, bytes).into_response())
}
