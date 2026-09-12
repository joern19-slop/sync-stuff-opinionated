use axum::{
  Json,
  http::StatusCode,
  response::{IntoResponse, Response},
};
use serde_json::json;
use thiserror::Error;

/// Errors from talking to CouchDB. Every hub route funnels these through
/// [`ApiError`] so the HTTP surface stays consistent.
#[derive(Debug, Error)]
pub enum CouchError {
  #[error("http transport error: {0}")]
  Transport(#[from] reqwest::Error),

  #[error("couchdb returned {status}: {body}")]
  Api { status: u16, body: String },

  #[error("invalid url: {0}")]
  BadUrl(String),

  #[error("revision conflict writing {0}")]
  RevConflict(String),

  #[error("unexpected response shape: {0}")]
  Decode(String),
}

pub struct ApiError(pub StatusCode, pub String);

impl From<CouchError> for ApiError {
  fn from(e: CouchError) -> Self {
    match &e {
      CouchError::RevConflict(_) => ApiError(StatusCode::CONFLICT, e.to_string()),
      CouchError::Transport(_)
      | CouchError::Api { .. }
      | CouchError::Decode(_)
      | CouchError::BadUrl(_) => ApiError(StatusCode::BAD_GATEWAY, e.to_string()),
    }
  }
}

impl IntoResponse for ApiError {
  fn into_response(self) -> Response {
    (self.0, Json(json!({ "error": self.1 }))).into_response()
  }
}
