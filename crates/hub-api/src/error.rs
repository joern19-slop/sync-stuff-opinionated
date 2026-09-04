use axum::{
  http::StatusCode,
  response::{IntoResponse, Response},
  Json,
};
use serde_json::json;
use sync_core::CouchError;

/// Every route error funnels through here so the HTTP surface is
/// consistent even as the CouchDB-facing error cases grow.
pub struct ApiError(pub StatusCode, pub String);

impl From<CouchError> for ApiError {
  fn from(e: CouchError) -> Self {
    match &e {
      CouchError::RevConflict(_) => ApiError(StatusCode::CONFLICT, e.to_string()),
      CouchError::Transport(_) | CouchError::Api { .. } | CouchError::Decode(_) => {
        ApiError(StatusCode::BAD_GATEWAY, e.to_string())
      }
    }
  }
}

impl IntoResponse for ApiError {
  fn into_response(self) -> Response {
    (self.0, Json(json!({ "error": self.1 }))).into_response()
  }
}
