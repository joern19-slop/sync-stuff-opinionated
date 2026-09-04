use axum::{
  extract::{Request, State},
  http::StatusCode,
  middleware::Next,
  response::Response,
};
use std::sync::Arc;

use crate::state::AppState;

/// Shared-token auth: every request must carry `Authorization: Bearer
/// <token>` where `<token>` is one of the hub's configured device tokens.
/// Intentionally simple for this stage - see `Config::device_tokens` for
/// the provisioning model.
pub async fn require_device_token(
  State(state): State<Arc<AppState>>,
  req: Request,
  next: Next,
) -> Result<Response, StatusCode> {
  let token = req
    .headers()
    .get(axum::http::header::AUTHORIZATION)
    .and_then(|v| v.to_str().ok())
    .and_then(|v| v.strip_prefix("Bearer "));

  match token {
    Some(t) if state.device_tokens.contains(t) => Ok(next.run(req).await),
    _ => Err(StatusCode::UNAUTHORIZED),
  }
}
