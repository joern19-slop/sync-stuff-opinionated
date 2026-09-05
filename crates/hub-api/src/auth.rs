use axum::{
  extract::{Request, State},
  http::StatusCode,
  middleware::Next,
  response::Response,
};
use axum_extra::{
  headers::{authorization::Bearer, Authorization},
  TypedHeader,
};
use std::sync::Arc;

use crate::state::AppState;

pub async fn require_device_token(
  State(state): State<Arc<AppState>>,
  TypedHeader(token_header): TypedHeader<Authorization<Bearer>>,
  req: Request,
  next: Next,
) -> Result<Response, StatusCode> {
  match state.device_tokens.contains(token_header.token()) {
    true => Ok(next.run(req).await),
    false => Err(StatusCode::UNAUTHORIZED),
  }
}
