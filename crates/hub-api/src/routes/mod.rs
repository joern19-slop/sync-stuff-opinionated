mod changes;
mod file;
mod push;

use axum::{middleware, routing::get, Router};
use std::sync::Arc;

use crate::{auth::require_device_token, state::AppState};

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/changes",
            get(changes::get_changes).post(push::post_changes),
        )
        .route("/changes/longpoll", get(changes::get_changes_longpoll))
        .route("/file/*path", get(file::get_file))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_device_token,
        ))
        .with_state(state)
}
