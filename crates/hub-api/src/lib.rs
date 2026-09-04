pub mod auth;
pub mod config;
pub mod error;
pub mod routes;
pub mod state;

use std::sync::Arc;
use sync_core::CouchClient;

use config::Config;
use state::AppState;

/// Build the full axum app from config: wires up the CouchDB client,
/// ensures the target database exists, and assembles the router. Shared by
/// `main.rs` and the e2e tests so there's exactly one startup path.
pub async fn build_app(cfg: &Config) -> anyhow::Result<axum::Router> {
    let couch = CouchClient::new(&cfg.couch_url, &cfg.couch_db, &cfg.couch_user, &cfg.couch_password);
    couch.ensure_db().await?;

    let state = Arc::new(AppState {
        couch,
        device_tokens: cfg.device_tokens.clone(),
    });

    Ok(routes::build_router(state))
}
