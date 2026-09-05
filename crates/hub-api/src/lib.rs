pub mod auth;
pub mod config;
pub mod couch;
pub mod diff3;
pub mod error;
pub mod notify;
pub mod resolver;
pub mod routes;
pub mod state;
pub mod watcher;

use std::sync::Arc;

use couch::CouchClient;

use config::Config;
use state::AppState;

/// The full axum app: CouchDB client + router. Shared by `main.rs` and the
/// e2e tests so there's one startup path.
///
/// Starts *no* background tasks (those live in `run()`), so HTTP-only tests
/// don't inherit a thread polling their mock CouchDB.
pub async fn build_app(cfg: &Config) -> anyhow::Result<axum::Router> {
  let couch = couch_client(cfg)?;
  couch.ensure_db().await?;

  let state = Arc::new(AppState {
    couch,
    device_tokens: cfg.device_tokens.clone(),
  });

  Ok(routes::build_router(state))
}

/// Canonical startup: `build_app` plus the background tasks, then serve.
pub async fn run(cfg: &Config) -> anyhow::Result<()> {
  let couch = couch_client(cfg)?;
  couch.ensure_db().await?;

  let state = Arc::new(AppState {
    couch: couch.clone(),
    device_tokens: cfg.device_tokens.clone(),
  });
  let app = routes::build_router(state);

  let _background = watcher::spawn(couch, cfg);

  let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
  tracing::info!(addr = %cfg.bind_addr, "hub-api listening");
  axum::serve(listener, app).await?;

  Ok(())
}

fn couch_client(cfg: &Config) -> Result<CouchClient, crate::error::CouchError> {
  CouchClient::new(
    &cfg.couch_url,
    &cfg.couch_db,
    &cfg.couch_user,
    &cfg.couch_password,
  )
}
