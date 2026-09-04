pub mod auth;
pub mod config;
pub mod error;
pub mod notify;
pub mod resolver;
pub mod routes;
pub mod state;
pub mod watcher;

use std::sync::Arc;
use sync_core::CouchClient;

use config::Config;
use state::AppState;

/// Build the full axum app from config: wires up the CouchDB client,
/// ensures the target database exists, and assembles the router. Shared by
/// `main.rs` and the e2e tests so there's exactly one startup path.
///
/// Note this starts *no* background tasks - the Stage 3 watcher / health
/// checker are spawned by `run()`, so HTTP-only tests can use this without
/// inheriting a background thread that polls their mock CouchDB.
pub async fn build_app(cfg: &Config) -> anyhow::Result<axum::Router> {
  let couch = couch_client(cfg)?;
  couch.ensure_db().await?;

  let state = Arc::new(AppState {
    couch,
    device_tokens: cfg.device_tokens.clone(),
  });

  Ok(routes::build_router(state))
}

/// Canonical startup: build the app *and* spawn the Stage 3 background tasks
/// (change watcher -> FCM, replication health -> Discord), then serve.
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

fn couch_client(cfg: &Config) -> Result<CouchClient, sync_core::CouchError> {
  CouchClient::new(
    &cfg.couch_url,
    &cfg.couch_db,
    &cfg.couch_user,
    &cfg.couch_password,
  )
}
