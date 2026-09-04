//! Desktop file-sync client: watches a directory with inotify and keeps it in
//! sync with the hub via the client sync engine.
//!
//! Configuration is via environment variables (see `config.rs`). On startup it
//! reconciles the directory against the hub (catching changes made while it
//! was off), then watches the tree and re-syncs after every debounced change.

mod config;
mod fs_store;
mod notifier;
mod poll;
mod reconcile;
mod watcher;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use client_core::{HubClient, SyncEngine};
use fs_store::{FsFileStore, FsMetaStore};
use tokio::signal::unix::{signal, SignalKind};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
  tracing_subscriber::fmt()
    .with_env_filter(
      tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "filesync_client=info".into()),
    )
    .init();

  let cfg = config::Config::from_env()?;

  std::fs::create_dir_all(&cfg.dir)
    .with_context(|| format!("creating sync dir {}", cfg.dir.display()))?;
  std::fs::create_dir_all(&cfg.state_dir)
    .with_context(|| format!("creating state dir {}", cfg.state_dir.display()))?;

  let meta = Arc::new(FsMetaStore::open(cfg.state_dir.clone())?);
  let files = Arc::new(FsFileStore::new(cfg.dir.clone()));
  let hubs: Vec<HubClient> = cfg
    .hubs
    .iter()
    .map(|u| HubClient::new(u, cfg.token.clone()))
    .collect();
  let engine = Arc::new(
    SyncEngine::new(hubs.clone(), meta, files)
      .with_notifier(Arc::new(notifier::LogNotifier::default())),
  );

  info!(
      dir = %cfg.dir.display(),
      state = %cfg.state_dir.display(),
      hubs = ?cfg.hubs,
      "starting desktop client"
  );

  // Bring the directory and the hub into agreement before watching, so
  // changes made while we were off aren't lost.
  reconcile::reconcile_and_sync(&engine, &cfg.dir).await?;

  let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
  watcher::spawn(cfg.dir.clone(), tx.clone())?;
  for hub in hubs {
    poll::spawn(engine.clone(), hub, tx.clone());
  }

  let mut sigterm = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
  let mut sigint = signal(SignalKind::interrupt()).context("install SIGINT handler")?;

  info!("watching for changes");

  loop {
    tokio::select! {
      _ = rx.recv() => {
        // Wait for the first event, then stay quiet until events have
        // stopped for `debounce`.
        while tokio::time::timeout(cfg.debounce, rx.recv()).await.is_ok() {}
        if let Err(e) = reconcile::reconcile_and_sync(&engine, &cfg.dir).await {
          tracing::error!(error = %e, "reconcile/sync failed");
        }
      }
      // An in-progress reconcile always completes first (the select arm body
      // runs to the end); we only reach here between syncs.
      _ = sigterm.recv() => break,
      _ = sigint.recv() => break,
    }
  }

  // Graceful stop: finish any queued work with one final sync, bounded so a
  // dead hub can't hang shutdown. The durable checkpoint makes an interrupted
  // sync safe regardless.
  info!("shutdown requested; finishing");
  match tokio::time::timeout(
    Duration::from_secs(30),
    reconcile::reconcile_and_sync(&engine, &cfg.dir),
  )
  .await
  {
    Ok(Ok(())) => info!("final sync complete"),
    Ok(Err(e)) => tracing::error!(error = %e, "final sync failed"),
    Err(_) => tracing::warn!("final sync timed out"),
  }

  Ok(())
}
