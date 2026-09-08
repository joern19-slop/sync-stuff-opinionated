use std::collections::HashSet;

use anyhow::Result;

use common::env;

/// All hub configuration comes from the environment, so one binary serves
/// docker, systemd, and tests alike.
#[derive(Debug, Clone)]
pub struct Config {
  pub bind_addr: String,
  pub couch_url: String,
  pub couch_db: String,
  pub couch_user: String,
  pub couch_password: String,
  /// Bearer tokens accepted for every route. No per-device scoping yet, but
  /// tokens are distinct per device so each can be revoked individually.
  /// Required: with none, the hub would silently reject every request.
  pub device_tokens: HashSet<String>,
  /// Legacy FCM server key; enables device wakeups. The legacy endpoint is
  /// deprecated upstream - the HTTP-v1 migration is a single point in
  /// `notify` - but is the simplest fit until device registration exists.
  pub fcm_server_key: Option<String>,
  /// FCM registration tokens to wake. Distinct from the bearer
  /// `device_tokens`: auth reaches the hub, FCM reaches the device. Flat
  /// env list for now, not a server-side registry.
  pub fcm_device_tokens: Option<Vec<String>>,
  /// When unset, replication alerts are only logged.
  pub discord_webhook_url: Option<String>,
  pub watcher_poll_secs: i32,
  pub repl_staleness_secs: i32,
}

impl Config {
  pub fn from_env() -> Result<Self> {
    Ok(Self {
      bind_addr: env::default("HUB_BIND_ADDR", "0.0.0.0:8080".to_string())?,
      couch_url: env::default("COUCH_URL", "http://localhost:5984")?,
      couch_db: env::default("COUCH_DB", "filesync")?,
      couch_user: env::default("COUCH_USER", "hub")?,
      couch_password: env::default("COUCH_PASSWORD", "hub-password")?,
      device_tokens: env::required("HUB_DEVICE_TOKENS")?,
      fcm_server_key: env::optional("FCM_SERVER_KEY")?,
      fcm_device_tokens: env::optional("HUB_FCM_TOKENS")?,
      discord_webhook_url: env::optional("DISCORD_WEBHOOK_URL")?,
      watcher_poll_secs: env::default("HUB_WATCHER_POLL_SECS", 2)?,
      repl_staleness_secs: env::default("HUB_REPL_STALENESS_SECS", 300)?,
    })
  }
}
