use std::collections::HashSet;

use anyhow::Result;

use common::env;

/// All hub configuration comes from the environment so the same binary
/// works unmodified in docker-compose, a systemd unit, or a test harness.
#[derive(Debug, Clone)]
pub struct Config {
  pub bind_addr: String,
  pub couch_url: String,
  pub couch_db: String,
  pub couch_user: String,
  pub couch_password: String,
  /// Shared bearer tokens, one (or more) per authorized device. Any token
  /// in this set is accepted for every route - there is no per-device
  /// scoping yet, only per-device *provisioning* (each device gets a
  /// distinct token so it can be revoked individually). Required: with no
  /// tokens the hub would silently reject every request.
  pub device_tokens: HashSet<String>,
  /// FCM server key (legacy HTTP API). When set, the change watcher wakes
  /// devices via FCM. See `notify::FcmClient` for the HTTP-v1 migration
  /// note - the legacy endpoint is deprecated upstream but is the
  /// simplest fit for personal-use scale until device registration is a
  /// real thing.
  pub fcm_server_key: Option<String>,
  /// FCM registration tokens for the devices the hub should wake. These
  /// are *different* from `device_tokens` (the bearer tokens used for
  /// auth): a device authenticates to the hub with its bearer token, but
  /// the hub *reaches* the device via its FCM registration token. For now
  /// this is a flat env-var list, not a server-side registry - device
  /// registration (device tells the hub its FCM token) is a later stage.
  pub fcm_device_tokens: Vec<String>,
  /// Discord webhook URL for hub-level alerts (replication errors /
  /// staleness). When unset, alerts are only logged.
  pub discord_webhook_url: Option<String>,
  /// How often the change watcher long-polls CouchDB's `_changes`.
  pub watcher_poll_secs: u64,
  /// Replication staleness threshold: a replication job whose
  /// `last_updated` is older than this is reported to Discord.
  pub repl_staleness_secs: u64,
}

impl Config {
  pub fn from_env() -> Result<Self> {
    Ok(Self {
      bind_addr: env::string_or("HUB_BIND_ADDR", "0.0.0.0:8080"),
      couch_url: env::string_or("COUCH_URL", "http://localhost:5984"),
      couch_db: env::string_or("COUCH_DB", "filesync"),
      couch_user: env::string_or("COUCH_USER", "hub"),
      couch_password: env::string_or("COUCH_PASSWORD", "hub-password"),
      device_tokens: env::list_required("HUB_DEVICE_TOKENS")?
        .into_iter()
        .collect(),
      fcm_server_key: env::optional("FCM_SERVER_KEY"),
      fcm_device_tokens: env::list_optional("HUB_FCM_TOKENS").unwrap_or_default(),
      discord_webhook_url: env::optional("DISCORD_WEBHOOK_URL"),
      watcher_poll_secs: env::u64_or("HUB_WATCHER_POLL_SECS", 2)?,
      repl_staleness_secs: env::u64_or("HUB_REPL_STALENESS_SECS", 300)?,
    })
  }
}
