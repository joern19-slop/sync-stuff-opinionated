use std::collections::HashSet;

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
    /// distinct token so it can be revoked individually).
    pub device_tokens: HashSet<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            bind_addr: env_or("HUB_BIND_ADDR", "0.0.0.0:8080"),
            couch_url: env_or("COUCH_URL", "http://localhost:5984"),
            couch_db: env_or("COUCH_DB", "filesync"),
            couch_user: env_or("COUCH_USER", "hub"),
            couch_password: env_or("COUCH_PASSWORD", "hub-password"),
            device_tokens: std::env::var("HUB_DEVICE_TOKENS")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
