//! Configuration for the desktop client, read from environment variables.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    /// Ordered hub base URLs (comma-separated in `FILESYNC_HUBS`).
    pub hubs: Vec<String>,
    /// Shared bearer token (`FILESYNC_TOKEN`).
    pub token: String,
    /// The directory to sync (`FILESYNC_DIR`).
    pub dir: PathBuf,
    /// Where sync metadata lives (defaults to `$XDG_STATE_HOME/filesync`).
    pub state_dir: PathBuf,
    /// Quiet period after the last filesystem event before a sync runs.
    pub debounce: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let hubs: Vec<String> = require("FILESYNC_HUBS")?
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if hubs.is_empty() {
            bail!("FILESYNC_HUBS must list at least one hub URL");
        }

        let token = require("FILESYNC_TOKEN")?;
        let dir = PathBuf::from(require("FILESYNC_DIR")?);

        let state_dir = match std::env::var("FILESYNC_STATE_DIR").ok() {
            Some(p) => PathBuf::from(p),
            None => default_state_dir(),
        };

        let debounce_ms: u64 = std::env::var("FILESYNC_DEBOUNCE_MS")
            .ok()
            .map(|v| {
                v.parse()
                    .context("FILESYNC_DEBOUNCE_MS must be an integer")
            })
            .transpose()?
            .unwrap_or(1000);
        let debounce = Duration::from_millis(debounce_ms);

        Ok(Self {
            hubs,
            token,
            dir,
            state_dir,
            debounce,
        })
    }
}

fn require(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("missing required env var {name}"))
}

fn default_state_dir() -> PathBuf {
    if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
        return PathBuf::from(xdg).join("filesync");
    }
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".local/state/filesync");
    }
    PathBuf::from("filesync")
}
