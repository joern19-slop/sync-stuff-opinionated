//! Configuration for the desktop client, read from environment variables.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use common::env;

#[derive(Debug, Clone)]
pub struct Config {
  /// Ordered hub base URLs (`FILESYNC_HUBS`).
  pub hubs: Vec<String>,
  /// Shared bearer token (`FILESYNC_TOKEN`).
  pub token: String,
  /// Directory to sync (`FILESYNC_DIR`).
  pub dir: PathBuf,
  /// Where sync metadata lives (`FILESYNC_STATE_DIR`; default XDG state).
  pub state_dir: PathBuf,
  /// Quiet period after the last event before a sync runs.
  pub debounce: Duration,
}

impl Config {
  pub fn from_env() -> Result<Self> {
    let hubs = env::list_required("FILESYNC_HUBS")?;
    let token = env::required("FILESYNC_TOKEN")?;
    let dir = PathBuf::from(env::required("FILESYNC_DIR")?);

    let state_dir = match env::optional("FILESYNC_STATE_DIR") {
      Some(p) => PathBuf::from(p),
      None => default_state_dir(),
    };

    let debounce_ms = env::u64_or("FILESYNC_DEBOUNCE_MS", 1000)?;
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

fn default_state_dir() -> PathBuf {
  if let Some(xdg) = std::env::var_os("XDG_STATE_HOME") {
    return PathBuf::from(xdg).join("filesync");
  }
  if let Some(home) = std::env::var_os("HOME") {
    return PathBuf::from(home).join(".local/state/filesync");
  }
  PathBuf::from("filesync")
}
