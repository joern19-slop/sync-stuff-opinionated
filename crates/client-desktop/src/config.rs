use std::path::PathBuf;
use std::time::Duration;

use anyhow::Result;

use common::env;

#[derive(Debug, Clone)]
pub struct Config {
  pub hubs: Vec<String>,
  pub token: String,
  pub dir: PathBuf,
  pub metadata_dir: PathBuf,
  pub debounce: Duration,
}

impl Config {
  pub fn from_env() -> Result<Self> {
    Ok(Self {
      hubs: env::required("FILESYNC_HUBS")?,
      token: env::required("FILESYNC_TOKEN")?,
      dir: env::required("FILESYNC_DIR")?,
      metadata_dir: env::default("FILESYNC_STATE_DIR", default_state_dir())?,
      debounce: Duration::from_millis(env::default("FILESYNC_DEBOUNCE_MS", 1000)?),
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
