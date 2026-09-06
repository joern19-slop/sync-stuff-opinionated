use anyhow::{bail, Context, Result};

pub fn required(key: &str) -> Result<String> {
  let value = std::env::var(key).with_context(|| format!("missing required env var {key}"))?;
  if value.trim().is_empty() {
    bail!("env var {key} is empty");
  }
  Ok(value)
}

pub fn optional(key: &str) -> Option<String> {
  std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

pub fn string_or(key: &str, default: &str) -> String {
  std::env::var(key).unwrap_or_else(|_| default.to_string())
}

pub fn u64_or(key: &str, default: u64) -> Result<u64> {
  match std::env::var(key) {
    Ok(value) => value
      .trim()
      .parse::<u64>()
      .with_context(|| format!("{key} must be an integer, got {value:?}")),
    Err(_) => Ok(default),
  }
}

pub fn list_optional(key: &str) -> Option<Vec<String>> {
  let value = std::env::var(key).ok()?;
  let items: Vec<String> = value
    .split(',')
    .map(str::trim)
    .filter(|s| !s.is_empty())
    .map(str::to_string)
    .collect();
  if items.is_empty() {
    None
  } else {
    Some(items)
  }
}

pub fn list_required(key: &str) -> Result<Vec<String>> {
  let items = list_optional(key).unwrap_or_default();
  if items.is_empty() {
    bail!("missing required env var {key}");
  }
  Ok(items)
}
