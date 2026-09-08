use std::{collections::HashSet, env::VarError, path::PathBuf};

use anyhow::{anyhow, bail, Context, Result};

pub type EnvValue = String;

pub trait TryFromEnv: Sized {
  fn try_from_env(value: EnvValue) -> Result<Self, anyhow::Error>;
}

pub fn default<T: TryFromEnv>(key: impl Into<String>, default: impl Into<T>) -> Result<T> {
  RequiredEnv {
    key: key.into(),
    default: Some(default.into()),
  }
  .load()
}

pub fn required<T: TryFromEnv>(key: impl Into<String>) -> Result<T> {
  RequiredEnv {
    key: key.into(),
    default: None,
  }
  .load()
}

pub fn optional<T: TryFromEnv>(key: impl Into<String>) -> Result<Option<T>> {
  OptionalEnv { key: key.into() }.load()
}

pub struct RequiredEnv<T> {
  key: String,
  default: Option<T>,
}

pub struct OptionalEnv {
  key: String,
}

fn fetch_env(key: String) -> Result<String> {
  match std::env::var(key.clone()) {
    Ok(value) => Ok(value),
    Err(VarError::NotPresent) => Ok("".to_string()),
    Err(VarError::NotUnicode(_)) => Err(anyhow!("env var {} isn't unicode!", key)),
  }
}

impl<T: TryFromEnv> RequiredEnv<T> {
  pub fn load(self) -> Result<T> {
    let value = fetch_env(self.key.clone())?;
    if value.trim().is_empty() {
      if let Some(default) = self.default {
        return Ok(default);
      }
      bail!("env var {} is empty", self.key);
    }
    T::try_from_env(value as EnvValue)
  }
}

impl OptionalEnv {
  pub fn load<T: TryFromEnv>(self) -> Result<Option<T>> {
    let fetched_env = fetch_env(self.key)?;
    if fetched_env.trim().is_empty() {
      Ok(None)
    } else {
      T::try_from_env(fetched_env as EnvValue).map(|result| Some(result))
    }
  }
}

impl TryFromEnv for String {
  fn try_from_env(value: EnvValue) -> std::result::Result<Self, anyhow::Error> {
    Ok(value)
  }
}

impl TryFromEnv for i32 {
  fn try_from_env(value: EnvValue) -> std::result::Result<Self, anyhow::Error> {
    value
      .parse::<i32>()
      .with_context(|| format!("must be an integer, got {value:?}"))
  }
}

impl TryFromEnv for Vec<String> {
  fn try_from_env(value: EnvValue) -> Result<Self, anyhow::Error> {
    let items: Vec<String> = value
      .split(',')
      .map(str::trim)
      .filter(|s| !s.is_empty())
      .map(str::to_string)
      .collect();
    Ok(items)
  }
}

impl TryFromEnv for HashSet<String> {
  fn try_from_env(value: EnvValue) -> Result<Self, anyhow::Error> {
    let a: Vec<String> = Vec::<String>::try_from_env(value)?;
    Ok(a.into_iter().collect())
  }
}

impl TryFromEnv for PathBuf {
  fn try_from_env(value: EnvValue) -> Result<Self, anyhow::Error> {
    Ok(PathBuf::from(value))
  }
}
