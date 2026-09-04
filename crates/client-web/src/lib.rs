//! Web (browser) filesync client: the sync engine wired to OPFS and exposed
//! to JavaScript via wasm-bindgen. A JS frontend (the Tuta calendar) drives
//! this through a thin shim - this crate knows nothing about calendars, only
//! the generic file-sync operations the engine already provides.

// The whole crate is wasm-only; on native it compiles to an empty lib so the
// workspace still builds.
#![cfg(target_arch = "wasm32")]

mod opfs;

use std::sync::Arc;

use client_core::{HubClient, SyncEngine};
use opfs::{OpfsFileStore, OpfsMetaStore};
use wasm_bindgen::prelude::*;

/// Thin wasm handle around the [`SyncEngine`]. Construct it with
/// [`init`], then call the file-sync primitives below.
#[wasm_bindgen]
pub struct WebSync {
  engine: Arc<SyncEngine>,
}

#[wasm_bindgen]
impl WebSync {
  /// Push then pull. Errors are mapped to a thrown JS `Error`.
  pub async fn sync(&self) -> Result<(), JsValue> {
    self.engine.sync().await.map_err(|e| JsValue::from_str(&e.to_string()))?;
    Ok(())
  }

  /// All file paths the engine currently knows about.
  pub async fn list_files(&self) -> Result<Vec<String>, JsValue> {
    self
      .engine
      .list_file_paths()
      .await
      .map_err(|e| JsValue::from_str(&e.to_string()))
  }

  /// Reads a file's raw bytes, or `undefined` if it's not known locally.
  pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, JsValue> {
    self
      .engine
      .read_file(path)
      .await
      .map_err(|e| JsValue::from_str(&e.to_string()))
  }

  /// Records a local create/edit (queues a push). `mtime` is Unix seconds.
  pub async fn put_file(
    &self,
    path: &str,
    mtime: f64,
    content_type: &str,
    data: &[u8],
  ) -> Result<(), JsValue> {
    self
      .engine
      .record_upsert(path, mtime as i64, content_type, data)
      .await
      .map_err(|e| JsValue::from_str(&e.to_string()))
  }

  /// Records a local delete (queues a push). `mtime` is Unix seconds.
  pub async fn delete_file(&self, path: &str, mtime: f64) -> Result<(), JsValue> {
    self
      .engine
      .record_delete(path, mtime as i64)
      .await
      .map_err(|e| JsValue::from_str(&e.to_string()))
  }

  /// The current checkpoint for a hub (its base URL), or `undefined`.
  pub async fn checkpoint(&self, hub: &str) -> Result<Option<String>, JsValue> {
    self
      .engine
      .checkpoint(hub)
      .await
      .map_err(|e| JsValue::from_str(&e.to_string()))
  }
}

/// Builds a [`WebSync`] backed by OPFS, talking to the given hubs with the
/// given device token.
#[wasm_bindgen]
pub async fn init(hub_urls: Vec<String>, device_token: String) -> Result<WebSync, JsValue> {
  let files = Arc::new(
    OpfsFileStore::open()
      .await
      .map_err(|e| JsValue::from_str(&e.to_string()))?,
  );
  let meta = Arc::new(
    OpfsMetaStore::open()
      .await
      .map_err(|e| JsValue::from_str(&e.to_string()))?,
  );
  let hubs = hub_urls
    .into_iter()
    .map(|u| HubClient::new(u, device_token.clone()))
    .collect();
  let engine = Arc::new(SyncEngine::new(hubs, meta, files));
  Ok(WebSync { engine })
}
