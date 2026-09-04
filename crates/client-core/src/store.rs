//! The local durable-storage abstractions behind the client sync engine.
//!
//! The engine needs two distinct kinds of storage, so the platform provides
//! two implementations:
//!
//! - [`MetaStore`]: small bookkeeping blobs - the checkpoint, the pending-push
//!   queue, and per-file metadata (revision / mtime / content type). A native
//!   client backs this with a small private directory, a web client with
//!   IndexedDB.
//! - [`FileStore`]: the actual file bytes, keyed by path. A native client
//!   points this at the directory it syncs; a web client at OPFS.
//!
//! Splitting them keeps the "where does my data live" answer separate from
//! the "how do I track sync progress" answer. [`MemStore`] is an in-memory
//! reference implementing both, used by tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
  #[error("store failure: {0}")]
  Io(String),
}

/// Small bookkeeping blobs, addressed by an opaque key. The engine owns the
/// key convention (see `engine`); backends just persist bytes by key.
#[async_trait]
pub trait MetaStore: Send + Sync {
  async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
  async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StoreError>;
  async fn delete(&self, key: &str) -> Result<(), StoreError>;
  /// All stored keys beginning with `prefix` (e.g. `file/`), for
  /// reconciliation at startup.
  async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

/// Actual file content, addressed by file path. `mtime` on [`FileStore::put`]
/// is the file's modification time (Unix seconds) - the native backend uses it
/// to keep the on-disk mtime consistent with the sync metadata so a later
/// reconciliation doesn't mistake its own write for a new local edit.
#[async_trait]
pub trait FileStore: Send + Sync {
  async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, StoreError>;
  async fn put(&self, path: &str, data: Vec<u8>, mtime: i64) -> Result<(), StoreError>;
  async fn delete(&self, path: &str) -> Result<(), StoreError>;
}

/// In-memory `MetaStore` + `FileStore` for tests and non-persistent
/// environments. Keeps the two key spaces separate so a metadata key can
/// never collide with a file path.
#[derive(Default, Clone)]
pub struct MemStore {
  meta: Arc<Mutex<HashMap<String, Vec<u8>>>>,
  files: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl MetaStore for MemStore {
  async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
    Ok(self.meta.lock().unwrap().get(key).cloned())
  }

  async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StoreError> {
    self.meta.lock().unwrap().insert(key.to_string(), value);
    Ok(())
  }

  async fn delete(&self, key: &str) -> Result<(), StoreError> {
    self.meta.lock().unwrap().remove(key);
    Ok(())
  }

  async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
    Ok(
      self
        .meta
        .lock()
        .unwrap()
        .keys()
        .filter(|k| k.starts_with(prefix))
        .cloned()
        .collect(),
    )
  }
}

#[async_trait]
impl FileStore for MemStore {
  async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, StoreError> {
    Ok(self.files.lock().unwrap().get(path).cloned())
  }

  async fn put(&self, path: &str, data: Vec<u8>, _mtime: i64) -> Result<(), StoreError> {
    self.files.lock().unwrap().insert(path.to_string(), data);
    Ok(())
  }

  async fn delete(&self, path: &str) -> Result<(), StoreError> {
    self.files.lock().unwrap().remove(path);
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn mem_meta_store_roundtrips() {
    let s = MemStore::default();
    assert_eq!(MetaStore::get(&s, "a").await.unwrap(), None);
    MetaStore::put(&s, "a", b"hello".to_vec()).await.unwrap();
    assert_eq!(
      MetaStore::get(&s, "a").await.unwrap(),
      Some(b"hello".to_vec())
    );
    assert_eq!(
      MetaStore::list_keys(&s, "file/").await.unwrap(),
      Vec::<String>::new()
    );
    MetaStore::put(&s, "file/x", b"m".to_vec()).await.unwrap();
    assert_eq!(
      MetaStore::list_keys(&s, "file/").await.unwrap(),
      vec!["file/x"]
    );
    MetaStore::delete(&s, "a").await.unwrap();
    assert_eq!(MetaStore::get(&s, "a").await.unwrap(), None);
  }

  #[tokio::test]
  async fn mem_file_store_roundtrips() {
    let s = MemStore::default();
    assert_eq!(FileStore::get(&s, "a.txt").await.unwrap(), None);
    FileStore::put(&s, "a.txt", b"hello".to_vec(), 1)
      .await
      .unwrap();
    assert_eq!(
      FileStore::get(&s, "a.txt").await.unwrap(),
      Some(b"hello".to_vec())
    );
    FileStore::delete(&s, "a.txt").await.unwrap();
    assert_eq!(FileStore::get(&s, "a.txt").await.unwrap(), None);
  }
}
