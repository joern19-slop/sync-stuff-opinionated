//! The local durable-storage abstraction behind the client sync engine.
//!
//! The architecture plan requires a platform-agnostic `BlobStore` trait:
//! native builds back it with the filesystem, web builds with OPFS/IndexedDB.
//! The engine only ever talks to this trait, so those two implementations are
//! the only per-platform code. `MemStore` is a reference implementation used
//! by tests (and usable as a placeholder until a real backing store exists).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("store failure: {0}")]
    Io(String),
}

/// A key/value blob store. Keys are opaque strings; the engine owns the key
/// convention (see `engine`), backends just persist bytes by key.
#[async_trait]
pub trait BlobStore: Send + Sync {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError>;
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StoreError>;
    async fn delete(&self, key: &str) -> Result<(), StoreError>;
}

/// In-memory `BlobStore` for tests and non-persistent environments.
#[derive(Default, Clone)]
pub struct MemStore {
    inner: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

#[async_trait]
impl BlobStore for MemStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.inner.lock().unwrap().get(key).cloned())
    }

    async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StoreError> {
        self.inner.lock().unwrap().insert(key.to_string(), value);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.lock().unwrap().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mem_store_roundtrips_get_put_delete() {
        let s = MemStore::default();
        assert_eq!(s.get("a").await.unwrap(), None);
        s.put("a", b"hello".to_vec()).await.unwrap();
        assert_eq!(s.get("a").await.unwrap(), Some(b"hello".to_vec()));
        s.delete("a").await.unwrap();
        assert_eq!(s.get("a").await.unwrap(), None);
    }
}
