//! Filesystem-backed [`MetaStore`] and [`FileStore`] for the desktop client.
//!
//! - [`FsMetaStore`] keeps the sync bookkeeping (checkpoints, the pending
//!   queue, per-file revision/mtime/content-type) in a private directory,
//!   keyed by URL-encoded key so any engine key is a safe flat filename.
//! - [`FsFileStore`] is the sync directory itself: file bytes are written
//!   atomically and the on-disk mtime is set to the sync mtime so a later
//!   reconciliation doesn't mistake the client's own write for a new edit.

use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use client_core::{FileStore, MetaStore, StoreError};

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_name() -> String {
  let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
  format!(".{}-{n}.tmp", std::process::id())
}

async fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
  let dir = path.parent().unwrap_or_else(|| Path::new("."));
  tokio::fs::create_dir_all(dir).await.map_err(io_err)?;
  let tmp = dir.join(tmp_name());
  tokio::fs::write(&tmp, bytes).await.map_err(io_err)?;
  tokio::fs::rename(&tmp, path).await.map_err(io_err)?;
  Ok(())
}

fn io_err(e: std::io::Error) -> StoreError {
  StoreError::Io(e.to_string())
}

/// A [`MetaStore`] persisted as one file per key in a directory.
pub struct FsMetaStore {
  dir: PathBuf,
}

impl FsMetaStore {
  pub fn open(dir: PathBuf) -> Result<Self, StoreError> {
    std::fs::create_dir_all(&dir).map_err(io_err)?;
    Ok(Self { dir })
  }

  fn path_for(&self, key: &str) -> PathBuf {
    self.dir.join(urlencoding::encode(key).into_owned())
  }
}

#[async_trait]
impl MetaStore for FsMetaStore {
  async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
    match tokio::fs::read(self.path_for(key)).await {
      Ok(bytes) => Ok(Some(bytes)),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
      Err(e) => Err(io_err(e)),
    }
  }

  async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StoreError> {
    atomic_write(&self.path_for(key), &value).await
  }

  async fn delete(&self, key: &str) -> Result<(), StoreError> {
    match tokio::fs::remove_file(self.path_for(key)).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
      Err(e) => Err(io_err(e)),
    }
  }

  async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
    let mut keys = Vec::new();
    let mut rd = tokio::fs::read_dir(&self.dir).await.map_err(io_err)?;
    while let Some(entry) = rd.next_entry().await.map_err(io_err)? {
      let name = entry.file_name();
      let Some(name) = name.to_str() else { continue };
      // Our own atomic-write temp files (`.{pid}-{n}.tmp`) are never a real
      // key.
      if name.ends_with(".tmp") {
        continue;
      }
      if let Ok(decoded) = urlencoding::decode(name) {
        if decoded.starts_with(prefix) {
          keys.push(decoded.into_owned());
        }
      }
    }
    Ok(keys)
  }
}

/// A [`FileStore`] backed by a directory on disk - the directory the client
/// actually syncs.
pub struct FsFileStore {
  root: PathBuf,
}

impl FsFileStore {
  pub fn new(root: PathBuf) -> Self {
    Self { root }
  }

  /// Resolves a sync path to a filesystem path, refusing anything that
  /// would escape the sync root (`..`, absolute paths, ...).
  fn path_for(&self, path: &str) -> Result<PathBuf, StoreError> {
    let p = Path::new(path);
    if p.components().any(|c| {
      matches!(
        c,
        Component::ParentDir | Component::RootDir | Component::Prefix(_)
      )
    }) {
      return Err(StoreError::Io(format!("unsafe path: {path}")));
    }
    Ok(self.root.join(p))
  }
}

#[async_trait]
impl FileStore for FsFileStore {
  async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, StoreError> {
    match tokio::fs::read(self.path_for(path)?).await {
      Ok(bytes) => Ok(Some(bytes)),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
      Err(e) => Err(io_err(e)),
    }
  }

  async fn put(&self, path: &str, data: Vec<u8>, mtime: i64) -> Result<(), StoreError> {
    let full = self.path_for(path)?;
    let dir = full.parent().unwrap_or_else(|| Path::new("."));
    tokio::fs::create_dir_all(dir).await.map_err(io_err)?;

    let tmp = dir.join(tmp_name());
    tokio::fs::write(&tmp, &data).await.map_err(io_err)?;
    // Set the mtime on the temp file before the rename so the destination
    // never exists with a wrong mtime (the rename preserves it).
    if mtime > 0 {
      let _ = filetime::set_file_mtime(&tmp, filetime::FileTime::from_unix_time(mtime, 0));
    }
    tokio::fs::rename(&tmp, &full).await.map_err(io_err)?;
    Ok(())
  }

  async fn delete(&self, path: &str) -> Result<(), StoreError> {
    match tokio::fs::remove_file(self.path_for(path)?).await {
      Ok(()) => Ok(()),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
      Err(e) => Err(io_err(e)),
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[tokio::test]
  async fn meta_store_roundtrips_and_lists_keys() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsMetaStore::open(dir.path().to_path_buf()).unwrap();

    assert_eq!(MetaStore::get(&store, "checkpoint/h").await.unwrap(), None);
    MetaStore::put(&store, "checkpoint/h", b"cp-1".to_vec())
      .await
      .unwrap();
    MetaStore::put(&store, "file/a.txt", b"meta".to_vec())
      .await
      .unwrap();

    assert_eq!(
      MetaStore::get(&store, "checkpoint/h").await.unwrap(),
      Some(b"cp-1".to_vec())
    );
    let mut keys = MetaStore::list_keys(&store, "file/").await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["file/a.txt"]);

    MetaStore::delete(&store, "file/a.txt").await.unwrap();
    assert_eq!(MetaStore::get(&store, "file/a.txt").await.unwrap(), None);
  }

  #[tokio::test]
  async fn file_store_roundtrips_and_rejects_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsFileStore::new(dir.path().to_path_buf());

    assert_eq!(FileStore::get(&store, "notes/a.txt").await.unwrap(), None);
    FileStore::put(&store, "notes/a.txt", b"hello".to_vec(), 42)
      .await
      .unwrap();
    assert_eq!(
      FileStore::get(&store, "notes/a.txt").await.unwrap(),
      Some(b"hello".to_vec())
    );
    // The on-disk file is exactly where we expect, with the sync mtime.
    let full = dir.path().join("notes/a.txt");
    assert!(full.is_file());
    let mtime = std::fs::metadata(&full).unwrap().modified().unwrap();
    assert_eq!(
      mtime
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs(),
      42
    );

    // Path traversal is refused.
    assert!(FileStore::put(&store, "../escape.txt", b"x".to_vec(), 1)
      .await
      .is_err());
    assert!(FileStore::put(&store, "/abs.txt", b"x".to_vec(), 1)
      .await
      .is_err());

    FileStore::delete(&store, "notes/a.txt").await.unwrap();
    assert_eq!(FileStore::get(&store, "notes/a.txt").await.unwrap(), None);
  }
}
