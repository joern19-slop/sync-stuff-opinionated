//! OPFS-backed stores: `FileStore` (content) and `MetaStore` (bookkeeping)
//! over the browser's Origin Private File System.
//!
//! Content lives at `content/<path>`; metadata is one flat file per key at
//! `meta/<url-encoded key>` (same shape as the desktop `FsMetaStore`).
//! Neither preserves an on-disk mtime - the web client drives the engine
//! directly instead of scanning a filesystem, so mtime lives only in sync
//! metadata.

use async_trait::async_trait;
use client_core::{FileStore, MetaStore, StoreError};
use js_sys::{Array, Promise, Reflect};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
  FileSystemDirectoryHandle, FileSystemFileHandle, FileSystemGetDirectoryOptions,
  FileSystemGetFileOptions, FileSystemHandleKind, FileSystemWritableFileStream,
};

fn js_err(e: wasm_bindgen::JsValue) -> StoreError {
  StoreError::Io(format!("{e:?}"))
}

/// Awaits a `Promise`, then downcasts the resolved value to `T`.
async fn await_promise<T: JsCast>(promise: Promise) -> Result<T, StoreError> {
  JsFuture::from(promise)
    .await
    .map_err(js_err)?
    .dyn_into::<T>()
    .map_err(js_err)
}

/// Awaits a `Promise` whose resolved value is ignored (`write`, `close`).
async fn await_void(promise: Promise) -> Result<(), StoreError> {
  JsFuture::from(promise).await.map_err(js_err)?;
  Ok(())
}

fn is_not_found(e: &wasm_bindgen::JsValue) -> bool {
  js_sys::Reflect::get(e, &wasm_bindgen::JsValue::from_str("name"))
    .ok()
    .and_then(|n| n.as_string())
    .is_some_and(|name| name == "NotFoundError")
}

/// Shared handle to the OPFS root directory.
#[derive(Clone)]
struct OpfsRoot {
  handle: FileSystemDirectoryHandle,
}

impl OpfsRoot {
  async fn open() -> Result<Self, StoreError> {
    // `navigator.storage.getDirectory()` exists on both Window and
    // WorkerGlobalScope; read it off the global scope (not `web_sys::window()`,
    // which is `None` in a worker) so this works inside a Web Worker.
    let global = js_sys::global();
    let navigator =
      Reflect::get(&global, &wasm_bindgen::JsValue::from_str("navigator")).map_err(js_err)?;
    let storage =
      Reflect::get(&navigator, &wasm_bindgen::JsValue::from_str("storage")).map_err(js_err)?;
    let storage: web_sys::StorageManager = storage.dyn_into().map_err(js_err)?;
    let handle: FileSystemDirectoryHandle = await_promise(storage.get_directory()).await?;
    Ok(Self { handle })
  }

  /// Walks `path` (slash-separated), creating intermediate directories when
  /// `create` is set, and returns the file handle for the last segment.
  async fn file_handle(
    &self,
    path: &str,
    create: bool,
  ) -> Result<FileSystemFileHandle, StoreError> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let Some((last, parents)) = parts.split_last() else {
      return Err(StoreError::Io("empty path".into()));
    };

    let mut dir = self.handle.clone();
    for part in parents {
      let opts = FileSystemGetDirectoryOptions::new();
      opts.set_create(create);
      dir = await_promise(dir.get_directory_handle_with_options(part, &opts)).await?;
    }

    if create {
      let opts = FileSystemGetFileOptions::new();
      opts.set_create(true);
      await_promise(dir.get_file_handle_with_options(last, &opts)).await
    } else {
      await_promise(dir.get_file_handle(last)).await
    }
  }

  async fn read(&self, path: &str) -> Result<Option<Vec<u8>>, StoreError> {
    let handle = match self.file_handle(path, false).await {
      Ok(h) => h,
      Err(_) => return Ok(None),
    };
    let file: web_sys::File = await_promise(handle.get_file()).await?;
    let buffer: js_sys::ArrayBuffer = await_promise(file.array_buffer()).await?;
    Ok(Some(js_sys::Uint8Array::new(&buffer).to_vec()))
  }

  async fn write(&self, path: &str, data: &[u8]) -> Result<(), StoreError> {
    let handle = self.file_handle(path, true).await?;
    let writable: FileSystemWritableFileStream = await_promise(handle.create_writable()).await?;
    let write = writable.write_with_u8_array(data).map_err(js_err)?;
    await_void(write).await?;
    await_void(writable.close()).await?;
    Ok(())
  }

  async fn remove(&self, path: &str) -> Result<(), StoreError> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let Some((last, parents)) = parts.split_last() else {
      return Ok(());
    };
    let mut dir = self.handle.clone();
    for part in parents {
      let resolved = match JsFuture::from(dir.get_directory_handle(part)).await {
        Ok(v) => v,
        // A missing parent means the file can't exist - nothing to remove.
        Err(e) if is_not_found(&e) => return Ok(()),
        Err(e) => return Err(js_err(e)),
      };
      dir = resolved.dyn_into().map_err(js_err)?;
    }
    // Idempotent: removing an already-removed file is a no-op.
    match JsFuture::from(dir.remove_entry(last)).await {
      Ok(_) => Ok(()),
      Err(e) if is_not_found(&e) => Ok(()),
      Err(e) => Err(js_err(e)),
    }
  }

  /// Flat listing of `dir`: returns the entry names (files only), no recursion.
  async fn list(&self, dir: &str) -> Result<Vec<String>, StoreError> {
    let mut handle = self.handle.clone();
    for part in dir.split('/').filter(|s| !s.is_empty()) {
      let resolved = match JsFuture::from(handle.get_directory_handle(part)).await {
        Ok(v) => v,
        // A missing directory simply has no keys.
        Err(e) if is_not_found(&e) => return Ok(Vec::new()),
        Err(e) => return Err(js_err(e)),
      };
      handle = resolved.dyn_into().map_err(js_err)?;
    }

    let mut out = Vec::new();
    let iterator = handle.entries();
    loop {
      let result = JsFuture::from(iterator.next().map_err(js_err)?)
        .await
        .map_err(js_err)?;
      let done = Reflect::get(&result, &wasm_bindgen::JsValue::from_str("done"))
        .map_err(js_err)?
        .as_bool()
        .unwrap_or(true);
      if done {
        break;
      }
      let value =
        Reflect::get(&result, &wasm_bindgen::JsValue::from_str("value")).map_err(js_err)?;
      let pair = Array::from(&value);
      let Some(name) = pair.get(0).as_string() else {
        continue;
      };
      // Skip subdirectories (only flat files are our keys/content).
      if let Some(handle) = pair.get(1).dyn_ref::<web_sys::FileSystemHandle>() {
        if handle.kind() == FileSystemHandleKind::Directory {
          continue;
        }
      }
      out.push(name);
    }
    Ok(out)
  }
}

/// [`FileStore`] over OPFS, rooted at `content/`.
pub struct OpfsFileStore {
  root: OpfsRoot,
}

impl OpfsFileStore {
  pub async fn open() -> Result<Self, StoreError> {
    Ok(Self {
      root: OpfsRoot::open().await?,
    })
  }

  fn path(&self, p: &str) -> String {
    format!("content/{p}")
  }
}

#[async_trait(?Send)]
impl FileStore for OpfsFileStore {
  async fn get(&self, path: &str) -> Result<Option<Vec<u8>>, StoreError> {
    self.root.read(&self.path(path)).await
  }

  async fn put(&self, path: &str, data: Vec<u8>, _mtime: i64) -> Result<(), StoreError> {
    self.root.write(&self.path(path), &data).await
  }

  async fn delete(&self, path: &str) -> Result<(), StoreError> {
    self.root.remove(&self.path(path)).await
  }
}

/// [`MetaStore`] over OPFS, rooted at `meta/`, one flat file per key.
pub struct OpfsMetaStore {
  root: OpfsRoot,
}

impl OpfsMetaStore {
  pub async fn open() -> Result<Self, StoreError> {
    Ok(Self {
      root: OpfsRoot::open().await?,
    })
  }

  fn path(&self, key: &str) -> String {
    format!("meta/{}", urlencoding::encode(key))
  }
}

#[async_trait(?Send)]
impl MetaStore for OpfsMetaStore {
  async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
    self.root.read(&self.path(key)).await
  }

  async fn put(&self, key: &str, value: Vec<u8>) -> Result<(), StoreError> {
    self.root.write(&self.path(key), &value).await
  }

  async fn delete(&self, key: &str) -> Result<(), StoreError> {
    self.root.remove(&self.path(key)).await
  }

  async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
    let names = self.root.list("meta").await?;
    Ok(
      names
        .into_iter()
        .filter_map(|n| urlencoding::decode(&n).ok().map(|c| c.into_owned()))
        .filter(|k| k.starts_with(prefix))
        .collect(),
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use wasm_bindgen_test::wasm_bindgen_test;

  wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

  #[wasm_bindgen_test]
  async fn file_store_roundtrips_nested_paths() {
    let store = OpfsFileStore::open().await.unwrap();
    assert_eq!(FileStore::get(&store, "a/b.txt").await.unwrap(), None);
    FileStore::put(&store, "a/b.txt", b"hello".to_vec(), 42)
      .await
      .unwrap();
    assert_eq!(
      FileStore::get(&store, "a/b.txt").await.unwrap(),
      Some(b"hello".to_vec())
    );
    FileStore::delete(&store, "a/b.txt").await.unwrap();
    assert_eq!(FileStore::get(&store, "a/b.txt").await.unwrap(), None);
  }

  #[wasm_bindgen_test]
  async fn meta_store_roundtrips_and_lists() {
    let store = OpfsMetaStore::open().await.unwrap();
    MetaStore::put(&store, "file/x", b"meta".to_vec())
      .await
      .unwrap();
    MetaStore::put(&store, "checkpoint/h", b"cp-1".to_vec())
      .await
      .unwrap();
    assert_eq!(
      MetaStore::get(&store, "file/x").await.unwrap(),
      Some(b"meta".to_vec())
    );
    let mut keys = MetaStore::list_keys(&store, "file/").await.unwrap();
    keys.sort();
    assert_eq!(keys, vec!["file/x"]);
    MetaStore::delete(&store, "file/x").await.unwrap();
    assert_eq!(MetaStore::get(&store, "file/x").await.unwrap(), None);
  }
}
