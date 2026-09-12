//! The client sync engine: pull hub changes into the local store with a
//! durable checkpoint, and push locally-recorded changes back up.
//!
//! The engine has no merge logic - it only ever sees the hub's *resolved*
//! view of the world. Its safety guarantees:
//!
//! 1. A checkpoint advances only after the whole batch it covers is durable
//!    (a crash mid-pull re-pulls the same idempotent batch).
//! 2. A push the hub rejects is never dropped: it stays queued and is
//!    reported to the caller.
//!
//! Multi-hub failover (Stage 7): hubs are tried in order. Checkpoints are
//! keyed per hub because CouchDB `seq` is node-local; the pending-push queue
//! is global because revisions are consistent across replicas.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use protocol_types::{ChangeEntry, PushChange};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::hub::{FileContent, HubClient, HubError};
use crate::notify::Notifier;
use crate::store::{FileStore, MetaStore, StoreError};

pub const KEY_PENDING: &str = "pending";

pub fn file_key(path: &str) -> String {
  format!("file/{path}")
}

/// Key for a hub's checkpoint. `hub_id` is `HubClient::id()`.
pub fn checkpoint_key(hub_id: &str) -> String {
  format!("checkpoint/{hub_id}")
}

#[derive(Debug, Error)]
pub enum SyncError {
  #[error("store error: {0}")]
  Store(#[from] StoreError),
  #[error("hub error: {0}")]
  Hub(#[from] HubError),
}

/// Local per-file metadata: the revision to use as `base_rev` on the next
/// push, plus the mtime/content-type to push it back. Content lives in the
/// [`FileStore`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
  pub rev: String,
  pub mtime: i64,
  pub content_type: String,
}

/// A queued local change; content is read from the [`FileStore`] at push
/// time, not stored here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingChange {
  pub path: String,
  pub deleted: bool,
  pub base_rev: Option<String>,
  pub mtime: i64,
  pub content_type: Option<String>,
}

#[derive(Debug, Default)]
pub struct PullReport {
  pub hub: String,
  pub pulled: usize,
  pub deleted: usize,
  /// Paths skipped because an unpushed local edit exists for them; surfaces
  /// a local-vs-remote conflict without losing either side.
  pub local_conflicts: Vec<String>,
  pub checkpoint: Option<String>,
}

#[derive(Debug, Default)]
pub struct PushReport {
  pub hub: String,
  pub pushed: usize,
}

#[derive(Debug, Default)]
pub struct SyncReport {
  pub pull: PullReport,
  pub push: PushReport,
}

pub struct SyncEngine {
  hubs: Vec<HubClient>,
  meta: Arc<dyn MetaStore>,
  files: Arc<dyn FileStore>,
  notifier: Option<Arc<dyn Notifier>>,
}

impl SyncEngine {
  pub fn new(hubs: Vec<HubClient>, meta: Arc<dyn MetaStore>, files: Arc<dyn FileStore>) -> Self {
    Self {
      hubs,
      meta,
      files,
      notifier: None,
    }
  }

  pub fn with_notifier(mut self, notifier: Arc<dyn Notifier>) -> Self {
    self.notifier = Some(notifier);
    self
  }

  /// Push then pull: a push can trigger a hub-side merge, so the resolved
  /// content is only known after the push; pulling last converges on it.
  pub async fn sync(&self) -> Result<SyncReport, SyncError> {
    let push = self.push().await?;
    let pull = self.pull().await?;
    Ok(SyncReport { pull, push })
  }

  pub async fn pull(&self) -> Result<PullReport, SyncError> {
    let mut last_hub_err = None;
    for hub in &self.hubs {
      match self.pull_from(hub).await {
        Ok(report) => return Ok(report),
        // A store failure is fatal to every hub - don't fail over.
        Err(e @ SyncError::Store(_)) => return Err(self.notify(e)),
        Err(e) => {
          tracing::warn!(hub = hub.id(), error = %e, "pull from hub failed");
          last_hub_err = Some(e);
        }
      }
    }
    Err(self.notify(last_hub_err.unwrap_or_else(Self::no_hubs)))
  }

  async fn pull_from(&self, hub: &HubClient) -> Result<PullReport, SyncError> {
    let ck_key = checkpoint_key(hub.id());
    let since = self
      .meta
      .get(&ck_key)
      .await?
      .and_then(|b| String::from_utf8(b).ok());

    let resp = hub.changes(since.as_deref()).await?;

    let mut report = PullReport {
      hub: hub.id().to_string(),
      checkpoint: Some(resp.checkpoint.clone()),
      ..Default::default()
    };

    for entry in &resp.changes {
      self.apply_remote(hub, entry, &mut report).await?;
    }

    // Checkpoint only once the batch is durable; a failure here just
    // re-pulls next time.
    self.meta.put(&ck_key, resp.checkpoint.into_bytes()).await?;

    Ok(report)
  }

  async fn apply_remote(
    &self,
    hub: &HubClient,
    entry: &ChangeEntry,
    report: &mut PullReport,
  ) -> Result<(), SyncError> {
    // Skip paths with pending local edits - the remote version would
    // overwrite (lose) them.
    if self.pending_contains(&entry.path).await? {
      report.local_conflicts.push(entry.path.clone());
      return Ok(());
    }

    if entry.deleted {
      self.files.delete(&entry.path).await?;
      self.meta.delete(&file_key(&entry.path)).await?;
      report.deleted += 1;
      return Ok(());
    }

    let file = hub.get_file(&entry.path).await?.ok_or_else(|| {
      SyncError::Hub(HubError::Decode(format!(
        "change listed {} but file fetch returned 404",
        entry.path
      )))
    })?;

    self
      .files
      .put(&entry.path, file.content.clone(), file.mtime)
      .await?;
    self
      .meta
      .put(&file_key(&entry.path), encode_meta(&file))
      .await?;
    report.pulled += 1;
    Ok(())
  }

  pub async fn push(&self) -> Result<PushReport, SyncError> {
    let pending = self.report(self.load_pending().await)?;
    if pending.is_empty() {
      return Ok(PushReport::default());
    }

    let changes = self.report(self.build_push_changes(&pending).await)?;

    let mut last_hub_err = None;
    for hub in &self.hubs {
      match self.push_to(hub, &pending, &changes).await {
        Ok(report) => return Ok(report),
        // A store failure is fatal to every hub - don't fail over.
        Err(e @ SyncError::Store(_)) => return Err(self.notify(e)),
        Err(e) => {
          tracing::warn!(hub = hub.id(), error = %e, "push to hub failed");
          last_hub_err = Some(e);
        }
      }
    }
    Err(self.notify(last_hub_err.unwrap_or_else(Self::no_hubs)))
  }

  async fn push_to(
    &self,
    hub: &HubClient,
    pending: &[PendingChange],
    changes: &[PushChange],
  ) -> Result<PushReport, SyncError> {
    let results = hub.push(changes).await?;
    let mut report = PushReport {
      hub: hub.id().to_string(),
      ..Default::default()
    };

    // For upserts adopt the hub's new rev as the local base; deletes have
    // nothing to record (the local file is already gone).
    for (change, result) in pending.iter().zip(&results) {
      if !change.deleted {
        self.bump_rev(&change.path, &result.rev).await?;
      }
      report.pushed += 1;
    }

    // Clear only on a full, successful batch; fewer results than sent is a
    // protocol violation - keep everything queued.
    if results.len() == changes.len() {
      self
        .meta
        .put(
          KEY_PENDING,
          serde_json::to_vec(&Vec::<PendingChange>::new()).unwrap(),
        )
        .await?;
    }

    Ok(report)
  }

  /// Record a local create/edit durably, then queue a push based on the
  /// previous revision (so the hub can CAS-check it). Coalesces per path.
  pub async fn record_upsert(
    &self,
    path: &str,
    mtime: i64,
    content_type: &str,
    content: &[u8],
  ) -> Result<(), SyncError> {
    self.report(
      self
        .record_upsert_inner(path, mtime, content_type, content)
        .await,
    )
  }

  async fn record_upsert_inner(
    &self,
    path: &str,
    mtime: i64,
    content_type: &str,
    content: &[u8],
  ) -> Result<(), SyncError> {
    let key = file_key(path);
    let base_rev = self
      .meta
      .get(&key)
      .await?
      .and_then(|b| serde_json::from_slice::<FileMeta>(&b).ok())
      .map(|f| f.rev)
      .filter(|r| !r.is_empty());

    let meta = FileMeta {
      rev: base_rev.clone().unwrap_or_default(),
      mtime,
      content_type: content_type.to_string(),
    };
    self.files.put(path, content.to_vec(), mtime).await?;
    self
      .meta
      .put(&key, serde_json::to_vec(&meta).unwrap())
      .await?;

    self
      .upsert_pending(PendingChange {
        path: path.to_string(),
        deleted: false,
        base_rev,
        mtime,
        content_type: Some(content_type.to_string()),
      })
      .await
  }

  /// Record a local delete durably, then queue a delete.
  pub async fn record_delete(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
    self.report(self.record_delete_inner(path, mtime).await)
  }

  async fn record_delete_inner(&self, path: &str, mtime: i64) -> Result<(), SyncError> {
    let key = file_key(path);
    let base_rev = self
      .meta
      .get(&key)
      .await?
      .and_then(|b| serde_json::from_slice::<FileMeta>(&b).ok())
      .map(|f| f.rev)
      .filter(|r| !r.is_empty());

    self.files.delete(path).await?;
    self.meta.delete(&key).await?;

    self
      .upsert_pending(PendingChange {
        path: path.to_string(),
        deleted: true,
        base_rev,
        mtime,
        content_type: None,
      })
      .await
  }

  pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SyncError> {
    Ok(self.files.get(path).await?)
  }

  pub async fn read_file_meta(&self, path: &str) -> Result<Option<FileMeta>, SyncError> {
    let Some(bytes) = self.meta.get(&file_key(path)).await? else {
      return Ok(None);
    };
    Ok(serde_json::from_slice(&bytes).ok())
  }

  /// A hub's current checkpoint, for clients that long-poll without pulling.
  pub async fn checkpoint(&self, hub_id: &str) -> Result<Option<String>, SyncError> {
    Ok(
      self
        .meta
        .get(&checkpoint_key(hub_id))
        .await?
        .and_then(|b| String::from_utf8(b).ok()),
    )
  }

  /// Every path with stored metadata, for reconciling deletions that
  /// happened while the client was off.
  pub async fn list_file_paths(&self) -> Result<Vec<String>, SyncError> {
    Ok(
      self
        .meta
        .list_keys("file/")
        .await?
        .into_iter()
        .filter_map(|k| k.strip_prefix("file/").map(str::to_string))
        .collect(),
    )
  }

  pub async fn pending(&self) -> Result<Vec<PendingChange>, SyncError> {
    self.load_pending().await
  }

  async fn load_pending(&self) -> Result<Vec<PendingChange>, SyncError> {
    let Some(bytes) = self.meta.get(KEY_PENDING).await? else {
      return Ok(Vec::new());
    };
    Ok(serde_json::from_slice(&bytes).unwrap_or_default())
  }

  async fn pending_contains(&self, path: &str) -> Result<bool, SyncError> {
    Ok(self.load_pending().await?.iter().any(|p| p.path == path))
  }

  async fn upsert_pending(&self, change: PendingChange) -> Result<(), SyncError> {
    let mut pending = self.load_pending().await?;
    pending.retain(|p| p.path != change.path);
    pending.push(change);
    self
      .meta
      .put(KEY_PENDING, serde_json::to_vec(&pending).unwrap())
      .await?;
    Ok(())
  }

  async fn bump_rev(&self, path: &str, rev: &str) -> Result<(), SyncError> {
    let key = file_key(path);
    let Some(bytes) = self.meta.get(&key).await? else {
      return Ok(());
    };
    let mut stored: FileMeta = serde_json::from_slice(&bytes).unwrap();
    stored.rev = rev.to_string();
    self
      .meta
      .put(&key, serde_json::to_vec(&stored).unwrap())
      .await?;
    Ok(())
  }

  async fn build_push_changes(
    &self,
    pending: &[PendingChange],
  ) -> Result<Vec<PushChange>, SyncError> {
    let mut out = Vec::with_capacity(pending.len());
    for p in pending {
      let (content_type, content_base64) = if p.deleted {
        (None, None)
      } else {
        let content = self.files.get(&p.path).await?.unwrap_or_default();
        let meta = self
          .meta
          .get(&file_key(&p.path))
          .await?
          .and_then(|b| serde_json::from_slice::<FileMeta>(&b).ok());
        let content_type = meta
          .as_ref()
          .map(|m| m.content_type.clone())
          .filter(|c| !c.is_empty())
          .or_else(|| p.content_type.clone())
          .unwrap_or_else(|| "application/octet-stream".to_string());
        (Some(content_type), Some(STANDARD.encode(&content)))
      };

      out.push(PushChange {
        path: p.path.clone(),
        deleted: p.deleted,
        base_rev: p.base_rev.clone(),
        mtime: p.mtime,
        content_type,
        content_base64,
      });
    }
    Ok(out)
  }

  fn notify(&self, error: SyncError) -> SyncError {
    if let Some(notifier) = &self.notifier {
      notifier.notify_error(&error);
    }
    error
  }

  fn report<T>(&self, result: Result<T, SyncError>) -> Result<T, SyncError> {
    match result {
      Ok(v) => Ok(v),
      Err(e) => Err(self.notify(e)),
    }
  }

  fn no_hubs() -> SyncError {
    SyncError::Hub(HubError::Api {
      status: 0,
      body: "no hubs configured".to_string(),
    })
  }
}

fn encode_meta(file: &FileContent) -> Vec<u8> {
  serde_json::to_vec(&FileMeta {
    rev: file.rev.clone(),
    mtime: file.mtime,
    content_type: file.content_type.clone(),
  })
  .unwrap()
}
