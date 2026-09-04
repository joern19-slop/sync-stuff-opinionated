//! The client sync engine: pull hub changes into the local store with a
//! durable checkpoint, and push locally-recorded changes back up.
//!
//! The engine owns no merge logic - per the architecture plan it only ever
//! sees the hub's *resolved* view of the world. Its safety guarantees are:
//!
//! 1. A checkpoint is only advanced after the whole batch it covers has been
//!    written durably (so a crash mid-pull re-pulls the same batch - the
//!    writes are idempotent).
//! 2. A push that the hub rejects (`Conflict`) is never dropped: the local
//!    change stays queued and is reported to the caller, which decides what
//!    to do (the exact client UX is deferred, see NOTES.md).
//!
//! Multi-hub failover (build order Stage 7): the engine holds an ordered list
//! of hubs and tries them in turn. Checkpoints are keyed per hub because a
//! CouchDB `seq` is node-local - two hubs are replicas but do not share `seq`
//! values. Revisions, by contrast, are CouchDB revision hashes and *are*
//! consistent across replicas, so the pending-push queue is global.

use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use sync_core::{ChangeEntry, PushChange, PushResult, PushStatus};
use thiserror::Error;

use crate::hub::{FileContent, HubClient, HubError};
use crate::notify::Notifier;
use crate::store::{FileStore, MetaStore, StoreError};

/// Key holding the serialized pending-push queue.
pub const KEY_PENDING: &str = "pending";

/// Key for a file's stored metadata.
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

/// Per-file metadata stored locally: the revision to use as `base_rev` on the
/// next push, plus the mtime/content-type needed to push it back correctly.
/// The content itself lives in the [`FileStore`], not here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
  pub rev: String,
  pub mtime: i64,
  pub content_type: String,
}

/// One local change waiting to be pushed. Content is *not* duplicated here -
/// it lives in the [`FileStore`] and is read at push time.
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
  /// Id of the hub that served this pull.
  pub hub: String,
  pub pulled: usize,
  pub deleted: usize,
  /// Paths whose incoming change was skipped because a local (not-yet-
  /// pushed) edit exists for the same path. Surfaces a local-vs-remote
  /// conflict to the caller without losing either side.
  pub local_conflicts: Vec<String>,
  pub checkpoint: Option<String>,
}

#[derive(Debug, Default)]
pub struct PushReport {
  /// Id of the hub that accepted this push.
  pub hub: String,
  pub pushed: usize,
  /// Paths the hub rejected with `Conflict` (kept queued locally).
  pub conflicts: Vec<String>,
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

  /// Attach a [`Notifier`] that the engine calls whenever it returns an
  /// unexpected error (a hub that's down after every fallback, a local
  /// store failure, ...) so the app can show it to the user.
  pub fn with_notifier(mut self, notifier: Arc<dyn Notifier>) -> Self {
    self.notifier = Some(notifier);
    self
  }

  /// Push, then pull - the app layer calls this on any wake trigger (FCM
  /// push, app foreground-open, charging-started).
  ///
  /// Push first, then pull: pushing can trigger a hub-side merge (when the
  /// push is based on a stale revision), so the authoritative content is
  /// only known *after* the push. Pulling last converges the local copy
  /// with the hub's resolved result in the same cycle.
  pub async fn sync(&self) -> Result<SyncReport, SyncError> {
    let push = self.push().await?;
    let pull = self.pull().await?;
    Ok(SyncReport { pull, push })
  }

  /// Pulls remote changes, advancing the checkpoint only once the whole
  /// batch is durable.
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

    // The batch is fully durable; only now is it safe to remember where
    // we got to. If this write fails, the batch just re-pulls next time.
    self.meta.put(&ck_key, resp.checkpoint.into_bytes()).await?;

    Ok(report)
  }

  async fn apply_remote(
    &self,
    hub: &HubClient,
    entry: &ChangeEntry,
    report: &mut PullReport,
  ) -> Result<(), SyncError> {
    // A pending local change means we have unsynced local work for this
    // path; overwriting it with the remote version would lose it. Skip
    // and flag it for the caller instead.
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

  /// Pushes the pending queue, clearing entries only on an explicit `Ok`
  /// from the hub.
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

    let mut still_pending = Vec::new();
    for (change, result) in pending.iter().zip(&results) {
      match result {
        PushResult {
          status: PushStatus::Ok { rev },
          ..
        } => {
          // The hub now has our content. For an upsert, record the
          // hub's new revision as the local base for next time; for
          // a delete the local file is already gone.
          if !change.deleted {
            self.bump_rev(&change.path, rev).await?;
          }
          report.pushed += 1;
        }
        PushResult {
          status: PushStatus::Conflict,
          ..
        } => {
          // Keep the change queued - do not lose local work.
          report.conflicts.push(change.path.clone());
          still_pending.push(change.clone());
        }
      }
    }

    // Persist the queue with accepted entries removed (conflicted ones
    // remain). If a hub returned fewer results than we sent, that's a
    // protocol violation - keep everything queued to stay safe.
    if results.len() == changes.len() {
      self
        .meta
        .put(KEY_PENDING, serde_json::to_vec(&still_pending).unwrap())
        .await?;
    }

    Ok(report)
  }

  /// Record a local create/edit: persist content + metadata durably, then
  /// queue a push based on the previous revision (so the hub can CAS-check
  /// it). Coalesces per path - recording twice keeps one queued entry.
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

  /// Record a local delete: remove the file durably, then queue a delete.
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

  /// Reads a file's raw content back from the file store.
  pub async fn read_file(&self, path: &str) -> Result<Option<Vec<u8>>, SyncError> {
    Ok(self.files.get(path).await?)
  }

  /// Reads a file's stored metadata (revision / mtime / content type).
  pub async fn read_file_meta(&self, path: &str) -> Result<Option<FileMeta>, SyncError> {
    let Some(bytes) = self.meta.get(&file_key(path)).await? else {
      return Ok(None);
    };
    Ok(serde_json::from_slice(&bytes).ok())
  }

  /// Reads a hub's current checkpoint, if any (used by a client that wants
  /// to long-poll for changes without pulling them itself).
  pub async fn checkpoint(&self, hub_id: &str) -> Result<Option<String>, SyncError> {
    Ok(
      self
        .meta
        .get(&checkpoint_key(hub_id))
        .await?
        .and_then(|b| String::from_utf8(b).ok()),
    )
  }

  /// Every path the engine currently has metadata for (i.e. every file it
  /// believes exists locally). Used by a client to reconcile deletions that
  /// happened while it was not running.
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

  /// The currently-queued local changes (for observability/testing).
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

  /// Reports an unexpected error to the configured notifier (if any) and
  /// returns it unchanged, so callers can `notify` and `return`/`?` in one
  /// step.
  fn notify(&self, error: SyncError) -> SyncError {
    if let Some(notifier) = &self.notifier {
      notifier.notify_error(&error);
    }
    error
  }

  /// Notifies on an `Err` and passes the result through unchanged.
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
