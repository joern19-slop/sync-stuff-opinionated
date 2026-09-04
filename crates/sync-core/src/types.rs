//! Wire types for the client-facing Hub Sync API.
//!
//! These are deliberately separate from the raw CouchDB JSON shapes (see
//! `couch.rs`) - clients never see CouchDB's document/revision model
//! directly, only this contract. Keeping it in `sync-core` means the hub
//! and the future Rust/WASM client share one definition instead of two
//! independently-drifting copies.

use serde::{Deserialize, Serialize};

/// Opaque continuation token. Callers must not parse or compare it - just
/// store the last one seen and send it back as `since` on the next call.
/// Currently backed 1:1 by CouchDB's `_changes` `seq`, but that's an
/// implementation detail callers shouldn't rely on.
pub type Checkpoint = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEntry {
  pub path: String,
  pub deleted: bool,
  /// Opaque revision tag for this path's current state. Used by the
  /// client as `base_rev` on a subsequent `POST /changes` for that path.
  pub rev: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangesResponse {
  pub changes: Vec<ChangeEntry>,
  pub checkpoint: Checkpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
  pub path: String,
  pub rev: String,
  pub content_type: String,
  pub size: u64,
  /// Unix seconds. Client-supplied on push, echoed back on read - the hub
  /// does not trust wall-clock time from itself for this field so that
  /// conflict resolution's "keep the newer file by mtime" rule (see the
  /// architecture doc) is driven by the client's view of edit time, not
  /// upload time.
  pub mtime: i64,
}

/// One local change a client wants to push. `base_rev` is the revision the
/// client last observed for this path (`None` if the client believes the
/// path doesn't exist on the hub yet, e.g. a brand new file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushChange {
  pub path: String,
  pub deleted: bool,
  pub base_rev: Option<String>,
  pub mtime: i64,
  /// Present unless `deleted` is true.
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub content_type: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub content_base64: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PushStatus {
  /// Write applied. `rev` is the new current revision for this path.
  Ok { rev: String },
  /// `base_rev` was stale - someone else changed this path first. No
  /// merge logic exists yet at this stage (see Stage 5 in the build
  /// order); the client should just re-pull via `/changes` and decide
  /// whether to retry.
  Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushResult {
  pub path: String,
  #[serde(flatten)]
  pub status: PushStatus,
}
