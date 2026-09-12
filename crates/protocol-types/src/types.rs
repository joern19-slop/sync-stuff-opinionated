//! The client-facing Hub Sync API's wire types.
//!
//! These are deliberately separate from the raw CouchDB JSON shapes (see
//! `hub-api`'s `couch` module) - clients never see CouchDB's document/revision
//! model directly, only this contract.

use serde::{Deserialize, Serialize};

/// Opaque continuation token. Callers must not parse or compare it - store
/// the last one seen and send it back as `since`. Backed by CouchDB's
/// `_changes` `seq`, an implementation detail callers shouldn't rely on.
pub type Checkpoint = String;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangeEntry {
  pub path: String,
  pub deleted: bool,
  /// This path's current state; the client sends it back as `base_rev`.
  pub rev: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangesResponse {
  pub changes: Vec<ChangeEntry>,
  pub checkpoint: Checkpoint,
}

/// One local change a client wants to push. `base_rev` is the revision the
/// client last observed (`None` when it believes the path isn't on the hub
/// yet, e.g. a brand-new file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushChange {
  pub path: String,
  pub deleted: bool,
  pub base_rev: Option<String>,
  pub mtime: i64,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub content_type: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")]
  pub content_base64: Option<String>,
}

/// The hub's acknowledgement of one pushed change. `rev` is the path's new
/// current revision (after the write, or after branching + resolving a stale
/// `base_rev`). A change the hub can't apply surfaces as an HTTP error on the
/// whole request, never as a per-item status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushResult {
  pub path: String,
  pub rev: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
  // Passed through from couchdb. Unique Id of the server.
  pub uuid: String,
}
