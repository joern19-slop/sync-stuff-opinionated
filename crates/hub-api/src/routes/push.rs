use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::json;
use std::sync::Arc;
use protocol_types::{PushChange, PushResult};

use crate::couch::Revisions;
use crate::error::{ApiError, CouchError};

use crate::resolver;
use crate::state::AppState;

/// `POST /changes` - push a batch of local changes.
///
/// Writes are conditioned on the client's `base_rev`. A stale `base_rev` is
/// not rejected outright: the hub *branches* the revision tree with the
/// client's edit and resolves the conflict in-process, so the client always
/// sees a resolved outcome and never has to merge - even when two devices
/// reach the same hub.
pub async fn post_changes(
  State(state): State<Arc<AppState>>,
  Json(pushes): Json<Vec<PushChange>>,
) -> Result<Json<Vec<PushResult>>, ApiError> {
  let mut results = Vec::with_capacity(pushes.len());
  for change in pushes {
    results.push(apply_one(&state, change).await?);
  }
  Ok(Json(results))
}

enum ApplyError {
  Conflict,
  Couch(CouchError),
}

impl From<CouchError> for ApplyError {
  fn from(e: CouchError) -> Self {
    ApplyError::Couch(e)
  }
}

impl ApplyError {
  /// `Conflict` (bogus `base_rev`, blind delete) is the client's mistake ->
  /// 409; CouchDB failures reuse `From<CouchError>` (409 rev conflict, 502
  /// backend/transport).
  fn into_api(self, path: &str) -> ApiError {
    match self {
      ApplyError::Conflict => ApiError(
        StatusCode::CONFLICT,
        format!("cannot apply change to {path}"),
      ),
      ApplyError::Couch(e) => {
        tracing::error!(path, error = %e, "push failed");
        e.into()
      }
    }
  }
}

async fn apply_one(state: &AppState, change: PushChange) -> Result<PushResult, ApiError> {
  let path = change.path.clone();
  let rev = apply(state, &change).await.map_err(|e| e.into_api(&path))?;
  Ok(PushResult { path, rev })
}

async fn apply(state: &AppState, change: &PushChange) -> Result<String, ApplyError> {
  if change.deleted {
    apply_delete(state, change).await
  } else {
    apply_upsert(state, change).await
  }
}

async fn apply_delete(state: &AppState, change: &PushChange) -> Result<String, ApplyError> {
  let Some(rev) = change.base_rev.as_deref() else {
    // No known rev to condition on. If the hub agrees the file doesn't
    // exist, the delete is a no-op; otherwise we can't safely delete
    // blind - tell the client to re-pull first.
    return match state.couch.get_doc(&change.path).await? {
      None => Ok(String::new()),
      Some(_) => Err(ApplyError::Conflict),
    };
  };

  match state.couch.delete_doc(&change.path, rev).await {
    Ok(result) => Ok(result.rev),
    Err(CouchError::RevConflict(_)) => {
      // The path moved on - branch a competing tombstone and let the
      // resolver decide (edit-vs-delete: the edit wins).
      branch_and_resolve(state, change, true, None).await
    }
    Err(e) => Err(ApplyError::Couch(e)),
  }
}

async fn apply_upsert(state: &AppState, change: &PushChange) -> Result<String, ApplyError> {
  let content_type = change
    .content_type
    .clone()
    .unwrap_or_else(|| "application/octet-stream".to_string());
  let content: bytes::Bytes = STANDARD
    .decode(change.content_base64.as_deref().unwrap_or_default())
    .map_err(|e| ApplyError::Couch(CouchError::Decode(format!("bad base64: {e}"))))?
    .into();

  let doc_body = json!({
      "path": change.path,
      "mtime": change.mtime,
      "content_type": content_type,
  });

  match state
    .couch
    .put_doc(&change.path, change.base_rev.as_deref(), doc_body)
    .await
  {
    Ok(doc_result) => {
      let att_result = state
        .couch
        .put_attachment(&change.path, &doc_result.rev, &content_type, content)
        .await?;
      Ok(att_result.rev)
    }
    Err(CouchError::RevConflict(_)) => {
      branch_and_resolve(state, change, false, Some(&content)).await
    }
    Err(e) => Err(ApplyError::Couch(e)),
  }
}

/// Branch the tree with the client's change and resolve in-process; returns
/// the winning rev so the client converges instead of pinning its transient
/// branch leaf.
async fn branch_and_resolve(
  state: &AppState,
  change: &PushChange,
  deleted: bool,
  content: Option<&[u8]>,
) -> Result<String, ApplyError> {
  let path = &change.path;

  // The branch hangs off the client's claimed base. For a brand-new path
  // collision (base_rev None) it becomes an independent root, which the
  // resolver then merges against an empty common ancestor.
  let parent: Revisions = match change.base_rev.as_deref() {
    Some(base_rev) => {
      let doc = state
        .couch
        .get_doc_at_rev(path, base_rev)
        .await?
        .ok_or(ApplyError::Conflict)?;
      doc
        .get("_revisions")
        .cloned()
        .and_then(|v| serde_json::from_value(v).ok())
        .ok_or(ApplyError::Conflict)?
    }
    None => Revisions {
      start: 0,
      ids: vec![],
    },
  };

  // Deterministic leaf hash so a retried push branches to the same revision
  // instead of piling up duplicate leaves.
  let mut seed = Vec::new();
  seed.extend_from_slice(change.base_rev.as_deref().unwrap_or_default().as_bytes());
  seed.push(0);
  seed.push(if deleted { 1 } else { 0 });
  seed.extend_from_slice(&change.mtime.to_be_bytes());
  seed.extend_from_slice(content.unwrap_or_default());
  let hash = rev_hash(&seed);

  let new_start = parent.start + 1;
  let mut ids = vec![hash.clone()];
  ids.extend(parent.ids);

  let mut doc = json!({
      "_id": path,
      "_rev": format!("{new_start}-{hash}"),
      "_revisions": { "start": new_start, "ids": ids },
      "path": path,
      "mtime": change.mtime,
  });
  if deleted {
    doc["_deleted"] = true.into();
  } else {
    doc["content_type"] = change.content_type.clone().into();
    doc["_attachments"] = json!({
        "content": {
            "content_type": change.content_type.as_deref().unwrap_or("application/octet-stream"),
            "data": STANDARD.encode(content.unwrap_or_default()),
        }
    });
  }

  state.couch.put_revision(doc).await?;

  if let Err(e) = resolver::resolve(&state.couch, path).await {
    // The branch stays in the tree for the watcher/retry; report conflict so
    // the client keeps its local change and doesn't trust a stale rev.
    tracing::warn!(path = %path, error = %e, "post-branch resolve failed");
    return Err(ApplyError::Conflict);
  }

  let winner = state
    .couch
    .get_doc(path)
    .await?
    .ok_or(ApplyError::Conflict)?;
  Ok(winner["_rev"].as_str().unwrap_or_default().to_string())
}

/// FNV-1a 64-bit, hex-encoded. Deterministic (unlike `DefaultHasher`), which
/// is all we need for a stable branch revision hash.
fn rev_hash(input: &[u8]) -> String {
  let mut h: u64 = 0xcbf2_9ce4_8422_2325;
  for &b in input {
    h ^= b as u64;
    h = h.wrapping_mul(0x100_0000_01b3);
  }
  format!("{h:016x}")
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rev_hash_is_deterministic_and_sensitive() {
    assert_eq!(rev_hash(b"a"), rev_hash(b"a"));
    assert_ne!(rev_hash(b"a"), rev_hash(b"b"));
    assert_eq!(rev_hash(b"a").len(), 16);
  }
}
