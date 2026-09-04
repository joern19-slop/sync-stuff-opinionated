use axum::extract::State;
use axum::Json;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::json;
use std::sync::Arc;
use sync_core::{CouchError, PushChange, PushResult, PushStatus, Revisions};

use crate::resolver;
use crate::state::AppState;

/// `POST /changes` - push a batch of local changes.
///
/// The happy path is plain optimistic concurrency: write conditioned on the
/// client's `base_rev`. A stale `base_rev` (someone else changed the path
/// first) is *not* rejected outright - instead the hub branches the revision
/// tree (writes the client's edit as a new leaf via `new_edits:false`) and
/// resolves the resulting conflict in-process, so the client always sees a
/// resolved outcome and never has to merge anything itself. This is what
/// keeps the "clients never see an unresolved conflict" guarantee true even
/// when two devices reach the *same* hub.
pub async fn post_changes(
  State(state): State<Arc<AppState>>,
  Json(pushes): Json<Vec<PushChange>>,
) -> Json<Vec<PushResult>> {
  let mut results = Vec::with_capacity(pushes.len());
  for change in pushes {
    results.push(apply_one(&state, change).await);
  }
  Json(results)
}

enum ApplyError {
  Conflict,
  Couch(CouchError),
}

impl From<CouchError> for ApplyError {
  fn from(e: CouchError) -> Self {
    match e {
      CouchError::RevConflict(_) => ApplyError::Conflict,
      other => ApplyError::Couch(other),
    }
  }
}

async fn apply_one(state: &AppState, change: PushChange) -> PushResult {
  let path = change.path.clone();

  let outcome = if change.deleted {
    apply_delete(state, &change).await
  } else {
    apply_upsert(state, &change).await
  };

  match outcome {
    Ok(rev) => PushResult {
      path,
      status: PushStatus::Ok { rev },
    },
    Err(ApplyError::Conflict) => PushResult {
      path,
      status: PushStatus::Conflict,
    },
    Err(ApplyError::Couch(e)) => {
      // Surface as a conflict so the client re-pulls; operators get the
      // real error from the hub's own logs.
      tracing::error!(path = %change.path, error = %e, "push failed");
      PushResult {
        path,
        status: PushStatus::Conflict,
      }
    }
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

/// Branches the revision tree with the client's change (an edit or a
/// tombstone) and resolves the resulting conflict in-process. Returns the
/// current *winning* revision so the client converges immediately rather than
/// holding a reference to the transient branch leaf it just created.
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

  // Deterministic leaf hash so a retry of the same push branches to the
  // same revision instead of piling up duplicate leaves.
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

  state
    .couch
    .put_revision(doc)
    .await
    .map_err(ApplyError::from)?;

  if let Err(e) = resolver::resolve(&state.couch, path).await {
    // The branch is still in the tree; the watcher (or the client's next
    // retry) will resolve it. Report conflict so the client keeps its
    // local change and doesn't trust a stale rev.
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
