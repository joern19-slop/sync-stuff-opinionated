use axum::extract::State;
use axum::Json;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::json;
use std::sync::Arc;
use sync_core::{CouchError, PushChange, PushResult, PushStatus};

use crate::state::AppState;

/// `POST /changes` - push a batch of local changes. Each item is applied
/// independently and conditioned on the client's `base_rev` (plain
/// optimistic concurrency, the same CAS CouchDB gives every write - not to
/// be confused with the merge/conflict-resolution logic that lands in a
/// later stage). A `Conflict` result means "someone else changed this path
/// first"; the client is expected to re-pull via `GET /changes` and decide
/// what to do next.
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
        Ok(rev) => PushResult { path, status: PushStatus::Ok { rev } },
        Err(ApplyError::Conflict) => PushResult { path, status: PushStatus::Conflict },
        Err(ApplyError::Couch(e)) => {
            // Stage 2's PushStatus has no distinct "server error" variant
            // yet - surface as a conflict so the client re-pulls, and rely
            // on the hub's own logs for operators to notice. Worth a
            // dedicated variant once the client side exists to handle one.
            tracing::error!(path = %change.path, error = %e, "push failed");
            PushResult { path, status: PushStatus::Conflict }
        }
    }
}

async fn apply_delete(state: &AppState, change: &PushChange) -> Result<String, ApplyError> {
    let rev = match &change.base_rev {
        Some(r) => r.clone(),
        None => {
            // No known rev to condition on. If the hub agrees the file
            // doesn't exist, the delete is a no-op; otherwise we can't
            // safely delete blind - tell the client to re-pull first.
            return match state.couch.get_doc(&change.path).await? {
                None => Ok(String::new()),
                Some(_) => Err(ApplyError::Conflict),
            };
        }
    };
    let result = state.couch.delete_doc(&change.path, &rev).await?;
    Ok(result.rev)
}

async fn apply_upsert(state: &AppState, change: &PushChange) -> Result<String, ApplyError> {
    let content_type = change
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let bytes = STANDARD
        .decode(change.content_base64.as_deref().unwrap_or_default())
        .map_err(|e| ApplyError::Couch(CouchError::Decode(format!("bad base64: {e}"))))?;

    let doc_body = json!({
        "path": change.path,
        "mtime": change.mtime,
        "content_type": content_type,
    });

    // Two writes: doc metadata, then the attachment, each conditioned on
    // the revision returned by the previous one. If the process dies
    // between them the doc is left pointing at stale/no content until the
    // next push retries this path - acceptable for Stage 2, called out in
    // NOTES.md as worth collapsing into a single multipart write later.
    let doc_result = state
        .couch
        .put_doc(&change.path, change.base_rev.as_deref(), doc_body)
        .await?;

    let att_result = state
        .couch
        .put_attachment(&change.path, &doc_result.rev, &content_type, bytes.into())
        .await?;

    Ok(att_result.rev)
}
