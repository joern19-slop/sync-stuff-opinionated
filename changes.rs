use axum::extract::{Query, State};
use axum::Json;
use serde::Deserialize;
use std::sync::Arc;
use sync_core::{ChangeEntry, ChangesResponse};

use crate::{error::ApiError, state::AppState};

#[derive(Debug, Deserialize)]
pub struct ChangesQuery {
    pub since: Option<String>,
}

/// `GET /changes?since=<checkpoint>` - plain pass-through of CouchDB's
/// `_changes` feed, reshaped into the client-facing contract. No merge or
/// conflict-resolution logic here (Stage 5); a path with more than one
/// leaf revision still surfaces as a single entry using CouchDB's current
/// "winning" revision, same as `GET /file/{path}` would return.
pub async fn get_changes(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ChangesQuery>,
) -> Result<Json<ChangesResponse>, ApiError> {
    let raw = state.couch.changes(q.since.as_deref()).await?;

    let changes = raw
        .results
        .into_iter()
        // CouchDB system/design docs (ids starting with "_") are never
        // user files - never surface them to clients.
        .filter(|row| !row.id.starts_with('_'))
        .filter_map(|row| {
            row.changes.first().map(|rev| ChangeEntry {
                path: row.id,
                deleted: row.deleted,
                rev: rev.rev.clone(),
            })
        })
        .collect();

    Ok(Json(ChangesResponse {
        changes,
        checkpoint: seq_to_checkpoint(&raw.last_seq),
    }))
}

fn seq_to_checkpoint(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_to_checkpoint_unwraps_string_seqs_without_quoting() {
        assert_eq!(seq_to_checkpoint(&serde_json::json!("42-abc")), "42-abc");
    }

    #[test]
    fn seq_to_checkpoint_falls_back_to_json_repr_for_non_strings() {
        assert_eq!(seq_to_checkpoint(&serde_json::json!([42, "abc"])), "[42,\"abc\"]");
    }
}
