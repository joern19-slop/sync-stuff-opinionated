//! Hub-side conflict resolver (Build Order Stage 5 + 6).
//!
//! Lives entirely on the hub, driven by the change watcher. Clients never
//! see an unresolved conflict - they only observe the outcome as an ordinary
//! change. Every write is CAS-conditioned on the revision the resolver read;
//! a `409` means the doc moved underneath us (a third write landed, or the
//! other hub resolved first), so we re-read and retry. Both hubs run the
//! same deterministic algorithm, so when they race on the same conflict one
//! wins the CAS and the other sees "already resolved" and stops.

use crate::couch::{CouchClient, Revisions};
use crate::error::CouchError;
use crate::diff3;

const MAX_RETRIES: usize = 5;

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
  #[error("couchdb error: {0}")]
  Couch(#[from] CouchError),
  #[error("write hit a revision conflict; retry")]
  CasConflict,
  #[error("conflict resolution exceeded retry limit")]
  MaxRetries,
  #[error("leaf revision vanished while resolving")]
  MissingLeaf,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
  NoConflict,
  Resolved(Resolved),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Resolved {
  pub path: String,
  /// How the resolution happened, for logging/tests.
  pub kind: ResolutionKind,
  /// Paths of any `.conflict-*` copies written (content that would
  /// otherwise have been overwritten, preserved as separate files).
  pub conflict_copies: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResolutionKind {
  Merged,
  KeptNewer,
  EditWon,
  KeptWinner,
}

struct Leaf {
  rev: String,
  deleted: bool,
  mtime: i64,
  content_type: String,
  content: Vec<u8>,
  revisions: Option<Revisions>,
}

struct Plan {
  kind: ResolutionKind,
  winner_rev: String,
  /// `None` means "no content" (every leaf is a tombstone).
  final_content: Option<Vec<u8>>,
  final_mtime: i64,
  final_content_type: String,
  /// Content to preserve as `.conflict-*` copies (the losing side of a
  /// failed merge, or extra leaves in an N-way conflict).
  copies: Vec<Copy>,
  /// Leaf revs to CAS-delete from the tree (everything except the winner).
  loser_revs: Vec<String>,
}

struct Copy {
  content: Vec<u8>,
  mtime: i64,
  content_type: String,
}

/// Resolve a conflict on `path` if one exists. Returns `NoConflict` when the
/// doc is unconflicted, deleted, or missing. Idempotent and safe to call on
/// every hub for every change.
pub async fn resolve(couch: &CouchClient, path: &str) -> Result<Outcome, ResolveError> {
  for _ in 0..MAX_RETRIES {
    let Some(winner) = couch.get_doc(path).await? else {
      // Winning rev is a tombstone (or the doc is missing). Could be a
      // delete-winner edit-vs-delete conflict, which `get_doc` can't
      // see; check for a hidden edit leaf.
      return resolve_delete_winner(couch, path).await;
    };

    let winner_rev = winner["_rev"].as_str().unwrap_or_default().to_string();
    let conflict_revs: Vec<String> = winner
      .get("_conflicts")
      .and_then(|c| c.as_array())
      .map(|arr| {
        arr
          .iter()
          .filter_map(|v| v.as_str().map(String::from))
          .collect()
      })
      .unwrap_or_default();

    if conflict_revs.is_empty() {
      return Ok(Outcome::NoConflict);
    }

    let mut revs = vec![winner_rev.clone()];
    revs.extend(conflict_revs);

    let mut leaves = Vec::with_capacity(revs.len());
    for rev in &revs {
      leaves.push(fetch_leaf(couch, path, rev).await?);
    }

    let plan = plan_resolution(couch, path, &leaves, &winner_rev).await?;

    match apply_plan(couch, path, &plan).await {
      Ok(copies) => {
        return Ok(Outcome::Resolved(Resolved {
          path: path.to_string(),
          kind: plan.kind,
          conflict_copies: copies,
        }));
      }
      Err(ResolveError::CasConflict) => continue,
      Err(e) => return Err(e),
    }
  }

  Err(ResolveError::MaxRetries)
}

async fn fetch_leaf(couch: &CouchClient, path: &str, rev: &str) -> Result<Leaf, ResolveError> {
  let doc = couch
    .get_doc_at_rev(path, rev)
    .await?
    .ok_or(ResolveError::MissingLeaf)?;
  leaf_from_doc(couch, path, &doc).await
}

async fn leaf_from_doc(
  couch: &CouchClient,
  path: &str,
  doc: &serde_json::Value,
) -> Result<Leaf, ResolveError> {
  let rev = doc["_rev"].as_str().unwrap_or_default().to_string();
  let deleted = doc["_deleted"].as_bool().unwrap_or(false);
  let mtime = doc["mtime"].as_i64().unwrap_or_default();
  let content_type = doc["content_type"]
    .as_str()
    .unwrap_or("application/octet-stream")
    .to_string();
  let content = if deleted {
    Vec::new()
  } else {
    couch
      .get_attachment_at_rev(path, "content", Some(&rev))
      .await?
      .to_vec()
  };
  let revisions = doc
    .get("_revisions")
    .cloned()
    .and_then(|r| serde_json::from_value::<Revisions>(r).ok());

  Ok(Leaf {
    rev,
    deleted,
    mtime,
    content_type,
    content,
    revisions,
  })
}

/// Handles the delete-winner edit-vs-delete case: the winning rev is a
/// tombstone so `get_doc` 404s, but `open_revs=all` reveals a losing edit
/// leaf. Per the plan, the edit wins unconditionally - we resurrect it on top
/// of the winning tombstone and CAS-delete the other leaves.
async fn resolve_delete_winner(couch: &CouchClient, path: &str) -> Result<Outcome, ResolveError> {
  for _ in 0..MAX_RETRIES {
    let raws = couch.get_doc_leaves(path).await?;
    if raws.len() < 2 {
      return Ok(Outcome::NoConflict);
    }

    let mut leaves = Vec::with_capacity(raws.len());
    for raw in &raws {
      leaves.push(leaf_from_doc(couch, path, raw).await?);
    }

    let edits: Vec<&Leaf> = leaves.iter().filter(|l| !l.deleted).collect();
    let tombstones: Vec<&Leaf> = leaves.iter().filter(|l| l.deleted).collect();
    if edits.is_empty() || tombstones.is_empty() {
      // No hidden edit, or no tombstone to write on top of.
      return Ok(Outcome::NoConflict);
    }

    let edit = edits.iter().max_by_key(|l| l.mtime).copied().unwrap();
    let winner_tombstone = tombstones
      .iter()
      .max_by_key(|l| rev_generation(&l.rev))
      .copied()
      .unwrap();

    let plan = Plan {
      kind: ResolutionKind::EditWon,
      winner_rev: winner_tombstone.rev.clone(),
      final_content: Some(edit.content.clone()),
      final_mtime: edit.mtime,
      final_content_type: edit.content_type.clone(),
      copies: vec![],
      loser_revs: leaves
        .iter()
        .filter(|l| l.rev != winner_tombstone.rev)
        .map(|l| l.rev.clone())
        .collect(),
    };

    match apply_plan(couch, path, &plan).await {
      Ok(copies) => {
        return Ok(Outcome::Resolved(Resolved {
          path: path.to_string(),
          kind: plan.kind,
          conflict_copies: copies,
        }));
      }
      Err(ResolveError::CasConflict) => continue,
      Err(e) => return Err(e),
    }
  }

  Err(ResolveError::MaxRetries)
}

fn rev_generation(rev: &str) -> u64 {
  rev
    .split('-')
    .next()
    .and_then(|s| s.parse().ok())
    .unwrap_or(0)
}

async fn plan_resolution(
  couch: &CouchClient,
  path: &str,
  leaves: &[Leaf],
  winner_rev: &str,
) -> Result<Plan, ResolveError> {
  let winner = leaves
    .iter()
    .find(|l| l.rev == winner_rev)
    .ok_or(ResolveError::MissingLeaf)?;
  let losers: Vec<&Leaf> = leaves.iter().filter(|l| l.rev != winner_rev).collect();
  let loser_revs: Vec<String> = losers.iter().map(|l| l.rev.clone()).collect();
  let non_deleted: Vec<&Leaf> = leaves.iter().filter(|l| !l.deleted).collect();

  // Pure edit-vs-edit: two leaves, both alive.
  if non_deleted.len() == 2 && leaves.len() == 2 {
    let other = non_deleted
      .iter()
      .find(|l| l.rev != winner.rev)
      .copied()
      .unwrap();
    return plan_edit_vs_edit(couch, path, winner, other, loser_revs).await;
  }

  // Single non-deleted leaf => edit-vs-delete (or N-way with one survivor):
  // the edit wins unconditionally, nothing is lost, no conflict copy.
  if non_deleted.len() == 1 {
    let edit = non_deleted[0];
    return Ok(Plan {
      kind: ResolutionKind::EditWon,
      winner_rev: winner_rev.to_string(),
      final_content: Some(edit.content.clone()),
      final_mtime: edit.mtime,
      final_content_type: edit.content_type.clone(),
      copies: vec![],
      loser_revs,
    });
  }

  // Anything else (3+ alive leaves, or all tombstones): keep the winner if
  // alive, else the first alive leaf, else nothing. Preserve every other
  // alive leaf as a conflict copy. Conservative, but never loses data.
  let keep = if !winner.deleted {
    Some(winner)
  } else {
    non_deleted.first().copied()
  };
  let keep_rev = keep.map(|k| k.rev.as_str()).unwrap_or(winner_rev);

  let copies: Vec<Copy> = leaves
    .iter()
    .filter(|l| !l.deleted && l.rev != keep_rev)
    .map(|l| Copy {
      content: l.content.clone(),
      mtime: l.mtime,
      content_type: l.content_type.clone(),
    })
    .collect();

  Ok(Plan {
    kind: ResolutionKind::KeptWinner,
    winner_rev: winner_rev.to_string(),
    final_content: keep.map(|k| k.content.clone()),
    final_mtime: keep.map(|k| k.mtime).unwrap_or_default(),
    final_content_type: keep
      .map(|k| k.content_type.clone())
      .unwrap_or_else(|| "application/octet-stream".to_string()),
    copies,
    loser_revs,
  })
}

async fn plan_edit_vs_edit(
  couch: &CouchClient,
  path: &str,
  winner: &Leaf,
  loser: &Leaf,
  loser_revs: Vec<String>,
) -> Result<Plan, ResolveError> {
  let winner_rev = winner.rev.clone();

  // Determine the common ancestor. When the two leaves share no history
  // (e.g. two devices independently created the same new path), fall back
  // to an empty base: diff3 against empty content merges identical content
  // and conflicts otherwise - the "edit-vs-edit with empty base" rule for
  // new-file collisions.
  let base = match (&winner.revisions, &loser.revisions) {
    (Some(a), Some(b)) => match diff3::common_ancestor(&a.ids, b.start, &b.ids) {
      Some(rev) => couch
        .get_attachment_at_rev(path, "content", Some(&rev))
        .await
        .map(|b| b.to_vec())
        .unwrap_or_default(),
      None => Vec::new(),
    },
    _ => Vec::new(),
  };

  if let Ok(merged) = diff3::merge(&base, &winner.content, &loser.content) {
    return Ok(Plan {
      kind: ResolutionKind::Merged,
      winner_rev,
      final_content: Some(merged),
      final_mtime: winner.mtime.max(loser.mtime),
      final_content_type: winner.content_type.clone(),
      copies: vec![],
      loser_revs,
    });
  }

  // Merge failed (or no ancestor): keep the newer file, preserve the older
  // one as a `.conflict-*` copy.
  let (newer, older) = if winner.mtime >= loser.mtime {
    (winner, loser)
  } else {
    (loser, winner)
  };

  Ok(Plan {
    kind: ResolutionKind::KeptNewer,
    winner_rev,
    final_content: Some(newer.content.clone()),
    final_mtime: newer.mtime,
    final_content_type: newer.content_type.clone(),
    copies: vec![Copy {
      content: older.content.clone(),
      mtime: older.mtime,
      content_type: older.content_type.clone(),
    }],
    loser_revs,
  })
}

async fn apply_plan(
  couch: &CouchClient,
  path: &str,
  plan: &Plan,
) -> Result<Vec<String>, ResolveError> {
  let mut written = Vec::new();

  // 1. Preserve losing content as `.conflict-*` files before overwriting.
  for copy in &plan.copies {
    let copy_path = conflict_copy_path(couch, path).await?;
    let doc = couch
      .put_doc(
        &copy_path,
        None,
        serde_json::json!({
            "path": copy_path,
            "mtime": copy.mtime,
            "content_type": copy.content_type,
        }),
      )
      .await
      .map_err(map_couch)?;
    couch
      .put_attachment(
        &copy_path,
        &doc.rev,
        &copy.content_type,
        copy.content.clone().into(),
      )
      .await
      .map_err(map_couch)?;
    written.push(copy_path);
  }

  // 2. Write the resolved content on top of the current winner.
  if let Some(content) = &plan.final_content {
    let doc = couch
      .put_doc(
        path,
        Some(&plan.winner_rev),
        serde_json::json!({
            "path": path,
            "mtime": plan.final_mtime,
            "content_type": plan.final_content_type,
        }),
      )
      .await
      .map_err(map_couch)?;
    couch
      .put_attachment(
        path,
        &doc.rev,
        &plan.final_content_type,
        content.clone().into(),
      )
      .await
      .map_err(map_couch)?;
  }

  // 3. CAS-delete every losing leaf so `_conflicts` clears. CouchDB never
  // clears the flag on its own, even after a new winner exists.
  for rev in &plan.loser_revs {
    couch.delete_doc(path, rev).await.map_err(map_couch)?;
  }

  Ok(written)
}

async fn conflict_copy_path(couch: &CouchClient, base_path: &str) -> Result<String, ResolveError> {
  let date = time::OffsetDateTime::now_utc().date();
  let today = format!(
    "{:02}.{:02}.{}",
    date.day(),
    u8::from(date.month()),
    date.year()
  );

  let mut n = 0u32;
  loop {
    let candidate = match n {
      0 => format!("{base_path}.conflict-{today}"),
      k => format!("{base_path}.conflict-{today}.{}", k - 1),
    };
    if couch.get_doc(&candidate).await?.is_none() {
      return Ok(candidate);
    }
    n += 1;
  }
}

fn map_couch(e: CouchError) -> ResolveError {
  match e {
    CouchError::RevConflict(_) => ResolveError::CasConflict,
    other => ResolveError::Couch(other),
  }
}
