//! Reconciliation: bring the sync engine's view of the directory up to date
//! with what's actually on disk, then sync.
//!
//! This runs at startup (to catch changes made while the client was off) and
//! after every debounced inotify signal. It is idempotent: it detects change
//! by comparing each file's mtime (seconds) against the sync metadata, and
//! the engine writes pulled files back with that same mtime, so the client's
//! own writes don't show up as new edits on the next pass.

use std::collections::HashMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use client_core::SyncEngine;
use tracing::{debug, warn};

/// Scans the directory, records local upserts/deletes, then pushes and pulls.
pub async fn reconcile_and_sync(engine: &SyncEngine, root: &Path) -> Result<()> {
    let local = scan(root)?;

    for (path, mtime) in &local {
        let known_mtime = engine.read_file_meta(path).await?.map(|m| m.mtime);
        let changed = known_mtime.map(|k| k != *mtime).unwrap_or(true);
        if !changed {
            continue;
        }

        let content = match tokio::fs::read(root.join(path)).await {
            Ok(c) => c,
            // The file vanished between the scan and the read; the next pass
            // will pick up its deletion.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let content_type = mime_guess::from_path(path)
            .first_or_octet_stream()
            .essence_str()
            .to_string();
        engine
            .record_upsert(path, *mtime, &content_type, &content)
            .await?;
        debug!(path, "recorded local change");
    }

    for path in engine.list_file_paths().await? {
        if !local.contains_key(&path) {
            engine.record_delete(&path, now_secs()).await?;
            debug!(path, "recorded local deletion");
        }
    }

    let report = engine.sync().await?;
    debug!(
        pushed = report.push.pushed,
        pulled = report.pull.pulled,
        deleted = report.pull.deleted,
        "sync complete"
    );
    Ok(())
}

/// Walks the directory, returning regular files as `(relative path, mtime
/// seconds)`. Directories, symlinks, and non-UTF-8 names are skipped.
fn scan(root: &Path) -> Result<HashMap<String, i64>> {
    let mut out = HashMap::new();
    scan_dir(root, root, &mut out)?;
    Ok(out)
}

fn scan_dir(root: &Path, dir: &Path, out: &mut HashMap<String, i64>) -> Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            warn!(dir = %dir.display(), error = %e, "read_dir failed");
            return Ok(());
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let Ok(file_type) = entry.file_type() else { continue };
        if file_type.is_dir() {
            scan_dir(root, &path, out)?;
        } else if file_type.is_file() {
            let Some(rel) = path.strip_prefix(root).ok().and_then(|p| p.to_str()) else {
                warn!(path = %path.display(), "skipping non-UTF-8 path");
                continue;
            };
            let mtime = std::fs::metadata(&path)
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            out.insert(rel.to_string(), mtime);
        }
    }
    Ok(())
}

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
