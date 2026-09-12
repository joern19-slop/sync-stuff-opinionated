//! Background tasks owned by `run()`: a change watcher that resolves
//! conflicts (Stage 5/6) and wakes devices via FCM (Stage 3), and a
//! replication-health checker that reports hub-to-hub errors/staleness to a
//! Discord webhook.
//!
//! `build_app()` does *not* start these, so HTTP-only unit tests don't
//! inherit a background thread poking at their mock CouchDB.

use std::time::Duration;

use crate::couch::{CouchClient, RawChangesResponse, SchedulerJob};

use crate::config::Config;
use crate::notify::{DiscordClient, FcmClient};
use crate::resolver;

/// The change watcher always runs (it does conflict resolution); FCM
/// wakeups and Discord alerts only when their config is present.
pub fn spawn(couch: CouchClient, cfg: &Config) -> Vec<tokio::task::JoinHandle<()>> {
  let mut handles = Vec::new();

  let fcm = cfg
    .fcm_server_key
    .as_ref()
    .map(|key| FcmClient::new(key.clone()));
  let tokens = cfg.fcm_device_tokens.clone();
  let interval = Duration::from_secs(cfg.watcher_poll_secs.max(1));
  handles.push(tokio::spawn(change_watcher(
    couch.clone(),
    fcm,
    tokens,
    interval,
  )));

  if let Some(url) = &cfg.discord_webhook_url {
    let discord = DiscordClient::new(url.clone());
    let couch = couch.clone();
    let staleness = cfg.repl_staleness_secs;
    handles.push(tokio::spawn(replication_health_checker(
      couch, discord, staleness,
    )));
  }

  handles
}

/// Resolves conflicts and wakes devices per change, then long-polls again.
/// Starts from `since=now` so startup doesn't replay history as a burst of
/// wakeups. Long-polling (not `feed=continuous`) reuses `CouchClient` and
/// bounds latency by `interval`.
async fn change_watcher(
  couch: CouchClient,
  fcm: Option<FcmClient>,
  tokens: Vec<String>,
  interval: Duration,
) {
  let mut since = "now".to_string();

  loop {
    match couch.changes_with_docs(Some(&since)).await {
      Ok(resp) => {
        for row in &resp.results {
          if row.id.starts_with('_') {
            continue;
          }
          if has_conflict(&row.doc) {
            match resolver::resolve(&couch, &row.id).await {
              Ok(resolver::Outcome::Resolved(resolved)) => {
                tracing::info!(path = %row.id, kind = ?resolved.kind, copies = ?resolved.conflict_copies, "conflict resolved");
              }
              Ok(resolver::Outcome::NoConflict) => {}
              Err(e) => {
                tracing::error!(path = %row.id, error = %e, "conflict resolution failed");
              }
            }
          }
        }

        let paths = changed_paths(&resp);
        if !paths.is_empty() {
          if let Some(fcm) = &fcm {
            if let Err(e) = fcm.send_wakeup(&tokens).await {
              tracing::error!(error = %e, "fcm wakeup failed");
            }
          }
        }
        since = seq_to_checkpoint(&resp.last_seq);
      }
      Err(e) => {
        // Don't advance the checkpoint on error - changes landing while
        // we're down would be skipped.
        tracing::error!(error = %e, since = %since, "changes poll failed");
      }
    }

    tokio::time::sleep(interval).await;
  }
}

fn has_conflict(doc: &Option<serde_json::Value>) -> bool {
  doc
    .as_ref()
    .and_then(|d| d.get("_conflicts"))
    .and_then(|c| c.as_array())
    .is_some_and(|arr| !arr.is_empty())
}

fn changed_paths(resp: &RawChangesResponse) -> Vec<String> {
  resp
    .results
    .iter()
    .filter(|row| !row.id.starts_with('_'))
    .map(|row| row.id.clone())
    .collect()
}

/// Reports a job's error or staleness to Discord. Only what the hub itself
/// can observe about its own replications is reported.
async fn replication_health_checker(
  couch: CouchClient,
  discord: DiscordClient,
  staleness_secs: u64,
) {
  let check_interval = Duration::from_secs((staleness_secs / 2).max(10));

  loop {
    match couch.replication_jobs().await {
      Ok(jobs) => {
        for job in jobs {
          if let Some(problem) = describe_problem(&job, staleness_secs) {
            let message = format!(
              "replication `{}` {}: {} -> {}",
              job.id, problem, job.source, job.target
            );
            tracing::warn!("{message}");
            if let Err(e) = discord.send(&message).await {
              tracing::error!(error = %e, "discord alert failed");
            }
          }
        }
      }
      Err(e) => {
        tracing::error!(error = %e, "replication scheduler poll failed");
      }
    }

    tokio::time::sleep(check_interval).await;
  }
}

fn describe_problem(job: &SchedulerJob, staleness_secs: u64) -> Option<&'static str> {
  if let Some(err) = &job.info.error {
    if !err.trim().is_empty() {
      return Some("errored");
    }
  }
  if job
    .info
    .last_updated
    .as_deref()
    .is_some_and(|ts| last_updated_stale(ts, staleness_secs))
  {
    return Some("is stale");
  }
  None
}

/// Unparseable timestamps count as *not* stale - better to miss a report
/// than to spam Discord on a format change.
fn last_updated_stale(last_updated: &str, threshold_secs: u64) -> bool {
  use time::OffsetDateTime;
  use time::format_description::well_known::Rfc3339;

  let Ok(ts) = OffsetDateTime::parse(last_updated, &Rfc3339) else {
    return false;
  };
  let now = OffsetDateTime::now_utc();
  let elapsed = now - ts;
  elapsed.whole_seconds() > threshold_secs as i64
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
  use serde_json::json;

  #[test]
  fn changed_paths_drops_design_docs() {
    let resp: RawChangesResponse = serde_json::from_value(json!({
        "results": [
            {"id": "notes/a.txt", "deleted": false, "changes": [{"rev": "1-a"}]},
            {"id": "_design/foo", "deleted": false, "changes": [{"rev": "1-b"}]},
            {"id": "gone.txt", "deleted": true, "changes": [{"rev": "2-c"}]}
        ],
        "last_seq": "3"
    }))
    .unwrap();

    assert_eq!(changed_paths(&resp), vec!["notes/a.txt", "gone.txt"]);
  }

  #[test]
  fn seq_to_checkpoint_round_trips_string_and_array_forms() {
    assert_eq!(seq_to_checkpoint(&json!("42-abc")), "42-abc");
    assert_eq!(seq_to_checkpoint(&json!([42, "abc"])), "[42,\"abc\"]");
  }

  #[test]
  fn has_conflict_detects_nonempty_conflict_list() {
    assert!(has_conflict(&Some(json!({ "_conflicts": ["2-b"] }))));
    assert!(!has_conflict(&Some(json!({ "_conflicts": [] }))));
    assert!(!has_conflict(&Some(json!({ "mtime": 1 }))));
    assert!(!has_conflict(&None));
  }

  #[test]
  fn describe_problem_reports_error_over_staleness() {
    let job: SchedulerJob = serde_json::from_value(json!({
        "id": "a-to-b", "source": "filesync", "target": "http://b/filesync",
        "info": { "error": "timeout", "last_updated": "2000-01-01T00:00:00Z" }
    }))
    .unwrap();
    assert_eq!(describe_problem(&job, 300), Some("errored"));
  }

  #[test]
  fn describe_problem_reports_staleness() {
    let job: SchedulerJob = serde_json::from_value(json!({
        "id": "a-to-b", "source": "filesync", "target": "http://b/filesync",
        "info": { "last_updated": "2000-01-01T00:00:00Z" }
    }))
    .unwrap();
    assert_eq!(describe_problem(&job, 300), Some("is stale"));
  }

  #[test]
  fn describe_problem_is_quiet_for_recent_jobs() {
    let job: SchedulerJob = serde_json::from_value(json!({
        "id": "a-to-b", "source": "filesync", "target": "http://b/filesync",
        "info": {}
    }))
    .unwrap();
    assert_eq!(describe_problem(&job, 300), None);
  }

  #[test]
  fn last_updated_stale_rejects_unparseable_timestamps() {
    assert!(!last_updated_stale("not-a-timestamp", 1));
  }
}
