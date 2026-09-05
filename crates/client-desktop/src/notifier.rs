//! The concrete [`Notifier`]: surfaces sync errors to the log and to the
//! user's desktop (via notify-rust / libnotify).

use std::sync::Mutex;
use std::time::{Duration, Instant};

use client_core::{Notifier, SyncError};

/// Identical errors are suppressed within this window so a persistently-down
/// hub doesn't toast on every retry; a fresh failure after recovery still
/// re-notifies.
const DEDUPE_WINDOW: Duration = Duration::from_secs(300);

/// Pops a desktop notification, best-effort (a missing notification daemon
/// is logged, not fatal). Shared by the [`Notifier`] impl and the file
/// watcher.
pub(crate) fn show_notification(summary: &str, body: &str) {
  let shown = notify_rust::Notification::new()
    .summary(summary)
    .body(body)
    .show();
  if let Err(e) = shown {
    tracing::warn!(error = %e, "desktop notification failed");
  }
}

/// Logs each error and pops a desktop notification, de-duplicated by message
/// within [`DEDUPE_WINDOW`].
pub struct LogNotifier {
  last: Mutex<Option<(String, Instant)>>,
}

impl Default for LogNotifier {
  fn default() -> Self {
    Self {
      last: Mutex::new(None),
    }
  }
}

impl Notifier for LogNotifier {
  fn notify_error(&self, error: &SyncError) {
    tracing::error!(error = %error, "sync failed");

    let message = error.to_string();
    let now = Instant::now();
    let mut last = self.last.lock().unwrap();
    if let Some((prev, at)) = last.as_ref() {
      if prev == &message && now.duration_since(*at) < DEDUPE_WINDOW {
        return;
      }
    }
    *last = Some((message.clone(), now));
    drop(last);

    show_notification("filesync", &message);
  }
}
