//! The concrete [`Notifier`]: surfaces sync errors to the log and to the
//! user's desktop (via notify-rust / libnotify).

use std::sync::Mutex;

use client_core::{Notifier, SyncError};

/// Logs each error and pops a desktop notification. Notifications are
/// de-duplicated by message so a persistently-down hub doesn't spam a toast
/// on every retry.
pub struct LogNotifier {
  last: Mutex<Option<String>>,
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
    let mut last = self.last.lock().unwrap();
    if last.as_deref() == Some(message.as_str()) {
      return;
    }
    *last = Some(message.clone());
    drop(last);

    let shown = notify_rust::Notification::new()
      .summary("filesync")
      .body(&message)
      .show();
    if let Err(e) = shown {
      tracing::warn!(error = %e, "desktop notification failed");
    }
  }
}
