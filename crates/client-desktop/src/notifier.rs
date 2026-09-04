//! The concrete [`Notifier`]: surfaces sync errors to the log.

use client_core::{Notifier, SyncError};

pub struct LogNotifier;

impl Notifier for LogNotifier {
    fn notify_error(&self, error: &SyncError) {
        tracing::error!(error = %error, "sync failed");
    }
}
