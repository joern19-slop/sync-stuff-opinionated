//! Platform hook for surfacing sync problems to the user. The engine is
//! UI-agnostic; when it hits an error it can't silently recover from, it
//! reports through this trait (and still returns the error to its caller).

use crate::engine::SyncError;

/// Implemented by the client app to report sync problems to the user.
pub trait Notifier: Send + Sync {
  /// An error the app should surface (a hub unreachable after every
  /// fallback, a local store failure, ...).
  fn notify_error(&self, error: &SyncError);
}
