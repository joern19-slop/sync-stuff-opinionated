//! The platform hook for surfacing sync problems to the user.
//!
//! The sync engine is UI-agnostic; it never knows whether it's running in a
//! native app, a web page, or a headless daemon. When it hits an error it
//! can't silently recover from, it reports it through this trait - the
//! actual client provides an implementation that shows a toast/dialog/log
//! entry. The engine still returns the error to its caller as well, so the
//! two are complementary, not either/or.

use crate::engine::SyncError;

/// Implemented by the client app (native / web / ...) to report sync problems
/// to the user.
pub trait Notifier: Send + Sync {
    /// An unexpected error that should be surfaced to the user (a hub that's
    /// unreachable after every fallback, a local store failure, ...).
    fn notify_error(&self, error: &SyncError);
}
