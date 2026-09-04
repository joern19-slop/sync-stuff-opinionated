//! Long-poll wake path: holds an open `GET /changes/longpoll` against each
//! hub and signals "something changed" whenever remote changes arrive, so the
//! desktop client reacts to other devices promptly without FCM or a poll
//! timer.
//!
//! Each loop advances its own `since` - a wake-detection checkpoint, separate
//! from the engine's pull checkpoint. Advancing it on every response (even an
//! empty timeout) means a returned batch isn't re-signalled; the engine's own
//! checkpoint is untouched until it actually pulls.

use std::sync::Arc;
use std::time::Duration;

use client_core::{HubClient, SyncEngine};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, warn};

const LONGPOLL_TIMEOUT_SECS: u64 = 25;

pub fn spawn(engine: Arc<SyncEngine>, hub: HubClient, wake: UnboundedSender<()>) {
  tokio::spawn(async move {
    let mut since = match engine.checkpoint(hub.id()).await {
      Ok(s) => s,
      Err(e) => {
        warn!(hub = hub.id(), error = %e, "reading checkpoint for long-poll failed");
        None
      }
    };
    let mut backoff = Duration::from_secs(1);

    loop {
      match hub.longpoll(since.as_deref(), LONGPOLL_TIMEOUT_SECS).await {
        Ok(resp) => {
          backoff = Duration::from_secs(1);
          since = Some(resp.checkpoint);
          if !resp.changes.is_empty() {
            debug!(
              hub = hub.id(),
              n = resp.changes.len(),
              "remote changes arrived"
            );
            if wake.send(()).is_err() {
              break; // main loop is gone
            }
          }
        }
        Err(e) => {
          warn!(hub = hub.id(), error = %e, "long-poll failed");
          tokio::time::sleep(backoff).await;
          backoff = (backoff * 2).min(Duration::from_secs(30));
        }
      }
    }
  });
}
