//! Rust/WASM client sync core (Build Order Stage 4 + 7).
//!
//! The engine pulls hub changes into local storage (advancing a durable
//! checkpoint only after each batch is fully written) and pushes
//! locally-recorded changes back up, with ordered multi-hub failover. It
//! owns no merge logic: clients only ever observe the hub's resolved view.
//!
//! Storage is split across two platform-provided traits:
//! - [`MetaStore`] for bookkeeping (checkpoints, the pending queue, per-file
//!   revision/mtime/content-type metadata);
//! - [`FileStore`] for the actual file bytes.
//!
//! Platform layers (native, WASM+web) implement both and wire
//! `SyncEngine::sync()` to the wake triggers (FCM background handler, app
//! foreground-open, charging-started, inotify on desktop).

pub mod engine;
pub mod hub;
pub mod notify;
pub mod store;

pub use engine::{
  checkpoint_key, file_key, FileMeta, PendingChange, PullReport, PushReport, SyncEngine, SyncError,
  SyncReport, KEY_PENDING,
};
pub use hub::{FileContent, HubClient, HubError};
pub use notify::Notifier;
pub use store::{FileStore, MemStore, MetaStore, StoreError};
