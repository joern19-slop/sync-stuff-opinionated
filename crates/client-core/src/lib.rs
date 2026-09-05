//! Rust/WASM client sync core (Build Order Stage 4 + 7).
//!
//! Pulls hub changes into local storage (durable checkpoint per batch),
//! pushes locally-recorded changes back, with ordered multi-hub failover. No
//! merge logic - clients only ever see the hub's resolved view. Storage is
//! two platform-provided traits: [`MetaStore`] for bookkeeping, [`FileStore`]
//! for file bytes; platform layers implement both and drive
//! `SyncEngine::sync()` from their wake triggers.

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
