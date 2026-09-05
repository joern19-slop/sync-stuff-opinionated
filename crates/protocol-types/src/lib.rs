//! Wire types for the client-facing Hub Sync API: the JSON shapes shared by
//! the hub and every client, plus their (de)serialization. No transport,
//! storage, or merge logic - those live in `hub-api` and `client-core` - so
//! both sides share one definition instead of two drifting copies.

pub mod types;

pub use types::*;
