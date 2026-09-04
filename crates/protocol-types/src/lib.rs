//! Wire types for the client-facing Hub Sync API.
//!
//! This crate is *only* the contract shared by the hub and every client: the
//! JSON shapes they exchange over HTTP, plus their (de)serialization. It has
//! no transport, storage, or merge logic - those live in `hub-api` and
//! `client-core` respectively, so both sides share one definition instead of
//! two independently-drifting copies.

pub mod types;

pub use types::*;
