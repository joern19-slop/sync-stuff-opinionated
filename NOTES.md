# Notes for review

## I could not compile or run any of this

The sandbox I wrote this in has no Docker and no package-manager network
access (apt/rustup were both blocked), so nothing here has been through
`cargo build`, `cargo test`, or `cargo clippy`. Please treat the first
`cargo build` as the real first review pass, not a formality. I tried to
write carefully and keep the CouchDB-facing pieces behind narrow,
independently-testable seams for exactly this reason, but I'd bet on there
being at least a few compile errors on first try, most likely in:

1. **`testcontainers` API surface** (`crates/sync-core/tests/replication_e2e.rs`,
   `crates/hub-api/tests/hub_api_e2e.rs`) - the network/hostname wiring
   (`ImageExt::with_network`, `with_container_name`) in particular has
   shifted across `testcontainers` versions and I wrote it from memory
   without a version pinned against real docs. If it doesn't compile,
   check docs.rs for whatever version `cargo` resolves and adjust; the
   `docker-compose.yml` + `deploy/init-replication.sh` prove the identical
   Stage 1 claim without touching this crate at all, so that path isn't
   blocked on it.
2. **CouchDB's `_cluster_setup` single-node bootstrap** - I'm fairly but
   not fully confident recent official `couchdb` images auto-finish this
   when `COUCHDB_USER`/`COUCHDB_PASSWORD` are set, which is why the
   bootstrap calls tolerate an "already done" response rather than
   requiring success.
3. **axum 0.7's exact middleware signature** (`crates/hub-api/src/auth.rs`)
   - I used the 0.7-style unified `Request`/`Next` (no generic body param);
   double check this if you're not pinned to 0.7.

## Decisions made without asking

- **Doc shape**: a file is one CouchDB doc (`_id` = the file path,
  percent-encoded) with metadata (`path`, `mtime`, `content_type`) as the
  JSON body and the actual bytes as a `content` attachment. Two HTTP
  writes per push (doc, then attachment) rather than one multipart write -
  simpler to implement/test, at the cost of a narrow window where a crash
  mid-push leaves stale/missing content until retried. Flagged in
  `push.rs`; worth collapsing into one multipart write if that window ever
  matters in practice.
- **Auth**: one flat set of bearer tokens (`HUB_DEVICE_TOKENS`), any valid
  token can access anything. This gives per-device *provisioning*
  (revoke one device's token) but no per-device scoping of what it can
  touch - the doc didn't specify multi-user isolation for Stage 2, so I
  didn't invent any.
- **Conflict handling**: pushes are conditioned on `base_rev` via CouchDB's
  own CAS (a stale `base_rev` -> `409` -> `PushStatus::Conflict`). This is
  *not* the merge logic from Stage 5 - there's no attempt to reconcile
  anything, just "tell the client it lost the race."
- **`GET /changes` checkpoint**: passed through opaquely from CouchDB's
  `_changes.last_seq`. I didn't invent a hub-specific checkpoint format
  since CouchDB's is already exactly the "opaque cursor" shape the doc
  asks for.

## Questions for you

- Any preference on the exact `couchdb` image tag to pin long-term (I used
  `3.3` everywhere)?
- Do you want `PushStatus` to eventually distinguish "server error" from
  "conflict" (right now `push.rs` collapses both into `Conflict` and logs
  the real error server-side - noted inline)?
- Ready for me to keep going into Stage 3 (FCM/Discord notifications) next,
  or do you want to review Stages 1-2 first?
