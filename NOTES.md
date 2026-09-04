# Notes for review

## State

Everything builds and tests pass, including the Docker-backed e2e tests
(verified under podman with a docker socket). `cargo test`, `cargo test --
--ignored`, and `cargo clippy --all-targets` are all clean.

## Device token provisioning (per your decision)

Bearer tokens (`HUB_DEVICE_TOKENS`) are provisioned by **manual copy into each
device's config file** - no on-device registration flow in this phase. The
hub just trusts the static set it's given; revoking a device means removing
its token from the hub's config (and the device's config, which is out of the
hub's control).

## What got built since the last review

- **Fixed the workspace.** The previous commit left sources at the repo root
  while the manifest pointed at `crates/`; moved everything into place and
  bumped `testcontainers` 0.15 -> 0.23 (the API the tests were written for).
- **Stage 3 (watcher + FCM/Discord)** - see `watcher.rs` / `notify.rs`.
- **Stage 4 (client core)** - see `client-core/src/{engine,hub,store,notify}.rs`.
- **Stage 5 + 6 (conflict resolver)** - see `resolver.rs` (hub-api) and
  `diff3.rs` (sync-core).
- **Stage 7 (multi-hub failover)** - part of the client engine.

## Client error reporting (your request)

- `client-core` exposes a `Notifier` trait (`fn notify_error(&self, &SyncError)`)
  that the actual client implements to show errors to the user (toast/dialog/
  log). The engine calls it whenever it returns an unexpected error it can't
  silently recover from: a hub that's unreachable after *every* fallback, or a
  local store failure. A hub failure that failover recovered from is *not*
  surfaced (it self-healed).
- Store failures are treated as fatal-local and short-circuit the hub
  failover loop (retrying another hub can't fix a broken local disk); only
  hub/transport errors trigger fallover.

## Conflict resolver decisions

- **diff3 is now delegated to `threeway_merge`** (Git's own xdiff `xdl_merge`,
  the engine behind `git merge-file`) rather than my hand-rolled LCS diff3.
  This is the "battle-tested lib" upgrade you asked for. The module still
  exposes the same conservative `merge(bytes) -> Result<Vec<u8>, MergeError>`
  contract: any conflict or engine failure is returned as an error so the
  resolver falls back to "keep newer + `.conflict-*` copy", which never loses
  data. Non-UTF-8 input fails closed (`NotText`).
  - **Behavior note:** xdiff matches real `git merge` - edits to *adjacent*
    lines (no unchanged line between them) are flagged as a conflict, which is
    stricter than the hand-rolled diff3 it replaced. That's conservative (a
    spurious conflict copy, never data loss), but worth knowing: a text file
    whose edits touch neighboring lines will take the keep-newer+copied path
    rather than a clean merge.
  - **License note:** `threeway_merge` statically links xdiff, so its license
    is `MIT AND LGPL-2.1-or-later`. LGPL static linking means distributing a
    binary carries the usual relink-object-files obligation. Fine for
    personal use; flag if you ever ship binaries.
- **Push now branches instead of rejecting.** Per your decision, a client push
  with a stale `base_rev` (or a brand-new path that already exists) is no
  longer returned as `Conflict`. The hub writes the divergent edit into the
  revision tree via `_bulk_docs` `new_edits:false`, resolves the resulting
  conflict in-process, and returns the final winning revision - so a client
  *never* sees an unresolved conflict, even when two devices hit the same hub.
  This is the same resolver the watcher runs for replication-induced conflicts.
  - New-file collisions (both devices create the same path) branch as an
    independent root and are merged against an **empty common ancestor**.
- **Detection** is the watcher long-polling `_changes?include_docs=true&
  conflicts=true` and resolving any row whose winning doc carries a non-empty
  `_conflicts`; the push path additionally resolves synchronously after it
  branches.
- **Write-back** follows the plan exactly: write the resolved content
  CAS-conditioned on the winning `_rev`, then `DELETE` each losing leaf so
  `_conflicts` clears. A `409` anywhere retries the whole read-plan-write
  loop (deterministic on both hubs, so a racing hub either wins or sees
  "already resolved" and stops).
- **Edit-vs-delete is mostly CouchDB's job.** `_conflicts` only ever lists
  non-deleted losing leaves; a deletion sibling is *not* flagged, and CouchDB
  prefers the non-deleted leaf at equal generation, so "edit wins" already
  holds by default. The resolver still has a defensive `resolve_delete_winner`
  path that uses `open_revs=all` (multipart-parsed in `couch.rs`) to
  resurrect a hidden edit under a deleted winner, but I couldn't construct a
  real sibling case where CouchDB picks the delete. `edit_vs_delete` is
  covered by a test asserting the edit survives either way.
- **Conflict copies** are written as ordinary new docs named
  `path.conflict-DD.MM.YYYY` (`.0`, `.1`, ... if taken), with the losing
  leaf's mtime/content-type.

## e2e fixes needed for podman (also correct for Docker)

- CouchDB logs its "has started" line to **stderr**, not stdout - the wait
  condition was `message_on_stdout`.
- CouchDB 3.3 auto-finishes single-node setup when `COUCHDB_USER`/`PASSWORD`
  are set; re-issuing `_cluster_setup` against an already-setup node restarts
  chttpd and breaks the next request. Removed the manual bootstrap call.
- `_replicator` must be created explicitly (fresh single-node installs don't
  create it), and `_replicator` docs reject local endpoints
  ("local_endpoints_not_supported") - both source and target are now full
  URLs. `init-replication.sh` updated to match.
- testcontainers `ContainerAsync` removes the container on drop; the hub e2e
  helper had to hold it until the test body finishes.
- `localhost` -> `127.0.0.1` (podman's IPv6 port-forwarding drops bodies).

## Still open

- **Client conflict-safety is now a fallback only.** Since the hub branches +
  resolves stale pushes, a client's `Conflict` result is rare (a genuinely
  unresolvable or bogus `base_rev`, or a hub that's mid-failure). The engine
  still keeps such a local change queued and skips overwriting it on pull,
  and reports it - so nothing is ever silently lost - but the normal path no
  longer surfaces conflicts to the client. The remaining question is purely
  app-level UX for those rare cases (what to *tell* the user), which can wait
  for the client implementation details.
- **Per-platform `BlobStore` backings** (native FS, web OPFS/IndexedDB), a
  concrete `Notifier` implementation, and the FCM-wake / foreground-open /
  charging-started trigger wiring - the core exposes `SyncEngine::sync()`,
  the `BlobStore` trait, and the `Notifier` trait; the platform glue is next.
- **WASM target.** `client-core` currently builds for native (reqwest +
  tokio). Compiling to `wasm32` will want the `reqwest` `js` feature and a
  `?Send`/single-threaded executor for the store/engine; noted, not done.
- **Checkpoint identity.** A hub's checkpoint is keyed by its base URL, so a
  hub whose URL changes mid-life restarts from a full pull (safe, just
  slower). Fine at this scale.
- **CouchDB compaction schedule** - deferred, kept revisions grow by design.
- **FCM HTTP v1** (OAuth2 service-account) migration - single-point in
  `notify.rs`; legacy server-key API is deprecated upstream.
- **Device FCM registration** - `HUB_FCM_TOKENS` is a flat env list, not a
  server-side registry; a device telling the hub its FCM token is future work.

## Next

Stage 4/7 are in place and tested; the remaining work is platform-specific
(client targets) and is blocked on the implementation details you said will
follow.
