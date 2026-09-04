# Notes for review

## State

Everything builds and tests pass, including the Docker-backed e2e tests
(verified under podman with a docker socket). `cargo test`, `cargo test --
--ignored`, and `cargo clippy --all-targets` are all clean.

## Crate structure (reworked)

`sync-core` was the wrong name for a crate that only held the hub/client wire
types *and* a pile of hub-only CouchDB plumbing. It's now:

- `protocol-types` - only the Hub Sync API wire structs + serde (shared by
  `hub-api` and `client-core`).
- `hub-api` - gained the CouchDB client (`couch.rs`), `diff3.rs`, and
  `CouchError` as private modules; the Stage 1 `replication_e2e` test moved
  here too.
- `client-core` / `client-desktop` - unchanged responsibilities.

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
  `diff3.rs` (now in `hub-api`).
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
  with a stale `base_rev` (or a brand-new path that already exists) branches
  the revision tree via `_bulk_docs` `new_edits:false`, resolves the resulting
  conflict in-process, and returns the final winning revision - so a client
  *never* sees an unresolved conflict, even when two devices hit the same hub.
  This is the same resolver the watcher runs for replication-induced conflicts.
  - New-file collisions (both devices create the same path) branch as an
    independent root and are merged against an **empty common ancestor**.
- **`PushStatus::Conflict` is gone.** With branching + resolving, there is no
  per-item "conflict" outcome anymore - `PushResult` is just `{ path, rev }`.
  A change the hub genuinely can't apply (a bogus `base_rev`, a blind delete,
  or a backend failure) now aborts the request with an HTTP error - `409` for
  the client's mistake, `502` for a CouchDB problem - which the engine
  treats like any other hub failure: failover, then notify, with the pending
  queue left untouched (never dropped).
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

## First client: desktop (inotify)

- New crate `crates/client-desktop`: a headless CLI daemon that watches one
  directory with inotify and syncs it to the hub(s), configured entirely by
  env vars (`FILESYNC_HUBS` / `FILESYNC_TOKEN` / `FILESYNC_DIR`, plus optional
  `FILESYNC_STATE_DIR` and `FILESYNC_DEBOUNCE_MS`).
- **Storage split (per your decision).** `BlobStore` was replaced by two
  traits in `client-core`: `MetaStore` (checkpoints, the pending queue, and
  per-file `rev`/`mtime`/`content_type` metadata - the old `StoredFile` minus
  its base64 content, now `FileMeta`) and `FileStore` (the actual file bytes,
  keyed by path, with `put(path, bytes, mtime)`). The engine reads push
  content from the `FileStore` instead of decoding base64 out of metadata.
  - Desktop: `FsMetaStore` writes one URL-encoded-key file per key under the
    XDG state dir; `FsFileStore` is the sync directory itself, writing
    atomically and setting the on-disk mtime to the sync mtime.
  - `MemStore` implements both for tests.
- **Reconciliation, not event mapping.** The inotify watcher only signals
  "something changed" (and keeps watches current for new subdirectories). On
  startup and after each debounced signal, the client scans the tree and
  compares each file's mtime against metadata: new/changed files are
  `record_upsert`, known-but-gone paths are `record_delete`, then `sync()`.
  Because pulls write files back with the same mtime they stored, the client's
  own writes don't show up as new edits on the next pass - the loop is
  idempotent without a suppression set.
  - Engine gained `read_file` (raw bytes), `read_file_meta`, and
    `list_file_paths` to support this; `MetaStore` gained `list_keys`.
- **Known limitation:** change detection is mtime-at-second granularity, so an
  edit that lands within the same second as the last sync (or a tool that
  restores mtime) can be missed while the client is off. Matches the hub's
  second-resolution "keep newer" rule; can be hardened with size/hash later.

## Hub -> client wake (desktop): long-poll

- FCM is mobile-only, so the desktop client can't use the hub's Stage 3
  wakeup. Instead the hub exposes `GET /changes/longpoll?since=&timeout=`, a
  separate endpoint that forwards CouchDB's native `feed=longpoll`: CouchDB
  holds the request up to `timeout` (capped at 60s, default 25s) and returns
  immediately when a change lands. `GET /changes` stays a pure snapshot.
- The client spawns one long-poll task per hub (`client-desktop/src/poll.rs`).
  Each keeps its own `since` (a wake-detection checkpoint, advanced from every
  response) and signals the shared debounced reconcile/sync loop on any
  non-empty batch; on error it retries with exponential backoff. The engine's
  pull checkpoint is untouched until it actually pulls (`SyncEngine::checkpoint`
  was added to read it).
- This gives sub-second remote-change latency with ~one held connection per
  hub, and no idle polling. WebSocket/SSE was considered and skipped as
  overkill for 2-3 devices.

## Still open

- **Web + mobile targets.** The desktop client proves out the native
  `MetaStore`/`FileStore` backings. Web still needs OPFS/IndexedDB backings,
  and mobile needs the FCM-wake / foreground-open / charging-started trigger
  wiring (the core exposes `SyncEngine::sync()`; only the trigger differs).
- **Push rejection is now an HTTP error.** Since the hub branches + resolves
  stale pushes, a rejected push is rare (a genuinely bogus `base_rev`, or a
  hub mid-failure) and surfaces as a `409`/`502` on the whole `POST /changes`.
  The engine treats that like any hub failure - failover, then notify - and
  leaves the pending queue untouched, so nothing is ever silently lost. The
  remaining question is purely app-level UX for those rare cases (what to
  *tell* the user), which can wait for the client implementation details.
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

Stages 4/7 are in place and tested, and the first concrete client (desktop,
inotify) is shipped. Remaining platform work: web (OPFS/IndexedDB backings +
WASM) and mobile (FCM-wake / foreground / charge triggers).
