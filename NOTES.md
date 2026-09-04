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
- `common` - shared HTTP client (`http_client()`: connect + request timeouts,
  a User-Agent) and strict env parsing (`env::{required, optional, string_or,
  u64_or, list_optional, list_required}`). Both apps use it; a bad/missing
  value now fails loudly instead of silently defaulting.
- `hub-api` - gained the CouchDB client (`couch.rs`), `diff3.rs`, and
  `CouchError` as private modules; the Stage 1 `replication_e2e` test moved
  here too.
- `client-core` / `client-desktop` - unchanged responsibilities.

## Shared HTTP client + strict config

- `common::http_client()` sets a 10s connect timeout and a 30s default request
  timeout on every hub/client request, so a black-holed peer can't hang sync
  forever. The client's long-poll overrides per-request to 65s (the hub holds
  up to 60s).
- `HUB_DEVICE_TOKENS` is now **required** - the hub refuses to start with no
  device tokens instead of silently rejecting every request. Both the hub and
  the desktop config now fail on a bad integer rather than falling back to the
  default.

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
- **Temp files.** `FsFileStore`/`FsMetaStore` write via a temp file
  `.{pid}-{n}.tmp` in the target directory, then rename. Both the sync-dir scan
  (`reconcile.rs`) and the metadata `list_keys` skip names ending in `.tmp`, so
  a temp left behind by a crash between write and rename is ignored rather
  than synced up as a real file.
- **Atomic writes vs. open editor handles.** Both a local `record_upsert` and
  a pulled remote change replace a file via temp+rename, changing its inode.
  An editor holding the file open keeps the old (now-unlinked) inode, so a
  save after a pull writes to a file no longer in the sync dir - the classic
  atomic-sync/editor mismatch (Dropbox-style tools hit it too). The orphaned
  bytes are still in the editor (not lost from the *sync* layer), but it's a
  real footgun. Mitigations (not done): write in place when content is
  unchanged, or hold off pulling a path with a known open handle.

- **Symlinks & non-UTF-8 names.** Both are skipped, but now logged (`warn!`)
  rather than silently ignored: the sync model is byte content over UTF-8
  paths, and following symlinks risks escaping the sync root or cycling.
  Supporting either is possible (follow in-root file symlinks; base64-encode
  non-UTF-8 names with a marker) but deferred until there's a real need.

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
- **Missed-wake window.** The poll task advances its own `since` (a
  wake-detection checkpoint) on *every* response, including one that just
  signalled a wake. If the follow-up reconcile/sync fails (a transient error
  between the long-poll and the pull), the engine's pull checkpoint lags but
  the poll's `since` has already moved past those changes, so they won't
  re-wake. They're deferred until the *next* trigger of any kind (a new remote
  change, a local edit, or a restart). Accepted for now: it self-heals on the
  next event and can't lose data, but a hub that flakes *between* long-poll
  and pull can leave remote changes un-pulled while the client sits idle.
  Fix options (not done): re-read the engine checkpoint before each long-poll
  (self-healing, but tight-loops while a hub is persistently down), or advance
  `since` only on empty/timeout responses and reset it to the engine's
  checkpoint on a wake.

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
- **Web client (wasm).** `client-web` is a wasm-only crate: `OpfsFileStore` +
  `OpfsMetaStore` over OPFS, a `wasm-bindgen` façade (`init` + `WebSync`), and
  `?Send` on the store traits (wasm futures wrap `JsFuture` = `Rc<RefCell<_>>`).
  Unit-tested in headless Firefox (`wasm-pack test --headless --firefox`), and
  an e2e test (`tests/e2e.rs`, `--features e2e`) round-trips push/pull/delete
  through a real hub + CouchDB via `scripts/e2e-web.sh` (needs geckodriver).
  The hub now serves `CorsLayer::permissive()` (TODO: tighten). Deferred:
  metadata is OPFS-backed, not IndexedDB, and the Tuta fork + shim are next.
  See `calendar/PLAN.md`.
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
