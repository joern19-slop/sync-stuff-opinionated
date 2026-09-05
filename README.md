# filesync

Implementation of `ArchitecturePlan.md`. Build order progress:

- **Stage 1** - prove CouchDB-only redundancy (two standalone nodes,
  continuous bidirectional replication, either one survives alone).
  `crates/hub-api/tests/replication_e2e.rs` automates this with testcontainers
  (`cargo test -p hub-api --test replication_e2e -- --ignored`).
- **Stage 2** - the Rust Hub Sync API (`GET /changes`, `POST /changes`,
  `GET /file/{path}`) against a single CouchDB.
- **Stage 3** - Change Watcher + notifier: the hub long-polls its own
  CouchDB `_changes` and wakes devices via FCM (silent data-only message);
  it also inspects the replication scheduler and posts hub-to-hub
  errors/staleness to a Discord webhook. See `crates/hub-api/src/watcher.rs`
  and `crates/hub-api/src/notify.rs`.
- **Stage 4** - the client sync engine (`crates/client-core`): pulls hub
  changes into local storage (advancing a durable checkpoint only after a
  whole batch is written), pushes locally-recorded changes back, and never
  drops a push the hub rejects. Two platform seams, split by concern:
  `MetaStore` (checkpoints, pending queue, per-file revision/mtime/content-type
  metadata) and `FileStore` (the actual file bytes) - native uses the
  filesystem, web uses OPFS/IndexedDB; `MemStore` is the test reference. A
  third seam, `Notifier`, lets the app surface unexpected errors to the user.
- **Stage 5** - hub-side conflict resolver: a conflicted doc (from
  replication, or from a client push whose `base_rev` was stale) is resolved
  with a diff3 three-way merge, written CAS-conditioned on the winning
  revision, with losing revisions explicitly deleted to clear `_conflicts`.
  A stale push *branches* the revision tree rather than being rejected, so a
  client never sees an unresolved conflict. `crates/hub-api/src/resolver.rs`
  orchestrates it.
- **Stage 6** - edit-vs-delete: the edit survives. CouchDB already prefers a
  non-deleted leaf over a deletion sibling, so this mostly falls out for
  free; the resolver also has a defensive "resurrect a hidden edit under a
  deleted winner" path.
- **Stage 7** - multi-hub client failover: the engine holds an ordered hub
  list and falls back to the next when one is unreachable. Checkpoints are
  keyed per hub (CouchDB `seq` is node-local; revisions are consistent across
  replicas, so the push queue stays global).

**First client shipped:** `crates/client-desktop` - a headless CLI daemon that
watches a directory with inotify and keeps it in sync with the hub. It
implements `MetaStore` (XDG state dir) and `FileStore` (the sync dir), a
concrete `Notifier` (logs to stderr), and an inotify watcher wired to
reconciliation + sync on a debounce. See "Running the desktop client" below.

**Web client:** `crates/client-web` compiles the engine to `wasm32` with
`OpfsFileStore`/`OpfsMetaStore` over OPFS and a wasm-bindgen façade (`WebSync`) -
built for, and currently powering, the web calendar (Tuta's UI on the filesync
backend). See `calendar/PLAN.md` and `calendar/STATUS.md`.

**Not implemented yet:** the FCM-wake / foreground / charge trigger wiring for
mobile - deferred pending a concrete mobile client. See `ArchitecturePlan.md`.

## Layout

```
Cargo.toml                 workspace
crates/protocol-types/      wire types for the Hub Sync API (shared by hub + clients)
crates/common/               shared HTTP client (timeouts) + strict env parsing
crates/hub-api/             axum service: Hub Sync API + CouchDB client + diff3 + resolver
crates/client-core/         client sync engine + MetaStore/FileStore traits (Stage 4/7)
crates/client-desktop/      headless inotify client (the first concrete client)
crates/client-web/          wasm32 sync engine + OPFS stores (web calendar backend)
docker-compose.yml          single local CouchDB for development (two-node proof is the e2e test)
calendar/                   plan + status for the web calendar (calendar/PLAN.md)
```

## Running it

Requires Docker.

```bash
# Development CouchDB (single node) - the e2e tests start their own
docker compose up -d
curl -u hub:hub-password localhost:5984/

# Stage 1 - CouchDB redundancy, no app code involved (automated, testcontainers)
cargo test -p hub-api --test replication_e2e -- --ignored --nocapture

# Stage 2+3 - the hub API against the local CouchDB, with FCM wakeups and Discord alerts
COUCH_URL=http://localhost:5984 \
COUCH_USER=hub COUCH_PASSWORD=hub-password \
HUB_DEVICE_TOKENS=dev-token-1,dev-token-2 \
FCM_SERVER_KEY=<your-legacy-fcm-server-key> \
HUB_FCM_TOKENS=<comma-separated-fcm-registration-tokens> \
DISCORD_WEBHOOK_URL=<your-discord-webhook-url> \
cargo run -p hub-api
```

Then, e.g.:

```bash
curl -H 'Authorization: Bearer dev-token-1' localhost:8080/changes

curl -H 'Authorization: Bearer dev-token-1' -X POST localhost:8080/changes \
  -H 'Content-Type: application/json' \
  -d '[{"path":"a.txt","deleted":false,"base_rev":null,"mtime":1700000000,
       "content_type":"text/plain","content_base64":"aGVsbG8="}]'

curl -H 'Authorization: Bearer dev-token-1' localhost:8080/file/a.txt

# long-poll: blocks up to 25s for a change, returns immediately when one lands
curl -H 'Authorization: Bearer dev-token-1' 'localhost:8080/changes/longpoll?timeout=25'
```

## Running the desktop client

The client watches one directory with inotify and syncs it to the hub(s).
Configure it with environment variables:

```bash
FILESYNC_HUBS=http://localhost:8080 \
FILESYNC_TOKEN=dev-token-1 \
FILESYNC_DIR=/home/you/filesync \
cargo run -p client-desktop
```

| Var | Default | Meaning |
|---|---|---|
| `FILESYNC_HUBS` | *(required)* | comma-separated hub base URLs (ordered failover) |
| `FILESYNC_TOKEN` | *(required)* | device bearer token |
| `FILESYNC_DIR` | *(required)* | the directory to sync |
| `FILESYNC_STATE_DIR` | `$XDG_STATE_HOME/filesync` | where sync metadata lives |
| `FILESYNC_DEBOUNCE_MS` | `1000` | quiet period after the last change before syncing |

On startup it reconciles the directory against the hub (picking up changes
made while it was off), then watches recursively and re-syncs after each
debounced change. Change detection is by mtime (seconds), consistent with the
hub's conflict-resolution rule.

Remote changes are picked up without a poll timer: the client holds a
`GET /changes/longpoll` against each hub, which the hub blocks (via CouchDB's
native `feed=longpoll`) until a change arrives, then returns immediately. That
signal feeds the same debounced reconcile/sync loop as local inotify events.

## Tests

```bash
# Unit + wiremock-backed HTTP tests - no Docker needed
cargo test

# e2e - needs Docker (or podman with a docker socket); each test starts its own container(s)
cargo test --test replication_e2e -- --ignored --nocapture   # in hub-api
cargo test --test hub_api_e2e -- --ignored --nocapture       # in hub-api
cargo test --test resolver_e2e -- --ignored --nocapture      # in hub-api
cargo test -p client-core --test client_e2e -- --ignored --nocapture
cargo test --test api_unit                                    # in hub-api (no --ignored needed)
```

## Config (env vars, `hub-api`)

| Var | Default | Meaning |
|---|---|---|
| `HUB_BIND_ADDR` | `0.0.0.0:8080` | listen address |
| `COUCH_URL` | `http://localhost:5984` | CouchDB base URL |
| `COUCH_DB` | `filesync` | database name |
| `COUCH_USER` / `COUCH_PASSWORD` | `hub` / `hub-password` | CouchDB creds |
| `HUB_DEVICE_TOKENS` | *(required)* | comma-separated bearer tokens; one per device |
| `FCM_SERVER_KEY` | *(unset)* | legacy FCM server key; enables device wakeups |
| `HUB_FCM_TOKENS` | *(empty)* | comma-separated FCM registration tokens to wake |
| `DISCORD_WEBHOOK_URL` | *(unset)* | enables hub-to-hub replication alerts |
| `HUB_WATCHER_POLL_SECS` | `2` | change-watcher long-poll interval |
| `HUB_REPL_STALENESS_SECS` | `300` | replication staleness threshold before alerting |

Device bearer tokens are provisioned by **manual copy into each device's
config file** (no on-device registration flow in this phase). `FCM_SERVER_KEY`
uses FCM's legacy server-key API; migrating to HTTP v1 (OAuth2) is a
single-point change in `notify.rs`.

See `NOTES.md` for decisions made along the way and what remains open.
