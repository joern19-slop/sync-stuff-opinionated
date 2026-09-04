# filesync

Implementation of `ArchitecturePlan.md`. Build order progress:

- **Stage 1** - prove CouchDB-only redundancy (two standalone nodes,
  continuous bidirectional replication, either one survives alone).
  `docker-compose.yml` + `init-replication.sh` is the primary proof;
  `crates/sync-core/tests/replication_e2e.rs` is an automated (testcontainers)
  version of the same thing.
- **Stage 2** - the Rust Hub Sync API (`GET /changes`, `POST /changes`,
  `GET /file/{path}`) against a single CouchDB.
- **Stage 3** - Change Watcher + notifier: the hub long-polls its own
  CouchDB `_changes` and wakes devices via FCM (silent data-only message);
  it also inspects the replication scheduler and posts hub-to-hub
  errors/staleness to a Discord webhook. See `crates/hub-api/src/watcher.rs`
  and `crates/hub-api/src/notify.rs`.
- **Stage 4** - the client sync engine (`crates/client-core`): pulls hub
  changes into a local `BlobStore` (advancing a durable checkpoint only after
  a whole batch is written), pushes locally-recorded changes back, and never
  drops a push the hub rejects. Two platform seams: `BlobStore` (native uses
  the filesystem, web uses OPFS/IndexedDB; `MemStore` is the test reference)
  and `Notifier` (the app implements it to surface unexpected errors to the
  user).
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

**Not implemented yet:** the per-platform `BlobStore` backings (native FS /
web OPFS-IndexedDB) and the FCM-wake/foreground/charge trigger wiring - both
deferred pending the concrete client targets. See `ArchitecturePlan.md`.

## Layout

```
Cargo.toml                 workspace
crates/sync-core/          shared types, CouchDB client, diff3 merge
crates/hub-api/             axum service: Hub Sync API + watcher/notifier/resolver
crates/client-core/         client sync engine + BlobStore trait (Stage 4/7)
docker-compose.yml          two standalone CouchDB nodes (Stage 1)
init-replication.sh         wires up bidirectional replication between them
```

## Running it

Requires Docker.

```bash
# Stage 1 - CouchDB redundancy, no app code involved
docker compose up -d
./init-replication.sh
# write to node-a (:5984), read from node-b (:5985) a couple seconds later
curl -u hub:hub-password -X PUT localhost:5984/filesync/hello \
  -H 'Content-Type: application/json' -d '{"msg":"hi"}'
curl -u hub:hub-password localhost:5985/filesync/hello

# Stage 2+3 - the hub API against node-a, with FCM wakeups and Discord alerts
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
```

## Tests

```bash
# Unit + wiremock-backed HTTP tests - no Docker needed
cargo test

# e2e - needs Docker (or podman with a docker socket); each test starts its own container(s)
cargo test --test replication_e2e -- --ignored --nocapture   # in sync-core
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
| `HUB_DEVICE_TOKENS` | *(empty)* | comma-separated bearer tokens; one per device |
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
