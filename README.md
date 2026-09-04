# filesync

Implementation of `new.md`, Stages 1-2 of the suggested build order:

- **Stage 1** - prove CouchDB-only redundancy (two standalone nodes,
  continuous bidirectional replication, either one survives alone).
  `docker-compose.yml` + `deploy/init-replication.sh` is the primary proof;
  `crates/sync-core/tests/replication_e2e.rs` is an automated (testcontainers)
  version of the same thing.
- **Stage 2** - the Rust Hub Sync API (`GET /changes`, `POST /changes`,
  `GET /file/{path}`) against a single CouchDB, with plain optimistic-
  concurrency writes and no merge/conflict-resolution logic yet.

`crates/client-core` is an empty stub - the workspace layout matches the
target architecture (hub/client/core split) now, but nothing lives there
until Stage 4.

**Not implemented yet:** conflict resolution, FCM/Discord notifications,
the client sync engine, WASM/native builds. See `new.md`'s build order for
what's next; happy to keep going stage by stage.

## Layout

```
Cargo.toml                 workspace
crates/sync-core/          shared types + CouchDB HTTP client
crates/hub-api/             axum service implementing the Hub Sync API
crates/client-core/         stub, Stage 4+
docker-compose.yml          two standalone CouchDB nodes (Stage 1)
deploy/init-replication.sh  wires up bidirectional replication between them
```

## Running it

Requires Docker.

```bash
# Stage 1 - CouchDB redundancy, no app code involved
docker compose up -d
./deploy/init-replication.sh
# write to node-a (:5984), read from node-b (:5985) a couple seconds later
curl -u hub:hub-password -X PUT localhost:5984/filesync/hello \
  -H 'Content-Type: application/json' -d '{"msg":"hi"}'
curl -u hub:hub-password localhost:5985/filesync/hello

# Stage 2 - the hub API against node-a
COUCH_URL=http://localhost:5984 \
COUCH_USER=hub COUCH_PASSWORD=hub-password \
HUB_DEVICE_TOKENS=dev-token-1,dev-token-2 \
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

# e2e - needs Docker; each test starts its own container(s)
cargo test --test replication_e2e -- --ignored --nocapture   # in sync-core
cargo test --test hub_api_e2e -- --ignored --nocapture       # in hub-api
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

See `NOTES.md` for open questions, assumptions I made without you, and one
important caveat about what I could/couldn't verify in this environment.
