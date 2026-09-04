# File Sync System — Architecture Plan (v2: Files Only)

## Scope

**In scope (v1):** peer-to-peer-via-hub file sync across 2-3 personal devices and
2 always-online hubs, push-triggered (not continuous) client sync, and
never-lose-data conflict handling.

**Explicitly out of scope (v1):** calendar and contacts — planned as a future
phase, modeled as files (`.ics` / `.vcf`) on this same engine once file sync is
proven.

## Goals, in priority order

1. **No data loss.** Ranked above storage efficiency, protocol elegance, and
   bandwidth. Nothing is ever silently overwritten or discarded.
2. **Multi-master.** Either hub alone is a complete, working replica. No
   coordination required between hubs to serve a request.
3. **Battery-friendly clients.** No polling, no long-lived open connections.
   Sync is triggered by FCM push (empty-body wakeup) or opportunistic local
   triggers (app foreground-open, device starts charging) — never a background
   poll loop.
4. **One protocol, one implementation.** A single Rust core (compiled to WASM
   for web, linked natively for mobile/desktop) implements sync + merge once.

## Decisions Locked In

| Area | Decision | Why |
|---|---|---|
| Hub-to-hub topology | **Standalone single-node CouchDB per hub** (`n=1` locally), continuously replicating to each other via CouchDB's native replicator | Erlang-native *clustering* with 2 nodes has a default quorum of `r=w=2` — both hubs must respond to every read/write. Standalone + replication is leaderless and gives true multi-master: either hub works alone. |
| File content storage | CouchDB attachments | Rides the same replication protocol as everything else — no second sync path to build. Dedup loss doesn't matter at your stated size (single-digit MB per file, upper-2-digit GB total). |
| File identity | `_id` = file path. Rename = delete old doc + create new doc | Simplest option. Trade-off: a rename loses CouchDB revision history for that file — acceptable since conflict copies are already preserved as separate files, not as revision history. |
| Auth | HTTPS + shared token per device | Simplest fit for personal-use scale. |
| Checkpoint format | Opaque token, not a date | CouchDB's `_changes?since=<seq>` already returns an opaque `seq`/`last_seq` — the hub API just passes this through as the checkpoint, no custom design needed. |
| Client storage interface | Rust trait (`BlobStore`), each platform (native / WASM+web) provides its own implementation behind it | Keeps the core sync logic platform-agnostic. |
| Conflict resolution location | **Hub-side**, event-driven — not the client | Race window shrinks from "network round trip to a phone" to a local retry loop; resolves hub-to-hub conflicts even when no client is online; CouchDB's `_rev`-conditioned writes give optimistic concurrency for free, no custom locking needed. |

## High-Level Architecture

```
                    ┌────────────────────┐   native CouchDB replicator   ┌────────────────────┐
                    │       Hub A         │◄─────────────────────────────►│       Hub B         │
                    │  ┌───────────────┐  │      (continuous, both        │  ┌───────────────┐  │
                    │  │ Rust Sync API  │  │       directions, no          │  │ Rust Sync API  │  │
                    │  ├───────────────┤  │       coordination needed)    │  ├───────────────┤  │
                    │  │ CouchDB        │  │                                │  │ CouchDB        │  │
                    │  │ (standalone,   │  │                                │  │ (standalone,   │  │
                    │  │  n=1, internal │  │                                │  │  n=1, internal │  │
                    │  │  only)         │  │                                │  │  only)         │  │
                    │  │  - docs = files│  │                                │  │  - docs = files│  │
                    │  │  - content as  │  │                                │  │  - content as  │  │
                    │  │    attachments │  │                                │  │    attachments │  │
                    │  ├───────────────┤  │                                │  ├───────────────┤  │
                    │  │ Change watcher │  │                                │  │ Change watcher │  │
                    │  │ + FCM sender   │  │                                │  │ + FCM sender   │  │
                    │  │ + Discord hook │  │                                │  │ + Discord hook │  │
                    │  │ + Conflict     │  │                                │  │ + Conflict     │  │
                    │  │   resolver     │  │                                │  │   resolver     │  │
                    │  │   (diff3, CAS) │  │                                │  │   (diff3, CAS) │  │
                    │  └───────────────┘  │                                │  └───────────────┘  │
                    └──────────┬──────────┘                                └──────────┬──────────┘
                               │ FCM push: empty-body wakeup                          │
                               ▼                                                       ▼
                    ┌──────────────────────────────────────────────────────────────────┐
                    │                    Client (phone / laptop / web)                    │
                    │  ┌────────────────────────────────────────────────────────────┐  │
                    │  │ Rust/WASM sync core                                          │  │
                    │  │  - FCM background handler (wake trigger)                     │  │
                    │  │  - Opportunistic triggers: app foreground-open, charge-start  │  │
                    │  │  - Talks ONLY to the hub's custom API — never speaks          │  │
                    │  │    CouchDB's protocol directly                                │  │
                    │  │  - No merge logic — sees resolved results via /changes,       │  │
                    │  │    same as any other change                                   │  │
                    │  └────────────────────────────────────────────────────────────┘  │
                    │  Local store: behind a BlobStore trait (native FS / OPFS/IndexedDB) │
                    └──────────────────────────────────────────────────────────────────┘
```

**Key principle, unchanged:** clients only ever speak one HTTP/JSON API,
exposed by the Rust hub server. CouchDB is an internal implementation detail —
never exposed to clients directly. Hub-to-hub sync, by contrast, uses CouchDB's
own replication protocol directly (no custom code needed there — see "What we
reuse" below).

## Components

### 1. CouchDB (per hub, standalone)
- One doc per file, `_id` = path.
- File content stored as a CouchDB attachment on that doc.
- Continuous bi-directional replication to the other hub via CouchDB's native
  replicator (`_replicate` / `/_scheduler/jobs`) — configuration, not code.
- Never queried directly by clients.

### 2. Rust Hub Sync API
Endpoints exposed to clients:
- `GET /changes?since=<checkpoint>` — wraps CouchDB's `_changes`; returns
  changed paths + an opaque checkpoint token (CouchDB's `seq`, passed through).
- `GET /file/{path}` — returns current content (the attachment) + metadata.
- `POST /changes` — client pushes its local diffs back.

No client-facing conflict endpoints — resolution is entirely a hub-side
concern now (see Component 3). Clients never see an unresolved conflict; they
only ever see the outcome, delivered as an ordinary change via `/changes`.

### 3. Change Watcher + Notifier + Conflict Resolver (per hub)
- Internally follows its own CouchDB `_changes?feed=continuous`.
- New local or replicated change → FCM push to all known devices (empty body,
  wakeup only).
- Hub-to-hub replication error, or staleness past a threshold → Discord
  webhook. (Client-offline detection is explicitly not this component's job —
  it only reports on what the hub can actually observe.)
- **New:** on seeing `_conflicts: true` on a doc, resolves it in-process — see
  Conflict Resolution below. Runs on both hubs independently; no coordination
  or leader election between them (see Conflict Resolution for why that's
  safe).

### 4. Rust/WASM Client Core
- Single crate, compiled to WASM for web, linked natively elsewhere.
- Wakes on FCM push or an opportunistic trigger (foreground-open, charge-start).
- Pulls via `/changes`, fetches content via `/file/{path}`, pushes local
  changes via `POST /changes`, advances checkpoint only after a full batch is
  durably written locally (crash/kill-safe).
- Local storage behind a `BlobStore` trait; native and web provide their own
  backing implementation.

## What We Reuse From CouchDB vs. What We Build

| | Reused from CouchDB | Built ourselves |
|---|---|---|
| Hub-to-hub transport | Native replicator (changes → revs-diff → bulk_docs), fully | — |
| Checkpointing | Opaque `seq` token from `_changes` | Passing it through the hub API unchanged |
| Conflict *detection* | Automatic — divergent writes branch the revision tree, exposed via `open_revs=all` | — |
| Conflict *resolution* (the actual merge) | — | diff3-style three-way merge, entirely custom |
| Clearing the conflict flag | — | Explicit `bulk_docs` delete of the losing revision — CouchDB never does this on its own, even after a new file exists elsewhere |
| Wake/checkpoint orchestration | — | Fully custom — CouchDB has no concept of "wake briefly, pull once, sleep" |
| Notifications (FCM, Discord) | — | Fully custom — no built-in webhooks |
| Client-facing API shape | — | Fully custom — CouchDB's raw HTTP surface is document-shaped, not client-contract-shaped |

## Conflict Resolution

**Lives entirely on the hub, inside the Change Watcher / Conflict Resolver —
not the client.** Clients never fetch conflicting revisions and never run
diff3; they only ever observe the resolved outcome via ordinary `/changes`
polling.

**Why hub-side:** the alternative (client fetches, computes, writes back) has
a race window as wide as however long a phone takes to wake up, fetch, and
diff3 — during which the doc can keep changing. Hub-side, the watcher already
has both conflicting revisions locally and can read → merge → write inside one
local operation, shrinking the window to microseconds, and it resolves
hub-to-hub conflicts even when no client is online at all.

**Safety without coordination:** every write is conditioned on the exact
`_rev` the resolver read (CouchDB's built-in optimistic concurrency — this
includes the delete-via-`_deleted:true` call). If the doc moved on in between
(a third revision landed, or the other hub resolved it first), the write is
rejected; the resolver just re-reads current state and retries. Since both
hubs run the same deterministic algorithm on the same inputs, if both notice
and resolve the same conflict independently they compute the same result —
whichever writes first wins the CAS, the other retries, sees it's already
resolved, and stops. No leader election needed here either.

Every resolution, success or failure, ends with an **explicit CAS-conditioned
delete** of the losing CouchDB revision — writing content to a new file does
not clear `_conflicts` on the original doc; the two are unrelated to CouchDB.
Skipping this step leaves the original doc permanently flagged as conflicted
even though nothing was actually lost.

**Edit vs. edit:**
1. Watcher reads the doc's conflicting leaves + common ancestor O (locally,
   via `open_revs=all`).
2. Attempt diff3(O, A, B).
3. **Clean merge** → write merged result C as the file's content, CAS-delete
   the losing revision from the doc's tree.
4. **Merge fails** → keep the newer file (by mtime) as the active content at
   `path`; copy the older one's bytes into a new file named
   `filename.ext.conflict-DD.MM.YYYY` (append `.0`, `.1`, etc. if that name is
   already taken that day); CAS-delete the losing revision from the original
   doc regardless.
5. On a CAS conflict (`409`) at any step: re-read current leaves, recompute,
   retry. Cheap and local — no client involvement, no network round trip.

**Edit vs. delete (one device deletes, another edits offline):**
- Diff3 doesn't apply — there's nothing to merge against a deletion.
- **Edit wins**: the file stays alive with the edited content as the active
  version, unconditionally (not just "if newer" — an edit always beats a
  delete). CAS-delete the losing (delete) revision from the doc's tree to
  clear `_conflicts`.
- No second/conflict-copy file is needed here — unlike edit-vs-edit, there's
  no content on the "losing" side to preserve. Nothing is lost.
  
## Calendar & Contacts (Phase 2)

Calendar (and later contacts) ride on Radicale as the CalDAV/CardDAV frontend,
with a custom Radicale *storage plugin* that persists collections/items directly
in fss over the Hub Sync API. No local `.ics`/`.vcf` directory to sync.

### Why Radicale + an fss storage plugin
- Radicale speaks CalDAV/CardDAV; standard clients attach for free.
- Storage is pluggable: `[storage] type = <module>` loads a class extending
  `radicale.storage.BaseStorage` (`BaseCollection` / `BaseItem`).
- The default `multifilesystem` backend already maps item -> `.ics`/`.vcf`
  file; an fss backend keeps that shape but stores each "file" as an fss doc,
  so replication / hub-side diff3 / never-lose-data come from the engine.

### Mapping
| Radicale | fss |
|---|---|
| collection (calendar/addressbook) | path prefix `calendars/<uid>/`, metadata in `.Radicale.props` |
| item (VEVENT/VTODO/VJOURNAL/VCARD) | one doc `calendars/<uid>/<item>.ics` / `.vcf` |
| item `etag` | fss `rev` |
| `BaseCollection.sync(old_token)` | `GET /changes?since=<checkpoint>` |
| item write (`upload`) | `POST /changes` (CAS via `base_rev`) |
| conflicts | hub-side diff3 + `.conflict-*` copies |

### Storage plugin (HTTP)
- Python `Storage` class implementing `BaseStorage`/`BaseCollection`/`BaseItem`,
  talking to the hub over the same Hub Sync API every client uses, with the
  shared bearer token. No FFI - honors "clients only speak the hub API".
- Replaces filesystem locking with CouchDB `_rev`/CAS.
- First cut: `sync()` derives a per-collection token from item etags; later
  optimization: persist fss checkpoints per collection.

### Backups
- Live redundancy: fss 2-hub CouchDB replication (already built).
- Point-in-time: restic/borg on the CouchDB data dir (no `.ics` dir remains).

### Clients
- **Android: DAVx5 + Etar/Fossify** - mature; syncs into the Android system
  calendar, which fires OS-level reminders reliably (even in background).
- **Web: STILL TODO.** No standalone open-source CalDAV web client is settled
  yet; see open question below. Candidates evaluated: Nextcloud Calendar,
  AgenDAV, InfCloud, SOGo, Calino (rejected - too small, no server reminders).

### Reminder gap (open)
CalDAV stores `VALARM` reminders *inside* events and Radicale round-trips them,
but no plain CalDAV server *fires* them - delivery is a client's job. Android
(system calendar) is reliable; a web client can only notify while open/PWA.
Server-side email/push reminders require Nextcloud or SOGo, not Radicale.

### Build order (phase 2)
1. Radicale + fss storage plugin against one hub (item CRUD, collection sync).
2. DAVx5 + Etar against Radicale (Android, incl. reminders).
3. Web client: decide from open question below.
4. Contacts: same plugin, CardDAV `.vcf` (no new work).

## Open Questions / Next Steps

Smaller items to resolve as implementation starts, not blockers:

1. **Device token provisioning** — how the shared token gets onto each device
   initially (manual copy, QR code, etc.).
2. **Partial-sync retry** — client wakes, pulls half a batch, gets killed by
   the OS; checkpoint must not advance until the batch is fully durable
   locally.
3. **CouchDB compaction schedule** — still not urgent given confirmed scale
   (single-digit MB per file, upper-2-digit GB available), but plan for it
   since kept revisions grow unbounded by design.
4. **Calendar/contacts (phase 2)** — model `.ics`/`.vcf` as files on this same
   engine once file sync is proven.

## Suggested Build Order

1. **Prove the redundancy story first, with zero custom code**: two standalone
   CouchDB nodes, continuous bi-directional replication configured between
   them. Confirm either node alone serves reads/writes fine.
2. Rust Hub Sync API: `/changes`, `/file/{path}`, `POST /changes` — plain
   non-conflicting replication only, no merge logic yet.
3. Change Watcher: internal `_changes` listener → FCM push; hub-to-hub
   error/staleness → Discord webhook.
4. Rust/WASM client core: checkpointing (via `BlobStore` trait) + pull/push
   against one hub; FCM wake handler + opportunistic triggers.
5. Conflict Resolver inside the hub's Change Watcher: diff3 merge logic, the
   CAS-conditioned write-back and explicit revision deletion, and the retry
   loop on `409`. Fully server-side — no client-facing endpoint needed.
6. Edit-vs-delete special case (also hub-side).
7. Multi-hub client failover (try Hub A, fall back to Hub B).

