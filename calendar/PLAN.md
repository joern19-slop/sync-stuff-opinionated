# Web Calendar — Implementation Plan

Reuse the **Tuta web calendar UI**, swap its persistence seam for the
**filesync sync engine compiled to WASM**, storing each event as an **`.ics`
file** over the Hub Sync API. Single user, no sharing/attendees first.

Complements (and supersedes the Phase-2 sketch in `ArchitecturePlan.md`, which
assumed a Radicale/CalDAV frontend): here the web client *is* the frontend, and
filesync *is* the storage backend.

## Decisions locked in

- **Storage**: one RFC-5545 `.ics` file per event, at `calendars/<calendar>/<uid>.ics`.
  - CalDAV-native (one resource per event), so a later CalDAV/Radicale layer drops in cleanly.
  - Sidesteps the "concurrent appends to one big file" diff3 conflict (each event is its own file).
  - Same-event concurrent edits still get the hub's existing diff3 resolver.
- **Frontend**: fork the `tutanota` monorepo (GPL-3.0) and reuse the calendar UI as-is; vendored as a git **submodule**.
- **Backend**: `client-core` (`SyncEngine`) compiled to `wasm32` and exposed via `wasm-bindgen`; the browser is a first-class filesync client.
- **Transport**: Hub Sync API (`GET /changes`, `POST /changes`, `GET /file`, `GET /changes/longpoll`) → CouchDB.
- **Toolchain**: `wasm32-unknown-unknown` + `wasm-pack`, and `bun` for the Tuta fork.
- **Token provisioning**: browser `prompt()` on first run, then persisted (localStorage); passed into the WASM `HubClient`.
- **CORS**: allow `*` for now (TODO: tighten to the app's origin).

## Architecture

```
Tuta calendar UI (TS/Mithril, unchanged)
        │  Tuta entities (CalendarEvent)
        ▼
EntityRestInterface impl (TS)          ← the swap point (see TUTA-CALENDAR-NOTES.md)
        │  translate Tuta entity ↔ .ics VEVENT
        ▼
client-core SyncEngine (wasm-bindgen)  ← checkpointing, pending queue, failover
        │
        ▼
Hub Sync API  (HTTP)
        │
        ▼
CouchDB  (calendars/<name>/<uid>.ics)
```

## The seam (from the Tuta notes)

`EntityClient` is constructed with an `EntityRestInterface`
(`src/platform-kit/network/EntityRestCacheInterface.ts`): `load`, `loadRange`,
`loadMultiple`, `setup`, `setupMultiple`, `update`, `erase`, `eraseMultiple`,
`onEntityUpdatesReceived`, plus cache/timestamp helpers. The production impl
(`DefaultEntityRestCache` → `EntityRestClient` → `InstancePipeline` → `RestClient`)
holds *all* the crypto; the calendar UI never sees it.

Our swap is a plaintext `EntityRestInterface` that talks to the WASM engine. The
three Tuta-side blockers from the notes map to:

1. **Implement `EntityRestInterface`** → against our WASM façade.
2. **Adopt the entity metamodel** → single-user scope collapses `loadRange`/pagination to "return all events"; list-id = calendar folder, element-id = event UID.
3. **Shim the event-batch sync** (websocket `onEntityUpdatesReceived`) → poll / long-poll, then re-emit updates.
4. **Swap auth** → bypass Tuta login/session; use the filesync device token.

## Entity ↔ `.ics` mapping

| Tuta `CalendarEvent` | RFC-5545 VEVENT |
|---|---|
| `summary` | `SUMMARY` |
| `description` | `DESCRIPTION` |
| `startTime` / `endTime` | `DTSTART` / `DTEND` |
| `location` | `LOCATION` |
| `uid` | `UID` |
| `sequence` | `SEQUENCE` |
| `repeatRule` | `RRULE` / `EXDATE` / `RDATE` |
| `alarmInfos` | `VALARM` |
| `attendees` / `organizer` | `ATTENDEE` / `ORGANIZER` |
| `startTimeZone` / `endTimeZone` | `TZID` / `VTIMEZONE` |

- Event file: `calendars/<calendarId>/<uid>.ics`.
- Calendar collection metadata (name, color, id): one small file per calendar,
  e.g. `calendars/<calendarId>/.calendar.json`.
- **Where translation lives**: the Tuta-side `EntityRestInterface` does the
  `CalendarEvent ↔ .ics` parse/serialize (via `ical.js`); the WASM engine stays
  calendar-agnostic — it just syncs `.ics` bytes like any other file.

## Toolchain
- `wasm32-unknown-unknown` target (`rustup target add wasm32-unknown-unknown`)
- `wasm-pack` (Rust → wasm-bindgen bundle)
- `bun` (Tuta fork: install + dev server)

> Risk to verify early (Phase 3): the `tutanota` repo historically builds with
> node/yarn; `bun` is mostly compatible but the 30k-LOC Mithril build may hit
> edge cases. Fall back to node/yarn *just for the fork* if so.

### Phase 1 — WASM core (Rust, self-contained)
- Make `client-core` compile to `wasm32`:
  - `reqwest` with the `js` feature (browser `fetch`).
  - `?Send` / single-threaded bounds on the engine + `async_trait`.
- `wasm-bindgen` façade exposing a small API: `list_files`, `read_file`,
  `record_upsert`, `record_delete`, `sync`, `checkpoint`.
- Web `FileStore` (event `.ics` bytes) on **OPFS**; `MetaStore` (checkpoint,
  pending queue, per-file metadata) on **IndexedDB**.
- A background sync loop mirroring the desktop client: long-poll
  `GET /changes/longpoll` → reconcile → `sync()`.

### Phase 2 — hub: browser access
- CORS on the hub (`Access-Control-Allow-Origin` for the web app's origin).
- Device-token provisioning for the browser (see open questions).

### Phase 3 — Tuta fork + seam
- Clone `tutanota`; confirm the calendar app builds and runs locally.
- Implement the plaintext `EntityRestInterface` in TS calling the WASM façade.
- Swap auth and shim the websocket sync (poll → `onEntityUpdatesReceived`).

### Phase 4 — entity ↔ `.ics` translation
- Implement the mapping table above; UID ↔ element-id resolution.
- Calendar collection metadata files.

### Phase 5 — feature parity (best-effort)
- Walk Tuta's calendar feature set; implement what's straightforward, defer the
  rest to a `NotImplemented`/`TODO` file in `calendar/`.
- Expected-heavy (defer): recurrence *expansion* for rendering, server-side
  reminders (the known gap), invites/attendees/sharing.

## Open questions / decisions

1. **Token provisioning** — `prompt()` on first run, persist (localStorage), pass to the WASM `HubClient`. *Resolved.*
2. **CORS scope** — allow `*` for now; TODO to tighten. *Resolved.*
3. **Tuta build integration** — git submodule. *Resolved.*
4. **Entity metamodel fidelity** — *open, leaning minimal.* My recommendation:
   implement `EntityRestInterface` with the minimum that compiles and serves
   the calendar paths we actually use — one Tuta "list" per calendar, element
   id = event UID, `loadRange`/`loadMultiple` return everything, stub out
   `_ownerGroup`/`_permissions`/pagination. Don't chase Tuta's full
   `TypeRef`/`PersistentEntity` semantics in the first throw.
5. **OPFS vs IndexedDB split** — OPFS for content, IndexedDB for metadata. *Resolved.*
6. **`bun` vs the tutanota build** — verify in Phase 3; node/yarn fallback if `bun` chokes.

## Risks

- **Entity metamodel is the heavy contract** (the Tuta notes flag it): IDs,
  list/element structure, range pagination, `_ownerGroup`/`_permissions`.
- **`?Send` / `reqwest`-js / OPFS** is the messiest Rust part (already flagged
  in `NOTES.md` as "WASM target, not done").
- **Fork maintenance**: tracking upstream Tuta while carrying a patched seam.
