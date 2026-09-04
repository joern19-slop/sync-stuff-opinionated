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
  - **Correction**: don't use `ical.js` — Tuta already ships a full iCalendar
    parser/serializer for its import/export feature:
    `calendar/export/CalendarParser.ts` (`parseCalendarStringData` /
    `parseICalendar` / `parseRrule` / …) and `CalendarExporter.ts`
    (`serializeEvent` / `serializeCalendar`). Reuse those.

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

#### Phase 3 reconnaissance (done, against upstream `tutanota` @ master)

The seam is real but **deeper than the notes implied**:

- `EntityRestInterface` — `src/platform-kit/network/EntityRestCacheInterface.ts`:
  9 methods (`load`, `loadRange`, `loadMultiple`, `setup`, `setupMultiple`,
  `update`, `erase`, `eraseMultiple`, `onEntityUpdatesReceived`), plus the
  `EntityRestCache` extension (7 more: `purgeStorage`, `recordSyncTime`,
  `timeSinceLastSyncMs`, `isOutOfSync`, `deleteFromCacheIfExists`,
  `updateCacheWithMissedEntityUpdates`, `setCacheSyncStatus`).
- Instantiated as `new DefaultEntityRestCache(entityRestClient,
  maybeUninitializedStorage, typeModelResolver, patchMerger, lastProcessed)` in
  `src/applications/calendar-app/workerUtils/index/CalendarWorkerLocator.ts:225`,
  then handed to `EntityClient` on the main thread (`calendarLocator.ts:639`)
  via the worker.
- The production chain is `DefaultEntityRestCache → EntityRestClient →
  InstancePipeline (crypto) → RestClient (HTTP)`; the worker also owns login
  (`LoginController`/`LoginFacade`/`CredentialsProvider`/2FA/Webauthn) and the
  websocket `EventController` that pushes entity updates.
- The calendar UI is coupled to mail (`MailboxModel`) and contacts
  (`ContactModel`) through the shared locator, so an in-place swap must shim
  login + the websocket + a dozen facades, not just `EntityRestInterface`.

**Toolchain**: `package.json` requires Node `>=24.17.0`; `bun 1.4.0` reports
node `26.3.0`, so the version gate is satisfiable, but the repo ships
`package-lock.json` (npm). Per "never use two systems", use `bun` consistently.

**Build (green).** `node webapp prod --app calendar` completes end-to-end under
bun. Reproducible setup:

1. Install **Emscripten 3.1.59** (`emsdk install 3.1.59 && activate`); put
   `emsdk/upstream/bin` (for `wasm2js`) and the emsdk `node` on `PATH`.
2. Root: `bun install --ignore-scripts` (after deleting the stale
   `workspaces: ["./src/licc"]` entry and `package-lock.json`); then
   `bun install` in `src/app-kit/mimimi` (its `preinstall` normally does this
   via `npm ci`).
3. The two desktop git deps (`@signalapp/sqlcipher`,
   `@indutny/simple-windows-notifications`) ship no `dist/`, so their `prepare`
   (tsc) never ran under `--ignore-scripts` — build their `.d.ts` once:
   `tsc` in each package's dir (needs `node` on PATH).
4. Build scripts call `npm run`; patched to `bun run` (committed on
   `filesync-backend`).

The build emits `build/` (bundled JS) but the **HTML entry** is generated by
the `release`/`createHtml` path, not `prod` — a follow-up before serving.

**Strategic note**: the bulk of that native/WASM build is *crypto*
(argon2/liboqs/InstancePipeline), which the plaintext swap removes. So a full
"unmodified baseline build" is expensive for code we're about to delete; the
leaner path is to swap first (stub the backend, drop crypto) and then build.

**Transitive closure (mapped).** The 57 calendar view/model/gui/export files
transitively import **1103 source files**:

| bucket | files |
|---|---|
| `common` (mail/contacts/api/worker) | 411 |
| `ui` (the UI kit) | 204 |
| `platform-kit` | 180 |
| `mail-app` | 146 |
| `calendar-app` | 62 |
| `app-kit` | 42 |
| `entities` | 21 |
| other apps | 18 |
| `common/calendar` | 18 |

Plus the whole `@tutao/*` library surface (`entities`, `app-env`, `utils`,
`meta`, `native-bridge`, `crypto`, `rest-client`, `instance-pipeline`).

**Consequence**: the calendar is *not* cleanly separable — it depends on mail
(`MailboxModel` for the user's calendar group, `SendMailModel` for invites) and
contacts (attendees) transitively (146 `mail-app` + most of the 411 `common`
files). So "extract the calendar" ≈ "keep the whole app". Realistic options:
- **swap in place** (fork monorepo, replace `restInterface`/`DefaultEntityRestCache`
  + shim login + websocket), keeping the mail/contacts coupling — matches
  "don't rewrite things".
- **rewrite a minimal single-user calendar** (strip attendees/invites UI) — a
  rewrite, out of scope per "don't rewrite".

**Direction (decided): extract, then delete the rest.**

- `EntityRestInterface` alone is **not** the whole seam: login is orthogonal to
  it. The calendar app boots through `LoginController`/`PostLoginActions` and
  19 of 73 `calendar-app` files read `logins.getUserController()` (timezone,
  mail addresses, calendar group id). So overwriting `restInterface` still
  leaves login + a user session to satisfy. Extraction deletes login and
  provides a minimal user stub instead of shimming it.
- The websocket (`EventController`) is Tuta's server→client *push*; we don't
  need it — our backend is long-poll. Delete it and drive entity updates from
  the `WebSync` long-poll → re-read `.ics` → emit updates.
- Sizes: `calendar-app` ~24k LOC (73 files), `common/calendar` ~5.7k LOC, the
  UI proper (`calendar/view`) is 19 files. Extraction = pull the view/model
  layer + its minimal `platform-kit`/`ui` closure into a small Mithril host.
- **Rebasing against upstream is deferred** to the end (the extraction will
  diverge; figure out the reconcile strategy then).
- **Translation is already implemented by Tuta** (no `ical.js`, no new mapper):
  `CalendarParser.ts` (`parseCalendarStringData`) for `.ics`→entity, and
  `CalendarExporter.ts` (`serializeCalendar`) for entity→`.ics`; entity
  construction (incl. `_id`/`_ownerGroup`) is `CalendarModel.createEvent` /
  `CalendarImporter.prepareEventForImport`. The swap is mostly *wiring*.
- **Keep crypto in place** (skip dropping argon2/liboqs/InstancePipeline): it
  stays unused-but-present once `restInterface` no longer goes through it, and
  removing it adds complexity for no functional gain. (Cost: the build stays
  heavy — acceptable.)
- **Repo**: fork `joern19-slop/tutanota` as a submodule at `third_party/tutanota`;
  work on branch `filesync-backend` (not master). Remotes: `origin` = fork,
  `upstream` = `tutao/tutanota`.

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
