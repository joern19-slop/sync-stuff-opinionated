# Web Calendar — Status & Handoff

Last updated: login bypass implemented (builds + typechecks green; not yet committed).

## What's done

The whole "swap Tuta's backend for filesync" is wired and **builds green**
(`node webapp prod --app calendar` → exit 0). Live on the fork at
`joern19-slop/tutanota`, branch `filesync-backend`.

1. **`client-web` wasm** (in the main `filesync` repo): the sync engine +
   OPFS `FileStore`/`MetaStore`, worker-compatible (`navigator.storage` via the
   global scope, not `window`), exposed via wasm-bindgen as
   `init(hubs, token) -> WebSync` with `list_files`/`read_file`/`put_file`/
   `delete_file`/`sync`/`checkpoint`.
2. **In the fork, `src/applications/calendar-app/filesync/`**:
   - `wasm.ts` — loads the wasm (mirrors the `crypto-primitives` pattern).
   - `ics.ts` — `eventToIcs`/`icsToEvents` over Tuta's own
     `CalendarExporter.serializeCalendar` + `CalendarParser.parseCalendarStringData`.
   - `entityFromIcs.ts` — verbatim extraction of Tuta's
     `makeCalendarEventFromIcsCalendarEvent` (+ its two helpers), so the worker
     doesn't drag in `ImportExportUtils`'s main-thread `lang` dependency.
   - `EntityRestCache.ts` — `FilesyncEntityRestCache implements EntityRestCache`:
     calendar events ↔ one `.ics` per event (`calendars/default/<uid>.ics`),
     hardcoded `CalendarGroupRoot`, everything else returns empty.
3. **Wiring** (all type-checks, all bundles):
   - `CalendarWorkerLocator.ts` — factory `() => new FilesyncEntityRestCache()`,
     `initLocator(..., hubUrl, deviceToken)` awaits `initFilesync` first.
   - Token flow: `WorkerClient` (main) reads `filesync-hub`/`filesync-token`
     from localStorage (prompt fallback) → `setup` message → `calendar-worker.ts`
     → `CalendarWorkerImpl.init` → `initFilesync`.
   - `CacheManagementFacade`/`CalendarFacade` widened `DefaultEntityRestCache` →
     `EntityRestCache`.
4. **Build config** (`buildSrc/`):
   - `buildWebapp.js` — `Copy filesync wasm` step → `build-calendar-app/client_web_bg.wasm`.
   - `RollupConfig.js` — `calendar/export` → `date` chunk, `worker` may import
     `date`, `EnumUtils` → `common`, `filesync/` → `worker`.

## The login bypass (done)

The app now boots straight into the calendar — no Tuta login page, no Tuta
server. The seam is a fabricated single-user "world" shared by both threads:

- `src/applications/calendar-app/filesync/world.ts` (`@bundleInto:common`)
  builds a fixed set of entities with stable ids (`filesync-user`,
  `filesync-user-group`, `filesync-calendar-group`, `filesync-customer`, ...):
  `User` (FREE account, user+mail+calendar memberships), `GroupInfo`/`Group`
  for the user and calendar groups, `CalendarGroupRoot`, `MailboxGroupRoot`,
  `TutanotaProperties`, `UserSettingsGroupRoot`, `Customer`/`CustomerInfo`
  (`plan = Free` so `isNewPaidPlan()` is false).
- **Main thread** (`calendarLocator.ts: filesyncLogin()`): fabricates a
  `UserController` and calls `LoginController.filesyncLogin()`, which sets it
  and runs the post-login actions directly. The heavy `PostLoginActions`
  (mailbox init / usage tests / news / approval checks — all server calls) is
  no longer registered; only `setupCalendarModels` remains.
- **Worker thread** (`CalendarWorkerLocator.initLocator`): after
  `createBaseLocator`, seeds `locator.base.user` (the `UserFacade`) with
  `setAccessToken` + `setUser(world.user)` so facades like `CalendarFacade`
  can read the logged-in user.
- `FilesyncEntityRestCache` now serves those infra entities from `load()` (and
  `loadRange`/`loadMultiple` return empty for the rest), and throws
  `NotFoundError` for unknown types so the various `.catch(ofClass(NotFoundError))`
  paths behave like the real client. Calendar events still come from the wasm
  `.ics` files.
- Routing: the root `/` resolver redirects to `/calendar` instead of forcing
  login.

Verified: `bun run calendar:types` and `node webapp prod --app calendar
--disable-minify` are both green.

## Smoke-tested end-to-end (working)

Ran the real stack (CouchDB via podman + `hub-api` on `:8080` + the served
`build-calendar-app/` on `:9000`) and drove it with headless Firefox. The
calendar boots into `/calendar/month/...` and renders events pulled from the
hub. This surfaced two bugs that are now fixed:

- **No initial sync.** `WebSync` (client-web wasm) only builds the engine; it
  needs an explicit `sync()` to pull. `wasm.ts: initFilesync()` now does an
  initial `sync()` (error-tolerant) after creating the engine.
- **`_ownerGroup` was unset.** `FilesyncEntityRestCache.loadAllEvents` assigned
  the event id but not `_ownerGroup`; `shouldDisplayEvent` asserts it, so
  events silently failed to render. Now set from the group root.
- **List-id filtering.** `loadRange`/`loadMultiple` now filter `CalendarEvent`
  by the requested list id (short vs long) instead of returning everything for
  both lists.

The serving setup used `index.html` (Browser mode, no CSP), not
`index-app.html` (App mode, has a `connect-src` CSP that would block the hub).
Hub CORS is already `CorsLayer::permissive()`.

## Build / run (reproducible)

Prereqs (already installed here): `emsdk` with **Emscripten 3.1.59** activated
(`source ~/emsdk/emsdk_env.sh` + put `emsdk/upstream/bin` on PATH for `wasm2js`),
Rust/cargo, wasm-pack, `bun`.

```bash
cd third_party/tutanota
# one-time deps (already done):
#   bun install --ignore-scripts  (root)
#   bun install  (in src/app-kit/mimimi)
#   tsc in node_modules/@signalapp/sqlcipher and @indutny/simple-windows-notifications (build their .d.ts)
node webapp prod --app calendar --disable-minify
# output: build-calendar-app/  (serve this; the HTML entry comes from the release/createHtml path, still TODO)
```

Type-check only: `bun run calendar:types`.

## Gotchas

- **Chunking is manual and strict.** Adding a worker dependency on a
  main-thread module fails the `bundle-dependency-check`; use the `date`/`common`
  chunks (or `@bundleInto:common`) and add to `worker`'s `allowedImports`.
- The `.ics` parser/exporter needed `EnumUtils` (moved to `common`) and the
  `makeCalendarEventFromIcsCalendarEvent` extraction (to drop `lang`/boot).
- `client-web` wasm runs in a **Web Worker**; keep it `window`-free (done).
- `bun install --ignore-scripts` skips `prepare` of the desktop git deps, so
  their `.d.ts` had to be built by hand once.
- Token is read in `WorkerClient` (main thread) from localStorage, not yet a
  proper settings UI.

## Next steps

1. **Live updates / long-poll.** `onEntityUpdatesReceived` is still a no-op and
   the only sync is the initial one in `initFilesync`. Wire a long-poll (or
   poll) loop that calls `WebSync.sync()` and re-emits updates so remote
   changes appear without a reload, and feed `SyncTracker` (which
   `calendarEventUpdateCoordinator.init()` currently blocks on via
   `waitSync()`).
2. **Range filtering.** `loadRange` returns all events and `loadReverseRangeBetween`
   only enforces the lower id bound, so events leak past the upper bound and
   are re-added across adjacent month loads (harmless visually, but not
   correct). Filter/sort by element id in `loadRange` to fix pagination.
3. **Event create/edit via the UI** (`CalendarFacade` path + alarms) — currently
   reads render, writes are untested.
4. **Clean up**: alarms/reminders (deferred), per-calendar (not hardcoded
   `default`), the stale document title / "Offline" indicator (both from
   skipping `PostLoginActions` + the websocket).
