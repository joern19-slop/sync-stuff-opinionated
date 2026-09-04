# Web Calendar — Status & Handoff

Last updated: end of session (everything below is committed/pushed).

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

## Remaining: the login bypass

The backend is wired, but the app still boots into the **login page**. This is
the last real chunk. Findings:

- Routing: `src/applications/calendar-app/calendar-app.ts:550`
  `if (requireLogin && !logins.isUserLoggedIn())` → login; `:553` redirects away
  from login when logged in.
- `LoginController.isUserLoggedIn()` (`.../api/main/LoginController.ts:198`)
  returns `this.userController != null`; `getUserController()` asserts it.
- `userController` is set during login: `this.userController = await initUserController(initData)`
  (`LoginController.ts:105`); `initUserController` (`.../api/main/UserController.ts:397`)
  loads a big graph from the server.
- `UserController` ctor needs: `user: User`, `userGroupInfo: GroupInfo`,
  `sessionId`, `props: TutanotaProperties`, `accessToken`, `userSettingsGroupRoot`,
  `sessionType`, `loginUsername`, `entityClient`, `serviceExecutor`, `customer`.

Two approaches (unpick tomorrow):

- **A — fabricate a minimal `UserController`** and set `logins.userController`
  directly (skip `initUserController`). Need to `create` the `User`,
  `GroupInfo`/`UserGroupInfo`, `TutanotaProperties`, `UserSettingsGroupRoot`
  entities with the fields the ~19 `getUserController()` consumers read
  (timezone, `alarmInfoList`, `userGroupInfo`, calendar group). Medium-large.
- **B — stub `LoginController`** so `isUserLoggedIn()` returns true and
  `getUserController()` returns a hand-built `UserController`. Same fabrication
  work, different seam.

Whichever: the calendar also needs a `CalendarGroupRoot` (we already return a
hardcoded one from the cache), and likely a `UserAlarmInfo` list (alarms are
stubbed out for now).

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

1. Login bypass (A or B above) → app boots straight into the calendar.
2. Serve `build-calendar-app/` and smoke-test against a live hub + CouchDB.
3. Wire the long-poll trigger (currently `onEntityUpdatesReceived` is a no-op;
   drive re-loads off the `WebSync.sync()`).
4. Clean up: alarms/reminders (deferred), per-calendar (not hardcoded `default`),
   the release-HTML entry point.
