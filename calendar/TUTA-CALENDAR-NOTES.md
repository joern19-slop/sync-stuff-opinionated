# Tuta (Tutanota) calendar client — architecture notes

Independent assessment of the Tuta web calendar client, focused on the goal
"patch an existing calendar client to talk to my own backend instead of the
vendor's server".

Repo: https://github.com/tutao/tutanota (GPL-3.0).
Calendar code: ~30,000 LOC across `src/applications/calendar-app` and
`src/applications/common/calendar`.

## Framework

Tuta is built on **Mithril** (`mithril@2.2.2`), not React. All views are Mithril
components depending on Tuta's own UI layer (`src/ui/`, `src/platform-kit/`).
This matters only if the goal is to port the UI into a React app; for the goal of
*patching in place*, the framework is irrelevant.

## Layering

From bottom to top:

```
entities (schema-generated data types + RPC metadata)
  -> repository (CalendarEventsRepository)   data access
  -> model (CalendarModel)                    business logic
  -> view-model (CalendarViewModel, EventWrapper)
  -> view (Mithril components)                rendering
```

## The entity: already decrypted

The core type `CalendarEvent` (`src/entities/tutanota/TypeRefs.ts:2625`) is a
**decrypted domain entity**, not a ciphertext wire object:

```
// readable values
summary, description, startTime, endTime, location, uid, hashedUid, sequence,
invitedConfidentially, recurrenceId, sender, pendingInvitation,
startTimeZone, endTimeZone

// associations
repeatRule: CalendarRepeatRule, alarmInfos, attendees, organizer

// encryption / framework metadata (schema-managed)
_id, _permissions, _format, _ownerGroup, _ownerEncSessionKey,
_ownerKeyVersion, _kdfNonce, bucketKey, _type, _errors, _original, isAdapter
```

Title/location/description are plaintext. Rendering does not require a separate
decrypt/verify pipeline — the entity *is* renderable. Encryption metadata leaks
in via the `_`-prefixed fields, but it is not ciphertext.

## The persistence seam (the important part)

The transport is layered behind a genuine interface. All calendar persistence
flows through:

```
CalendarModel -> CalendarFacade -> EntityClient -> EntityRestInterface   <- seam
        new EntityClient(restInterface, typeModelResolver)   (calendarLocator.ts:639)
    production impl: DefaultEntityRestCache -> EntityRestClient -> InstancePipeline (crypto) -> RestClient (HTTP)
```

- `EntityClient` takes an `EntityRestInterface`
  (`src/platform-kit/network/EntityRestCacheInterface.ts:15`): `load`, `loadRange`,
  `loadMultiple`, `setup`, `setupMultiple`, `update`, `erase`, `eraseMultiple`,
  `onEntityUpdatesReceived`, plus cache/timestamp helpers.
- The production implementation is `DefaultEntityRestCache` →
  `EntityRestClient`, and **all the encryption lives there** (`InstancePipeline`,
  `CryptoMapper`, `@tutao/crypto`: `AesKey`, `KdfNonce`, `_ownerEncSessionKey`,
  `bucketKey`).
- Crypto does **not** reach the calendar UI. The only leak is `CalendarModel`
  resetting `event._ownerEncSessionKey = null` for new entities (comment says it
  is "assigned by the CryptoFacade"). No crypto in views, form, or read path.

## The view-model seam

Leaf views consume `EventWrapper`
(`src/applications/calendar-app/calendar/view/CalendarViewModel.ts:160`):

```ts
interface EventWrapper {
    event: CalendarEvent          // the core event instance
    flags: EventWrapperFlags      // display concerns (isGhost, isConflict, ...)
    color: string                 // resolved display color
}
```

Rendering depends on `EventWrapper`, not the storage/entity type. A genuine
indirection layer.

## Write path

- Create: `CalendarModel.createEvent` → `CalendarFacade.createCalendarEvents` →
  `EntityClient.setupMultipleEntities(listId, events)`
- Update: `CalendarFacade.updateCalendarEvent` copies `_ownerEncSessionKey` from
  the existing event, then `EntityClient.update(event)`
- Delete: `EntityClient.erase(oldEvent)`

Encryption is handled inside `EntityRestClient`/`InstancePipeline`; the model
calls in with a plain entity. No mid-save key/`SharedEventID` dance.

## Real blockers for "point it at my own backend"

1. **Implement `EntityRestInterface`.** This is the swap point. Write a plaintext
   implementation (~15 methods) that talks to my own server. No crypto to
   reimplement — it is entirely isolated behind this interface.
2. **Adopt Tuta's entity metamodel.** The interface operates on `TypeRef` +
   `PersistentEntity` with Tuta's IDs, list/element structure, range pagination,
   and `_ownerGroup`/`_permissions`. My backend must store/return Tuta-shaped
   entities. Clean seam, heavy contract.
3. **Shim the event-batch sync.** Tuta pushes updates over a websocket
   (`EventController`, `onEntityUpdatesReceived`,
   `LastProcessedEventBatchProvider`). My server won't emit those, so I shim this
   to poll/refresh.
4. **Swap auth.** `RestClient` does Tuta's login/session.

## Decoupling assessment

**Reusable as code: not via a "lift the UI files" path** — Mithril + Tuta's own
UI kit means there is nothing to port to another framework.

**Patchable in place: yes, and cleanly.** Encryption is fully isolated in
`EntityRestClient`, and `EntityClient` exposes an `EntityRestInterface` that can
be swapped. Patching = (a) implement a plaintext `EntityRestInterface` against my
backend, (b) shim the websocket sync, (c) swap auth. No crypto work, no UI
rewrite.

**Useful as a reference: two specific things.**

1. **Decrypted domain entity.** The model the UI consumes has plaintext
   `summary`/`startTime`/`endTime`/`location`; encryption is separate.
2. **View-model indirection.** Views consume `EventWrapper { event, flags, color }`,
   not the storage type directly.
