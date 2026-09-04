# Decoupling Proton Calendar UI from the Proton backend

Notes on what is/isn't cleanly reusable, for the case of a single user with a
custom backend and no calendar sharing.

## Scope assumption

- Single user, no calendar sharing, no attendees/RSVP/invites for now.
- Goal: reuse the UI only; fetch/write against my own server with my own protocol.

## Key type confusion to avoid

There are two very different "event" types:

- `CalendarEvent` (`packages/shared/lib/interfaces/calendar/Event.ts`) is the
  **encrypted server wire format** — ciphertext cards + OpenPGP key packets
  (`CalendarKeyPacket`, `SharedKeyPacket`, `AddressKeyPacket`, `CalendarEventData`
  with `Type`/`Data`/`Signature`). Do **not** fabricate these.
- `VcalVeventComponent` (`packages/shared/lib/interfaces/calendar/VcalModel.ts:257`)
  is a **plain iCalendar** object (`uid`, `dtstart`, `dtend`, `summary`,
  `description`, `location`, `rrule`, `attendee[]`, `organizer`). This is the
  clean domain model the UI actually renders, produced via `propertiesToModel`.

## What is cleanly reusable as-is

The pure geometry/layout layer has zero domain/store dependencies:

- `components/calendar/layout.ts`, `sortLayout.ts`,
  `splitDayEventsInInterval.ts`, `splitTimeGridEventsPerDay.ts`,
  `useDayGridEventLayout.ts`
- `components/calendar/mouseHelpers/*`, `interactions/*`,
  `components/calendar/DayGrid/helper.ts`

This is the reusable "calendar rendering engine" (event layout, collision
splitting, drag/select math, time-to-pixel mapping).

Everything else (popover, event form/modal, sidebar, RSVP) imports Proton's
domain types and UI kit directly:
- 169 `.ts`/`.tsx` files import `@proton/shared/lib/interfaces/calendar`
- 118 of 190 `.tsx` files import `@proton/components`

## Read path seam

The UI reads an event via `useReadEvent.ts` (`components/events/useReadEvent.ts`),
consuming `CalendarViewEvent.data.eventReadResult.result` =
`DecryptedVeventResult`:

```
{ veventComponent, selfAddressData, verificationStatus, hasDefaultNotifications }
```

plus `eventData` metadata (`IsOrganizer`, `IsProtonProtonInvite`, `Permissions`).

So the injection point for display is: provide a plain `veventComponent` +
that small wrapper, and skip the decrypt/verify pipeline entirely.

## Write path seam

All event create/update/delete funnel through one place:

```
EventForm -> EventModel -> modelToVeventComponent -> VcalVeventComponent (clean iCal)
  -> getSaveEventActions -> SyncEventActionOperations[]
  -> getSyncMultipleEventsPayload   <- encryption happens here
  -> handleSyncActions -> api(syncMultipleEventsRoute)  <- single write call
```

- `handleSyncActions` in `containers/calendar/InteractiveCalendarView.tsx:1513`,
  single `api(...)` at line 1654.
- Input is already clean: `SyncEventActionOperations`
  (`containers/calendar/getSyncMultipleEventsPayload.ts:70`) carries a clean
  `veventComponent` + metadata (`hasDefaultNotifications`, `isAttendee`,
  `removedAttendeesEmails`, `color`, `isBreakingChange`).
- Encryption (`createCalendarEvent`, `packages/shared/lib/calendar/serialize.ts:45`)
  is downstream of this boundary.

For single-user events with no attendees, patching `handleSyncActions` to POST
the clean `veventComponent` + metadata to my own server skips the crypto.

## Three edges that are not clean (and how they shrink under this scope)

1. **Invites/shared events do crypto upstream of the funnel.**
   `getSaveEventActions.ts:97` and `getSaveEventActionsHelpers.ts:40`
   (`createIntermediateEvent`) need `SharedEventID` / session keys mid-save.
   -> Mostly irrelevant for a single-user, no-attendee scope.

2. **The write response must still be a Proton `CalendarEvent` shape.**
   `handleSyncActions` expects `{ Responses: [{ Response: { Code, Event } }] }`
   and feeds `Response.Event` into `fetchPaginatedAttendeesInfo` and the read
   path. A clean write still has to return a Proton-shaped `Event`
   (`ID`, `SharedEventID`, `AttendeesInfo`, ...) for the UI to reconcile.

3. **Post-write refresh is Proton's event loop.**
   State is reconciled via `calendarModelEventManager` (`bootstrap.ts:204`,
   external `@proton/calendar`) which polls Proton's model-event protocol.
   My server won't emit those, so the refresh path needs a shim too — which
   loops back to the read side.

## Practical path for single-user scope

1. Lift the geometry/layout layer as-is (already clean).
2. Read: inject `DecryptedVeventResult` (plain `veventComponent` + wrapper).
3. Write: intercept `handleSyncActions`, send clean `veventComponent` + metadata
   to my server, return Proton-shaped `Event` responses.
4. Refresh: shim `calendarModelEventManager` / the model-event listener to
   reconcile against my own backend (the read side).

The invite/sharing/member/attendee machinery (and its crypto) can be ignored for
now and reimplemented separately if ever needed.
