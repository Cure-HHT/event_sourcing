# EVS-DEV-destination-drain: Destination queue mechanics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations

## Purpose

This requirement holds the implementation mechanics of a destination's queue: how queue items change status, how the drainer records its attempts, how the fill enqueues items (including the replays that a registry operation requests), how an operator recovers a wedged head, and how a destination is deleted and registered again. A wedged head is a queue head whose status is wedged: the drainer stopped at it and delivery on that destination waits for an operator recovery.

## Assertions

A. Deleting a destination SHALL read the queue head inside the deletion's transaction, refuse a pending head, tombstone a wedged head, delete the pending items behind it, remove every per-destination record the library keeps (the fill position, the schedule and the replay request), and retain the terminal items and the queue-sequence counter; registering a destination SHALL write its hard-delete opt-in as the opt-in in effect, and a deletion SHALL act on, and record, the opt-in in effect; the registry's date, recovery and deletion operations SHALL act on persisted state, SHALL need no destination registered in the calling process, and SHALL refuse an unknown destination with nothing written but the database-wide check record.

B. The library SHALL change a queue item's status only from pending to sent, from pending to wedged, and from wedged to tombstoned.

C. The drainer SHALL commit each delivery attempt in one transaction with the status the attempt produces.

D. The drainer SHALL record an attempt only on the pending head it sent, and the storage layer SHALL refuse to record an attempt on a missing or terminal item.

E. Only the drainer's fill SHALL enqueue queue items: a registry operation that widens a destination's window SHALL record a replay request that the next fill performs with the destination the draining process registers; a gap replay SHALL enqueue only events at or below the fill position, and the fill only events above it; and the library SHALL refuse to move a start date earlier while the queue head is wedged.

F. An operator recovery SHALL read the queue head, remove the trail items and rewind the fill position in one transaction.

G. A fill SHALL write queue items, the fill position or a cleared replay request only when, inside its transaction, the persisted schedule (start date, end date, registration and hard-delete opt-in), the head status, the fill position and the replay request equal those its batch was computed from, and SHALL run the destination's transform and every walk of the log outside that transaction.

U. Every destination-registry operation SHALL decide its outcome, refusals and outcomes that change nothing included, inside one transaction that writes, so that on every backend the decision reflects every commit the storage orders before it; an outcome that changes nothing else SHALL write only a database-wide check record.

## Rationale

**Why refuse to delete a pending head, and retain terminal items (assertion A)?** A pending head may be in delivery in the process that drains: that process sends outside any transaction, and a deletion running elsewhere cannot see the send. Deleting the item would lose the record of a delivery the receiver may have accepted, and the drainer's outcome would then find no item to record it on. A wedged head is never in delivery, because only the drainer wedges an item and it does so after its own send returned. So a deletion waits for the wedge, tombstones the wedged head and deletes only the pending items behind it. Items that were delivered, wedged or recovered are the destination's delivery record; they are kept for the database's lifetime, and the queue-sequence counter is kept with them so that items enqueued after the destination is registered again continue above the retained ones instead of colliding with their keys. Every other per-destination record is removed, so a destination registered again under the same identifier starts a new registration with no fill position, schedule or replay request inherited from the old one; its refill may send again events the old registration's delivered items carried, which is at-least-once delivery.

**Why the latest registration's opt-in (assertion A)?** The hard-delete opt-in is part of a destination's code, so two releases of one application can register the same destination with different opt-ins while both run during a rollout. Refusing a registration whose opt-in differs from the persisted one would fail the new release's start; the latest registration therefore writes its opt-in as the one in effect, and the registration event records it, so the log's latest registration event for a destination always states the opt-in a deletion acts on. A deletion reads the opt-in from the persisted schedule rather than from a destination object, which is what lets a process that does not register the destination (because the destination was removed from its code) delete it.

**Why act on persisted state (assertion A)?** A destination's delivery configuration is code and lives in the process that drains; its schedule, queue and fill position live in the database. Operators and other processes change a destination's dates, recover its queue or delete it by writing persisted state that the drainer acts on at its next pass. An operation that consulted the calling process's own destination objects would refuse a destination registered only elsewhere, and would decide from a copy that another process may have changed.

**Why these status changes only (assertion B)?** Pending to sent and pending to wedged are the drainer's two terminal outcomes. Wedged to tombstoned is how a recovery or a deletion retires a wedged head. There is no path from pending to tombstoned: a pending head may be in delivery, and tombstoning it would race the drainer's outcome. A repeated status is refused as well: with one writer of each transition, a repeat can only come from a defect.

**Why one transaction per attempt and outcome (assertions C and D)?** An attempt recorded without the status it produced, or a status without its attempt, would misstate what happened: a spent retry budget with a pending item, or a wedged item with no failed attempt behind it. Committing both together means a crash between them loses neither alone; the send is then repeated, which is at-least-once delivery. Because no library operation removes or finalizes an item while the drainer attempts it (deletion and recovery refuse a pending head), an attempt on a missing or terminal item can only come from a defect, and the storage layer refuses it instead of tolerating it.

**Why only the drainer enqueues (assertion E)?** A queue item is built by the destination's transform, which is code in the process that drains. If a registry operation built items in the calling process, a process running an older or newer release could enqueue items built by a configuration the drainer does not use. The operation therefore records a replay request, and the drainer's fill performs it with the destination the drainer registers. The gap replay a backward start-date move requests covers only the events the fill has already passed (at or below its fill position) and the fill covers the events above it, so no event is enqueued by both, including an event with an old client timestamp appended after the fill passed the new start date's window. Moving the start date earlier while the head is wedged is refused: nothing is filled behind a wedged head, so the widened window would take effect only through a recovery the operator has not yet run; refusing it makes the operator recover first and widen afterwards, one fixed order instead of two that interleave.

**Why rewind below every removed item (assertion F)?** A recovery removes the wedged head and the pending items behind it, and one of those items may be a gap replay's item carrying events lower than the head's. Reading the head, sweeping the trail and rewinding the fill position in one transaction, to just below the lowest event any removed item carried, means the next fill evaluates every removed event again against the destination's current filter and schedule. Events carried by delivered items above that point are enqueued and delivered again: delivery is at-least-once. A replay request pending at the recovery survives and is bounded by the rewound position.

**Why compare and set (assertion G)?** The fill reads the schedule, the head, the fill position and the replay request, walks the log and awaits the destination's transform before it writes. Another process may delete, register again, reschedule or recover the destination meanwhile. The fill's transaction re-reads that state and writes nothing when any of it changed, so a fill never enqueues an item for a registration that no longer exists, never moves a fill position backwards, and never admits an event past an end date set while it ran. The head is compared by its status alone: the fill appends behind the head, so a head delivered meanwhile does not change what it may write, while a head wedged meanwhile means nothing may be filled behind it. The transform and the log walk run outside the transaction because a backend may run a transaction body again after a conflict, and consumer code must not run inside a body the storage may repeat without bound.

**Why decide every registry outcome in a writing transaction (assertion U)?** A browser database shared by several tabs validates a transaction against the other tabs' commits only when the transaction writes; a read-only transaction may read a tab's cached state. A refusal, or an outcome that changes nothing, therefore writes one small database-wide record and returns its outcome, and the operation throws or returns after the commit, so no outcome is decided on stale data. On a serializable server database the write is harmless.

## Changelog

- 2026-09-23 | c29a506a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-G and U: queue deletion and re-registration, status changes, attempt and outcome atomicity, drainer-only enqueue, recovery, fill compare-and-set, registry decisions in writing transactions

*End* *Destination queue mechanics* | **Hash**: c29a506a
