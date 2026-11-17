# EVS-PRD-destination-key-hold: Key-scoped holds on a destination

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations

## Purpose

A destination may declare a key for each event it queues, naming the subject whose events must reach the receiver in order. When the receiver refuses one subject's event for good, the library holds that subject's events back and keeps delivering every other subject's, and an operator releases the held events once the cause is fixed. A destination that declares no key behaves as every destination did before keys.

## Assertions

A. Where a destination declares a key for each event it queues, when its delivery implementation reports a permanent failure scoped to the key of the item at the head of its queue, the library SHALL take that item and every queued item of that key out of the queue, while every queued item of the destination has a key, and keep their events, in sequence order, as that key's held events.

B. While a key of a destination is held, the library SHALL continue delivering the destination's queued items of every other key.

C. While a key of a destination has held events, the library SHALL queue that key's events on the destination only through a release of that key, in sequence order.

D. If the delivery implementation reports a permanent failure scoped to the key of the head item while the head item or any other queued item of the destination has no key, then the library SHALL wedge the destination's queue.

E. When an operator releases a held key, the library SHALL queue the key's held events on the destination again, in sequence order.

F. If the destination's transform fails on a released event until the retry budget is exhausted, then the library SHALL hold that event's key again.

G. An operator recovery SHALL return the events of every removed item that a release queued to that key's held events, in sequence order.

H. The library SHALL append a hold event when it holds a key.

I. The library SHALL append a release event when an operator releases a held key.

J. The library SHALL append a clearing event when a released key's held events have all been queued.

K. The library's default destination-holds view SHALL TREAT a key of a destination of a database as held from the hold event that names it, as releasing from the release event that names that hold, and as no longer held from the clearing event that names that hold or from a deletion event that names that key list, deriving its rows solely from those events, other than those whose originating database is the holding database and that it does not hold as authored.

L. The library SHALL refuse to register a destination that declares no key while that destination has held events.

## Rationale

**Why hold one key instead of wedging the queue (assertions A to K)?** A destination's queue is ordered, so a definitive refusal at its head stops every later item. When the refusal concerns one subject only -- a receiver that rejects one participant's record because that participant's casebook lacks a form -- stopping every subject's delivery until an operator fixes that casebook turns one subject's problem into a whole destination's outage. A destination that declares a key per event names the subject whose events must stay in order. The library then takes the refused item (`EVS-PRD-destinations/I`) and the rest of that key's queued items out of the queue (assertion A), so the head moves on and every other key keeps delivering (assertion B), while every later event of the held key waits with the held ones instead of overtaking them (assertion C). Order holds per key: a held key's events are delivered, after a release, in the order they were recorded (assertion E), and no event of another key waits for them. The delivery implementation decides the scope, because only it knows whether the receiver's refusal concerns the subject or the destination. A failure that is not about one key stays destination-wide: an exhausted retry budget, an operator halt, a transform failure of the normal fill, and any refusal while an item with no key is queued, since the library cannot tell which subject a keyless item belongs to (assertion D). A destination that declares no key behaves as before. A transform failure of a released event holds its key again rather than wedging the destination (assertion F), since it concerns that key's events alone. A recovery puts released items back with their key's held events (assertion G), so the rewind it makes re-sends only items the fill queued (`EVS-PRD-destinations/N`) and the released events keep their order. The hold, release and clearing events record each change in the log (assertions H to J), and the default destination-holds view reads which keys are held from those events, and from the deletion events that name the key lists a deletion removes, alone (assertion K), as the destination-wedges view reads wedges from wedge, recovery and deletion events (`EVS-PRD-destinations/S`). A list a recovery re-creates under a cleared hold shows no row, since its hold's clearing event was appended once; the delivery-status read (`DestinationRegistry.readDeliveryStatus`) reports it. The mechanics -- the key list, the hold id a release names, the gating of a release on the fill position, and the events' fields -- are stated in `EVS-DEV-destination-key-hold`.

**Why refuse to register a destination without a key while it has held events (assertion L)?** While a key has held events, the key's later events wait behind them (assertion C). A destination that declares no key gives none of its events a key, so a registration without one, in a later process or after a code change, would queue a held key's new events ahead of its held ones. Registration refuses it instead; the operator registers the destination with its key again, or deletes the destination, which removes its held events.

## Changelog

- 2026-11-17 | 132cb6b8 | - | muhammadU-anspar (<muhammad@anspar.org>) | Auto-fix: update hash
- 2026-11-17 | - | - | muhammadU-anspar (<muhammad@anspar.org>) | Add L: registration refuses a destination that declares no key while it has held events. Code and tests cite L
- 2026-11-16 | 9b8c4b7a | - | muhammadU-anspar (<muhammad@anspar.org>) | Auto-fix: update hash
- 2026-11-16 | - | - | muhammadU-anspar (<muhammad@anspar.org>) | Amend K: the default destination-holds view also ends a key's row at a deletion event that names its key list, as the destination-wedges view ends a wedge at a deletion. Code and tests cite K
- 2026-11-16 | 6f55d2e0 | - | muhammadU-anspar (<muhammad@anspar.org>) | Auto-fix: update hash
- 2026-11-16 | 7101747f | - | muhammadU-anspar (<muhammad@anspar.org>) | Auto-fix: update hash
- 2026-11-16 | - | - | muhammadU-anspar (<muhammad@anspar.org>) | Add A-K: a key-scoped permanent failure holds that key's items out of the queue while other keys deliver, a held key's events are queued only through its release, a keyless item makes the failure a wedge, a release re-queues the held events in order, a release transform failure holds the key again, a recovery returns released items to their key, the hold, release and clearing events, and the default destination-holds view

*End* *Key-scoped holds on a destination* | **Hash**: 132cb6b8
