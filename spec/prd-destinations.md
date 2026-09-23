# EVS-PRD-destinations: Destinations

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

A destination is a configurable outbound channel through which a deployment delivers selected events to another system. The other system may be another event-sourcing deployment (e.g., a sponsor's data store, an EDC), a passive sink (e.g., a webhook), or a notification service (e.g., a push-notification gateway). The library handles event selection, ordering, durability, and hand-off; the actual transit is performed by a configurable per-destination delivery mechanism.

The library treats destinations as a write-side concern only. Inbound flow from another event-sourcing deployment is specified separately in EVS-PRD-ingest.

## Assertions

A. The library SHALL support configuring destinations on a deployment.

B. Each destination SHALL be configured with a filter that selects which events the destination receives.

C. Events matching a destination's filter SHALL be queued for delivery in FIFO order.

D. A destination's queue SHALL be durable: queued events survive a deployment restart and are delivered when the deployment resumes.

E. The library SHALL accept an application-supplied delivery implementation for each destination, to which the library hands queued events for transit (HTTP webhook, push notification, custom transport, and so on are example implementations the application may provide).

F. Destinations SHALL be addable and removable dynamically over the deployment's operating lifetime.

G. While delivery on one destination is failing, the library SHALL continue delivering queued events on every other destination.

H. If an application-supplied delivery implementation raises an error while transiting an event, then the library SHALL treat that error as a failed delivery attempt.

I. The library SHALL leave an event whose delivery attempt failed at the head of its destination's queue.

J. The library SHALL record each delivery attempt made for a queued event.

K. The library SHALL mark as internal every public member, other than its delivery cycle, its destination-registry and event-store operations, its view rebuild, and its storage backend's transaction and close operations, that changes a destination's queue, a view the library materializes, the library's persisted delivery state, the security context stored beside an event, the event sequence or the storage schema version, appends an event without the event store's checks, publishes to live subscribers something the library did not append, or exposes a raw handle to the database or to a transaction's underlying engine transaction.

L. The library SHALL state, as a precondition of its storage trust boundary, that its delivery guarantees, its views and its security-context records hold only while its persisted state (destination queues, the views it materializes, the records it keeps beside them, such as fill positions, schedules, replay requests and the registry check record, and the security context it stores beside each event) changes only through the library's operations.

M. The library SHALL refuse an operator recovery of a destination's queue unless the queue's head item is wedged.

N. An operator recovery SHALL rewind the destination's fill position below the lowest event carried by every queue item the recovery removes, so that a later fill re-evaluates each of those events against the destination's filter and schedule.

O. The library SHALL refuse to delete a destination while its queue head is pending; a deletion SHALL retain every queue item that was delivered, wedged or recovered; and a destination registered again under the same identifier SHALL start a new registration whose fill delivers, under its own schedule and filter, the events its window admits.

## Rationale

**Why a configurable filter per destination?** Different destinations care about different slices of the event log. A sponsor EDC wants only the events for participants in that sponsor's trial; a push-notification gateway wants only notification-emitting events. Pushing the entire log to every destination wastes everyone's resources and leaks data across boundaries. Filtering at the destination configuration concentrates that work in the library.

**Why FIFO queueing?** Downstream systems typically consume events incrementally and expect to see them in the order they were produced. Out-of-order delivery makes downstream materialization incorrect, even when the events themselves are individually well-formed. A FIFO queue per destination decouples the deployment's event-production rate from the destination's delivery rate while preserving order.

**Why durable queues?** A non-durable queue loses events on restart, which means the audit trail held by upstream and downstream diverge whenever a process restarts. Durability across restart is the property that lets the destination's recipient trust that "if I haven't seen event X yet, it has not been delivered" — a precondition for at-least-once delivery semantics.

**Why pluggable delivery mechanisms?** Real deployments transit events over a wide range of transports — HTTPS to a clinical EDC, push notifications to mobile devices, a relay's own HTTP endpoint, a sponsor's custom protocol. Picking one transport in the library would force every consumer to either use that transport or shim around it. Treating the transport as a per-destination plug-in keeps the library transport-agnostic.

**Why isolate failure between destinations?** Destinations fail independently and for unrelated reasons — a sponsor's endpoint is down for maintenance while a push gateway is healthy. Because each destination's queue preserves order, an undeliverable event at the head necessarily halts that queue; if that halt also stopped the other destinations, one unreachable recipient would silently stop delivery to every recipient. Isolation is what makes a per-destination queue a containment boundary rather than a shared point of failure.

**Why treat a raised error as a failed attempt rather than a lost event?** The delivery implementation is application-supplied and therefore outside the library's control: it may throw on a DNS failure, a timeout, or its own bug. Three outcomes are possible when it does — drop the event, propagate the error to the caller of the delivery pass, or record the failure and retry. Only the third preserves the at-least-once semantics the durable queue exists to provide, so it is the one the library commits to. Recording the attempt (rather than silently retrying) is what makes a wedged head diagnosable: an operator can see how many times an event has been tried and with what outcome, which a bare retry loop does not expose.

**Why mark the mutating surface internal (assertion K)?** A destination's delivery guarantees rest on its queue, its fill position and its schedule changing only in the order the library's own operations change them. A direct queue write can mark a row terminal behind the drain's back, a direct view write produces a row that no event in the log accounts for, and a direct write to an event's security context bypasses the redaction and purge events that record such a change. Marking every such member internal -- on the exported types and in the library's `src/` files alike -- makes a consumer's call to it an analyzer error while leaving the library's own operations as the consumer's entry points: the delivery cycle, the destination registry, the event store, the view rebuild, and the backend's transaction and close. The view rebuild qualifies because it folds the log at each entry type's registered version and refuses any other target, so it writes only rows that the log and the entry-type registry determine; it does not notify live subscribers. An event-store append inside a consumer's transaction is accepted only in the event store's own transaction, through the collector of the run that commits it, so live subscribers receive exactly the events that committed. The rule is an analyzer guard over the library's own surface, not a barrier. It reaches a storage backend in another package only where that package marks its own overrides of the internal members internal, and a backend declared in the application's own package is covered by assertion L alone. It does not reach the Sembast `Database` object, which the consumer opens and passes to the Sembast backend and therefore holds by construction. Assertion L names these residuals.

**Why state a precondition on the storage trust boundary (assertion L)?** The internal marking is enforced by the analyzer, not at run time. The consumer constructs the storage backend and keeps a reference to it (on Sembast it also holds the `Database` it opened), and a job with write access to the database can change any record directly. The library cannot detect such a write, so its delivery guarantees and its views are stated as holding only while its persisted state changes through its own operations. The records kept beside the queues include the fill positions, the schedules, the replay requests a registry operation leaves for the next fill, and the registry check record: a replay request written or removed directly would make the next fill enqueue a replay no operation requested, or skip one an operation recorded. Stating the precondition beside the storage trust boundary names the gap, as the charter's trust-boundary enumeration requires. The idempotency cache an application supplies to action dispatch is not part of this state: the cache is a pluggable interface the application may implement itself, so the library's guarantees do not rest on its records.

**Why does recovery require a wedged head (assertion M)?** A pending head may be in delivery: the process that drains sends outside any transaction, and an operator recovery running elsewhere cannot see the send. Retiring a pending head would race the delivery's outcome, and would record a recovery where no delivery halted. A wedged head is never in delivery, because only the drainer wedges a head, after its own send returned.

**Why rewind below every removed item (assertion N)?** Recovery is "discard and rebuild from the log", not "skip the bad item": the removed items' events are evaluated again by the next fill, under the destination's current filter and schedule, so a changed transform or filter takes effect on them. An item removed from behind the head can carry events lower than the head's (a replay of an earlier window enqueues old events at the tail), so the rewind goes below the lowest event of every removed item; rewinding only below the head would lose those events. Events that were already delivered and lie above the rewind point are delivered again: delivery is at-least-once.

**Why refuse to delete a pending head, and keep the delivery record (assertion O)?** Deleting a pending head could discard the record of a delivery the receiver accepted while the deletion ran. Items that were delivered, wedged or recovered are the destination's delivery record; they outlive the destination so that an auditor can still see what was sent. A destination registered again under the same identifier starts a new registration and delivers from its own schedule; its refill may send again events that the old registration delivered, which is at-least-once delivery.

**Why dynamic registration?** A diary user signs up for a sponsored trial after the diary has been operating for some time. Adding the sponsor's destination at that point must work without restarting the deployment or invalidating its log. The same property supports linking and unlinking destinations as a participant moves between trials.

## Changelog

- 2026-09-23 | 5b0601cf | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Add M-O: recovery requires a wedged head and rewinds below every removed item; deletion refuses a pending head, retains the delivery record, and a re-registered destination starts a new registration. Widen L to replay requests and the registry check record
- 2026-09-23 | 83d63785 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Add K-L: internal mutating surface, and the precondition that the library's persisted state changes only through its operations
- 2026-09-07 | 5c082273 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-07 | - | - | Michael Lewis (<michael@anspar.org>) | Add G-J: failure isolation between destinations, and failed-attempt handling for an application-supplied delivery implementation
- 2026-08-10 | 872fc0dc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | ec656743 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Destinations* | **Hash**: 5b0601cf
