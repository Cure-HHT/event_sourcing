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

## Rationale

**Why a configurable filter per destination?** Different destinations care about different slices of the event log. A sponsor EDC wants only the events for participants in that sponsor's trial; a push-notification gateway wants only notification-emitting events. Pushing the entire log to every destination wastes everyone's resources and leaks data across boundaries. Filtering at the destination configuration concentrates that work in the library.

**Why FIFO queueing?** Downstream systems typically consume events incrementally and expect to see them in the order they were produced. Out-of-order delivery makes downstream materialization incorrect, even when the events themselves are individually well-formed. A FIFO queue per destination decouples the deployment's event-production rate from the destination's delivery rate while preserving order.

**Why durable queues?** A non-durable queue loses events on restart, which means the audit trail held by upstream and downstream diverge whenever a process restarts. Durability across restart is the property that lets the destination's recipient trust that "if I haven't seen event X yet, it has not been delivered" — a precondition for at-least-once delivery semantics.

**Why pluggable delivery mechanisms?** Real deployments transit events over a wide range of transports — HTTPS to a clinical EDC, push notifications to mobile devices, a relay's own HTTP endpoint, a sponsor's custom protocol. Picking one transport in the library would force every consumer to either use that transport or shim around it. Treating the transport as a per-destination plug-in keeps the library transport-agnostic.

**Why dynamic registration?** A diary user signs up for a sponsored trial after the diary has been operating for some time. Adding the sponsor's destination at that point must work without restarting the deployment or invalidating its log. The same property supports linking and unlinking destinations as a participant moves between trials.

## Changelog

- 2026-08-10 | 872fc0dc | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | ec656743 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Destinations* | **Hash**: 872fc0dc
