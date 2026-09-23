# EVS-PRD-subscription: Subscription

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The library exposes the event log and its materialized projections through a subscription primitive: a consumer declares which events or which slices of materialized state it cares about, and the library delivers updates reactively as new events are ingested. Subscriptions are the consumer's only API for ongoing observation; one-shot reads are a degenerate case of the same primitive.

## Assertions

A. Consumers SHALL be able to subscribe to filtered streams of events or filtered streams of materialized state.

B. The library SHALL deliver subscription updates reactively as new events are ingested.

C. Subscription delivery SHALL preserve the order in which events appear in the log, within the scope of each subscription.

D. Subscription delivery SHALL be at-least-once: every event matching a subscription's filter SHALL be delivered, including across consumer reconnect.

E. The library SHALL publish the events, view changes and queue changes of a committed transaction to live subscribers and watchers once, after its commit, and SHALL publish nothing from a transaction run that did not commit.

## Rationale

**Why reactive delivery rather than polling?** Polling introduces latency proportional to the polling interval and a load floor proportional to the consumer count. Reactive delivery makes latency bounded by ingest, and lets idle subscriptions cost nothing.

**Why filtered subscriptions rather than firehose?** A consumer typically cares about a small slice of the log — events for a specific aggregate, events of a specific type, events from a specific source. Pushing the whole log to every consumer wastes their CPU, network, and memory; filtering at the substrate concentrates that work in the library where it can be optimized once.

**Why order-preserving delivery?** State derivation depends on per-aggregate event order. A consumer that receives events out of order would have to resort them itself, which is both extra complexity and a place where consumer bugs can corrupt state. The library guarantees order at delivery so the consumer can fold incrementally.

**Why at-least-once rather than exactly-once?** Exactly-once delivery requires either consumer-side acknowledgement protocols that complicate the API, or end-to-end transactionality that is impractical across reconnects and across tiers. At-least-once with hash-addressable events is operationally simpler: the consumer's deduplication is a one-line check against the event's hash, and loss — which is unrecoverable — is ruled out.

**Why publish only committed work?** A storage backend may run a transaction body more than once before one run commits: Postgres re-runs it after a serialization conflict, and the browser backend re-runs it when another tab committed first. Two transactions may also be in flight at once in one process. A subscriber that saw an event from a run that rolled back would fold state the log does not hold, and would see a sequence number that belongs to another event. The library therefore collects each run's publications separately and publishes those of the run that committed, once, after the commit.

**What "once" does and does not promise.** Assertion E governs the publication step: one committed transaction is published once. It is not an exactly-once delivery promise to a subscriber. A new aggregate-mode subscriber attaches its live listener before it reads its snapshot, so a change committed during that read can reach it both in the snapshot and as a delta; assertion D (at-least-once) governs that case. The library also appends some of its own records outside the publishing path -- the record of a rejected ingest batch, the library-version transition event, and the audits of the boot-time snapshot promotion -- and those are not published live; a subscriber reads them from the log. When a connection fails after the database committed but before the commit was acknowledged, the library cannot know the outcome and publishes nothing; the committed event is again read from the log.

## Changelog

- 2026-09-23 | 026033a7 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Add E: publish only a committed transaction's changes, once, after its commit
- 2026-08-10 | 57530d86 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-07-02 | 5d398de1 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: add missing changelog section

*End* *Subscription* | **Hash**: 026033a7
