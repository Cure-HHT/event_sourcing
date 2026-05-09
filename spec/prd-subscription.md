# EVS-PRD-subscription: Subscription

**Level**: PRD | **Status**: Draft | **Refines**: EVS-PRD-library-charter

## Purpose

The library exposes the event log and its materialized projections through a subscription primitive: a consumer declares which events or which slices of materialized state it cares about, and the library delivers updates reactively as new events are ingested. Subscriptions are the consumer's only API for ongoing observation; one-shot reads are a degenerate case of the same primitive.

## Assertions

A. Consumers SHALL be able to subscribe to filtered streams of events or filtered streams of materialized state.

B. The library SHALL deliver subscription updates reactively as new events are ingested.

C. Subscription delivery SHALL preserve the order in which events appear in the log, within the scope of each subscription.

D. Subscription delivery SHALL be at-least-once: every event matching a subscription's filter SHALL be delivered, including across consumer reconnect.

## Rationale

**Why reactive delivery rather than polling?** Polling introduces latency proportional to the polling interval and a load floor proportional to the consumer count. Reactive delivery makes latency bounded by ingest, and lets idle subscriptions cost nothing.

**Why filtered subscriptions rather than firehose?** A consumer typically cares about a small slice of the log — events for a specific aggregate, events of a specific type, events from a specific source. Pushing the whole log to every consumer wastes their CPU, network, and memory; filtering at the substrate concentrates that work in the library where it can be optimized once.

**Why order-preserving delivery?** State derivation depends on per-aggregate event order. A consumer that receives events out of order would have to resort them itself, which is both extra complexity and a place where consumer bugs can corrupt state. The library guarantees order at delivery so the consumer can fold incrementally.

**Why at-least-once rather than exactly-once?** Exactly-once delivery requires either consumer-side acknowledgement protocols that complicate the API, or end-to-end transactionality that is impractical across reconnects and across tiers. At-least-once with hash-addressable events is operationally simpler: the consumer's deduplication is a one-line check against the event's hash, and loss — which is unrecoverable — is ruled out.

*End* *Subscription* | **Hash**: 00000000
