# Roadmap: sync / destinations

Deferred work for the outbound-sync layer (`Destination`, `SyncCycle`,
FIFO drains). See `README.md` for how roadmap entries are read.

## Inbound tombstone propagation

**Baseline (what exists).** A `SyncCycle` (started with
`SyncCycle.start`) runs each pass as: fill and drain every registered
destination's outbound queue, then invoke the `pollInbound()` hook
(`event_sourcing/lib/src/sync/sync_cycle.dart`).
The hook's sequencing is in place and tested
(`event_sourcing/test/sync/sync_cycle_test.dart` asserts it runs after
all outbound drains complete), but its body is a no-op: deletions
authored by another party on a relay do not propagate to the local
store.

**Remaining.** Implement the polling body: query a relay's read-side
API for tombstones authored elsewhere, and apply them to the local
store through the normal ingest path (so the application is recorded,
ordered, and replayable like any other ingested event). Requires
pinning the relay read-side contract (endpoint shape, watermark or
cursor semantics) — none is specified today.

## Detecting undeclared delivery-configuration changes

**Baseline (what exists).** Each pass of the delivery cycle declares, per
destination, the configuration the library can read (the destination's
identifier, wire format, native serialization, accumulation window and
filter sets, whether the filter has a predicate) together with the
`configurationVersion` the cycle was started with, and fingerprints it.
A recovery of a reconfigure halt is accepted only once the drain-lock
holder declares another fingerprint, and a refill guard keeps a drainer
of the halted configuration from refilling (`EVS-DEV-destination-drain/F`).
Code the library cannot read -- the transform, the filter predicate, the
batching rule -- enters the fingerprint only through
`configurationVersion`, which the application is trusted to change with
it.

**Remaining.** Detect a change of that code without relying on the
application: for example, derive a fingerprint of the transform's output
over a fixed probe batch, or require destinations to declare a code
version the library checks against a registry of released builds. Either
changes what a destination must supply and is a new primitive under the
Append-Only Primitives discipline.
