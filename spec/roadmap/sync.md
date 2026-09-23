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

## A drain lock across the tabs of a browser origin

**Baseline (what exists).** A Sembast database in the browser grants no
drain lock (`EVS-DEV-destination-drain-lock/A`): the tabs of an origin
share one IndexedDB database, each tab in its own isolate, and the
isolate-local drain-lock registry excludes none of them from another. A
`SyncCycle.start` in the browser therefore throws
`DrainLockConfigurationException`, and no delivery cycle runs there. The
browser's lock manager already carries the incompatible-generation guard
across tabs (`EVS-DEV-version-compatibility/H`).

**Remaining.** Take the drain lock through the browser's lock manager,
named for the IndexedDB database and its identity, so the tabs of an
origin exclude one another as Postgres sessions do: one tab drains, the
others stand by, and a closed or discarded tab's lock passes to a waiting
one. Hand the lock to the visible tab: a tab that becomes hidden finishes
the outcomes of its sends in flight, releases the lock and does not
request it while hidden. A page without a lock manager (not a secure
context) refuses to start a cycle, as it refuses to open a shared
database today.
