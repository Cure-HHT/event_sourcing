# Roadmap: sync / destinations

Deferred work for the outbound-sync layer (`Destination`, `SyncCycle`,
FIFO drains). See `README.md` for how roadmap entries are read.

## Inbound tombstone propagation

**Baseline (what exists).** `SyncCycle` runs each cycle as: drain every
registered destination's outbound FIFO, then invoke the
`pollInbound()` hook (`event_sourcing/lib/src/sync/sync_cycle.dart`).
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
