# Roadmap — projection / materializer primitives

Deferred additions to the declarative projection model
(`spec/prd-materializer.md`).

## `TimeBucketProjectionSpec`

**Baseline.** The sealed `ProjectionSpec` hierarchy ships exactly two
shapes: `AggregateProjectionSpec` (one row per aggregate, deep-merged)
and `TableProjectionSpec` (insert/remove keyed by row-key). Time-bucketed
aggregation of high-volume telemetry — "per sensor, per 1-minute bucket,
min/max/avg over the bucket's events" — fits neither cleanly. The
documented workaround is an app-side `Events()`-mode subscription
maintaining its own bucket index; every telemetry-style consumer
reimplements the same primitive.

**Remaining.** A third `ProjectionSpec` shape with its own fold and the
matching interpreter / rebuild / promotion branches. The motivating
sketches are `docs/scenarios/iot-sensor-network.md` and
`docs/scenarios/retail-pos.md`. Demand-gated; shipped under the
Append-Only Primitives discipline when a real consumer needs it. Rough
shape:

```dart
TimeBucketProjectionSpec(
  viewName: 'sensor_metrics_per_minute',
  interest: SubscriptionFilter(eventTypes: {'sensor_reading'}),
  bucketField: 'data.timestamp',      // or event.clientTimestamp
  bucketGranularity: Duration(minutes: 1),
  groupBy: 'data.sensorId',
  aggregations: {
    'value_min': Min('data.value'),
    'value_max': Max('data.value'),
    'value_avg': Avg('data.value'),
    'sample_count': Count(),
  },
)
```

Open design questions to settle when it is built:

- **Late arrival.** A `sensor_reading` whose `bucketField` lands in an
  already-closed bucket — re-fold the bucket, refuse the event, or route
  it to a separate late bucket?
- **Retention / rollup.** Are fine-grained buckets compacted into coarser
  buckets after some age (one-minute into hour buckets after N days)?
- **Interaction with promoters.** The `bucketField` referent may rename
  across entry-type versions, so bucket assignment must compose with the
  promoter chain.

## Views that catch up with the log, whatever their interest

**Baseline.** A view catches up with the log for the entry types its
interest names (EVS-DEV-version-compatibility/L): a build that stores an
event of such an entry type without folding it into a view another build
registers marks the view, and the next open of a build that registers it
re-derives it; a newly registered view over events already in the log is
derived at its first open. A view whose interest names no entry types (it
selects by aggregate type, or matches every entry type) has no stored
targets and is neither marked nor caught up, and two builds whose interests
for one view differ only in aggregate types or in `includeSystemEvents`
both register the pair and mark nothing. Such a view needs `rebuildView`
once no build lacking it, or holding the narrower interest, still serves the
database.

**Remaining.** Catch up every registered view whatever its interest. The
stored state must record, per view, which interest last derived it (for
example a digest of the interest), so that a boot can tell a view it has not
derived, or derived under another interest, from one that is current, and a
build that stores an event must mark a view whose stored interest differs
from its own even when it registers the view. Re-deriving a view whose
interest names no entry types reads the whole log, so the cost falls in the
boot transaction that holds appends back.

## Snapshot promotion and view catch-up outside the boot transaction

**Baseline.** `EventStore.open` promotes lagging view rows
(`EVS-DEV-snapshot-promotion-on-open`) and re-derives views that are behind
the log (`EVS-DEV-version-compatibility/L`) inside its boot transaction.
The views therefore equal a replay of the log from the moment the open
returns, and the boot holds back every append to the database while it
runs: on Postgres every serving instance's appends wait for it, and on
the web every other tab's writes. The pause grows with the rows promoted
and the events re-derived (measured on Postgres 16: about 22 s to catch up
an added view over 20,000 events, about 30 s to promote a 2,000-row view),
another instance's open waits for the boot lock (`bootLockWait` must
exceed the boot), and the boot reports its progress to an observer
(`onBootProgress`) so a server can answer its health probes meanwhile.

**Remaining.** Finish before the first deployment whose log is large
enough for the pause to matter: move the re-derivation out of the boot
transaction while keeping every view a replay of the log whenever it is
served. The design this requires:

- A per-pair token for "folded under an older minor" and for "behind the
  log", in place of the boolean mark, written by the transaction that
  creates the gap and cleared only by a re-derivation that read the same
  token (clear-if-unchanged), so a gap created while a re-derivation runs
  is not lost.
- Re-derivation in chunks (of aggregates, or of log positions for a table
  view) in transactions of their own, each short enough that serving
  appends never wait long, and resumable across restarts.
- A fold into a marked pair that re-derives the aggregate it touches from
  the log rather than merging one delta into a row that lacks history, so
  a partly caught-up view never serves a single-delta row as current.
- Background convergence while the store is open, with its progress
  reported, and an amendment of the version-compatibility and
  snapshot-promotion requirements that states what a read of a view still
  converging returns.
