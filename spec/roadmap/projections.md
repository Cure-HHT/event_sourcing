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
