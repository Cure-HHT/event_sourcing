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

**Baseline.** A view converges with the log for the entry types its interest names (EVS-DEV-version-compatibility/L, EVS-DEV-view-convergence): a build that stores an event of such an entry type without folding it into a view another build registers records a catch-up gap, and a view registered over events already in the log is re-derived after its first open. A view whose interest names no entry types has a whole-view pair, for which a build lacking the view records every event it stores. Two builds whose interests for one view differ only in aggregate types, in `includeSystemEvents` or in a predicate both register the pair and record nothing for each other, and a whole-view pair carries no version, so a promotion of an entry type such a view folds is not tracked for it. The equality a read reports is stated under that precondition (EVS-DEV-converging-view-reads/E), and such a view needs `rebuildView` once no build holding the narrower interest, or an older minor, still serves the database.

**Remaining.** Converge every registered view whatever its interest. The stored state must record, per view, which interest last derived it (for example a digest of the interest, which a predicate closure prevents), so that a boot can tell a view derived under another interest from one that is current, and a build that stores an event must record a gap for a view whose stored interest differs from its own even when it registers the view; and a whole-view pair must record the versions of the entry types its view has folded, so that a promotion of one of them is tracked.
