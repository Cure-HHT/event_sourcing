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

## Views that catch up with the log

**Baseline.** A view is folded only by the builds that register it. Opening
a build that registers a new view, or a new entry type in an existing view's
interest, seeds the view's target versions and folds nothing already in the
log; `rebuildView` fills the view. While a build that does not register the
view or the entry type serves the same database -- a canary overlap, or a
rollback within a major -- the events it appends are not folded into that
view, and no open detects the gap. A fold under an older minor lowers only
the stored targets of the views the folding build registers
(EVS-DEV-version-compatibility/E).

**Remaining.** Make the gap impossible to miss. A build that stores an
event of an entry type marks every stored `(view, entry type)` target it
does not fold that event into, in the same transaction, and the next open of
a build that registers the view re-derives it inside the boot transaction;
opening a build that seeds a new pair on a log that already holds events of
its entry type likewise re-derives the view. The mark needs a read of the
stored targets by entry type on every backend and a representation that a
lowered version cannot give (a pair registered at minor 0 cannot be
lowered). Until then, the deployment guidance does not call a release that
adds a view, or an entry type to a view's interest, safe to run beside the
build it replaces.
