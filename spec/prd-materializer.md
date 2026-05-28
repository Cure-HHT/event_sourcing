# EVS-PRD-materializer: Materializer

**Level**: prd | **Status**: Draft | **Implements**: -
**Refines**: EVS-PRD-library-charter

## Purpose

The materializer is the library's component that derives typed application state from the event log. Application code consumes materialized state to do its work; auditors and tests reproduce the same state by running the same materializer over the same log. The materializer is part of the library's substrate — it lives alongside the storage and subscription primitives, not in host application code.

## Assertions

A. The library SHALL provide a materializer that derives typed application state from the event log.

B. Materialization SHALL be deterministic: applying the same events in the same order from the same starting state SHALL yield byte-identical resulting state.

C. The materializer's rules SHALL themselves be events recorded in the log; the rules in effect at any point in the log SHALL be reconstructable from the log alone.

## Rationale

**Why typed state, not row-maps or untyped JSON?** Application code relies on the materialized state for its work. Typed state lets the compiler catch shape mismatches between the materializer and its consumers; untyped state pushes those errors to runtime, where they are far harder to catch in regulated environments.

**Why deterministic?** Two audits of the same log must produce the same answer; otherwise the audit has no evidentiary value. Determinism is the property that makes "the materialized state derived from this log" an unambiguous statement rather than a per-run artifact.

**Why are materializer rules themselves events?** A rule that lives outside the log can change without any record. If the rule that maps event-X to state-change-Y can be edited silently, the meaning of every past event-X also changes silently. By recording rule changes as events in the same log, the library guarantees that "what the materializer did at time T" is reconstructable from the same audit trail as "what the application did at time T". The rule history is part of the audit, not adjacent to it.

**Why is the materializer in the library, not the host application?** Two reasons. First, the same library running an audit, a replay, or a fresh-rebuild must use exactly the materializer that produced the original state — host code that has evolved would derive different state. Second, regulators reviewing the audit story should review one component, not application code plus a parallel materialization service.

**Multi-source readiness.** The rules-as-events seam is what admits multi-source canonicalization later (see EVS-PRD-multi-source-canonicalization). The default rule preserves single-source semantics — only events from the aggregate's originating authority fold into state. Multi-source semantics are activated by additional rules expressed over event authorities, recorded as events on the same log; the materializer code path is identical in either case.

## Future work

- **`TimeBucketProjectionSpec` primitive.** v1 ships `AggregateProjectionSpec` (one row per aggregate, deep-merged) and `TableProjectionSpec` (insert/remove keyed by row-key). The IoT scenario (`docs/scenarios/iot-sensor-network.md`) surfaces a third shape neither covers cleanly: time-bucketed aggregation of high-volume telemetry — "per sensor, per 1-minute bucket, min/max/avg over the bucket's events." Today this can be done app-side via `Events()`-mode subscription and an app-maintained bucket index, but every IoT-style consumer reimplements the same primitive. The proposed shape (rough sketch, AOP-disciplined when designed for real):

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

  Open design questions (deferred): late-arrival semantics (a `sensor_reading` event whose `data.timestamp` lands in an already-closed bucket — re-fold, refuse, separate-late-bucket?), retention/rollup (one-minute buckets compacted to hour buckets after N days?), the interaction with promoters (the `bucketField` referent might rename across entry-type versions). Lands when a real consumer demand materializes; cited here so future work doesn't rediscover the gap.

*End* *Materializer* | **Hash**: 02028dcf
