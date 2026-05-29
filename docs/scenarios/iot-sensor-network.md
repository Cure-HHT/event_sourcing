# IoT Sensor Network on `event_sourcing` — Design Sketch

The substrate's center of gravity is human/service-driven actions producing a
low-to-medium-volume log with strong audit guarantees. Smart-agriculture
inverts that center: anonymous devices firing continuously, time-series the
dominant read shape, multi-source per farm baked in from day one. The
substrate's Layer 1 facts (per-source ordering, hash chain, atomic-with-view)
map onto IoT's needs surprisingly well; the Layer 2 conventions (one row per
aggregate, deep-merge, role/permission/scope, action-shaped writes) need real
extension or augmentation — and at the high end, the assumption of a single
Postgres log probably breaks.

## 1. Initialization and use

**Topology.** Each farm runs a **gateway** process — the smallest unit that
holds a substrate. Sensors themselves are too dumb to host one; they emit
MQTT/LoRaWAN frames to the gateway over symmetric-key transport. The gateway
opens `SembastBackend` (or SQLite, once a third backend lands) as a local edge
cache, with a registered `Destination` forwarding to a cloud cluster running
`PostgresBackend`. The agronomist dashboard and grower mobile app are pure
`RemoteScope` clients of cloud reaction handlers.

**Sensors-as-events, not actions.** Action dispatch (parse → validate →
authorize → execute → persist, all atomic) is overkill for 70k pure telemetry
events per farm per day. The gateway should **bypass the dispatcher** and call
`eventStore.append(EventDraft(...))` directly for `sensor_reading_recorded`
events, after a thin gateway-local check (HMAC verifies, sensor is registered,
timestamp window sane). The substrate already supports this for "system-driven
events" — the guide names seed loaders as the use case, and gateway-ingested
telemetry fits the same shape. Reserve action-dispatch for things that
*should* deny-with-audit: `AcknowledgeAlert`, `ScheduleIrrigation`,
`RegisterSensor`, `RotateKey`, `DecommissionSensor`. Those are human-driven,
low-rate, and benefit from the full audit pipeline.

**Auth.** Two principals:

- **Gateways → cloud** authenticate as a service `Principal` (one per gateway
  install). A custom `PrincipalAuthValidator` mints a
  `UserPrincipal { userId: "gateway:<farmId>:<gatewayId>", activeRole:
  "FarmGateway" }`. Sensor identity travels in the event *payload* (`sensorId`),
  not in `Principal`.
- **Humans (grower, agronomist)** authenticate via whatever the cloud already
  uses (Firebase/OIDC); roles `FarmOwner`, `Agronomist`, `Admin` scoped via a
  `farm` scope class, with `field` and `sensor` nested through
  `ContainmentRef`s.

**Projections to register:**

- `sensor_current` — `AggregateProjectionSpec` keyed on `sensorId`,
  deep-merging latest reading (the only "natural fit" for the substrate's
  built-ins).
- `sensor_metadata` — `TableProjectionSpec` for registration, calibration,
  decommissioning.
- `alerts_open` — `TableProjectionSpec` inserted on `alert_raised`, removed
  on `alert_acknowledged`.
- `sensor_to_field`, `field_to_farm` — containment indexes feeding scope
  expansion.
- Time-bucketed aggregates (hour/day rollups) — **gap** discussed in §3.

## 2. Layer 1 properties that are load-bearing

**Per-aggregate-per-Source ordering** is the standout. Each sensor is one
aggregate; each farm gateway is one `Source`. The substrate guarantees that
readings from sensor X via gateway G arrive in the order G saw them. For
agriculture this is operationally critical: a soil-moisture curve is
meaningless if samples interleave out of order, and **monotone-time violation
within a single (sensor, gateway) pair is a near-perfect tamper/clock-drift
signal**. A medical diary mostly has one writer per aggregate and humans don't
generate ordering ambiguity at machine speed; an IoT sensor at 15s cadence
will surface this constantly.

**Multi-source per farm.** A 1000-acre farm with two cellular gateways
(redundancy or geographic split) is the textbook v2 multi-source case the
substrate was designed for, and it slots in cleanly: each gateway is a
`Source`; the substrate preserves per-gateway-per-sensor order; **provenance**
records which gateway shipped each reading, which the cross-farm forecasting
service needs in order to attribute data quality, gateway outages, and
clock-skew incidents back to specific hardware.

**Hash chain + provenance** become load-bearing the moment a reading is cited
in a **crop-insurance claim** ("the frost-warning sensor recorded -3°C at
04:12 UTC"). That's the same regulatory shape ALCOA+ was designed for,
applied to weather instead of clinical data.

The Layer 1 promise that **doesn't** earn its keep in pure IoT is append-
atomic-with-view-update — `sensor_current` lag of a few ms is meaningless when
readings arrive every 15s anyway. It's load-bearing for *alerts*
(frost-warning must not lag), not for routine telemetry.

## 3. Layer 2 machinery — fits, gaps, extensions

**Time-bucketed views are a gap.** The substrate ships
`AggregateProjectionSpec` (one row per aggregate, deep-merge) and
`TableProjectionSpec` (insert/remove on event types). Neither expresses "one
row per (sensorId, hour)" with sum/avg/min/max folds. Two options: (a) the
app subscribes raw to `sensor_reading_recorded` with `Events()` mode and
writes its own bucketed tables outside the substrate's projection machinery —
a supported pattern per the guide's "Layer 1 fallback" framing, but you lose
append-atomic-with-view; (b) extend the substrate with a new declarative
primitive — `TimeBucketProjectionSpec(bucketField, bucketGranularity,
aggregations: {field: Sum/Avg/Min/Max})`. Honest answer: (a) ships now, (b) is
the right long-term move and is precisely the kind of new Layer-2 primitive
the Append-Only Primitives discipline contemplates.

**Permission scopes** map naturally: `farm` → `field` → `sensor` via
`ContainmentRef`s, exactly as the guide's `site` → `patient` example.

**Tombstones** apply to *sensor decommissioning* (the `sensor_metadata` view
drops the row) but never to readings — readings are append-only forever. The
default convention fits.

**Idempotency** uses `Idempotency.required` keyed on `(sensorId,
deviceTimestamp)` for the rare action-shaped writes (`RegisterSensor`,
`RotateKey`); for bulk telemetry, idempotency lives in the gateway's
offline-replay buffer, deduplicated before `append` — the dispatcher's
idempotency store is the wrong scale.

**Anonymous device auth — a real gap.** The substrate's `Principal` model
assumes humans or named services. The clean answer is: sensors **don't get a
`Principal`**; the gateway authenticates them with HMAC, attaches `sensorId`
to the *payload*, and appends under the gateway's own service `Principal`.
The audit trail records "gateway G appended a reading claiming to be from
sensor S" — which is the real epistemic state, and matches how device fleets
actually work. Introducing a `DevicePrincipal` variant would push device
identity into the substrate's auth model without earning its keep, since the
substrate has no way to verify a device anyway.

**Volume — the genuine architectural concern.** The substrate's design
assumes one Postgres log per deployment. At 10⁸ events/day across thousands
of farms, a single Postgres is wrong: writes are fine (it's append-only), but
global-sequence allocation becomes a contention point, and the hash chain
forces serialization. The realistic shape is **one substrate per farm-
cluster** (region/customer-shard), with cross-shard reads handled by the
forecasting service subscribing as a remote consumer of each shard. That's
*outside* the v1 substrate's stated scope, but doesn't violate any
architectural commitment — each shard remains closed-under-events; the
forecaster reads many shards the way any cross-installation consumer would.
Making the per-shard log itself horizontally partitionable is a much larger
change with no committed design.
