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

## Value-rewriting promoter primitives

**Baseline.** The sealed `TransformPrimitive` set ships three
shape-changers — `RenameField`, `DefaultField`, `DropField`. They move a
key, add a key that is absent, and remove a key. The only member that
writes a value writes a constant, so it produces the same result for
every row, and only where the key is absent. Nothing in the vocabulary
derives a new value from the value already present, and composing the
existing members does not get there: moving the old value aside leaves
nothing able to read it. The set is `sealed`, so a consumer cannot add a
member — correctly, since that closure is what keeps the set small
enough to audit against the fold.

The consequence is that a schema change altering the *form* of a value
rather than its *location* is unpromotable. A deployment facing one must
reset its store, which is affordable only before it holds records anyone
depends on.

**Motivating shape.** A consumer records a timestamp that already
carries a UTC offset — the recording device's — while the offset of the
zone the record is *about* is held in a sibling field, and the
wall-clock digits are shifted so the instant is correct. Moving to a
representation where the intended offset sits inside the timestamp it
belongs to means deriving one field from two: the timestamp's own
embedded offset and the sibling offset field.

### Why a single key is the easy case and two keys is not

The `AggregateProjectionSpec` fold is a recursive delta merge in which
an absent key preserves the prior value. Events are therefore not
required to carry every field, and two fields can be last written by
different events.

For a transform whose result depends only on the key it writes, the two
promotion routes agree:

```text
  merge-then-promote:  f(v_last)
  promote-then-merge:  each event's k becomes f(v_i); last wins -> f(v_last)
```

For a transform reading a second field, they do not:

```text
  merge-then-promote:  g(a_last, b_last)   <- may come from two events
  promote-then-merge:  g(a_m,    b_m)      <- always from one event
```

These diverge exactly when `a` and `b` were last written by different
events. So a single-key rewrite is safe to apply to a folded view row,
and a two-field derivation is not — in general. This is a property of
the primitive, not of the chain author's intentions, which is why the
restriction has to be structural.

### Deciding whether a chain may touch a folded row

Because the primitive set is closed, each member's commutativity is
known to the library. A chain is safe to apply to a folded row exactly
when every member is, and the substrate should compute that rather than
accept an assertion — an assertion it cannot check, whose falsity
produces divergent state with no error, is the failure
`EVS-DEV-snapshot-promotion-on-open`/D exists to prevent.

A declared override still earns a place, but only on top of the derived
default. The motivating shape above is safe despite reading two fields,
because that entry type writes the group atomically: every write carries
the whole group, so the two fields the derivation reads always come from
the same event. That is knowledge the application holds and the
substrate cannot derive. Settle, when this is built:

- How the claim is expressed, and how an auditor reading the log later
  can tell that a promotion relied on it.
- Whether the substrate can detect a violated claim after the fact —
  scanning for an event that wrote one field of a declared group without
  the others would falsify it cheaply, and an undetectable wrong claim
  is the worst version of this mechanism.
- Whether a violated claim should refuse the boot or fall back to the
  replay route below.

### The replay fallback

A chain that may not be applied to a folded row is not thereby
unusable. Event-wise promotion is always well-defined, because every
field of an event arrives together. The boot path for such a chain
rebuilds the affected view by replaying the log through the promoters
instead of promoting its snapshot.

`EVS-DEV-snapshot-promotion-on-open`/D asserts that snapshot promotion
is provably equivalent to event-replay-with-promotion. That assertion is
preserved rather than weakened: the replay route *is* the thing the
equivalence is stated against. The assertion needs restating to cover
both routes explicitly — snapshot-promote when the chain permits it,
rebuild when it does not — rather than presuming the first.

The cost is a full log replay at that version bump. `rebuildView`
currently runs its whole replay inside one backend transaction, which is
tolerable for a client-side store and a real constraint on a server-side
one; that limit becomes load-bearing if the replay route is adopted.

### Obligations any value-deriving member inherits

- **Determinism.** A pure function of the event data, with no ambient
  input (`EVS-PRD-materializer`/B). A derivation consulting the host's
  clock, locale, or zone would break replay. Where a derivation needs a
  format or a zone, it is named in the primitive's data, not read from
  the runtime.
- **Totality.** Every operation defined for every input it can receive.
  A partial operation — a pattern that does not match, a join against a
  value that is absent — must have a specified result, or a replay can
  diverge between observers or fail partway through a chain.
- **Idempotence.** A store can already hold records in the target form
  stamped at the source version, because a wire change often lands
  before the version bump describing it. Applying the derivation to a
  value already in the target form leaves it unchanged.

### Promotion versus recomputation

Not every derived value needs a promoter. `DerivedField` computations
already receive the whole row state and are recomputed on every fold and
again after every snapshot promotion. Because such a value is computed
from the finished row rather than merged into it, it cannot diverge
between the two routes, is idempotent by construction, and needs no
version bump at all — changing the computation changes every row at next
boot.

That makes recomputation the right home for a multi-field derivation
whose result may legitimately be recomputed forever, and the wrong home
for one that must be frozen as recorded, since a recomputed value
silently changes whenever its computation does. The design draws that
boundary explicitly; the existing computation set ships only a
single-path lookup, so combining several fields is an extension there
either way.

### Notation

Two candidate directions, to be settled before implementation:

- **A closed set of named, parameterised operations** — split, join,
  reformat with explicit patterns. Trivially serialisable, obviously
  auditable, no evaluator to freeze. Grows by one name per need.
- **An expression notation** carried as data — a postfix token list or
  equivalent — where one primitive covers a family of derivations.
  Fewer names, more semantics to freeze.

Either way, the notation is data in the log, never a host-supplied
callback: a callback lives in the consumer's build rather than in the
log, which is what the closed-under-events model refuses.

A pattern-matching notation carries two further obligations. Pattern
dialects differ between runtimes while `EVS-PRD-portability` requires
byte-identical behaviour on every platform the substrate runs on, so an
accepted subset must be specified normatively rather than inherited from
whatever engine the host provides. And constructs whose evaluation may
fail to terminate are excluded, since a promoter that does not return is
a boot that does not complete.

### What remains out of reach

A derivation whose inputs are not all present in the event cannot be a
promoter at all — an offset that must be looked up from a profile, a
site record, or the host environment is ambient input, and reading it
would violate determinism. Such a change is not a promotion. A consumer
facing one appends a correcting event carrying the new value as a fact
in its own right, which is what the log is for.
