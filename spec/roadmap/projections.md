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

## View fingerprints that cover code

**Baseline.** A view is stored per fingerprint of its definition (EVS-DEV-view-convergence), and the fingerprint covers what the library can read of the definition: its shape, its declared event types and derived fields, its interest's declared sets and the registered entry-type versions. An interest predicate, and a table view's row-key and row-data functions, are code the library cannot digest, so two builds that differ only there share one copy, and the equality a read reports holds only while they agree (EVS-DEV-converging-view-reads). A deployment that changes only such a function runs `rebuildView` once no build with the other function still serves the database.

**Remaining.** Make a change to such a function a new copy: a revision the definition declares and the fingerprint covers, or named functions from a registry the library can identify, so that a changed function is caught up like any other changed definition.

## Value-rewriting promoter primitives

**Baseline.** The sealed `TransformPrimitive` set ships three
shape-changers — `RenameField`, `DefaultField`, `DropField`. They move a
key, add a key that is absent, and remove a key. The only member that
writes a value writes a constant, and only where the key is absent.
Nothing in the vocabulary derives a new value from the value already
present, and composing the existing members does not get there: moving
the old value aside leaves nothing able to read it. The set is `sealed`,
so a consumer cannot add a member — correctly, since that closure is
what keeps the set small enough to audit.

The consequence is that a schema change altering the *form* of a value
rather than its *location* cannot be expressed. A deployment facing one
must reset its store, which is affordable only before it holds records
anyone depends on.

**Motivating shape.** A consumer records a timestamp that already
carries a UTC offset — the recording device's — while the offset of the
zone the record is *about* is held in a sibling field, and the
wall-clock digits are shifted so the instant is correct. Moving to a
representation where the intended offset sits inside the timestamp it
belongs to means deriving one field from two: the timestamp's own
embedded offset and the sibling offset field.

### Reading two fields is already safe

A derivation that reads a second field would once have been a problem.
When a lagging view row was promoted in place, the two fields a
derivation reads could have been last written by different events, so
promoting the merged row and merging the promoted events could disagree,
and only a single-key transform was sound.

That hazard is gone. Promotion now happens in exactly one place, on one
event's payload, before the fold
(`EVS-DEV-ingest-promotes-before-fold`/A and B). A view's rows live in a
copy keyed by a fingerprint that covers the registered version of every
entry type it folds, so bumping a version yields a new copy that is
brought up to date by replaying the log through the same event-wise fold
(`EVS-DEV-view-convergence`/A, B, J and K). No merged row is ever
promoted, so every field a derivation reads arrives together, and a
multi-field derivation is well defined without any claim from the
consumer about how its events are written.

What remains to design is therefore the vocabulary, not a safety
mechanism around it.

### Where such a member may appear in a chain

Promoter steps are already constrained by major and minor
(`EVS-DEV-version-compatibility`/B): a step within one major must lead
to the next minor and every transform in it must be a `DefaultField`,
while a step across majors must lead to minor 0 of the next major. A
value-deriving member is not a `DefaultField`, so it can only appear on
a step across majors. A consumer for whom the change is not worth a
major does not rewrite the value at all: it leaves the recorded form
alone and reads both forms, which is the recomputation case below.

### Obligations any value-deriving member inherits

- **Determinism.** A pure function of the event data, with no ambient
  input (`EVS-PRD-materializer`/B). A derivation consulting the host's
  clock, locale, or zone would break replay. Where one needs a format or
  a zone, it is named in the primitive's data, not read from the
  runtime.
- **Totality.** Every operation defined for every input it can receive.
  A partial operation — a pattern that does not match, a join against a
  value that is absent — must have a specified result, or two observers
  can diverge, or a catch-up can fail partway and retry forever.
- **Idempotence.** A store can already hold records in the target form
  stamped at the source version, because a wire change often lands
  before the version bump describing it. Applying the derivation to a
  value already in the target form leaves it unchanged.

### Promotion versus recomputation

Not every derived value needs a promoter. `DerivedField` computations
receive the whole row state and are recomputed by the fold, so such a
value needs no promoter and no version bump at all: change the
computation and the definition's fingerprint changes, which brings a
fresh copy up to date on its own.

That makes recomputation the right home for a derivation whose result
may legitimately be recomputed forever, and the wrong home for one that
must be frozen as recorded, since a recomputed value changes whenever
its computation does. The design draws that boundary explicitly; the
shipped computation set offers only a single-path lookup, so combining
several fields is an extension there either way.

### Notation

Two candidate directions, to be settled before implementation:

- **A closed set of named, parameterised operations** — split, join,
  reformat with explicit patterns. Trivially serialisable, obviously
  auditable, no evaluator to freeze. Grows by one name per need.
- **An expression notation** carried as data — a postfix token list or
  equivalent — where one primitive covers a family of derivations.
  Fewer names, more semantics to freeze.

Either way the notation is data in the log, never a host-supplied
callback: a callback lives in the consumer's build rather than in the
log, which is what the closed-under-events model refuses.

A pattern-matching notation carries two further obligations. Pattern
dialects differ between runtimes while `EVS-PRD-portability`/C requires
identical observable behaviour across supported runtimes for any given
input, so an accepted subset is specified normatively rather than
inherited from whatever engine the host provides. And constructs whose evaluation may
fail to terminate are excluded, since a promotion that does not return
stalls the catch-up of the copy it runs in.

### Cost, and one coupling to resolve

A major bump already costs a replay of the log into a fresh copy, so a
value-deriving member adds no new class of cost. The replay is
incremental: each catch-up transaction stops after 200 ms and resumes
from the copy's watermark (`EVS-DEV-view-convergence`/J and N), so the
work is bounded per transaction rather than held open across the whole
log.

One coupling deserves a decision. Ingest refuses an event below the
registered version for which a view it folds into has no promoter path
(`EVS-DEV-version-compatibility`/D), and the gap is looked for across
every interested view. Views may register different chains for the same
entry type (`EVS-DEV-ingest-promotes-before-fold`/C), so one view's
missing step refuses the event for all of them. Whether a richer
vocabulary makes that more likely, and whether such an event should
instead be admitted and kept out of only the view that cannot promote
it, is open.

### What shipping it costs

Adding the member leaves stored data alone. Each existing primitive
encodes into a view's fingerprint the same way whether or not a fourth
exists, so no stored fingerprint changes, no view copy is invalidated,
and a deployment that does not use the member replays nothing.

It is nonetheless a source-breaking change in one narrow way. The
primitive set is closed to subtyping, and exhaustiveness checking
reaches across library boundaries, so a consumer that switches over the
set exhaustively stops compiling when a member is added. No consumer has
reason to do so — a consumer composes chains and the library interprets
them — but the decision belongs in the release rather than in a
surprise: either the member ships in a major library release, or the set
is documented as one a consumer composes and does not switch over.

The cost of *using* it falls on the deployment that does. A
value-deriving member sits on a step across majors, so adopting one bumps
an entry type's major, which changes the fingerprint of every view that
folds that entry type, which gives each a fresh copy to bring up to date
by replaying the log. That work is bounded per transaction and resumes
from a watermark, so it is incremental rather than a stall, but it is a
replay of the whole log for each affected view.

A deployment running more than one instance pays one more cost. Two
instances whose shared entry type carries different majors hold
conflicting generations and may not serve one database at the same time
(`EVS-DEV-version-compatibility`/F), so adopting the member rolls out
with standby and takeover rather than side by side.

### What remains out of reach

A derivation whose inputs are not all present in the event cannot be a
promoter — an offset that must be looked up from a profile, a site
record, or the host environment is ambient input, and reading it would
violate determinism. Such a change is not a promotion. A consumer facing
one appends a correcting event carrying the new value as a fact in its
own right, which is what the log is for.
