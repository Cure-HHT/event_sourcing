# Medical Diary (eDiary) on `event_sourcing`

A clinical-trial eDiary fits the substrate's grain unusually well: every input
(symptom rating, dose-taken confirmation, vital sign, AE report) is an immutable
event with an identified initiator (the patient, the coordinator), a timestamp,
and a regulatory obligation to remain visible forever. The deployment splits
into two processes — the patient's mobile install owns the authoritative log
for that patient's entries, the portal server owns the consolidated study-wide
log — bridged by `reaction`'s wire layer and (eventually, Phase II)
cross-installation sync. ALCOA+ is not an afterthought; it is the reason this
substrate exists in this app's stack.

## 1. Initialization and use

**Storage.** `SembastBackend` on the patient's phone (already shipping; works
offline, encrypted at rest by Flutter Secure Storage on the database key).
`PostgresBackend` on the portal server. Both pass the same conformance harness,
so the *substrate* code on both sides is identical; only the backend handle
differs.

**Action shapes** — representative subset:

- `RecordSymptomEntryAction<SymptomInput, EntryId>` — patient logs daily
  symptom severity. Permission `entry.create` scoped to `patient`. Idempotency
  `required` (mobile retries on network flake must not double-write).
- `ConfirmDoseIntakeAction<DoseInput, EntryId>` — patient confirms dose taken
  at a timestamp. Same scope class. Idempotency `required`.
- `RecordAdverseEventAction<AEInput, EntryId>` — patient or coordinator logs
  an AE. Idempotency `required`; AEs often trigger out-of-band workflows
  downstream.
- `QueryEntryAction<QueryInput, QueryId>` — coordinator raises a data
  clarification question against an entry. Permission `entry.query` scoped to
  `patient`. The *entry itself remains* (Layer 2: hide-not-delete; see §3) —
  the query is an additional event referencing the original entry's
  `aggregateId`.
- `WithdrawConsentAction<WithdrawInput, void>` — patient or coordinator
  records consent withdrawal. Triggers a downstream policy (no new entries
  accepted) but, critically, the existing log stays intact for regulatory
  retention.

**Projection shapes.** Mostly `AggregateProjectionSpec`s — one row per entry,
merged from the entry-create event plus any subsequent amendments or queries.
Plus several `TableProjectionSpec`s:

- `patient_site_index` — `(patientId → siteId)`, the substrate's
  `ContainmentRef` for the scope hierarchy (see §3).
- `daily_diary_index` — `(patientId, date) → entryIds[]` so coordinators can
  find missing days.
- `ae_log` — flat audit-style table of all `adverse_event_recorded` events
  for cross-site dashboards.
- `query_open_index` — open data-clarification queries per coordinator.

**Sync topology.** Phase I: each patient install is single-source for its own
entries; the portal is single-source for its derived study-management data
(queries, role assignments, site config). Patient install opens a `Destination`
to the portal that forwards `patient_*` events; the portal accepts them via the
ingest path. Coordinators only ever read patient data on the portal — they do
not produce events on the patient's behalf except via `Action`s that emit *new*
aggregate events (queries, AE follow-ups). The "originator of first event"
convention naturally pins canonicalization to the phone for entries and to the
portal for queries.

**Auth model.** Three roles to start: `PatientSelf`, `StudyCoordinator`,
`Supervisor`. Two scope classes: `site` (top-level) and `patient`
(`containedIn: ContainmentRef('patient_site_index', keyColumn: 'patientId',
parentColumn: 'siteId')`). `PatientSelf` users hold `entry.create` scoped to
their own patientId; coordinators hold `entry.view` and `entry.query` scoped to
their site (the substrate's containment expansion gets them per-patient access
for free); supervisors hold the same at total-wildcard. An `Auditor` role
requiring read-only access across the study uses `ValueWildcardScope('site')`.

**Cross-process.** Two composition roots. The phone runs `LocalScope` against
`SembastBackend` — `AuthSession.Authenticated(Principal(patientId,
PatientSelf))` is set once during enrollment and never changes. The portal's
Flutter web app runs `RemoteScope` against the portal server's
`ReactionHandlers`; the portal server hosts the Postgres-backed `EventStore`
and the dispatcher, with `authMiddleware(FirebasePrincipalAuthValidator())`
populating the request context. Patient apps additionally open a `Destination`
to the portal's ingest endpoint to forward their entries upstream.

## 2. Layer 1 guarantees that are load-bearing

Two Layer 1 properties carry the regulatory weight in a way they wouldn't for,
say, a multiplayer game.

**Hash-chain integrity over the cryptographic trail.** When an FDA inspector
or a sponsor's GCP auditor asks "prove no one has altered this patient's diary
entries after the fact," the answer cannot be procedural ("we have access
controls"); it has to be structural. The substrate's
`event_hash`/`previous_event_hash` chain plus the operational obligation in
`EVS-PRD-regulatory-alignment` (B: hash-chain mismatch surfaces as an integrity
violation, not silent absorption) gives the auditor a computation they can run
themselves on the exported log: recompute SHA-256 forward from genesis,
compare. Any in-place modification of a prior event — a coordinator's
well-meaning "fix" of a symptom rating, a database admin's accidental UPDATE —
breaks the chain from that point and is detectable without trust in the
substrate's authors. A multiplayer game can absorb a corrupted record and move
on; an eDiary defending an NDA submission cannot.

**Per-aggregate-per-Source ordering plus append-atomic-with-row-update.** Each
patient's diary is one aggregate (`aggregateType: 'patient_diary'`,
`aggregateId: patientId`) and each entry within is its own aggregate keyed by
`entryId`. The substrate guarantees that the dose-taken event and the
post-dose pain rating event cannot be reordered — the pre-dose pain rating
cannot show up after the dose confirmation in the materialized view, even
under aggressive concurrent ingest. For pharmacokinetics interpretation this
is not cosmetic; the temporal ordering *is* the data. And because append is
atomic with the projection's row update inside one storage transaction, the
`daily_summary` row reflecting "average pain today" cannot drift out of sync
with the underlying entry events the way it would in an eventually-consistent
denormalization. The audit answer "what did the coordinator see when they made
this decision?" is reconstructable from the log: `EventStore.read(uptoSequence:
N)` reproduces both the events and the derived view exactly as they stood at N.

Provenance is the supporting cast: when the patient's phone forwards an entry
to the portal, the `ProvenanceEntry` chain records the originating install
(UUIDv4 per phone), the timestamp at originator, the forwarding hop, and the
software version at each. The audit trail does not just say "this entry
exists"; it says "patient install X-123 created it at T₁ running app v1.4.2;
portal received it at T₂ running app v2.0.1." That's the multi-hop attribution
Part 11 §11.10(e) wants.

## 3. Layer 2 machinery — defaults vs. customizations

The eDiary domain is unusually well-served by the substrate's defaults but
needs three deliberate departures.

**Projection shapes — defaults work, with one addition.**
`AggregateProjectionSpec` with deep-merge is exactly right for an
entry-with-amendments. The substrate's auto-columns (`firstEventTimestamp`,
`latestEventId`, `updatedAt`, `sequence`) match what the coordinator UI's "last
modified" column needs out of the box. *However:* the audit UI also needs a
per-event view ("show me every change to this entry, by whom, when, why"),
which `AggregateProjectionSpec` flattens away. This is built as a sibling
`TableProjectionSpec` (`entry_audit_log`, keyed by `event_id`, `rowData:
WholePayload()`) over the same event types — the same source-of-truth, two
materializations, both reconstructable from the log.

**Scope class hierarchy — straight use of the primitive.** Two-level `site →
patient`. The eDiary doesn't need `study` as a third level in v1 (single-study
deployments); a multi-study sponsor portal adds a `study` scope class as a
registered `ScopeClassSpec` with `patient_site_index`'s parent class set to `study`.
The substrate's containment expansion gives coordinators per-patient grants
from their site assignment without any code change.

**Tombstone semantics MUST be overridden.** This is the most important Layer 2
departure for eDiary. The substrate ships `tombstoneEventTypes` as row-deletion
semantics ("the row disappears from the view"). For an eDiary, *no entry is
ever deleted* — Part 11 §11.10(c) forbids it. Instead, "remove an entry" is
modeled as a `entry_marked_invalid` event that sets a `validity: 'invalid'`
field via the deep-merge fold; the row stays in the view, the UI hides it from
the patient's daily list but the coordinator's audit view still shows it with
strikethrough. The substrate's existing convention (`tombstoneEventTypes` set,
row deletes) is the wrong default here — the diary's `AggregateProjectionSpec`s
simply do not declare any tombstone event types, and the app's UI implements
hide-not-delete via field filtering. This is the kind of Layer-2 override the
substrate explicitly expects (per the guide's "Two layers of trust" framing)
and supports without extension.

**Role-permission model — straight use, with one substrate gap.** The
role-permission-scope mechanism handles coordinators-at-site and
patients-edit-own without extension. The one gap, called out in CLAUDE.md as a
known incomplete trust boundary: `Principal.userId` is trusted on faith from
the auth layer. The portal's `FirebasePrincipalAuthValidator` closes this from
outside, and `reaction`'s middleware enforces it on every HTTP request;
*however*, the patient app currently has no concept of a logged-out patient
(the device is the credential after enrollment). For a multi-patient shared
device (rare but not impossible in some trial designs), an app-level session
abstraction is needed — not a substrate change, but worth flagging because the
substrate does not provide one.

**One non-gap.** Cross-installation canonicalization conflicts (a phone sends
an entry the portal also somehow has) are squarely deferred to Phase II's
multi-source machinery. v1's "originator-of-first-event" convention is fine:
the phone is canonical for its patient's entries, period. The portal's job is
to accept and propagate, not to arbitrate.
