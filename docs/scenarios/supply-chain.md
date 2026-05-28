# Supply Chain / Cold-Chain Provenance on `event_sourcing`

Supply chain is the substrate's archetypal multi-org provenance scenario: each
lot or pallet is a long-lived aggregate whose authoritative custody record is
the union of events authored by independent organizations (producer, multi-leg
shipper, distribution centers, retailer, dispensing pharmacist). The
substrate's provenance chain and hash-chain integrity (Layer 1) are a
near-perfect mechanical fit for "prove this vaccine was continuously
refrigerated and never left a verifiable hop." The role/permission/scope model
and projection interpreter (Layer 2) handle in-org workflows today; **the true
cross-org chain-of-custody flow is unlocked only by Phase II multi-source
activation**, which is currently designed but dormant.

## 1. Initialization and use

**Topology.** Each org runs its own substrate instance — one `EventStore` per
legal entity, backed by `PostgresBackend` (server-side; mobile handhelds in the
warehouse subscribe via the substrate's cross-process surfaces described in
`spec/reaction-remote.md`). A centralized multi-tenant substrate is rejected:
it would put one org's `StorageBackend` in the trust boundary of every other
org, and cross-org litigation/regulatory holds would entangle. Per-org
sovereignty is the table stakes.

**Scope-class registry** (per `spec/scoped-permissions.md`):

```text
ScopeClassSpec(name: 'org')                          // top-level
ScopeClassSpec(name: 'facility', containedIn: ContainmentRef(
  parentClass: 'org', projection: 'facility_org_index', ...))
ScopeClassSpec(name: 'lot',      containedIn: ContainmentRef(
  parentClass: 'facility', projection: 'lot_facility_index', ...))
```

**Action sketches** (each is an `Action<TInput, TResult>` whose dispatched
events carry the substrate-stamped authority + provenance):

- `CreateLot(productCode, lotId, manufacturedAt, expiry, initialFacility)` —
  emits `lot_created`; aggregate id = `lotId`; originator establishes Layer-2
  canonical authority for the lot (originator-of-first-event convention).
- `Ship(lotId, fromFacility, toFacility, carrierId, manifestId)` — emits
  `shipment_dispatched`. Permission `lot.ship` scoped to `facility`.
- `Receive(lotId, manifestId, atFacility, conditionAtReceipt)` — emits
  `shipment_received`; **idempotency keyed on `(manifestId, atFacility)`** so
  handheld-scanner retries after a network blip don't double-receive.
- `LogTemperatureExcursion(lotId, sensorId, startedAt, endedAt, peakTempC)` —
  emits `temperature_excursion`. Permission `lot.observe` scoped to `lot` (via
  facility containment).
- `Recall(productCode, lotsAffected[], reason, originDate)` — emits
  `recall_initiated` events targeting each lot aggregate. Permission
  `recall.issue` scoped to `org` and gated to the producer.

**Sync topology — the load-bearing piece.** Each org configures one
`Destination` per peer it ships to (filter: `aggregate_type == 'lot' AND lotId
∈ activeShipments`). The peer org's `Source` ingests, the substrate verifies
the upstream hash chain at the boundary (`EVS-PRD-ingest` assertion D),
extends the `ProvenanceEntry` chain with its own hop, and admits the event
into its local log. Per `EVS-PRD-ingest` assertion E, ingested events
participate in the local projection interpreter identically to locally-
originated events — meaning the receiving warehouse's `lot_timeline` projection
updates immediately on the manufacturer's `lot_created` event being relayed
in. Idempotency on upstream hash (`EVS-PRD-ingest` F) absorbs transport retries.

**Cross-org auth gap (explicit).** The substrate trusts the `Principal` on
faith (CLAUDE.md "Trust boundaries"). Cross-org, this means org B trusts that
the inbound `Source` connection from org A really is org A. This is an
inter-org PKI / mTLS problem that lives *outside* the substrate; the substrate
gives you the cryptographic verifiability of *what was said* (hash chain +
provenance), but not authentication of *who said it* at the transport edge. A
consumer-supplied `PrincipalAuthValidator` is the planned seam.

## 2. Layer 1 properties that are load-bearing

Two properties are the entire value proposition:

**Provenance chain.** A vaccine vial event ingested at the dispensing pharmacy
carries the full `List<ProvenanceEntry>` of every substrate it traversed:
producer → outbound carrier → DC → retail DC → pharmacy. Each hop is
immutable, append-only, attributed, time-stamped, and software-version-stamped
(`spec/prd-provenance.md` A, B). Pharma regulators (GDP) and food regulators
(FSMA) can answer "where has this been?" from a single event in hand — the
answer travels with the event rather than requiring a distributed query across
N org logs. This is precisely the audit shape a multiplayer game does **not**
need: a game cares about state convergence, not transit attribution.

**Hash-chain integrity, multiple chains per log** (`spec/prd-hash-chain-
integrity.md`, "Multiple chains per log" para). Each org's locally-authored
events form a chain anchored to that org. After ingest, a receiving org's log
holds N chains — its own plus one per upstream — each independently verifiable
by a regulator with no privileged access. This makes "Carrier C forged a
temperature-OK reading" computationally infeasible: the forgery would break
Carrier C's own chain, detectable by any party (including a downstream
pharmacy two hops away) who holds the chain segment containing the contested
event. Atomic append-with-row-update (`EVS-PRD-event-log`) closes the "row
says delivered, event says not yet" inconsistency window.

Per-aggregate-per-Source ordering matters too, but as a precondition for the
projection interpreter rather than the headline regulatory feature.

## 3. Layer 2 machinery — what's needed

**Projections** (declarative `ProjectionSpec`s — no fold code):

- `LotTimelineProjection` (`AggregateProjectionSpec`, keyed on `lotId`) — one
  row per lot, deep-merge of every custody event into a JSON state with arrays
  per excursion + ordered hop list. Drives "show me this lot's history."
- `FacilityInventoryProjection` (`TableProjectionSpec`) — one row per
  `(facility, lotId)` currently on-hand; insert on `shipment_received`, remove
  on `shipment_dispatched`. Drives operator dashboards.
- `RecallDownstreamProjection` — for each `recall_initiated`, materializes the
  transitive set of `shipment_dispatched` events where `lotId ∈ affected`,
  producing `(lotId, destinationFacility, dispatchedAt)` rows. Sketch: a
  `TableProjectionSpec` keyed on `(recallId, destinationFacility)`, populated
  as the projection interpreter sees `shipment_dispatched` events whose
  `lotId` already appeared in a `recall_initiated`. Because shipment events
  are ingested from peer orgs, the downstream set is computed using the
  **same** projection interpreter regardless of which org's substrate is
  running the query — every org sees the slice of the recall tree visible to
  them.

**Permissions.** Substrate role/permission/scope handles in-org workflows:
`WarehouseOperator` is `role_assigned` with `BoundScope(class: facility,
value: F-12)`; the lot-containment chain resolves `lot → facility → org` so
`lot.ship` checks containment. Cross-org, scoping does not span orgs — each
org's substrate authorizes within its own role projection only. There is no
substrate primitive for "Carrier C is granted lot-write on this manifest by
Org A"; that authorization happens by org A *choosing to ingest* org C's
events for those lots, and it is recorded in the log via the
`Destination`/`Source` configuration events. Cross-org delegation is, in
substrate terms, a sync-topology question, not a permission question.

**Event-type conventions used.** No `delete_lot` event: a recalled lot is
`recall_initiated`, never tombstoned — the lot's custody history must remain
forever under GDP/FSMA. Idempotency uses the substrate's hash-identity
(ingest F) for cross-org retries plus an app-side `manifestId` natural key on
`Receive` for handheld retries.

**Integration seam.** `Destination`/`Source` is the natural inter-org seam
*if* peer orgs are also substrate users — the durable FIFO queue
(`EVS-PRD-destinations` C/D), the hash chain, and the provenance chain are
then preserved end-to-end. For non-substrate peers (a legacy EDI partner), the
seam degrades to a webhook `Destination` exporting canonical JSON; the partner
gets verifiable single-event evidence but cannot reconstruct multi-hop
provenance themselves.

## Gaps to flag explicitly

1. **Multi-source activation (Phase II).**
   `prd-multi-source-canonicalization.md` specifies the rule grammar — "events
   from the originator's authority are canonical by default; other authorities
   admit via approval events." Today, the substrate behaves as
   single-source-per-aggregate. **A supply chain lot has many legitimate
   writers** (every facility it passes through). v1 works in practice because
   per-org logs see only their own + ingested events and each org's
   projections present a local-perspective view; **but a "global custody
   truth"** across orgs — one canonical timeline merging all writers —
   requires Phase II canonicalization rules (e.g., "facility currently-holding
   the lot is the canonical author until handoff"). Until then, the global
   view is reconstructed app-side by walking each org's local projection.

2. **Cross-org transport auth.** The substrate's `Principal`-on-faith and the
   lack of an inbound `AuthenticationProvider` means inter-org mTLS / signed
   handshakes live in app code. Documented in CLAUDE.md as a known incomplete
   trust boundary.

3. **Hierarchical containment across orgs.** The scope-class containment chain
   (`lot → facility → org`) assumes the `lot_facility_index` projection exists
   in the asking org's local substrate. For a lot currently held by org B,
   org A's substrate has no row; this is correct (org A *shouldn't* authorize
   an action on org B's lot) but it means cross-org custody handoff workflows
   are not modelled by extending containment — they are modelled as ingest of
   org B's events into org A's log under multi-source rules (Phase II again).

Net: the substrate's Layer 1 properties (provenance + hash chain) make it the
most natural fit for supply chain of any use case in the substrate's design
vocabulary; the Layer 2 conventions cover the in-org and the per-org-
perspective cross-org case today; **the symmetric cross-org canonical-timeline
case is exactly what Phase II multi-source canonicalization was designed for,
and is the only material gap.**
