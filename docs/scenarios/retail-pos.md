# Retail POS on `event_sourcing`: A Design Sketch

Retail POS is a near-ideal fit for an append-only event-sourced substrate:
regulators and auditors care about cash, every register is a partially-
disconnected node that must keep working, and every operation needs to
reconcile across a hierarchical org (register → store → region → corporate).
The fit also stretches the substrate in two ways: (1) registers want to
**dispatch actions offline** with cryptographic continuity, which means each
register must run a full substrate as its own `Source` rather than be a
`RemoteScope` thin client; and (2) cash reconciliation needs **time-windowed
roll-ups** that aren't a natural `AggregateProjectionSpec` shape. The rest of
the design follows from those two facts.

## 1. Initialization and use

**Backend choice per role.** Each register opens a `SembastBackend` against a
local SQLite/file store — this is non-negotiable for offline operation. The
store-level aggregator and the corporate office both run `PostgresBackend`.
Three deployment tiers, three `EventStore` instances per tier, each with its
own `Source(identifier=installId)`.

**Sync topology.** Each register is its own `Source`; the store-level server
is a destination for every register in that store; corporate is a destination
from each store. Outbound `Destination`s queue events durably, so a register
that's offline for a shift catches up at end-of-day. The hash chain and
provenance ride along: corporate audit can see "this sale was rung at register
R-7 (originator), forwarded by store S-12, ingested at corp at T+3h." Per-
aggregate-per-Source ordering means each register's transaction stream is
monotonic on arrival even when registers are reconciled out of wall-clock
order. This is precisely the v1 single-source-per-aggregate-type model: a
`transaction` aggregate is "owned" by the register that opened it; the
store/corp installs ingest read-only.

**Action shapes (representative).** `OpenTransactionAction` (creates the
`transaction` aggregate), `RingItemAction(transactionId, sku, qty)`,
`ApplyDiscountAction(transactionId, discountCode)`, `TenderAction(transactionId,
method, amount)` (`Idempotency.required` — this is the critical retry-safe
path), `VoidLineAction(transactionId, lineNumber, reason)`, `RefundAction(
originalTransactionId, lineNumbers, reason)`, `ReceiveShipmentAction(storeId,
sku, qty, poNumber)`, `EndOfDayReconcileAction(registerId, countedCash)`.

**Projections.** Cashier UI: `transaction_in_progress` (`AggregateProjectionSpec`
keyed on `transactionId`) showing line items and running totals, plus
`sku_lookup` (a static-ish `TableProjectionSpec` synced down from corporate).
Manager dashboard: `register_session_summary`, `voids_today`. Corporate:
`inventory_by_store_sku` (composite-key table) and `daily_sales_by_store`.

**Auth model.** Roles: `Cashier`, `Manager`, `DistrictManager`, `Corporate`.
Scope classes: `register` contained in `store`, `store` contained in `region`.
A Cashier is assigned `BoundScope(register, R-7)`; a Manager
`ValueWildcardScope(store)` for their store; a DistrictManager `BoundScope(
region, NorthEast)`. Permissions: `pos.ring` (scoped to `register`), `pos.void`
(scoped to `register`, granted only to Manager+), `pos.refund` (scoped to
`store`), `inventory.adjust` (scoped to `store`), `reconcile.close` (scoped to
`register`). Containment lets a Manager-at-store-S void a line at any
register-in-S without per-register assignments.

**Cross-process.** Registers are NOT `RemoteScope` clients of the store
server — they need to dispatch when the network is down. Instead, each
register runs the substrate directly (it's pure Dart; embed in the Flutter POS
app). Store and corporate run shelf servers exposing `reaction` handlers for
back-office browsers (the manager dashboard, corporate inventory web app).
Register-to-store is purely event sync via `Destination`s; browser-to-server
uses `reaction`'s `Remote*` interfaces.

## 2. Layer 1 properties that are load-bearing

Two Layer-1 guarantees are distinctive for retail POS in a way they aren't for,
say, a medical diary:

**Append-atomic-with-row-update is what makes inventory non-racy.** A
`TenderAction` whose `execute` emits both the `tender_received` event and
(implicitly via projection) the `inventory_by_store_sku` decrement commits
both inside one storage transaction. There is no window in which the sale was
recorded but the inventory wasn't, or vice versa. For a diary, this matters as
a nicety; for POS it's the difference between honest inventory and continuous
drift that fraud investigators can't distinguish from theft.

**Hash chain + provenance is the cash-drawer audit primitive.** A voided
transaction can't be silently removed because the chain anchors at the
originating register and any tampering breaks the chain from that point
forward. The provenance entries — register-7 → store-12 → corp — mean an
auditor investigating a shrinkage pattern can verify "every sale corporate sees
came from a real register install with a continuous chain back to genesis,"
not "trust the database snapshot." That's exactly the ALCOA+ "Original" +
"Attributable" + "Complete" combination the substrate ships structurally. A
diary doesn't need defense against an insider rewriting yesterday's row; a
cash drawer does.

Idempotency (`Idempotency.required` on `TenderAction`) is also load-bearing —
a register retrying a tender across a flaky link must not double-charge — but
that's a Layer-2 affordance built on the Layer-1 ordering guarantee.

## 3. Layer 2 conventions: what fits, what extends

**Reused as-is.** The role/permission/scope model with `ContainmentRef`-driven
hierarchy (register-in-store-in-region) is exactly what POS needs; the default
deep-merge `AggregateProjectionSpec` works for `transaction_in_progress`;
`TableProjectionSpec` with composite keys covers `inventory_by_store_sku`;
`Idempotency.required` covers tender retry.

**Extended via custom projections (not new substrate machinery).** Voids are
NOT modeled with `tombstoneEventTypes` — a voided line item must remain visible
in the audit trail with the void event next to it, not vanish. So the
`transaction_in_progress` projection treats `line_voided` as a normal merge
event setting `voided: true` rather than as a tombstone; receipt-rendering
code reads both. This is the substrate's recommended pattern when Layer-2
default semantics don't fit.

**End-of-day cash reconciliation is its own pipeline.** Rolling daily totals
don't fit `AggregateProjectionSpec` (the aggregate is "register-day" — a
synthetic key) and they need a closing event that freezes the day. Two
options: (a) a `register_day` aggregate with composite id
`"{registerId}:{businessDate}"` where every sale appends to it — works but
creates a hot aggregate with thousands of events/day; or (b) subscribe to raw
events with `Events()` mode and compute the rollup app-side, emitting a
`register_day_reconciled` event at close. Option (b) is the Layer-1 escape
hatch the guide explicitly endorses ("subscribe to raw events with `Events()`
mode and compute your own state"). The `register_day_reconciled` event then
participates normally in the chain so corporate can audit each register's
close.

**Gap worth flagging.** The substrate today is single-source-per-aggregate-
type. That's fine for `transaction` (one register owns it) but awkward for
`inventory_by_store_sku`: a SKU's quantity at store S is modified by sales at
multiple registers in S AND by `ReceiveShipmentAction` dispatched at a
back-office terminal. Today this works because the *projection* is the
aggregator and the underlying events live on different aggregate types — but
a cleaner model would let multiple Sources contribute events to one logical
`store_sku_inventory` aggregate. That's the multi-source roadmap item
(`spec/roadmap/multi-source-editing.md`), currently dormant. The workaround
today is to keep inventory state in the projection rather than in an
aggregate, which is fine but means inventory reconciliation arithmetic lives
in projection-row updates rather than in event payloads.
