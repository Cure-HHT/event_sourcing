# Scenarios

Speculative architecture sketches for how a downstream consumer would
initialize and use `event_sourcing` for their domain. Each sketch answers
three questions:

1. **How would they initialize and use the library?** (composition: backends,
   actions, projections, sync, auth, cross-process)
2. **What unique or key aspect of their data model relies on Layer 1
   guarantees?** (hash chain, append-only, per-aggregate-per-Source ordering,
   atomic append+row, provenance — the substrate's load-bearing facts)
3. **What Layer 2 machinery is needed?** (projection shapes, permission
   scopes, event-type conventions — and which substrate defaults must be
   overridden)

The Layer 1 / Layer 2 framing is documented in `docs/event-sourcing-guide.md`
under "Two layers of trust" and in `CLAUDE.md` under "Epistemic layers" (with
`spec/prd-library-charter.md` as canonical source).

## Index

| Scenario | Best-fit summary |
|---|---|
| [Medical diary](medical-diary.md) | Near-perfect fit. Hash chain → FDA tamper evidence; per-aggregate ordering → pharmacokinetics correctness; provenance → multi-hop chain-of-custody. Hide-not-delete instead of tombstones. |
| [Banking ledger](banking-ledger.md) | Near-perfect fit. Append-atomic-with-view is *existential* (projection drift = insolvency). NO tombstones — reversals are compensating events. Double-entry enforced at the Action level. |
| [Supply chain](supply-chain.md) | Provenance chain literally IS the value proposition. v1 works per-org; true cross-org canonical timeline needs Phase II multi-source. |
| [Multiplayer game](multiplayer-game.md) | Per-aggregate ordering makes turn correctness fall out without locks. The hidden-information problem stretches scoping — every privacy boundary must be its own aggregate-with-scope. |
| [Retail POS](retail-pos.md) | Offline-first registers as Sources. Append-atomic-with-view → honest inventory; hash chain → fraud-resistant cash drawer. End-of-day reconciliation needs Events() mode. |
| [IoT sensor network](iot-sensor-network.md) | Per-aggregate ordering doubles as a clock-drift detector. Multi-source per farm is built-in. Three gaps: bypass dispatcher for telemetry volume; TimeBucketProjectionSpec doesn't exist; volume forces sharding. |
| [Collaborative editing](collaborative-editing.md) | Partial fit. Use substrate as event bus + audit; deep-merge fold is wrong for text — layer CRDT/OT on top via Events() mode. |

## Patterns across scenarios

### Layer 1 properties earn their keep differently per domain

| Property | Most load-bearing for |
|---|---|
| Hash chain integrity | Medical diary, banking, supply chain (regulatory tamper evidence) |
| Append-atomic-with-row-update | Banking (existential), retail POS (inventory accuracy), collaborative editing (real-time sync) |
| Per-aggregate-per-Source ordering | Multiplayer game (turn correctness), IoT (clock drift detection), medical diary (PK timing) |
| Provenance chain | Supply chain (cross-org), medical diary (multi-hop), retail POS (cash drawer audit) |
| Reactive subscribe<T> | Multiplayer game, collaborative editing (flagship real-time feature) |

### Layer 2 conventions: when to use, when to override

- **Tombstones**: rejected outright by medical diary, banking, supply chain
  (regulatory retention); used by retail POS for inventory removal but NOT
  for transaction voids.
- **Deep-merge fold**: works for medical diary, banking, retail POS,
  multiplayer game state, IoT sensor current values. **Wrong for collaborative
  editing's text inserts** — needs Events() mode + app-side CRDT.
- **Role/permission/scope with containment**: works essentially as-shipped
  across all scenarios. Scope hierarchies map naturally: site → patient,
  branch → account, facility → lot, table → match, store → register, farm →
  field → sensor.
- **`Idempotency.required`**: load-bearing for banking (ATM retry), retail
  POS (tender retry), medical diary (mobile network flake). Less important
  for game moves (turn semantics give effective uniqueness) and IoT (gateway
  deduplicates before append).

### Substrate gaps surfaced by ≥2 scenarios

| Gap | Hit by | Status |
|---|---|---|
| Phase II multi-source canonicalization | Supply chain, IoT, retail POS | Designed; dormant in v1 |
| Per-row authorization predicates | Multiplayer game (hidden info) | Not in spec; `RowFilterSpec` would help |
| Time-bucketed projection primitive | IoT | Future Layer-2 primitive candidate |
| Volume scaling beyond single Postgres | IoT at scale | Substrate-per-shard works; per-shard partitioning is out-of-scope |
| Cross-org transport auth federation | Supply chain | Known incomplete trust boundary (CLAUDE.md) |
| CRDT/OT semantics | Collaborative editing | Explicitly outside scope; app-side concern |

### The "Layer 1 escape hatch" gets cited often

Four of seven designs (retail POS reconciliation, IoT time-bucketing, banking
point-in-time reconstruction, collaborative editing CRDT fold) explicitly
reach for the pattern documented in `docs/event-sourcing-guide.md` under "Two
layers of trust": subscribe to raw events with `Events()` mode and compute
domain-specific state app-side. This is the substrate's most useful escape
hatch — Layer 1 facts remain intact while Layer 2 conventions are bypassed for
domain-specific reasons.
