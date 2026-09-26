# Roadmap — multi-source editing and canonicalization

The headline deferred capability is letting multiple event-sourcing deployments contribute events to the same aggregate, and resolving which of those events are canonical. `spec/prd-multi-source-canonicalization.md` pins the rule grammar (assertions A–F). This file records what already exists to build it on, and what remains to build it.

## Verified baseline (what exists in code)

The substrate keeps the seams that authority identity, ordering and causality need. The canonicalization layer itself is unbuilt. What exists:

- **Per-event authority and database identity.** `Source` (`event_sourcing/lib/src/storage/source.dart`) and the originator entry, `provenance[0]`, give every event an attribution. The originator entry also records the originating database's identity, which keys origin chains, delivery channels and succession.
- **Per-aggregate-per-authority ordering.** The log keeps each authority's write order per aggregate along each path an event arrives by.
- **Causal parents on every event.** Every event carries a causal record, stamped by the library inside the append transaction and covered by the event hash: its kind (version or annotation), its eligibility, and the parents it names within its aggregate. Entry-type definitions declare kind and eligibility per event type; every reader reads the values each event records. The chain verification checks each event's parents against the stamping rule wherever the log holds the history that decides them (`EVS-DEV-causal-parents`).
- **Forks recorded as findings.** A database that went back in time and appended forks its origin chain. Every holder that meets the fork, or an origin position two of its events share, records a security finding, and the default views fold the aggregates the findings reach and mark them as having an outstanding finding (`EVS-DEV-chain-verification`, `EVS-PRD-materializer`). The regressed sender is rebuilt by the application as a successor that restores what its receiver holds, both branches included, and the restore records the forks it stores as findings of its own.
- **Succession lineage.** Succession events state which databases form one writer's lineage. A successor holds its predecessor's events, so its first edit of a predecessor's entry names the predecessor's last version as its parent.
- **Integrity-verifying ingest and restore with recorded findings.** Ingest and the restore store an event that fails a hash-chain, predecessor, fork or reused-position check as received and record a security finding for it (`EVS-DEV-security-findings`). `EventStore.logRejectedBatch` records an inbound batch refused because it cannot be decoded.
- **Forward-compatible delivery envelopes.** A receiver keeps, hashes and stores an optional envelope attribute a later release adds, so per-delivery facts (such as which parents a filter withheld from a channel) can be added without a data-format major step.
- **A comment-level gate in `ProjectionRegistry.seal()`** marks where settings-event-driven registration would hook in.

What does not exist: canonicalization code (`CanonicalView` / `ProposalView`), the `set_canonicalizer` / `delegate_canonicalization` event types, rule interpretation, canonical-event filtering in the folds, and authorization-aware ingest refusal. Conflict detection does not exist: the library records a fork as a finding and folds both branches in log order, marking the aggregates. The single-source-per-aggregate-type invariant holds.

## Remaining work

### Structural conflict detection and reconciliation

Detect a conflict from the causal record alone: two versions of one aggregate, neither an ancestor of the other. Two roots of one aggregate count as concurrent creation. A single mechanism would then cover the two branches a restore leaves (which the library today records only as fork findings), two devices editing one participant's entry, and edits by another party. It needs:

- an ancestry read over parents that states when the holder lacks the history to decide, rather than guessing;
- a conflicted status for the default views: an aggregate whose two branches each wrote a version is served with each branch's state and no single state, and every read passes each branch to the consumer's mapper on its own;
- a reconciliation: a version, appended only on the application's request, whose parents are both heads and whose data is the aggregate's whole state, so the default views fold it onto an empty row and fold neither branch after it; which principal may reconcile is the application's permission question;
- while an aggregate is conflicted, a refusal of new versions and annotations (drafts stay possible and name both heads), and a typed refusal of every library decision, such as an authorization, that would read a conflicted row;
- a rule for aggregates whose history reached a holder through a channel that withheld parents.

### Completeness of an aggregate's history

A holder can lack events that fold into an aggregate: a channel's filter withheld a parent, or a fork left positions of one branch the holder never received. The default views serve the state the holder has without a mark beyond any finding's. This needs:

- a per-delivery fact stating, per carried event, whether a parent was withheld from the channel, carried as an optional envelope attribute;
- a read that reports, per aggregate, the recorded facts that bear on its completeness, so an application can flag an entry for a person to check;
- a rule for when a withheld parent is a horizon (a start date, a destination registered late, a successor's first edit of its predecessor's entry) rather than a gap;
- a decision whether a default fold applies a writer lineage's events in origin order, so that a parent that arrives after its child is folded before it and the state becomes whole once every parent is held.

### Stale annotations

An annotation whose parent is no longer the aggregate's current version is stale. This needs a read operation that reports stale annotations, and a default-view flag on them. The first case to test: a receiver's score annotation on a sender's survey version, where the sender later re-finalizes the survey.

### Drafts on a superseded version

A draft (an ineligible event) whose parent is no longer the aggregate's current version was written against an outdated state. This is derivable from the log. It needs a read that reports such drafts, and a default-view flag, so an application can warn before a draft is finalized.

### Proposals and their acceptance

Proposals, and the events that accept or reject them, become entry types of their own. An older build of the same data-format major then stores them without folding them into the canonical state. This needs:

- a proposal view beside the canonical view, so an application can show pending proposals against the current version;
- acceptance as a version whose parents are the current version and the accepted proposal.

### Authority rules as events

The rule events (`set_canonicalizer`, `delegate_canonicalization`) are recorded on the same log, so the rules in effect for an aggregate at any point can be reconstructed from the log alone. Their interpretation classifies each event as canonical or non-canonical by its authority. The materializer folds only canonical events into typed state, while non-canonical events stay visible in the log and in opt-in subscriptions. An event from an authority with rule-granted approval power admits an otherwise non-canonical edit from a different authority. Per-entry-type resolution policies decide what a reconciliation between authorities looks like.

### Inbound delivery

A receiver delivers events back to its senders on channels of its own: its annotations, proposals, and acceptances or rejections. Each database still delivers only the events it authored. This needs an inbound channel per sender, the receiver's events kept apart from the sender's own origin chain, and a sender's views folding what it receives under the canonicalization rules.

### Relaxing the single-source commitment

Once structural conflict detection and the authority rules exist, more than one database may write versions of one aggregate. The parent verification then checks each event against the history its own writer held, and conflicts between writers are detected structurally.

### Authorization-aware grant visibility

Ingest rejection is integrity-only. Multi-source authorization adds an authorization-aware ingest refusal and a recorded refusal event: an ingesting deployment may refuse to apply another authority's events to its own views, and records that refusal as an event. Each authority remains the authority over its own log. A remote authority's authorization state at the time it emitted an event is what its log records, and ingesting peers cannot retroactively unmake the remote log. This item depends on the canonicalization layer above.

## Motivating scenarios

Multi-user collaborative editing (`docs/scenarios/collaborative-editing.md`) depends on this capability. So do the multi-authority consolidations in `docs/scenarios/supply-chain.md`, `docs/scenarios/iot-sensor-network.md` and `docs/scenarios/retail-pos.md`. Each has a single-authority per-aggregate story that works today, and a cross-authority merge story that needs structural conflict detection and the canonicalization layer.
