# Roadmap — multi-source editing and canonicalization

The headline deferred capability is letting multiple event-sourcing deployments contribute events to the same aggregate, and resolving which of those events are canonical. `spec/prd-multi-source-canonicalization.md` pins the rule grammar (assertions A–F). This file records what already exists to build it on, and what remains to build it.

## Verified baseline (what exists in code)

The substrate keeps the seams that authority identity, ordering and causality need. The canonicalization layer itself is unbuilt. What exists:

- **Per-event authority and database identity.** `Source` (`event_sourcing/lib/src/storage/source.dart`) and the originator entry, `provenance[0]`, give every event an attribution. The originator entry also records the originating database's identity, which keys origin chains, delivery channels and succession.
- **Per-aggregate-per-authority ordering.** The log keeps each authority's write order per aggregate along each path an event arrives by.
- **Causal parents on every event.** Every event carries a causal record, stamped by the library inside the append transaction and covered by the event hash: its kind (version or annotation), its eligibility, the parents it names within its aggregate, and, for a reconciliation, the skip events it resolves. Entry-type definitions declare kind and eligibility per event type; every reader reads the values each event records. The chain verification checks each event's parents against the stamping rule wherever the log holds the history that decides them (`EVS-DEV-causal-parents`).
- **Branch conflicts from a restore.** When a restored sender resumes a delivery channel, its skip event records the abandoned head of its origin chain and, as the branch point, the last event it can prove both branches share, when it can prove one; the range between them bounds the forks and reused origin positions of the restore, and every aggregate for which both branches wrote a version, with each branch's head and events. The default views serve such an aggregate as conflicted, with both states, until a reconciliation, a version whose parents are both heads appended only on the application's request as the aggregate's whole state, closes it. The library's own decisions refuse on a conflicted row (`EVS-PRD-branch-conflicts`).
- **Facts about incomplete history.** Each delivery records, for each event it carries, whether the channel withheld one of its parents, and the receiver keeps that record on the event. A skip event lists the origin positions above its branch point, up to the abandoned head, at which the sender holds no event of its own origin chain after the recovery. The library records both and draws no conclusion from them.
- **Succession lineage.** A read derives, from succession events, which databases form one writer's lineage. Causal parents and branch conflicts treat that lineage as one writer.
- **Integrity-verifying ingest with recorded findings.** Ingest stores an event that fails a hash-chain, predecessor, fork, reused-position or withheld-parent check as received and records a security finding for it (`EVS-DEV-security-findings`). `EventStore.logRejectedBatch` records an inbound batch refused because it cannot be decoded.
- **A comment-level gate in `ProjectionRegistry.seal()`** marks where settings-event-driven registration would hook in.

What does not exist: canonicalization code (`CanonicalView` / `ProposalView`), the `set_canonicalizer` / `delegate_canonicalization` event types, rule interpretation, canonical-event filtering in the folds, and authorization-aware ingest refusal. Structural conflict detection over causal parents does not exist either. The only conflicts the library detects are those a skip event records. The single-source-per-aggregate-type invariant holds.

## Remaining work

### Structural conflict detection

Detect a conflict from the causal record alone: two versions of one aggregate, neither an ancestor of the other. Two roots of one aggregate count as concurrent creation. A single mechanism would then cover a restore's branches, two devices editing one participant's entry, and edits by another party, and it would replace the skip event's recorded conflict set as the source of restore conflicts. It needs:

- an ancestry read over parents that states when the holder lacks the history to decide, rather than guessing;
- the conflicted status of the default views driven by it, with the reconciliation that closes a conflict unchanged;
- a rule for aggregates whose history reached a holder through a channel that withheld parents.

### Completeness of an aggregate's history

A holder can lack events that fold into an aggregate: a channel's filter withheld a parent, or a recovery left positions of an abandoned branch at which the sender holds no event it recovered. The facts are recorded (the withheld-parent record on each delivered event, the unrecovered positions in the skip event), and the default views serve the state the holder has without a mark. This needs:

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

Once structural conflict detection and the authority rules exist, more than one database may write versions of one aggregate. The parent verification then checks each event against the history its own writer held, and conflicts between writers are detected structurally rather than recorded by a skip event.

### Authorization-aware grant visibility

Ingest rejection is integrity-only. Multi-source authorization adds an authorization-aware ingest refusal and a recorded refusal event: an ingesting deployment may refuse to apply another authority's events to its own views, and records that refusal as an event. Each authority remains the authority over its own log. A remote authority's authorization state at the time it emitted an event is what its log records, and ingesting peers cannot retroactively unmake the remote log. This item depends on the canonicalization layer above.

## Motivating scenarios

Multi-user collaborative editing (`docs/scenarios/collaborative-editing.md`) depends on this capability. So do the multi-authority consolidations in `docs/scenarios/supply-chain.md`, `docs/scenarios/iot-sensor-network.md` and `docs/scenarios/retail-pos.md`. Each has a single-authority per-aggregate story that works today, and a cross-authority merge story that needs structural conflict detection and the canonicalization layer.
