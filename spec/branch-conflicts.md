# Branch Conflicts

## Overview

A sender restored from a backup can append events before it learns it was restored. When it recovers the events it lost from a receiver, it holds two histories that have both moved past a shared point: the branch it abandoned, and the branch it continued on. The skip event that records the resume records, as its branch point, the highest shared event the sender can prove, which may lie below the true fork, or none when it can prove none (`EVS-DEV-resume-event/E`). An aggregate for which both branches wrote a version after the branch point has two states, and neither is the one that counts. This file specifies what the library records about such aggregates, how its default views present them, and how the user's decision closes them. It also specifies how the default views mark an aggregate that a security finding names (`EVS-DEV-security-findings`).

The library does not decide between the branches. It records the set of conflicting aggregates in the skip event, which the sender writes when it holds both branches. Its default views serve each conflicting aggregate as conflicted, with both states, and serve a single state again only after the application appends a reconciliation on the user's decision. The reconciliation is the aggregate's whole state.

### Terms

- **Recovering skip.** For an event the sender holds through a recovery, the skip event the sender holds as authored with the highest local sequence number below that event's. A recovery appends its skip and then stores the events it brings back, in one transaction, so this is the skip whose recovery stored the event.
- **Abandoned-branch event of a skip S.** At the sender, an event it holds through a recovery, whose originator entry names the sender and whose recovering skip is S or another skip event the sender authored at an origin position above S's branch point, or at any position when S records no branch point.
- **Continuing event of S.** At the sender, an event it holds as authored, other than S, at an origin position above S's branch point, or at any position when S records no branch point.
- **Conflict record.** One entry of a skip event's `conflicted_aggregates`: an aggregate, the head of each branch, and the events of each branch.
- **Heads.** The two heads a conflict record names, each the latest eligible version on its branch.
- **Reconciliation.** An event whose causal record names, in `reconciles`, the skip events whose conflicts it resolves.
- **Closed, superseded and open.** At a holder, a conflict record is closed when the holder holds a reconciliation of its aggregate, from the skip's succession lineage, whose parents include both heads the record names. Otherwise it is superseded when the holder holds a later skip event of the same lineage that records the same aggregate. Otherwise it is open.
- **Conflicted aggregate.** An aggregate with an open conflict record at the holder.

```text
aggregate X at a holder of skip S (branch point bp)

  shared:     x1 <- x2                  (at or below bp)
  abandoned:          x2 <- a3 <- a4     (S: abandoned_events for X)
  continuing:         x2 <- c3           (S: continuing_events for X)

  row of X while open:   $integrity.status = conflicted
                         branch abandoned  = fold(x1, x2, a3, a4)
                         branch continuing = fold(x1, x2, c3)
  reconciliation r:      parents {a4, c3}, reconciles [S]
  row of X after r:      unconflicted = fold(r)   (r is the whole state)
```

### Reading order

`EVS-PRD-branch-conflicts` states what the library records and what its default views guarantee. `EVS-DEV-branch-conflicts` fixes the conflict record's content, how a holder derives each record's status, the conflicted row forms for the Aggregate and Table shapes, what a read returns, the transactions that change them, and the reconciliation operation. The causal record a reconciliation carries, and the stamping rule behind heads, are specified with causal parents (`spec/causal-history.md`). What a holder may conclude from the origin positions above the branch point that the sender does not hold after a recovery, or from a parent a channel withheld, is left to applications (`spec/roadmap/multi-source-editing.md`).

## EVS-PRD-branch-conflicts: Branch conflicts and their reconciliation

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel, EVS-PRD-materializer

### Purpose

When a sender's origin chain forks at a recorded branch point, the library records, in the skip event, every aggregate for which both branches wrote a version. Its default views then serve such an aggregate as conflicted with both branches' states until the user's decision, appended through the library's reconciliation operation as the aggregate's whole state, closes the conflict. Every holder of the skip event derives the same conflicts, and every holder of the reconciliation closes them.

### Assertions

A. When the library appends a skip event that records an abandoned head, it SHALL record in that event each aggregate the sender holds for which an abandoned-branch event of that skip and a continuing event of it are eligible versions, with the head and the events of each branch.

B. The library's default views SHALL TREAT an aggregate as conflicted exactly while a conflict record for it that the holder holds is open, deriving each record's status solely from the skip events, reconciliations and succession events the holder holds.

C. For a conflicted aggregate, the library's default views SHALL serve the state each branch yields and no single state.

D. Every read of a default view SHALL deliver each row that a conflict makes conflicted as conflicted, with each branch's state, and never as a single state.

E. The library SHALL refuse to append, on an aggregate conflicted at the appending database, an eligible version or an eligible annotation that is not a reconciliation.

F. The library SHALL append a reconciliation only on a consumer's request through its reconciliation operation, which names the aggregate and carries its resolved state.

G. The library SHALL refuse a reconciliation requested by a database outside the succession lineage of the database whose skip event records the conflict it resolves.

H. The library's default views SHALL TREAT a reconciliation as closing each conflict record of its aggregate whose heads are both among its parents, and SHALL TREAT the events that each closed record lists on either branch as superseded, folding none of them.

I. The library's default views SHALL NOT serve, as a row of the view, a row whose conflict status or findings a stored skip event, reconciliation or security finding changed, until the view has re-derived that row under these rules.

J. The rows of every default view SHALL equal the rows a rebuild of that view from the log derives, conflicts and closures included.

K. Every conflict record the library records SHALL name two heads that are eligible versions authored within the succession lineage of the skip event's originating database.

L. The library's default views SHALL TREAT a reconciliation that closes a conflict record as the whole state of its aggregate, derived from no earlier event of the aggregate.

M. Every library operation that decides from a default view's rows SHALL refuse, appending no event, when a row it would decide from is conflicted.

N. The library's default views SHALL TREAT every aggregate that a security finding the holder holds names as having an outstanding security finding, for as long as the holder holds that finding, and SHALL fold its events as they fold any aggregate's.

### Rationale

**Why record the conflict set in the skip event (assertions A and B)?** The sender holds both branches only after the recovery that brings the abandoned one back, and a receiver may never hold both. The sender therefore computes the conflicts in the transaction that appends the skip, and records them under its hash; the skip reaches every receiver of the sender's events (`EVS-PRD-delivery-channel`), so every holder derives the same conflicts whatever its channel filtered. Detection is per aggregate, the unit a version replaces, and only versions count, so an annotation or a draft makes no conflict. The recorded set is a Layer 1 fact, what the sender held when it wrote the skip; treating the aggregate as conflicted is the default views' Layer 2 interpretation. The abandoned-branch events were served by the recovery's receiver, which is trusted not to fabricate events under the sender's identity (`EVS-PRD-delivery-channel`, Trust), so the set inherits that trust.

**Why serve both states and no single one (assertions C and D)?** Picking a branch, or the last writer, would decide on the user's behalf which edit counts, and the log would record nothing of the choice. In the sender's own log the recovered branch is stored after the continuing one, so a last-writer fold would silently put the abandoned state back over the user's later edits. A read never passes a conflicted row to the consumer's mapper as if it were resolved.

**Why refuse edits of a conflicted aggregate (assertion E)?** A new version would have to name one head (and silently extend one branch) or both (and silently reconcile). Either would settle the conflict without the user deciding. Refusing the edit makes the decision come first. Drafts and other ineligible events stay possible, since they are never a parent: they record the branches they were written against, and the user can compose the resolution in them.

**Why only on the user's request, and only by the writer (assertions F, G and K)?** The library cannot tell which state the user meant, so it reconciles only when the application asks. The resolution is an edit of the aggregate, so it belongs to the aggregate's writer or its successor; both heads are versions that writer's lineage authored, so every recorded conflict is one it can reconcile. A receiver that reconciled would become a second editor, which the single-source invariant does not admit.

**Why is the reconciliation the whole state (assertions H and L)?** The user looks at two states and decides what the entry is; recording that as the whole state makes the reconciliation say exactly what the user chose, with nothing inherited from a history the user did not look at, and independent of where the branch point lies. The events of both branches stay in the log and every event subscription; the default views stop folding them. The reconciliation is an event of the aggregate's own entry type, so it closes the conflict on every holder that receives it.

**Why no row is served until it is re-derived (assertion I)?** A skip event and a reconciliation each change which events a row folds, and a finding changes its mark, so each leaves the rows it affects pending until they are re-derived. A recovery stores its skip with the recovered branch, and before it (`EVS-PRD-delivery-channel`), so no holder folds a recovered event without the conflict records that decide how it folds.

**Why rebuild equality (assertion J)?** Conflicts and closures are functions of the log, so a view rebuilt from the log must show them exactly as the incremental fold does. This is the materializer's determinism (`EVS-PRD-materializer/B`), extended to the rows these rules produce.

**Why refuse a decision from a conflicted row (assertion M)?** The library's own decisions read default views too, such as the authorization policy's role and grant views, and a restore can put a grant in conflict. A decision read from one branch would record an outcome the other branch contradicts, so the operation refuses and records nothing, as it does while a view converges (`EVS-DEV-converging-view-reads`).

**Why mark, not withhold, an aggregate a finding names (assertion N)?** A finding says the holder stored something it could not verify, not that the data is false. Withholding the aggregate would hide the record the user needs to judge, and choosing a state would decide on the user's behalf; folding it and marking it keeps the view truthful about both. The mark lasts while the finding is held, since clearing a finding is a separate event (`spec/roadmap/security-findings.md`). The finding is a Layer 1 fact; the mark is the default views' Layer 2 convention.

**What is recorded and not interpreted.** A recovery may leave origin positions of the abandoned range that it did not bring back, which the skip lists (`EVS-DEV-resume-event/C`), and a channel's filter may withhold an event's parent. The skip event lists the positions, and each delivery records, per event, whether a parent was withheld. These are Layer 1 facts. Whether a holder's state of an aggregate is complete depends on which events the application's views need, which the library cannot know, so the default views draw no conclusion from them; an application reads them and decides, and a person reconciles what it flags.

**Relation to canonicalization.** Canonicalization decides between events from different authorities (`EVS-PRD-multi-source-canonicalization`). A branch conflict is between two histories of one authority's writer, which a restore split. The two do not interact while the single-source invariant holds. The vocabulary of branches, heads and reconciliation is the one the roadmap's structural conflict detection builds on (`spec/roadmap/multi-source-editing.md`).

### Changelog

- 2026-09-25 | abb9e6ef | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Add N: the default views mark an aggregate a held security finding names, and fold it. I: a stored finding also leaves the rows it changes unserved until re-derived. No code or test references I or N
- 2026-09-25 | 00a1f931 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | A: every skip event that records an abandoned head records its conflicts, a null branch point included; the Overview and Terms count from position 0 when a skip records no branch point; Rationale names the positions at which the sender holds no event of its own. No code or test references A
- 2026-09-25 | e4dea720 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | A: every authored event above the branch point counts as continuing, with no ambiguous events. E: only an eligible version or annotation is refused on a conflicted aggregate, so drafts stay allowed. No code or test references A or E
- 2026-09-25 | 3f424b3b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale shortened to the reason for each rule
- 2026-09-25 | 3f424b3b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Remove the retired I and re-letter: J-I, K-J, L-K, M-L. A: the abandoned branch of a skip is what the recoveries of its restore brought back, so an earlier restore's recovered events make no conflict. Add M: a library decision from a conflicted row refuses and appends nothing. C and F state one obligation each. No code or test references any of these letters
- 2026-09-25 | 1f26a2df | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Retire I: the library records the facts behind completeness and draws no conclusion from them. Add M: a reconciliation is its aggregate's whole state. Amend F, H, J, K and the Purpose: no possibly-incomplete mark; ambiguous events are superseded with the branches; J no longer holds back recovered events, which a recovery stores with their skip
- 2026-09-25 | f6d1a7c2 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-L: conflicts fixed in the skip event over eligible versions, conflicted aggregates served with both branches' states and never as a single state, edits of a conflicted aggregate refused, reconciliation only on the consumer's request and only by the writer's succession lineage, closure and supersession by a reconciliation with ambiguous events kept, possibly-incomplete marks, recovered events held out of the folds until a skip event records them and rows not served while their status changes, rebuild equality, and heads always reconcilable by the writer's lineage

*End* *Branch conflicts and their reconciliation* | **Hash**: abb9e6ef

## EVS-DEV-branch-conflicts: Conflict records, conflicted rows and reconciliation

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-branch-conflicts

### Purpose

This requirement fixes the content of the skip event's `conflicted_aggregates` and how a holder derives the status of each conflict record. It fixes the stored and read form of a conflicted or unconflicted row for the Aggregate and Table shapes, the reserved row namespace, the fold's exclusions, the transactions that re-derive rows, the refusal of edits on a conflicted aggregate, the refusal of the library's decisions on a conflicted row, the reconciliation operation, and the security findings each row carries.

### Assertions

A. A skip event's `conflicted_aggregates` SHALL be null when its `abandoned_head` is null, and otherwise SHALL hold, in ascending order of aggregate identifier, one object for each aggregate of an entry type other than a reserved one for which a counted abandoned-branch event of the skip is an eligible version and a counted continuing event of it is an eligible version, counting no event that a closed conflict record at the sender lists on either branch.

B. Each object of `conflicted_aggregates` SHALL carry exactly `aggregate_id`; `abandoned_head`, the latest eligible version by origin position among the counted abandoned-branch events of the aggregate; `continuing_head`, the latest eligible version by origin position among its counted continuing events; each head an object with exactly `event_id` and `event_hash`; and `abandoned_events` and `continuing_events`, each the identifiers of the counted events of the aggregate on that branch, whatever their kind and eligibility, in ascending order of origin position.

C. A holder SHALL treat a conflict record of skip event S for aggregate X as closed when it holds a reconciliation of X whose originating database is in the succession lineage of S's originating database and whose parents include both heads the record names; otherwise as superseded when it holds a later skip event of that lineage that records X, ordering skip events by origin position within one database, placing every event of a predecessor before every event of its successor, and ordering equal positions by event identifier; and otherwise as open.

D. Every fold of every default view SHALL exclude the events that a closed conflict record lists as abandoned or continuing.

E. While an aggregate has an open conflict record, its Aggregate-shape row SHALL hold only `aggregateId`, `aggregateType` and the reserved key `$integrity`, whose object carries `status` `conflicted`, the skip event's identifier, the two heads, and, for each branch, the row the view's fold yields over the aggregate's events in log order without the events the record lists on the other branch (null when that fold leaves no row), every other key the fold stamps appearing only inside those branch rows.

F. A Table-shape row whose key an event listed as abandoned or continuing by an open conflict record produces SHALL hold only its key columns and `$integrity`, whose object carries `status` `conflicted` and one entry per open record producing its key: the skip event's identifier, the two heads, and, for each branch, the row the view's fold yields for that key without the events the record lists on the other branch and without the abandoned and continuing events every other open record lists (null when that fold leaves no row).

G. Every other default-view row SHALL hold `$integrity` with `status` `unconflicted`.

H. Every read of a default view, snapshots and live updates alike, SHALL deliver each row with its `$integrity` object, passing a conflicted row's branch rows, each on its own, through the consumer's row mapper, and never a conflicted row as a single state.

I. The library SHALL apply the conflict, closure and supersession rules to every default view, whatever the view's interest.

J. The transaction that stores a skip event, a reconciliation or a security finding SHALL, for every default view, either re-derive each row whose conflict status, set of folded recovered events or findings the stored event changes, or record a catch-up gap covering the events of that row's aggregate on the view.

K. The library SHALL refuse, naming the open conflict record, an append of an event whose `causal` records an eligible version or an eligible annotation, other than a reconciliation, on an aggregate that has an open conflict record at the appending database.

L. The library SHALL offer a reconciliation operation, both as a transaction of its own and inside a caller's transaction as the append operations are, taking an aggregate, an entry type, an event type, the aggregate's whole resolved state as the event's data, and an initiator, and appending the event stamped as a reconciliation.

M. The reconciliation operation SHALL refuse a request, before any write, when the aggregate has no open conflict record at the appending database, when the appending database is outside the succession lineage of the originating database of the skip event whose record is open, when the declared kind of the event type is not an eligible version, or when the entry type is not that of a head the reconciliation would name.

N. The library SHALL refuse, by name and before any write, an append whose data holds a top-level key beginning with `$` and the registration of a projection whose key, column or derived field name begins with `$`, and SHALL store no event, naming the reason, for a record whose data holds such a key that ingest, a recovery or a restore receives.

O. Every fold of an Aggregate-shape default view SHALL fold a reconciliation that closes a conflict record onto an empty row, so that the aggregate's row derives from that reconciliation and the events the fold admits after it.

P. Every library operation that decides from a default view's rows, the authorization policy's reads of the role-assignment and permission-grant views among them, SHALL, when a row it would decide from is conflicted, refuse with a typed refusal naming the aggregate and the skip event of its open conflict record, appending no event.

Q. Every default-view row SHALL hold in `$integrity` the key `security_findings`: the `finding_id`s, in ascending order, of the security findings the holder holds that name the row's aggregate or, for a Table-shape row, an aggregate whose event produces its key; an empty list when there is none.

### Rationale

**Why this conflict record (assertions A and B)?** The record must let every holder compute both branches' states from the events it holds. The heads name the versions a reconciliation must follow, and the event lists say which events belong to which branch, which a holder with a filtered view of the sender's events could not infer. The abandoned branch of a skip is what its restore's recoveries stored; an earlier restore's recovered events lie at or below a later restore's true fork and are shared history. The branch point can lie below the true fork, where a filter kept every proof of a later shared event from the sender, or be null, where nothing proved one (`EVS-DEV-resume-event/E`); a null branch point counts from position 0. The sender cannot tell an authored event above the branch point that its backup kept from one it appended after the restore, so it counts every such event as continuing, and it counts as abandoned an earlier restore's recovered events whose skip lies above the branch point, every one when the branch point is null. That can record a conflict the branches do not have, even for a sender that appended nothing after its restore, and leaves such a shared event out of the abandoned branch's state. It never hides a conflict: every event appended after the restore, and every event this restore's recoveries stored, lies above the true fork and so above the branch point, and the person reconciling sees both states. Events an earlier reconciliation superseded are not counted again. Reserved entry types are the library's own records: a fork in them is recorded by the skip itself, not a conflict a user resolves, and the recovered ones stay in the log as facts of the abandoned history, which the library's views of its own operational state do not fold as current (`EVS-PRD-destinations/S`).

**Why closed, superseded and open (assertion C)?** One restore can produce several skip events: each channel whose receiver is ahead resumes with its own, and a later one records at least what the earlier ones did, since its abandoned branch includes theirs. The latest record for an aggregate describes the conflict as the sender last knew it, and earlier ones are superseded, so an aggregate has at most one open record at a holder. Closure is decided by heads, not by which skip event the reconciliation names: a later record whose heads the reconciliation already follows then stays closed, while a later record with a new head, from an abandoned version brought back after the reconciliation, is open. Ordering by origin position within one database, and predecessor before successor, needs nothing but the events themselves, so every holder orders the records alike.

**Why a reserved key, and nothing to map (assertions E to H and N)?** A consumer's mapper handed a conflicted row would produce a resolved-looking value, so the conflicted row holds no state at the top level: only the identity both branches share and the `$integrity` object holding a row per branch. The `$` prefix is reserved, so an application key never collides with the stamp. The read maps each branch on its own and delivers the status with it. A Table row is conflicted when a branch event produces its key, and a key two records produce shows each record's branches against the shared history, not against the other's unresolved branch. The integrity status is distinct from a view's convergence state (`EVS-DEV-converging-view-reads`).

**Why these exclusions, and the empty row (assertions D and O)?** A closed record's branch events are superseded by the reconciliation, which the user wrote as the whole state. For the Aggregate shape, the reconciliation is folded onto an empty row, so the row after it is the resolved state and the events after it; the shape's merge convention then applies to later events as to any. For the Table shape, a row belongs to a key, not to an aggregate, and other aggregates' events produce keys too, so the fold keeps its per-event rule: the reconciliation produces its own key, and the keys that only superseded events produced fall away.

**Why regardless of interest (assertion I)?** Skip events are reserved events, which most views' interest excludes (`EVS-PRD-event-log/F`), but the conflicts they record govern rows of every view that folds the aggregate.

**Why re-derive, or record a catch-up gap, in the changing transaction (assertion J)?** Re-deriving an Aggregate-shape row reads one aggregate's events, which fits in the transaction; a Table-shape key may not. A catch-up gap hands the rows to view convergence (`EVS-DEV-view-convergence`), whose reads report them as pending (`EVS-DEV-converging-view-reads`) and whose transactions re-derive through the same fold step.

**Why refuse edits by naming the record (assertion K)?** The refusal tells the application which conflict stands in the way, so it can show the user both states and offer the reconciliation.

**Why these reconciliation refusals (assertions L and M)?** Each keeps a reconciliation from resolving something it cannot: without an open conflict there is nothing to resolve, and outside the writer's lineage the resolver would be a second editor. A reconciliation is an eligible version because later edits name it as parent, and it takes a head's entry type so a filter admitting the branches admits it too. The stamping rule sets its parents and `reconciles` (`EVS-DEV-causal-parents`). Which principal may reconcile is the application's permission question; the operation runs inside an action's transaction, so action dispatch can authorize it (`EVS-PRD-action-dispatch`). It is a public operation of the event store, not one of the library's reserved appends.

**Why a typed refusal on a conflicted row (assertion P)?** The refusal names what stands in the way, the aggregate and the skip whose record is open, so the application can route the user to the reconciliation. Appending no outcome event keeps the log from recording a decision, such as an authorization denial, taken from half of a conflicted state. The refusal is permanent until a reconciliation closes the record, unlike the transient refusal of a converging view.

**Why a list of findings beside the status (assertion Q)?** An aggregate can be conflicted and suspect at once, so the findings are a key of their own rather than a third status. The row carries the identities, so an application can show the evidence and, once clearing exists, which findings remain.

**Layer.** The records in the skip event, the reconciliation's causal record and the findings are Layer 1 facts. The rest are the library's default views' Layer 2 conventions: conflicted status, closure by heads, supersession, the reconciliation as whole state, and the finding mark. An application that wants another reading builds it on the same events.

### Changelog

- 2026-09-25 | dde3f2e5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | N: a received record whose data holds a top-level key beginning with $ is stored as no event, kept in a security finding, instead of refusing its delivery. No code or test references N
- 2026-09-25 | 4e375ad2 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | ba53815b | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | J: a stored security finding also re-derives the rows it changes or records a catch-up gap. Add Q: every row carries in `$integrity` the identities of the security findings that name its aggregate. No code or test references J or Q
- 2026-09-25 | ffeae945 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | A: `conflicted_aggregates` is null only when the skip records no abandoned head, and is computed when the branch point is null, counting from position 0; Rationale of A and B: a conflict is over-reported, never hidden, in every case. No code or test references A
- 2026-09-25 | c47c78d8 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | A, B and D: every authored event above the branch point counts as continuing; no lowest abandoned event, no ambiguous events and no `ambiguous_events`, and the continuing head is taken from the continuing events alone; Rationale of A and B states the over-report this allows. K: only an eligible version or annotation is refused on a conflicted aggregate, so drafts stay allowed. No code or test references A, B, D or K
- 2026-09-25 | 06201836 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of A: recovered reserved events stay in the log as facts and are not folded as current state by the library's views of its own operational state; Rationale shortened to the reason for each rule
- 2026-09-25 | 06201836 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Remove the retired B, D and E and re-letter: split A into A-B; C-C, F-D, G-E, H-F, I-G, J-H, K-I, L-J, M-K; split N into L-M; O-N, P-O. A: the abandoned-branch events of a skip are those its restore's recoveries stored (recovering skip at or above the branch point), so an earlier restore's recovered events make no conflict. Add P: a library decision from a conflicted row refuses, typed and naming the aggregate and the skip, appending no event. No code or test references any of these letters
- 2026-09-25 | 1a87b2e9 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Retire B, D and E: no possibly-incomplete list or mark. Add P: an Aggregate-shape fold derives a reconciled aggregate's row from the reconciliation alone. Amend C: any held reconciliation or later skip of the lineage decides; F: ambiguous events of a closed record are excluded; G: one open record per aggregate; I, K, N: no marks, whole resolved state; L: a catch-up gap
- 2026-09-25 | 0578049a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-O: the content of a skip event's conflict records over eligible versions, with ambiguous events, and its possibly-incomplete list over every recorded configuration; how a holder derives open, superseded and closed records and possibly-incomplete marks; the fold's exclusion of superseded and uncovered recovered events, keeping ambiguous ones; the conflicted and unconflicted row forms of the Aggregate and Table shapes, with the $integrity object; what a read delivers; rules applied whatever a view's interest; the transactions that re-derive rows or record a convergence gap for them; the refusal of edits on a conflicted aggregate; the reconciliation operation with its refusals; the reserved $ row namespace

*End* *Conflict records, conflicted rows and reconciliation* | **Hash**: dde3f2e5
