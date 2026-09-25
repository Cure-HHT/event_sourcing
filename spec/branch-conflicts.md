# Branch Conflicts

## Overview

A sender restored from a backup can append events before it learns it was restored. When it recovers the events it lost from a receiver, it holds two histories that have both moved past a shared point: the branch it abandoned, and the branch it continued on. The skip event that records the resume records that branch point (`EVS-DEV-delivery-resume`). An aggregate for which both branches wrote a version after the branch point has two states, and neither is the one that counts. This file specifies what the library records about such aggregates, how its default views present them, and how the user's decision closes them.

The library does not decide between the branches. It fixes the set of conflicting aggregates when the sender writes the skip event, which is when the sender holds both branches. It serves each conflicting aggregate as conflicted, with both states, and serves a single state again only after the application appends a reconciliation on the user's decision.

### Terms

- **Abandoned-branch event.** At the sender, an event it holds through a recovery whose originator entry names the sender.
- **Lowest abandoned event.** The abandoned-branch event with the lowest origin position.
- **Continuing event.** At the sender, an event it holds as authored, other than the skip event, at an origin position above the branch point when the lowest abandoned event's predecessor is null or held as authored, and otherwise at or above the lowest abandoned event's origin position.
- **Ambiguous event.** At the sender, when the lowest abandoned event's predecessor is neither null nor held as authored, an event it holds as authored at an origin position above the branch point and below the lowest abandoned event's. The sender cannot tell whether such an event precedes the backup (shared) or follows the restore (continuing).
- **Conflict record.** One entry of a skip event's `conflicted_aggregates`: an aggregate, the head of each branch, the events of each branch, and the ambiguous events of the aggregate.
- **Heads.** The two heads a conflict record names, each being the latest eligible version on its branch; the continuing head is taken from the ambiguous events when no continuing event is an eligible version.
- **Reconciliation.** An event whose causal record names, in `reconciles`, the skip events whose conflicts or marks it resolves.
- **Closed, superseded and open.** At a holder, a conflict record is closed when the holder holds a reconciliation of its aggregate, from the skip's succession lineage, whose parents include both heads the record names. Otherwise it is superseded when the holder holds a later skip event of the same lineage that records the same aggregate. Otherwise it is open.
- **Conflicted aggregate.** An aggregate with an open conflict record at the holder.
- **Possibly-incomplete mark.** A mark that the holder's state of an aggregate may lack events that fold into it, or may fold events it cannot place.

```text
aggregate X at a holder of skip S (branch point bp)

  shared:     x1 <- x2                  (at or below bp)
  abandoned:          x2 <- a3 <- a4     (S: abandoned_events for X)
  continuing:         x2 <- c3           (S: continuing_events for X)

  row of X while open:   $integrity.status = conflicted
                         branch abandoned  = fold(x1, x2, a3, a4)
                         branch continuing = fold(x1, x2, c3)
  reconciliation r:      parents {a4, c3}, reconciles [S]
  row of X after r:      unconflicted = fold(x1, x2, r)   (a3, a4, c3 superseded)

  an ambiguous event y between bp and the lowest abandoned event
  folds into both branch states, is never superseded, and marks X
  possibly incomplete until r
```

### Reading order

`EVS-PRD-branch-conflicts` states what the library records and what its default views guarantee. `EVS-DEV-branch-conflicts` fixes the conflict record's content, how a holder derives the status of each record, the conflicted and possibly-incomplete row forms for the Aggregate and Table shapes, what a read returns, the transactions that change them, and the reconciliation operation. The causal record a reconciliation carries, and the stamping rule behind heads, are specified with causal parents (`spec/causal-history.md`).

## EVS-PRD-branch-conflicts: Branch conflicts and their reconciliation

**Level**: PRD | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-delivery-channel, EVS-PRD-materializer

### Purpose

When a sender's origin chain forks at a recorded branch point, the library fixes, in the skip event, every aggregate for which both branches wrote a version. Its default views then serve such an aggregate as conflicted with both branches' states until the user's decision, appended through the library's reconciliation operation, closes the conflict. Every holder of the skip event derives the same conflicts, and every holder of the reconciliation closes them. Aggregates whose recovered or delivered history may be partial, or holds events the sender cannot place on a branch, are served with a possibly-incomplete mark.

### Assertions

A. When the library appends a skip event that records a branch point, it SHALL record in that event each aggregate the sender holds for which an abandoned-branch event and a continuing or ambiguous event are eligible versions, with the head and the events of each branch and the ambiguous events.

B. The library's default views SHALL TREAT an aggregate as conflicted exactly while a conflict record for it that the holder holds is open, deriving each record's status solely from the skip events, reconciliations and succession events the holder holds.

C. For a conflicted aggregate, the library's default views SHALL serve no single state and SHALL expose the state each branch yields.

D. Every read of a default view SHALL deliver each row that a conflict makes conflicted as conflicted, with each branch's state, and never as a single state.

E. The library SHALL refuse to append, on an aggregate conflicted at the appending database, a version or an annotation that is not a reconciliation.

F. The library SHALL append a reconciliation only on a consumer's request through its reconciliation operation, which names the aggregate and carries the resolved state, and SHALL append none of its own accord.

G. The library SHALL refuse a reconciliation requested by a database outside the succession lineage of the database whose skip event records the conflict or mark it resolves.

H. The library's default views SHALL TREAT a reconciliation as closing each conflict record of its aggregate whose heads are both among its parents, and SHALL TREAT the events that each closed record lists on either branch as superseded, folding none of them, while folding the record's ambiguous events.

I. The library's default views SHALL TREAT an aggregate as possibly incomplete while an uncleared possibly-incomplete mark holds for it, and SHALL serve its state marked possibly incomplete.

J. The library's default views SHALL fold no event a database recovered as its own until a skip event that database authored records it, and SHALL NOT serve, as a row of the view, a row whose conflict or mark status a stored skip event or reconciliation changed, until the view has re-derived that row under these rules.

K. The rows of every default view SHALL equal the rows a rebuild of that view from the log derives, conflicts, closures and marks included.

L. Every conflict record the library records SHALL name two heads that are eligible versions authored within the succession lineage of the skip event's originating database, so that its lineage can always reconcile it.

### Rationale

**Why fix the conflict set in the skip event (assertions A and B)?** The sender holds both branches only after the recovery that brings the abandoned one back, and a receiver may never hold both. The sender therefore computes the conflicts in the transaction that appends the skip event, and records them with the event and under its hash. The skip event reaches every receiver of the sender's events (`EVS-PRD-delivery-channel/V`), so every holder derives the same conflicts from the same records, whatever each one's channel filtered. A receiver that derived conflicts from the events it happened to hold would find different conflicts, or none. Detection is per aggregate, because an aggregate is the unit a version replaces. Only versions count: an annotation or a draft on one branch does not replace the aggregate's state, so it creates no conflict, and an aggregate a database only annotates never becomes one it cannot reconcile. A semantic overlap between two aggregates is outside what the library can detect from its record, and is the application's concern. The recorded set is a Layer 1 fact: what the sender held when it wrote the skip. Treating the aggregate as conflicted is the default views' Layer 2 interpretation of that fact. The abandoned-branch events the set is computed from were served by the recovery's receiver, which is trusted not to fabricate events under the sender's identity (`EVS-PRD-delivery-channel`, Trust), so the set inherits that trust.

**Why serve both states and no single one (assertions C and D)?** Picking a branch, or the last writer, would decide on the user's behalf which of two edits of the same entry counts, and the log would record nothing of the choice. A last-writer fold is also wrong in a way nobody sees: in the sender's own log, the recovered branch is stored after the branch the sender continued on, so a last-writer fold would silently put the abandoned state back over the edits the user made after the restore. The default views therefore present the conflict as a conflict. A read never passes a conflicted row to the consumer's row mapper as if it were resolved, so an application that reads the view without checking the status cannot show a conflicted entry as resolved.

**Why refuse edits of a conflicted aggregate (assertion E)?** A new version would have to name one head (and silently extend one branch) or both (and silently reconcile). Either would settle the conflict without the user deciding. Refusing the edit forces the decision first. Drafts and other ineligible events stay possible, since they are never a parent: they record the branches they were written against, and the user can compose the resolution in them.

**Why only on the user's request, and only by the writer (assertions F, G and L)?** A reconciliation settles which state of an entry counts, and the library cannot tell which state the user meant. It appends a reconciliation only when the application asks, naming the aggregate and carrying the resolved state the user chose. The resolution is an edit of the aggregate, so it belongs to the aggregate's writer: the sender whose history forked, or its successor. Both heads are versions that writer authored, so every recorded conflict is one that writer's lineage can reconcile. A receiver that reconciled a sender's aggregate would become a second editor of it, which the single-source-per-aggregate invariant does not admit. Receivers close the conflict when the sender's reconciliation reaches them.

**Why do the branches' events stop folding once reconciled, but ambiguous events keep folding (assertion H)?** The reconciliation is a version whose parents are both heads. It follows both branches, and the state it carries replaces what either branch said since the branch point. The events of both branches stay in the log and in every event subscription. The default views stop folding them because the reconciliation supersedes them. An ambiguous event may be one both branches share, from before the backup; excluding it would drop keys only it set, with nothing in the log saying they were meant to go. It is therefore folded into both branch states while the conflict is open, and before the reconciliation after it closes. The reconciliation is an event of the aggregate's own entry type, so every view that folds the aggregate folds it, and the conflict closes on every holder that receives it. A holder whose channel filter excludes that event keeps the conflict open, and says so.

**Why mark possibly incomplete (assertion I)?** A sender that recovers an aggregate through a destination whose filter might have left out events that fold into it cannot know whether its recovered state is whole. An aggregate with ambiguous events holds events the sender cannot place on either branch. A receiver that holds an aggregate and then receives an edit whose parent its channel withheld holds a state built without that parent. Each serves the state, since it is the best the holder has, but marks it, so an application can show that the entry may be missing changes. A withheld parent before the receiver's first event of the aggregate is a horizon, not a loss: a start date, a destination registered late, or a successor's first edit of its predecessor's entry. The receiver holds no state that the missing parent would have changed, so it sets no mark.

**Why hold recovered events out of the folds until the skip (assertion J)?** In the sender's recovery, the recovered branch is stored delivery by delivery, before the skip event that fixes the conflicts is appended, and a recovery can stop part way. A row that folded recovered events in between would be the last-writer mix that assertion C excludes. The views therefore serve the state they served before the recovery until a skip records what was recovered: the skip that resumes the channel, or the one the library appends when it stops the channel (`EVS-DEV-delivery-resume/H`), so every recovery that stored anything ends in a recorded state. The skip event and a reconciliation each change which events a row folds, so each leaves the rows it affects pending until they have been re-derived.

**Why rebuild equality (assertion K)?** Conflicts, closures and marks are functions of the log, so a view rebuilt from the log must show them exactly as the incremental fold does. This is the materializer's determinism (`EVS-PRD-materializer/B`), extended to the rows these rules produce.

**Relation to canonicalization.** Canonicalization decides between events from different authorities (`EVS-PRD-multi-source-canonicalization`). A branch conflict is between two histories of one authority's writer, which a restore split. The two do not interact while the single-source invariant holds. The vocabulary of branches, heads and reconciliation is the one the roadmap's structural conflict detection builds on (`spec/roadmap/multi-source-editing.md`).

### Changelog

- 2026-09-25 | f6d1a7c2 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-L: conflicts fixed in the skip event over eligible versions, conflicted aggregates served with both branches' states and never as a single state, edits of a conflicted aggregate refused, reconciliation only on the consumer's request and only by the writer's succession lineage, closure and supersession by a reconciliation with ambiguous events kept, possibly-incomplete marks, recovered events held out of the folds until a skip records them and rows not served while their status changes, rebuild equality, and heads always reconcilable by the writer's lineage

*End* *Branch conflicts and their reconciliation* | **Hash**: f6d1a7c2

## EVS-DEV-branch-conflicts: Conflict records, conflicted rows and reconciliation

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-branch-conflicts

### Purpose

This requirement fixes the content of the skip event's `conflicted_aggregates` and `possibly_incomplete_aggregates`, and how a holder derives the status of each conflict record and each mark. It fixes the stored and read form of a conflicted, possibly-incomplete or unconflicted row for the Aggregate and Table shapes, the reserved row namespace, the fold's exclusions, the transactions that re-derive rows, the refusal of edits on a conflicted aggregate, and the reconciliation operation.

### Assertions

A. A skip event's `conflicted_aggregates` SHALL be null when its `branch_point` is null. Otherwise it SHALL hold, in ascending order of aggregate identifier, one object for each aggregate of an entry type other than a reserved one for which a counted abandoned-branch event is an eligible version and a counted continuing or ambiguous event is an eligible version, counting no event that a closed conflict record at the sender lists on either branch. Each object SHALL carry exactly `aggregate_id`, `abandoned_head`, `continuing_head`, `abandoned_events`, `continuing_events` and `ambiguous_events`. The abandoned head SHALL be the latest eligible version by origin position among the counted abandoned-branch events of the aggregate; the continuing head SHALL be the latest eligible version by origin position among its counted continuing events, or, when none is an eligible version, among its ambiguous events; each head SHALL be an object with exactly `event_id` and `event_hash`. Each events list SHALL hold the identifiers of the counted events of the aggregate of that kind, whatever their kind and eligibility, in ascending order of origin position.

B. A skip event's `possibly_incomplete_aggregates` SHALL hold, in ascending order, the identifiers of the aggregates of abandoned-branch events the sender holds whose aggregate type is not provably admitted, for every entry type the registered projections fold for it, by every configuration of the destination registration they were recovered through that the sender's log records and by the one the drainer declares; a filter with a predicate admits none provably, and a registration for which the log records no configuration admits none provably.

C. A holder SHALL treat a conflict record of skip event S for aggregate X as closed when it holds a reconciliation of X whose originating database is in the succession lineage of S's originating database and whose parents include both heads the record names. Otherwise it SHALL treat the record as superseded when it holds a later skip event of that lineage that records X, ordering skip events by origin position within one database, placing every event of a predecessor before every event of its successor, and ordering equal positions by event identifier. Otherwise it SHALL treat the record as open.

D. The library's default views SHALL TREAT an aggregate as possibly incomplete at every holder in the succession lineage of S's originating database while it is listed in the `possibly_incomplete_aggregates` of a skip event S, until that holder holds a reconciliation of the aggregate whose `reconciles` names S; and at every holder while a conflict record naming it with non-empty `ambiguous_events` is open.

E. The library's default views SHALL TREAT an aggregate as possibly incomplete at a receiver from the first event of it that is delivered with a withheld parent, is not a reconciliation, follows in the receiver's log an event of the same aggregate, and names a parent the receiver did not hold when it stored the event.

F. Every fold of every default view SHALL exclude the events that a closed conflict record lists as abandoned or continuing, SHALL NOT exclude the events it lists as ambiguous, SHALL exclude at the recovering database every uncovered recovered event, and SHALL fold a reconciliation at its position in the log as it folds any event of its entry type.

G. While an aggregate has an open conflict record, its Aggregate-shape row SHALL hold only `aggregateId`, `aggregateType` and the reserved key `$integrity`, whose object carries `status` `conflicted` and one entry per open record: the skip event's identifier, the two heads, and, for each branch, the row the view's fold yields over the aggregate's events in log order without the events the record lists on the other branch and without the abandoned and continuing events every other open record of the aggregate lists (null when that fold leaves no row). Every other key the fold stamps (the entry type, the latest event identifier, timestamps, sequence and derived fields) SHALL appear only inside those branch rows.

H. A Table-shape row whose key an event listed as abandoned or continuing by an open conflict record produces SHALL hold only its key columns and `$integrity`, whose object carries `status` `conflicted` and one entry per open record producing its key: the skip event's identifier, the two heads, and, for each branch, the row the view's fold yields for that key without the events the record lists on the other branch and without the abandoned and continuing events every other open record lists (null when that fold leaves no row).

I. Every other default-view row SHALL hold `$integrity` with `status` `unconflicted`, and every row SHALL carry in `$integrity` `possibly_incomplete`, true exactly when the view's fold for that row folds an event of an aggregate the view treats as possibly incomplete.

J. Every read of a default view, snapshots and live updates alike, SHALL deliver each row with its `$integrity` object. It SHALL pass a conflicted row's branch rows, each on its own, through the consumer's row mapper, and SHALL NOT pass a conflicted row to the mapper as a single state.

K. The library SHALL apply the conflict, closure, supersession and mark rules to every default view, whatever the view's interest.

L. The transaction that stores a skip event or a reconciliation SHALL, for every default view, either re-derive each row whose conflict or mark status, or whose set of folded recovered events, the stored event changes, or record a convergence gap naming that row's aggregate on the view, under which every read reports the row as pending until the view's convergence has re-derived it.

M. The library SHALL refuse, naming the open conflict record, an append of an event whose `causal` records a version or an annotation, other than a reconciliation, on an aggregate that has an open conflict record at the appending database.

N. The library SHALL offer a reconciliation operation, both as a transaction of its own and inside a caller's transaction as the append operations are, taking an aggregate, an entry type, an event type, the resolved state as the event's data, and an initiator. It SHALL refuse a request in each of these cases: the aggregate has neither an open conflict record nor an uncleared possibly-incomplete mark from a skip event at the appending database; the appending database is outside the succession lineage of that skip event's originating database; the declared kind of the event type is not an eligible version; or the entry type is not that of a head the reconciliation would name. Otherwise it SHALL append the event, stamped as a reconciliation, in one transaction with the row changes it causes.

O. The library SHALL reserve for itself every top-level row key beginning with `$`, and SHALL refuse, by name and before any write, an append or an ingested event whose data holds such a top-level key, and the registration of a projection whose key, column or derived field name begins with `$`.

### Rationale

**Why this conflict record (assertion A)?** The record must let every holder compute both branches' states from the events it holds, without holding the sender's log. The heads name the versions a reconciliation must follow, and both are eligible versions the sender authored, so they are never missing. The event lists say which events belong to which branch, which a holder with the sender's events cut by a filter could not infer; they list every event of the branch, annotations and drafts included, so each branch state is the state that branch left. When the lowest abandoned event's predecessor is held, the branch point is exact and every authored event above it is continuing. When it is not, the sender knows only that the true fork lies between the recorded branch point and the lowest abandoned event (`EVS-DEV-delivery-resume/M`): an authored event there can be shared or continuing, and the sender cannot tell which. Such events are listed as ambiguous rather than assigned. An ambiguous eligible version may still make a conflict, which can over-report and never hides one. Events that an earlier reconciliation superseded are not counted again, so a later skip event of the same restore (from another channel's recovery) reopens an aggregate only when it brings back an abandoned version the user has not yet reconciled. Reserved entry types are the library's own records. A fork in them is recorded by the skip event itself and is not a conflict a user resolves.

**Why this possibly-incomplete rule (assertion B)?** A filter that provably admits every entry type the registered projections fold for an aggregate type carries every event those views need. The recovered deliveries were filled under whatever configuration was in effect when each was sent, not under the one in effect at the recovery, so every configuration the log records for the registration must admit provably; a filter with a predicate cannot be decided, and a registration with no recorded configuration cannot be checked, so each marks the aggregate. A configuration changed without any record in the log is the delivery configuration the library trusts on faith. The mark applies to the sender and its succession lineage (assertion D), because only their state came back through the filter. A receiver's state of the aggregate was built from its own deliveries, and a receiver marks by the withheld-parent rule instead (assertion E). An aggregate with ambiguous events is marked at every holder, since every holder folds the events the sender could not place.

**Why closed, superseded and open (assertion C)?** One restore can produce several skip events: each channel resumes when its receiver answers, and a later one records at least what the earlier ones did (`EVS-DEV-delivery-resume/L`). The latest record for an aggregate describes the conflict as the sender last knew it, and earlier ones are superseded. Closure is decided by heads, not by which skip event the reconciliation names. A later record whose heads the reconciliation already follows then stays closed, while a later record with a new head, from an abandoned version brought back after the reconciliation, opens again. Ordering by origin position within one database, and predecessor before successor, needs nothing but the events themselves, so every holder orders the records alike.

**Why is a receiver's mark permanent (assertion E)?** The default views fold an aggregate's events in the order the receiver's log holds them, and the Aggregate shape folds deltas. A missing version leaves its keys wrong, and if the withheld parent arrives later, on another channel, the fold applies its delta after its child's, which is wrong again. No later event can say which keys those are, so the mark stays for as long as the row is built from that history. It is the signal that the channel's filter gives this receiver a partial history of the aggregate. The remedy is a filter that carries every eligible version of the aggregate, or none of them. A reconciliation is exempt: it supersedes both branches, so a head the receiver lacks leaves nothing wrong.

**Why a reserved key, and nothing to map (assertions G to J and O)?** A consumer's mapper turns a row into the application's type. Handed a conflicted row, it would produce a resolved-looking value from whichever state the row held. The conflicted row therefore holds no state at the top level: only the aggregate's identity and type, which both branches share, and the `$integrity` object, which holds a row per branch. The fold stamps library keys, such as the entry type and the latest event identifier, beside the event's data keys; those differ between branches, so they live in the branch rows. The `$` prefix is reserved, and event data and projection names using it are refused, so an application data key can never collide with the stamp or be mistaken for it. The read maps each branch on its own and delivers the status with it. A consumer that reads rows through the storage backend, rather than a read, finds no state keys to mistake for a resolved one. Table rows are keyed by extracted key, not by aggregate, so a Table row is conflicted when a branch event produces its key. It carries, for that key, the row each branch's fold leaves. A key that two open records produce shows each record's branches with the other record's branch events set aside, so each conflict is shown against the shared history and not against the other's unresolved branch. The integrity status is distinct from a view's convergence state (`EVS-DEV-converging-view-reads`): a row can be settled and conflicted, and a read reports both.

**Why these exclusions after closure (assertion F)?** The reconciliation's data is the resolved state, and it is folded onto the shared history before the branch point and the ambiguous events. For the Aggregate shape, a key the resolved state omits therefore keeps its value from before the branch point, and a key it sets to null is cleared, as the shape's merge convention does for every event. An application that composes the resolution from one branch's state carries that state's keys. For the Table shape, the reconciliation event produces its own key, and the keys that only branch events produced fall away. An application that keeps them appends ordinary versions after the reconciliation. An uncovered recovered event is excluded until a skip records it (`EVS-PRD-branch-conflicts/J`).

**Why regardless of interest (assertion K)?** Skip events are reserved events, which most views' interest excludes (`EVS-PRD-event-log/F`), but the conflicts they record govern rows of every view that folds the aggregate.

**Why re-derive, or record a convergence gap, in the changing transaction (assertion L)?** Re-deriving an Aggregate-shape row reads one aggregate's events, which fits in the transaction. Re-deriving a Table-shape key may need every event producing that key, which does not always fit. Recording a convergence gap hands the row to the library's view convergence (`EVS-DEV-view-convergence`), whose reads report such a row as pending rather than serving it (`EVS-DEV-converging-view-reads`). A recovered own event changes no row when it is stored, because the folds exclude it until the skip records it; the skip's transaction then brings those events in together with the conflict records that decide how they fold.

**Why refuse edits by naming the record (assertion M)?** The refusal tells the application which conflict stands in the way, so it can show the user both states and offer the reconciliation.

**Why these reconciliation refusals (assertion N)?** Each refusal keeps a reconciliation from resolving something it cannot. Without an open conflict or a mark there is nothing to resolve. Outside the writer's lineage, the resolver would be a second editor. A reconciliation must be an eligible version, because it becomes the parent later edits name. The entry type is that of a head, so that a filter admitting the branches' entry type admits the reconciliation too, and the conflict closes at every receiver that holds it. The stamping rule sets the reconciliation's parents and `reconciles` itself (`EVS-DEV-causal-parents/F`), so the application names only what it resolves and what the resolution is. Which principal may reconcile is the application's permission question, as for any other edit. The operation runs inside an action's transaction, so an application that routes reconciliations through action dispatch has its authorization policy decide it (`EVS-PRD-action-dispatch`), and the initiator is recorded on the event. The reconciliation operation is a public operation of the event store, like its append operations, not one of the library's reserved appends.

**Layer.** The records in the skip event, the reconciliation's causal record, and the withheld-parent record in each delivery are Layer 1 facts. The rest are the library's default views' Layer 2 conventions: conflicted and possibly-incomplete status, closure by heads, supersession, keeping ambiguous events, and the fold onto the shared history. An application that wants another reading builds it on the same events.

### Changelog

- 2026-09-25 | 0578049a | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-O: the content of a skip event's conflict records over eligible versions, with ambiguous events, and its possibly-incomplete list over every recorded configuration; how a holder derives open, superseded and closed records and possibly-incomplete marks; the fold's exclusion of superseded and uncovered recovered events, keeping ambiguous ones; the conflicted and unconflicted row forms of the Aggregate and Table shapes, with the $integrity object; what a read delivers; rules applied whatever a view's interest; the transactions that re-derive rows or record a convergence gap for them; the refusal of edits on a conflicted aggregate; the reconciliation operation with its refusals; the reserved $ row namespace

*End* *Conflict records, conflicted rows and reconciliation* | **Hash**: 0578049a
