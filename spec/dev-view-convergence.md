# EVS-DEV-view-convergence: View copies and their catch-up

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the library keeps each registered view level with the log when the builds sharing a database define the view differently, or register it over a log that already holds events. The library stores a view's rows per definition. Builds whose definitions of a view agree share one copy and fold each event into it as they store it. A build whose definition is new or changed -- an added view, a changed interest or shape, a newer version of an entry type the view folds -- gets a copy of its own, which starts empty and catches up with the log after `EventStore.open` returns, in short transactions of its own, one at a time per copy whatever the number of instances. A copy records one watermark, the log position it has folded through, and a copy no live instance registers is deleted. What a read of a view returns while its copy catches up is stated in EVS-DEV-converging-view-reads.

Terms used below:

- The **fingerprint** of a view's definition is a digest of the view's name, its shape, the event types and derived fields its shape declares, the entry types, event types and aggregate types its interest names and whether its interest includes system events, the registered version of every entry type its interest names, or of every registered entry type when its interest names none, and the promoter chain the view registers for each of those entry types.
- A **copy** is the stored rows of a view under one fingerprint, with an identity of its own and a **watermark**: the log position through which the copy has folded every event its definition folds.
- An instance **registers** the fingerprint of each view it registers, and the copy of that fingerprint is the instance's copy of the view.
- A copy is **behind** in a transaction when its watermark is before the position of the latest event of the log that the transaction reads.
- A copy is **current** in a transaction when the log that the transaction reads holds no event past the copy's watermark that the copy's definition folds, and **converging** otherwise.
- The events a copy's definition **folds** include every security finding event, whatever the view's interest, which the copy folds for its outstanding-finding marks.
- A **catch-up transaction** is a transaction, other than the boot of `EventStore.open` and a transaction that stores events, that folds events into a copy or deletes rows of a copy marked for deletion. A **step** of it is the fold of one event, or the passing over of one event its copy's definition does not fold, or the deletion of up to 500 rows.
- A copy's **progress** is its watermark, the log's latest position, and the last failure of a catch-up transaction the instance ran on it.
- The **measured database** is a Postgres database whose log holds 20,000 events of 2,000 aggregates, ten each, all folded into one aggregate view of 2,000 rows. The **serving loop** is an instance appending on it continuously, alternating events outside the view's interest and events of the view's entry type into the view's aggregates. The **measured scenarios** are three: beside the serving loop, three further instances of one build of the same data-format major open together, each registering the view's entry type in an added view; beside the serving loop, a second instance of the same data-format major opens registering a newer minor of the view's entry type; and the instance running the serving loop is itself one that opened registering a newer minor of the view's entry type. A scenario's **window** is the 60 seconds from the moment the first converging instance begins to open; its **converging copy** is the copy of the definition the opening instances register and the serving loop's instance does not, or, in the third scenario, the serving loop's instance's copy of the view.

## Assertions

A. The library SHALL store the rows of each registered view in the copy of the fingerprint of the view's definition, and SHALL keep at most one copy of a fingerprint that is not marked for deletion.

B. For every view it registers whose fingerprint has no stored copy that is not marked for deletion, the boot of `EventStore.open` SHALL, in its boot transaction, create an empty copy whose watermark is the position before the first event of the log.

C. On a backend whose generation guard keeps live registrations, an instance SHALL register with that guard the fingerprint of each view it registers, from before its boot transaction until its event store closes.

D. The boot of `EventStore.open` SHALL, in its boot transaction, mark for deletion every stored copy whose fingerprint neither the opening build nor a live registration names.

E. A transaction that stores an event, other than the boot transaction of `EventStore.open`, SHALL, for each of the instance's copies that is current in that transaction, fold the event into the copy when the copy's definition folds it, and set the copy's watermark to the event's position.

F. A transaction that stores an event SHALL leave unchanged the watermark of every copy it does not set to the event's position.

G. The library SHALL begin no catch-up transaction before `EventStore.open` has returned.

H. The library SHALL begin no catch-up transaction after `EventStore.close` is called.

I. `EventStore.close` SHALL return only once the instance's catch-up transaction in flight, if any, has committed or rolled back.

J. While one of its copies is behind, an instance SHALL run catch-up transactions on it, each reading the log in order from the copy's watermark, folding the events the copy's definition folds, and setting the watermark to the position of the last event it read.

K. A catch-up transaction SHALL fold each event through the fold step the instance's appends use, under the instance's registered version of the event's entry type.

L. On a `PostgresBackend`, each catch-up transaction SHALL, as its first statement, lock the table holding the log's sequence counter in `SHARE` mode, and SHALL write nothing to that table.

M. Each catch-up transaction SHALL, after its first statement and before it reads its copy, take a lock on that copy without waiting and hold it until the transaction ends, and SHALL end without writing when another transaction holds that lock: on Postgres a transaction-scoped advisory lock, on the web a Web Lock, and on Sembast outside the browser an isolate-local lock per open database handle.

N. Each catch-up transaction SHALL begin no further step once it has run for 200 ms.

O. Each catch-up transaction SHALL perform at least one step.

P. <RETIRED> The library's own views are caught up before the consumer's.

Q. When a catch-up transaction throws, the library SHALL log the failure, record it with its error in the copy's progress, and retry that copy's catch-up after a delay that starts at 1 s and doubles with each consecutive failure up to 5 minutes, without closing the event store or delaying the catch-up of other copies.

R. <RETIRED> The retry of a failed catch-up is stated with its logging.

S. After `EventStore.open` returns, the library SHALL delete the rows and the record of every copy marked for deletion, in catch-up transactions.

T. An instance that finds no copy of a fingerprint it registers that is not marked for deletion SHALL create an empty one, as the boot does, before it next folds into or reads that view.

U. `rebuildView` SHALL, in one transaction, mark the instance's copy of the view for deletion and create an empty copy of the same fingerprint.

V. `rebuildView` SHALL return once the new copy is current for the instance, and SHALL throw a typed error naming the view and the copy's progress when a deadline the caller supplies passes first.

W. In each measured scenario, every append of the serving loop SHALL commit within 1 second of its call throughout the window.

X. In each measured scenario, the watermark of the converging copy SHALL reach, within the window, the latest position the log held when the window began.

Y. <RETIRED> The time to become current after the serving loop stops.

## Rationale

**Which layer?** Everything here serves a Layer 2 claim: that a view the library reports as current, or a row it reports as settled, is what a replay of the log produces under the library's default projection conventions and the registered definitions -- the closed-under-events claim, scoped to those conventions (EVS-DEV-converging-view-reads). Copies, their watermarks and their deletion marks are storage state kept beside the views (EVS-PRD-destinations/L), not facts of the log. A catch-up only reads the log and appends nothing to it.

**Why one copy per definition (assertions A and B)?** Builds that share a database -- a canary beside the serving revision, a rollback -- need not define a view alike. A copy per fingerprint lets each build fold exactly its own definition: builds that agree share a copy and keep it current as they store events, so an unchanged view costs a deploy nothing; a build whose definition differs folds into a copy no other build writes, so nothing one build folds is ever in a shape another cannot read. The promoter chains are part of the definition because they decide what an older event folds to: a release that adds a chain, or changes a default a step supplies, changes the replay without changing a version. A newer minor of an entry type the view folds is a new fingerprint, so promoting a view after a version change is an ordinary catch-up of a new copy, each event promoted before its fold (EVS-DEV-ingest-promotes-before-fold), and an older build of the same major keeps folding its own copy during the overlap. A view whose interest names no entry type includes every registered entry type's version, so a version change of any entry type it can match is a new copy. The fingerprint covers only what the library can read of a definition: two builds whose interest predicate, or whose table view's row-key or row-data function, differ under one fingerprint share a copy the library cannot tell apart; that precondition is stated with the equality a read reports, and closing it is on the roadmap (`spec/roadmap/projections.md`).

**Why one watermark, and why is "current" decided in each transaction against the copy's own definition (assertions E and F)?** The watermark is the whole of a copy's progress, written in the transaction that did the work, so it is exactly as durable as the work: a catch-up that rolls back leaves the watermark as the last commit left it, and whichever instance next runs a catch-up on the copy continues from there. A copy is current exactly when no event its definition folds lies past its watermark, so no stored flag can disagree with the log, and an event the copy does not fold never makes it converging: a canary's appends outside a serving copy's definition cost the serving revision nothing. An append folds inline only into a copy that is current, where folding one event keeps the copy a replay, and moves the watermark past any unfolded events before it; into a converging copy it folds nothing, and the catch-up folds the event in log order with those before it. A copy that is current but behind -- only events it does not fold lie past its watermark -- is moved to the log's head by the instance's next append or catch-up, so the events a currency check reads stay few. A build that does not register a copy leaves its watermark where it was; how a backend encodes a watermark at the log's head is the implementation's choice, provided the stored value never passes an event the copy has not folded. An append's own transaction stays the size of one fold per current copy, whatever the aggregate's history.

**Why does the boot fold nothing (assertion E)?** The boot transaction holds back every append (EVS-DEV-event-store-open) and reads and writes no view row, so the events it appends itself -- the library-version event and the registry audit -- are folded by catch-up like any other event past a copy's watermark. The boot moves no watermark, so a copy whose definition folds those events is converging until its first catch-up after the open, which is at most a few events.

**Security findings and the copies.** A security finding changes the outstanding-finding marks of rows it reaches, including rows of aggregates already folded (EVS-PRD-materializer), so every copy, whatever its interest, folds each finding event for its marks: inline in the transaction that stores it when the copy is current, and in log order during its catch-up otherwise. The fingerprint does not name this, because every build of the data format marks alike.

**Why catch up after the open, in short transactions (assertions G, J to O)?** The boot transaction holds back every append to the database while it runs (EVS-DEV-event-store-open). Folding a view inside it makes that pause grow with the log -- tens of seconds for a view of a few thousand rows over a log of tens of thousands of events -- while every serving append waits. The boot therefore only creates and marks copies, and catch-up runs after the open returns, in transactions bounded by elapsed time, so the wait an append sees is tied to the bound whatever the speed of the database; performing at least one step keeps a catch-up moving however slow the database is. A step is one event, so no single aggregate, however long its history, holds a catch-up transaction past the bound by more than one fold. The bounds are fixed library constants because a deployment that could raise them could also break the measured one-second bound.

**Why lock the sequence counter's table, in `SHARE` mode (assertion L)?** Every Postgres transaction of the library runs serializable. An append reads a copy's watermark to decide whether to fold inline, and a catch-up writes that watermark and reads the events after it, so without an order between them an append could lose serialization races to one catch-up after another until its retries ran out, or a catch-up could lose to the short appends on nearly every attempt. Locking the table first, before any statement fixes the snapshot, waits for the appends that have written the table and holds later appends' writes to it back until the catch-up commits: its snapshot includes every append committed before it, and an append whose snapshot predates it fails at most once and succeeds on its re-run (EVS-PRD-event-log). `SHARE` conflicts with the row writes every append makes to that table and with the boot's lock, and not with itself, so concurrent catch-up transactions -- of several instances, or of several copies -- hold it together, and an append waits for the ones in flight when it arrives, one time bound, whatever their number: Postgres queues a later catch-up's request behind the waiting append. A catch-up writes nothing to the table, since two catch-ups that each held `SHARE` and then wrote it would deadlock.

**Why a lock per copy, taken without waiting (assertion M)?** Several instances may register one copy -- every instance of a revision that added a view. They hold the table lock together, so each then tries the copy's lock, one of them does the catch-up transaction and the others end at once rather than repeat the same work: one catch-up transaction at a time per copy, whatever the number of instances. The lock is taken after the table lock because on Postgres taking it is a query, which fixes the snapshot. No guarantee rests on it: the database orders every catch-up against every writer of the watermark, so two catch-ups that overlap cost duplicated work, not wrong rows.

**Why keep the store open when a catch-up fails (assertion Q)?** A failure while folding -- a promoter that throws on a stored payload, a storage failure -- concerns one copy. The store stays open for everything else, the failure is logged and recorded in the copy's progress so an operator sees which view is stuck and why, the view stays converging so its unsettled rows are never served, and the retry backs off, since a failure the build itself causes repeats until the build changes.

**Why delete a copy no live instance registers (assertions C, D, S and T)?** A copy nobody folds only falls further behind, and a rollback that brings its build back finds it missing and creates a new one, which catches up as any new copy does. The boot decides under the generation guard's boot lock, so no instance boots meanwhile, and a live instance keeps its copies registered with the same guard that keeps its generation, so a canary's boot never deletes the serving revision's copy. Deletion is marked in the boot and done after the open, in bounded transactions, because deleting a large view in the boot would hold back appends as long as folding it would. On Sembast outside the browser the library relies on one opener of the database file, so the opening build's own views are the only live ones. A copy has an identity of its own, so rows of a copy being deleted never mix with a new copy of the same fingerprint, and an instance whose copy was marked while its registration was lost creates a new one (assertion T).

**Why does `rebuildView` replace the copy (assertions U and V)?** A rebuild folds the whole view again, and in one transaction it would hold back every append for as long as that takes. A new empty copy of the same fingerprint hands the work to the same bounded catch-up as every new copy, and the old copy's rows are deleted the same way. The caller's deadline turns a long catch-up into a typed error that names the view and how far it has come.

**What do the measured assertions state (assertions W and X)?** They are the bounds the library is built to: beside a serving instance, several instances of a revision that adds a view, a second instance that promotes one, and an instance that promotes a view while it serves, hold no serving append for more than a second -- which includes that no serving append fails after exhausting its retries -- and still fold the whole existing log into the new copy within the minute. The first scenario has several instances register one copy so that the test exercises the copy lock. In the first two scenarios the serving loop does not register the converging copy, so each of its appends of the view's entry type makes the copy converging again until the next catch-up folds it. They are measured on real Postgres because the bounds depend on how Postgres orders a catch-up transaction against an append, which no other backend reproduces. The measured database is the size at which folding the view inside a boot transaction holds appends back for tens of seconds.

**A fenced instance catches up nothing.** Every catch-up transaction is an ordinary transaction of the backend, so on Postgres it runs the generation fence first (EVS-DEV-version-compatibility), and an instance whose generation the record does not admit commits no catch-up.

## Changelog

- 2026-09-26 | 1f251ccb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | This requirement absorbs the deleted EVS-DEV-snapshot-promotion-on-open and EVS-DEV-view-target-versions-seeding. Code and tests still cite them: event_sourcing/lib/src/event_store.dart, projections/snapshot_promotion.dart, promoters/promoter_registry.dart and promoters/promoter_spec.dart; test/projections/snapshot_promotion_test.dart, test/promoters/promoter_spec_test.dart, test/storage/postgres/postgres_versions_test.dart and test/test_support/version_compatibility_conformance.dart. No assertion changes
- 2026-09-25 | 1f251ccb | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Terms: a copy is behind when its watermark is before the log's head and current when no event past it is one its definition folds, so an event outside a copy's definition never makes it converging; progress is the watermark, the log's head and the last failure; the first measured scenario opens three instances of one build. Amend E: the boot transaction folds nothing, its events are folded by catch-up. Amend J: catch-up runs while a copy is behind. Amend L: the catch-up takes the table lock in SHARE mode and writes nothing to the table, so concurrent catch-ups hold it together and an append waits one bound. Amend M: the copy lock follows the table lock. Merge R into Q; retire P, R and Y. No code or test references these assertions
- 2026-09-25 | ed87be70 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Terms: the events a copy folds include every security finding event, whatever its interest
- 2026-09-25 | ed87be70 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale: every copy folds each security finding for the outstanding-finding marks, whatever its interest
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Rewrite as view copies (all letters reassigned; no code or test references them): each view is stored per fingerprint of its definition (its declared parts, the registered entry-type versions and the promoter chains), shared by builds that agree and folded inline while current; a new or changed definition, a newer entry-type minor included, is a new copy with one watermark, caught up after the open in transactions bounded by 200 ms, ordered by the sequence counter's table lock on Postgres and taken one at a time per copy by a lock taken without waiting; a copy no live instance registers is deleted; rebuildView replaces the copy. Removes the convergence gaps, their positions, named and re-derived aggregates, the counts-for-instance rule and stored target raising. Absorbs the scheduling requirement (time bound, table lock, library views first, failure retry, close, measured 1 s bound); removes its convergence lease, pacing, hidden-page rule and progress observers
- 2026-09-25 | ed333a41 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Purpose and Rationale: a security finding also records catch-up gaps for the rows they change
- 2026-09-25 | ed333a41 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rewrite A-V (all letters reassigned; no code or test references them): progress is kept on the gap and ordered by the database, replacing rounds, tokens, round stamps and planned aggregates; a view has one catch-up gap and a pair one promotion gap, neither version-limited; a whole-log gap is converged by a scan of the log; a fold into a covered aggregate no longer re-derives it; rebuildView records one whole-log catch-up gap and writes no target; a skip event or a reconciliation records a catch-up gap
- 2026-09-25 | 823e933e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-Z: the boot records catch-up and promotion gaps; rounds after the open re-derive what the gaps cover and remove a gap only while its token is unchanged; a fold into a covered aggregate re-derives it; rebuildView and the whole-view pair record gaps

*End* *View copies and their catch-up* | **Hash**: 1f251ccb
