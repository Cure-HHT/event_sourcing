# EVS-DEV-view-convergence: View convergence after EventStore.open

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the library brings a registered view level with the log when its stored rows are not what a replay of the log produces under the registered definitions: events the view has not folded (a build that does not register the view stored them, or the view was registered over a log that already held them), a stored target version below the registered version of an entry type (snapshot promotion), rows a build of an older minor folded, or a requested rebuild. The library records each such difference, in the transaction that creates it, as a convergence gap of the (view, entry type) pair, and re-derives the rows the gap covers from the log after `EventStore.open` returns, while the store serves, in transactions of their own. It removes a gap only in a transaction that reads the gap unchanged since a round took it. How those transactions are sized, locked, paced, resumed and reported is stated in EVS-DEV-view-convergence-scheduling; what a read of a view returns while it converges, in EVS-DEV-converging-view-reads.

Terms used below:

- A **pair** is a view and an entry type its interest names. A view whose interest names no entry type has one **whole-view pair**, which carries no version.
- A pair carries at most one gap of each of two kinds. A **catch-up gap** records events the view has not folded; it carries no version. A **promotion gap** records rows to be re-derived at a newer version of the pair's entry type; it carries a **round version**, the version the rows are to be re-derived at, a **prior version**, the stored target the promotion started from, and whether it is **version-limited** to aggregates holding an event below its round version. A whole-view pair carries only catch-up gaps.
- Every gap carries a **token**; a **from position**, the lowest log position whose events a table view refolds; whether it covers the **whole log**; its **named aggregates**; and its **re-derived aggregates**, those re-derived since the gap last covered them.
- A gap **counts** for an instance when the instance registers the gap's pair and, for a promotion gap, its registered version of the pair's entry type is at least the gap's round version.
- A **round** is one pass of one instance over one view: it takes gaps of the view, plans, re-derives, and completes. It holds, for the gaps it took, their tokens, from positions, coverage, named aggregates and re-derived aggregates, and, once it has planned, its **planned aggregates**. Each of its transactions writes a fresh **round stamp**.
- A gap **covers** an aggregate when it does not hold the aggregate as re-derived and either names the aggregate or covers the whole log. A round keeps its coverage per gap it took: it **covers an aggregate for a gap** it took when it has planned the aggregate for that gap and has not re-derived it, or has not finished planning and the gap, as the round took it, covered the aggregate and the round has not re-derived it since. A round's coverage for a gap counts for an instance when that gap counts for it.

## Assertions

A. For every registered pair whose stored target version the boot of `EventStore.open` finds below the registered version of its entry type, the boot SHALL write the registered version as the pair's stored target in its boot transaction.

B. For every registered pair whose stored target version the boot finds below the registered version of its entry type, and that carries no promotion gap whose round version is the registered version, the boot SHALL, in its boot transaction, record a version-limited promotion gap covering the whole log, with the registered version as its round version and the stored target it found as its prior version.

C. For every pair the boot seeds while the log holds an event appended before the boot, the boot SHALL, in its boot transaction, record a catch-up gap covering the whole log.

D. A build that stores an event while registering no view of a whole-view pair's name SHALL record, in the storing transaction, a catch-up gap covering the event for that whole-view pair.

E. Every transaction that records a gap SHALL give the gap a token that gap has not held before.

F. A recording that does not cover the whole log SHALL set the gap's from position to the log position of the earliest event it covers and SHALL name the aggregate of every event it covers; a recording that covers the whole log SHALL set the from position to the first position of the log and name no aggregate.

G. A recording SHALL remove from the gap's re-derived aggregates every aggregate it names, and a recording that covers the whole log SHALL remove them all.

H. A recording on a pair that already carries a gap of the same kind SHALL leave the pair one gap of that kind, with the lower of the two from positions, the named aggregates of both, whole-log coverage when either covers the whole log, and, for a promotion gap, the higher of the two round versions, the prior version of the gap already present, and version limitation only when both are version-limited.

I. A round SHALL begin, in one transaction, by taking every gap of the view that counts for the instance and that no round holds: recording as the round's each gap's token, from position, coverage, named aggregates and re-derived aggregates, and leaving each gap in place with its token and, for a promotion gap, its round version and version limitation, and with no from position, coverage, named aggregates or re-derived aggregates.

J. An instance that finds a round in progress on a view SHALL resume it when every gap the round holds counts for the instance, and otherwise SHALL, in one transaction, end the round and record on each gap it took a recording of the coverage, from position and named aggregates the round had not yet completed.

K. Each transaction of a round SHALL write only when it reads the round stamp that the round's previous transaction, in this instance, wrote or that this instance read when it resumed the round.

L. A round SHALL remove a gap only in a transaction that reads the token the round took for that gap.

M. The transaction in which a round removes a promotion gap SHALL write the converging instance's registered version of the pair's entry type as the pair's stored target.

N. When the transaction that would complete a round reads, for a gap it took, a token other than the one it took, the round SHALL leave that gap in place and remove only the gaps whose tokens are unchanged.

O. The library SHALL begin a round only after `EventStore.open` has returned.

P. An instance SHALL run a round only over gaps that count for it.

Q. A round SHALL re-derive every row through the fold step the instance's own folds use, folding each entry type under the version the instance's folds use for it.

R. Every write of a view row by a round's re-derivation or by a fold's re-derivation of an aggregate SHALL count, for each event it folds, as a fold of that event for EVS-DEV-version-compatibility/E and M.

S. When a build folds an event into an aggregate view and a gap of the view that counts for the build, or a round's coverage for such a gap, covers the event's aggregate, the fold SHALL, in its transaction, re-derive the aggregate's row from the log, the event included, and record the aggregate as re-derived on every such gap and round.

T. A round SHALL NOT re-derive an aggregate it no longer covers.

U. A round over an aggregate view SHALL plan, for each gap it took that covers the whole log, every aggregate holding an event that the view's interest matches, of the pair's entry type (of any entry type, for a whole-view pair) and, for a version-limited gap, below the gap's round version, and SHALL re-derive the row of every aggregate it covers.

V. A round over a table view that took a gap covering the whole log SHALL delete the view's rows and then fold, in log order, every event the view's interest matches.

W. A round over a table view whose gaps do not cover the whole log SHALL fold, in log order, every event the view's interest matches from the lowest from position of its gaps.

X. A round over a table view SHALL complete only in a transaction that has folded every event the round folds up to the latest event of the log that transaction reads.

Y. `rebuildView` SHALL, in one transaction that re-derives no row, write the registered version as the stored target of every versioned pair of the view and record for it a promotion gap covering the whole log, not version-limited, with the registered version as its round version and the stored target it replaced as its prior version, and record for a whole-view pair a catch-up gap covering the whole log.

Z. `rebuildView` SHALL return once the view is current for the instance, and SHALL throw a typed error naming the view and its latest convergence progress when a deadline the caller supplies passes first.

## Rationale

**Why do views converge after the open rather than in the boot?** The boot transaction holds back every append to the database while it runs: on Postgres its first statement locks the table holding the sequence counter, and on the web it holds the database's write lock exclusively (EVS-DEV-event-store-open). Re-deriving a view inside it makes that pause grow with the rows promoted and the events re-derived -- tens of seconds for a view of a few thousand rows over a log of tens of thousands of events -- while every serving append waits, holding a connection, and a platform that sees no sign of life may kill the boot and start the pause again. The boot therefore decides and records, and re-derives nothing (assertions A to C, and EVS-DEV-event-store-open/N and O). The re-derivation runs after the open returns (assertion O), and a view whose rows are not yet re-derived is reported as converging rather than served as current (EVS-DEV-converging-view-reads).

**Which layer?** Everything here serves a Layer 2 claim: that a view the library reports as current, or a row it reports as settled, is what a replay of the log produces under the library's default projection conventions and the registered definitions -- the closed-under-events claim, scoped to those conventions. Gaps, tokens and rounds are storage state kept beside the views (EVS-PRD-destinations/L), not facts of the log; convergence only reads the log, and appends nothing to it but a completed promotion's audit event.

**Why two kinds of gap?** A gap recording missing events and a gap recording a newer version concern different builds. Every build that registers the pair lacks the missing events, so a catch-up gap makes the view converge for each of them and any of them may converge it; a promotion gap concerns only builds at or above its round version, since a build of an older minor reads rows of its major at any minor (EVS-DEV-version-compatibility). Merging the two would give the merged gap one round version, which either hides missing events from an older build that then serves the view as current and may not converge it, or has an older build converge a promotion it cannot perform. Kept apart, an older build converges the catch-up gap under its own minor, which counts as a fold under that minor (assertion R), so it lowers the target and records the rows it re-derived on the promotion gap, and a newer build promotes them again.

**Why a gap with a token, and why clear-if-unchanged (assertions E, I, L and N)?** Re-derivation runs beside transactions that keep creating differences: a build that does not register the view stores events of its entry types, a build of an older minor folds rows under its version, another instance boots, a caller requests a rebuild. A mark that a round cleared at its end would lose a difference created while the round ran. Every recording therefore gives the gap a token it has not held before and adds what it covers; a round takes the token and what the gap recorded, leaving the gap in place and empty, and removes the gap only where the token is still the one it took. A difference created mid-round leaves a new token and its own coverage, so the round leaves the gap in place and the next round covers only what was recorded since, which keeps a steady trickle of differences from restarting the whole re-derivation. The token is not the log head: a boot can record gaps without appending an event, so the head need not move between two recordings. The merge rule (assertion H) keeps everything either recording requires, so a merge never shrinks the work: the higher round version, because rows must reach the newest minor a recording asked for, and version limitation only when both recordings were limited, because a limited recording would otherwise hide an unlimited one's rows.

**Why re-derived aggregates (assertions G, S and T)?** A gap covering the whole log names no aggregate, so without a record of what has been re-derived since, a fold that re-derives one aggregate could not take it out of the gap, and every read would withhold that aggregate until a round reached it. The re-derived aggregates are that record; a later recording that covers an aggregate puts it back (assertion G), so a row counts as re-derived only against the recordings that came before its re-derivation.

**Why a round stamp, and how does a round end that its instance cannot run (assertions J and K)?** A round spans many transactions and may pass between instances: a process stops, loses its lease or is rolled back. Every transaction checks that the round stamp is the one its own round last wrote, so after a lost lock session two instances that each believe they own a round cannot both write: the second to commit reads a stamp it did not write and stops. An instance that finds a round holding a gap it may not converge -- a newer build's round, after that build was rolled back -- ends the round and puts back on the gaps what the round had not completed, under new tokens, so the gaps it may converge are not stranded in a round nobody may run. Correctness therefore does not rest on the convergence lease (EVS-DEV-view-convergence-scheduling).

**Why does the boot raise a lagging target at once (assertions A and B)?** A build of an older minor that folds while a newer build's promotion runs must know that its fold undoes part of the promotion. Raising the stored target at the boot, and keeping the round version on the gap while a round holds it (assertion I), makes every such fold find a stored target or a round version above its own and record what it folded (EVS-DEV-version-compatibility/M); otherwise a round completing after such a fold would leave that fold's rows under the older minor while the target claims the newer one. The prior version kept on the gap is what the completing promotion's audit event records as its starting version. The boot records no second promotion gap when one at its round version is already open, so an instance restarting in the middle of a promotion resumes it rather than starting it again.

**Why does an instance converge only the gaps that count for it (assertion P)?** An instance below a promotion gap's round version would re-derive rows a newer build promoted back under its older minor, only for the newer build to promote them again. Leaving the promotion gap to an instance at or above its round version makes a canary overlap converge towards the newest minor, while any registering build converges the catch-up gaps.

**Why does a fold into a covered aggregate re-derive it (assertions S and T)?** A row that a round has not yet re-derived lacks part of its history, so merging one event's delta into it produces a row no replay produces. Re-deriving the touched aggregate in the fold's transaction makes the row settled at once, so a caller that writes an aggregate and reads it back sees it settled, and the round then leaves it alone. This holds while a whole-log gap waits for a round to take it, since such a gap covers every aggregate it does not hold as re-derived. The fold's cost, and the rows it reads, grow with that aggregate's events. A table view has no aggregate to re-derive: a fold into a table view whose round is in progress folds as usual, and the view reports no settled row until the round's refold reaches the head (assertion X).

**Which aggregates does a round re-derive (assertion U)?** Every event a recording covers after the boot names its aggregate, so the named aggregates are exactly the rows those differences touched. A gap covering the whole log -- a view registered over an existing log, a promotion found at the boot, a rebuild -- names none, so the round plans them from the log, and until its planning ends every aggregate is covered, so nothing it may yet plan is served as settled. Planning reads the log in units of a few hundred events, so it ends within seconds even on a log of tens of thousands of events. A promotion found at the boot is version-limited to aggregates holding an event below the round version, as snapshot promotion states (EVS-DEV-snapshot-promotion-on-open): an aggregate whose events are all at the round version already folded them unchanged under any build of that major.

**Why does a table view refold in place, from a position, up to the head (assertions V to X)?** A table view's row key is extracted from each event, so a row's final value is decided by the events for its key in log order. Folding every matching event in log order from a position, over rows that were correct before that position, yields the rows a replay yields, whatever the instance's own folds wrote meanwhile, as long as the refold reaches the latest event in the transaction that completes it: the refold applies again, in order, every event a live fold applied out of order. A live fold and a convergence transaction are ordered by the database (EVS-DEV-view-convergence-scheduling/C), so neither's write is lost. While the refold has not reached the head the view reports no settled row (EVS-DEV-converging-view-reads/C), so intermediate rows are never served, no shadow copy of the view is needed, and a gap from a position refolds only the events after it. The cost is that a table view serves no rows while it converges, which is why the library's own table views converge first (EVS-DEV-view-convergence-scheduling/N).

**Why does `rebuildView` record gaps instead of re-deriving (assertions Y and Z)?** A rebuild re-derives the whole view, the same work as the largest promotion, and in one transaction it would hold back every append touching the view for as long as that work takes. Recording whole-log gaps hands the work to rounds bounded like every other re-derivation; the new tokens make a round already in progress leave the gaps and start over from the rebuild's coverage, and the rows the rounds settle reach live subscribers as they settle (EVS-DEV-converging-view-reads/H). The view is reported as converging until they complete. A build that lacks the view and keeps storing its events keeps recording catch-up gaps, so a rebuild may not finish while such a build serves; the caller's deadline turns that wait into a typed error that names the view and how far it has come.

**Why a whole-view pair (assertion D, EVS-DEV-view-target-versions-seeding/E)?** A view whose interest names no entry type (it selects by aggregate type, or matches every entry type) has no per-entry-type pair, so without one it would have no record that it lacks events, and it would be served as current over a log it never folded. The whole-view pair gives it one: it is seeded like any pair, so a view registered over an existing log converges, and a build that does not register the view cannot evaluate the other build's interest, so it records every event it stores as a possible difference for every such view it lacks. That costs one write per such view in each storing transaction of a build that lacks the view, only while such a build shares the database. Two builds whose interests for one view differ outside the named entry types still record nothing for each other, and a whole-view pair carries no version, so a promotion of an entry type such a view folds is not tracked for it; both are Layer 2 limitations recorded on the roadmap (`spec/roadmap/projections.md`), and the equality a read reports is stated under that precondition (EVS-DEV-converging-view-reads/E).

**What does convergence cost a canary overlap?** While a build that lacks a view, or registers an older minor of its entry type, keeps storing events that the view folds, each such event records a gap, so the view converges for the build that registers it, or registers the newer minor, for as long as each recording waits for the next round: it lacks those events until a round catches up with them, and every read says so. When that build stops, the next round completes and the view becomes current. Reporting the view as current while events the log holds are missing from it is what the library does not do.

**A fenced instance converges nothing.** Every convergence transaction is an ordinary transaction of the backend, so on Postgres it runs the generation fence first (EVS-DEV-version-compatibility/I), and an instance whose generation the record does not admit commits no convergence work.

## Changelog

- 2026-09-25 | 823e933e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-Z: the boot records catch-up and promotion gaps; rounds after the open re-derive what the gaps cover and remove a gap only while its token is unchanged; a fold into a covered aggregate re-derives it; rebuildView and the whole-view pair record gaps

*End* *View convergence after EventStore.open* | **Hash**: 823e933e
