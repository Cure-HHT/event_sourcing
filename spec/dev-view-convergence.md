# EVS-DEV-view-convergence: View convergence after EventStore.open

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the library brings a registered view level with the log when its stored rows may not be what a replay of the log produces under the registered definitions: events the view has not folded (a build that does not register the view stored them, or the view was registered over a log that already held them), events whose fold or mark a later event changes (a skip event, a reconciliation or a security finding), a stored target version below the registered version of an entry type (snapshot promotion), or a requested rebuild. The library records each such difference as a convergence gap in the transaction that creates it, and re-derives the rows the gap covers from the log after `EventStore.open` returns, in convergence transactions of their own. Each convergence transaction reads a gap, does a bounded piece of its work, records the progress on the gap and removes the gap once nothing is left; the database orders it against every recording, so no recording is lost. How those transactions are sized, paced and reported is stated in EVS-DEV-view-convergence-scheduling; what a read of a view returns while it converges, in EVS-DEV-converging-view-reads.

Terms used below:

- A **pair** is a view and an entry type its interest names.
- A view carries at most one **catch-up gap**: rows that may differ from a replay and that any build registering the view may re-derive. A pair carries at most one **promotion gap**: rows to be re-derived at a newer version of the pair's entry type. A promotion gap carries a **round version**, the version the rows are to reach, and a **prior version**, the stored target the promotion started from.
- Every gap carries a **position** in the log, whether it covers the **whole log**, its **named aggregates** and its **re-derived aggregates**. A recording adds to the view's or the pair's gap of its kind, and creates the gap, empty, when none is stored.
- A gap **counts** for an instance when the instance registers its view and, for a promotion gap, registers the pair's entry type at a version not below the gap's round version.
- A gap of an aggregate view **covers** an aggregate when it does not hold the aggregate as re-derived and either names it or covers the whole log.
- A **convergence transaction** is a transaction, other than the boot of `EventStore.open` and a transaction that stores events, that writes a gap or a stored target: the re-derivation work described here and the recording of `rebuildView`.

## Assertions

A. For every registered pair whose stored target version the boot of `EventStore.open` finds below the registered version of its entry type, the boot SHALL write the registered version as the pair's stored target in its boot transaction.

B. For every registered pair whose stored target version the boot finds below the registered version of its entry type, and that carries no promotion gap whose round version is the registered version, the boot SHALL, in its boot transaction, record a promotion gap covering the whole log, with the registered version as its round version and the stored target it found as its prior version.

C. For every view whose pair or whole-view row the boot seeds while the log holds an event appended before the boot, the boot SHALL, in its boot transaction, record a catch-up gap covering the whole log.

D. A build that stores an event while registering no view of the name of a stored whole-view row SHALL record, in the storing transaction, a catch-up gap covering the event on that view.

E. A recording on an aggregate view that does not cover the whole log SHALL name the aggregate of every event it covers and SHALL remove those aggregates from the gap's re-derived aggregates.

F. A recording on a table view that does not cover the whole log SHALL lower the gap's position to the log position of the earliest event it covers.

G. A recording that covers the whole log SHALL set the gap's position to the first position of the log, mark the gap as covering the whole log and empty its re-derived aggregates.

H. A promotion recording on a pair whose promotion gap has a lower round version SHALL raise the gap's round version to its own and keep the gap's prior version.

I. The library SHALL begin no convergence transaction before `EventStore.open` has returned.

J. A convergence transaction SHALL write no gap that does not count for its instance.

K. A convergence transaction SHALL re-derive a row by folding each of the row's events from the log into the view, through the fold step the instance's own folds use, under the instance's registered version of the event's entry type.

L. For a gap of an aggregate view that covers the whole log, a convergence transaction SHALL read events in log order from the gap's position, re-derive the row of each aggregate the gap covers that holds such an event matching the view's interest (of the pair's entry type, for a promotion gap), and move the gap's position past the last event it read.

M. A convergence transaction that finds the position of a whole-log gap of an aggregate view past the latest event of the log it reads SHALL record that the gap no longer covers the whole log.

N. For a gap of an aggregate view that does not cover the whole log, a convergence transaction SHALL re-derive the row of each named aggregate the gap covers.

O. A convergence transaction that re-derives an aggregate's row SHALL record the aggregate as re-derived on every gap of the view that counts for its instance.

P. A convergence transaction SHALL remove a gap of an aggregate view that it finds covering no aggregate.

Q. For a gap of a table view that covers the whole log, a convergence transaction SHALL delete the view's rows before it folds any event, and SHALL record that the gap no longer covers the whole log once no row remains.

R. A convergence transaction over a table view SHALL fold, in log order, every event matching the view's interest from the lowest position of the gaps it works, and SHALL move each such gap whose position it passes to the position after the last event it folded.

S. A convergence transaction SHALL remove a gap of a table view only when it has folded every matching event from the gap's position up to the latest event of the log it reads.

T. The transaction that removes a promotion gap SHALL write its instance's registered version of the pair's entry type as the pair's stored target.

U. `rebuildView` SHALL, in one transaction that re-derives no row, record a catch-up gap covering the whole log on the view.

V. `rebuildView` SHALL return once the view is current for the instance, and SHALL throw a typed error naming the view and its latest convergence progress when a deadline the caller supplies passes first.

## Rationale

**Which layer?** Everything here serves a Layer 2 claim: that a view the library reports as current, or a row it reports as settled, is what a replay of the log produces under the library's default projection conventions and the registered definitions -- the closed-under-events claim, scoped to those conventions. Gaps are storage state kept beside the views (EVS-PRD-destinations/L), not facts of the log. Convergence only reads the log, and appends nothing to it but a completed promotion's audit event.

**Why converge after the open rather than in the boot?** The boot transaction holds back every append to the database while it runs: on Postgres its first statement locks the table holding the sequence counter, and on the web it holds the database's write lock exclusively (EVS-DEV-event-store-open). Re-deriving a view inside it makes that pause grow with the rows promoted and the events re-derived -- tens of seconds for a view of a few thousand rows over a log of tens of thousands of events -- while every serving append waits. The boot therefore decides and records, and re-derives nothing (assertions A to C). The re-derivation runs after the open returns (assertion I), in short transactions (EVS-DEV-view-convergence-scheduling), and a view whose rows are not yet re-derived is reported as converging rather than served as current (EVS-DEV-converging-view-reads).

**Why is the progress kept on the gap, with no separate record of a round?** Each convergence transaction reads the gap, does its piece of work and writes the gap back, and every recording writes the same gap. The database orders them: on Postgres every convergence transaction and every storing transaction is serializable and takes or writes the sequence counter's table first; Sembast runs one handle's writing transactions one at a time, and in the browser a tab whose commit another tab preceded runs again on the data that commit left. A recording made while the view converges therefore either precedes a convergence transaction, which then reads it, or follows it and adds its coverage back to the gap (assertions E to G). No token, stamp or round is needed to keep a recording from being lost, and whichever instance next works the gap continues from what the last committed transaction left, even after two instances overlapped.

**Why two kinds of gap?** A gap recording rows that may differ from a replay and a gap recording a newer version concern different builds. Every build that registers the view lacks the missing events, so any of them may converge a catch-up gap; a promotion gap concerns only builds at or above its round version, since a build of an older minor reads rows of its major at any minor (EVS-DEV-version-compatibility). One merged gap would carry one round version, which either hides missing events from an older build or has an older build converge a promotion it cannot perform. Kept apart, an older build converges the catch-up gap under its own minor; its re-derivation is a fold under that minor (assertion K), so the version-compatibility rules for such a fold lower the target and record the rows on the promotion gap, and a newer build promotes them again (assertion J keeps the older build off the promotion gap).

**Which gap does a skip event, a reconciliation or a security finding record?** A catch-up gap, which the transaction storing it records (EVS-DEV-branch-conflicts) whichever build stores it: naming aggregates needs no evaluation of the view's interest. It changes how events already folded fold, which is a row that may differ from a replay; any build registering the view can re-derive it, because the fold of a conflicted aggregate and the finding mark are library conventions every build of the data format shares. The recording covers the events of each aggregate whose row the stored event changes: on an aggregate view it names those aggregates, so exactly those rows are re-derived (assertion E); on a table view it lowers the position to the earliest of them, so the refold covers every key they produce (assertion F). No further kind of gap is needed for a per-aggregate re-derivation.

**Why re-derived aggregates?** A gap covering the whole log names no aggregate, so the gap must record which aggregates a convergence transaction has already re-derived, both to resume and so that a read serves those rows as settled. A later recording that covers an aggregate removes it again (assertions E and G), so a row counts as re-derived only against the recordings that came before its re-derivation.

**Why scan the log for a whole-log gap (assertions L and M)?** A gap covering the whole log -- a view registered over an existing log, a promotion found at the boot, a rebuild -- names no aggregate. The convergence transactions read the log from the gap's position and re-derive each aggregate they meet that is still covered, so the scan is both the plan and the work. Once the position passes the latest event, every aggregate holding a matching event has been re-derived since the scan began, unless a later recording named it again (assertion E), and a recording covering the whole log starts the scan over (assertion G). The gap then covers only its named aggregates (assertion N) and is removed when it covers none (assertion P). A promotion scan re-derives every aggregate holding an event of the pair's entry type; one whose events are all at the round version comes out unchanged, which costs a fold and nothing else.

**Why does a fold into a covered aggregate not settle it?** A fold merges one event into the stored row as usual. The row is unsettled while a gap covers it, so reads withhold it (EVS-DEV-converging-view-reads) and the convergence transaction that re-derives it from the log includes the event. A caller that writes a covered aggregate and reads it back sees it pending until convergence reaches it; the append's own transaction stays the size of one fold, whatever the aggregate's history.

**Why does a table view refold in place, from a position, up to the head (assertions Q to S)?** A table view's row key is extracted from each event, so a row's final value is decided by the events for its key in log order. Folding every matching event in log order from a position, over rows that were correct before that position, yields the rows a replay yields, whatever the instance's own folds wrote meanwhile, as long as the refold reaches the latest event in the transaction that removes the gap: the refold applies again, in order, every event a live fold applied out of order. While the refold has not reached the head the view reports no settled row, so intermediate rows are never served and no shadow copy of the view is needed. The cost is that a table view serves no rows while it converges, which is why the library's own table views converge first (EVS-DEV-view-convergence-scheduling).

**Why does the boot raise a lagging target at once (assertions A and B)?** A build of an older minor that folds while a newer build's promotion runs must know that its fold undoes part of the promotion. Raising the stored target at the boot, and keeping the round version on the gap, makes every such fold find a target or a round version above its own and record what it folded on the promotion gap (EVS-DEV-version-compatibility); otherwise the promotion would complete over rows still at the older minor. The prior version kept on the gap is what the completing promotion's audit event records as its starting version. The boot records no second promotion at a round version already open, so an instance restarting in the middle of a promotion continues it.

**Why does `rebuildView` record a gap instead of re-deriving (assertions U and V)?** A rebuild re-derives the whole view, and in one transaction it would hold back every append for as long as that takes. A whole-log catch-up gap hands the work to the same bounded transactions as every other re-derivation, and empties the re-derived aggregates of the view's catch-up gap, so rows re-derived before the rebuild are re-derived again. A stored target below the instance's registered version already carries a promotion gap, which the boot or an older-minor fold recorded, so the rebuild writes no target. A build that lacks the view and keeps storing its events keeps recording catch-up gaps, so a rebuild may not finish while such a build serves; the caller's deadline turns that wait into a typed error that names the view and how far it has come.

**Why a whole-view row (assertion D, EVS-DEV-view-target-versions-seeding)?** A view whose interest names no entry type (it selects by aggregate type, or matches every entry type) has no pair, so without a row of its own a build lacking the view would have no record that it exists, and the view would be served as current over events it never folded. A build that does not register the view cannot evaluate the other build's interest, so it records every event it stores on every such view it lacks: one write per such view in each storing transaction, only while such a build shares the database. Two builds whose interests for one view differ outside the named entry types record nothing for each other, and a whole-view row carries no version, so a promotion of an entry type such a view folds is not tracked for it; both are Layer 2 limitations recorded on the roadmap (`spec/roadmap/projections.md`), and the equality a read reports is stated under that precondition (EVS-DEV-converging-view-reads).

**What does convergence cost a canary overlap?** While a build that lacks a view, or registers an older minor of its entry type, keeps storing events the view folds, each such event records a gap, so the view converges for the build that registers it, or registers the newer minor, until the next convergence transaction catches up: it lacks those events until then, and every read says so. Reporting the view as current while it lacks events the log holds is what the library does not do.

**A fenced instance converges nothing.** Every convergence transaction is an ordinary transaction of the backend, so on Postgres it runs the generation fence first (EVS-DEV-version-compatibility), and an instance whose generation the record does not admit commits no convergence work.

## Changelog

- 2026-09-25 | ed333a41 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Purpose and Rationale: a security finding also records catch-up gaps for the rows they change
- 2026-09-25 | ed333a41 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rewrite A-V (all letters reassigned; no code or test references them): progress is kept on the gap and ordered by the database, replacing rounds, tokens, round stamps and planned aggregates; a view has one catch-up gap and a pair one promotion gap, neither version-limited; a whole-log gap is converged by a scan of the log; a fold into a covered aggregate no longer re-derives it; rebuildView records one whole-log catch-up gap and writes no target; a skip event or a reconciliation records a catch-up gap
- 2026-09-25 | 823e933e | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-Z: the boot records catch-up and promotion gaps; rounds after the open re-derive what the gaps cover and remove a gap only while its token is unchanged; a fold into a covered aggregate re-derives it; rebuildView and the whole-view pair record gaps

*End* *View convergence after EventStore.open* | **Hash**: ed333a41
