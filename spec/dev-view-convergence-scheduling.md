# EVS-DEV-view-convergence-scheduling: Scheduling of view convergence

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the library runs the rounds of view convergence (EVS-DEV-view-convergence) beside a serving database: in convergence transactions bounded in time and ordered against every append, paced so appends keep most of the database, persisting their progress so any instance resumes them, under a per-view lease, retrying after a failure without closing the store, reporting their progress, and stopping with the store. The bound on how long a serving append waits, and how long convergence takes, is stated as measured assertions on Postgres.

Terms used below: a **convergence transaction** is any transaction that writes a convergence gap, a round or a pair's stored target other than the boot of `EventStore.open` and a transaction that stores events: a round's transactions (its take, its work, its completion), the transaction that ends a round another instance began (EVS-DEV-view-convergence/J), and the recording transaction of `rebuildView`; a **unit** is the planning of up to 500 events of the log, the re-derivation of one aggregate's row, the fold of one event into a table view, or the deletion of up to 500 rows of a table view. The **measured database** is a Postgres database whose log holds 20,000 events of 2,000 aggregates, ten each, all folded into one aggregate view of 2,000 rows, and the **serving loop** is an instance appending on it continuously, alternating events outside the view's interest and events of the view's entry type into the view's aggregates.

## Assertions

A. Each convergence transaction SHALL begin no further unit once it has run for 200 ms.

B. Each convergence transaction SHALL perform at least one unit.

C. On a `PostgresBackend`, each convergence transaction SHALL, as its first statement and before the generation fence fixes its snapshot, lock the table holding the log's sequence counter in the mode the boot of `EventStore.open` takes.

D. No convergence transaction SHALL write the table or record holding the log's sequence counter, except a transaction that completes a promotion and appends its audit event.

E. An instance SHALL begin a convergence transaction no sooner after the end of its previous convergence transaction than that previous transaction ran.

F. Each convergence transaction SHALL persist, with its work, the round's progress: the gaps and tokens it took, its planning position, its planned aggregates still to re-derive, its re-derived aggregates, a table view's deletion and refold positions, its units done and counted, and its round stamp.

G. A round interrupted by a crash, a close, a lost lease or a failed transaction SHALL resume, in whichever instance next resumes it (EVS-DEV-view-convergence/J), from the progress its last committed convergence transaction persisted.

H. Within the scope its storage backend supports, the library SHALL let at most one instance at a time run rounds for a given view of a database, through a per-view convergence lease: on Postgres a session lock held on the backend's lock session, on the web a Web Lock, and on Sembast outside the browser an isolate-local registry per open database handle.

I. On the web, a page SHALL request a convergence lease only while it is visible, and SHALL release it once it is hidden and its convergence transaction in flight has ended.

J. An instance SHALL hold a view's convergence lease only while the view carries a gap that counts for it or a round it must resume or end (EVS-DEV-view-convergence/J), and SHALL release it when it closes or loses its lock session.

K. When a convergence transaction throws, the library SHALL leave the gaps and the round's committed progress as they were, log the failure, and report it with its error in the view's convergence progress.

L. After a convergence transaction of a view throws, the library SHALL retry the view's round after a delay that starts at 1 s and doubles with each consecutive failure up to 5 minutes.

M. A failed convergence transaction of one view SHALL NOT close the event store or delay the rounds of other views.

N. An instance SHALL run the rounds of the views the library registers itself before the rounds of views the consumer registers.

O. The library SHALL report to the event store's convergence observers, after each convergence transaction commits, the view, the round's stage (planning, re-deriving or completing), its units done and the units counted so far, the time since the round began, the longest convergence transaction of the round, and the round's last failure.

P. After `EventStore.close` is called, the library SHALL begin no convergence transaction.

Q. `EventStore.close` SHALL return only once the convergence transaction in flight, if any, has committed or rolled back.

R. Beside the serving loop on the measured database, for the 60 seconds from the moment a second instance of the same data-format major that registers the view's entry type in an added view begins to open the database, every serving append SHALL commit within 1 second of its call.

S. Beside the serving loop on the measured database, for the 60 seconds from the moment a second instance of the same data-format major that registers a newer minor of the view's entry type begins to open the database, every serving append SHALL commit within 1 second of its call.

T. When the instance running the serving loop on the measured database is itself one that opened registering a newer minor of the view's entry type, every append of the loop SHALL commit within 1 second of its call for the 60 seconds after its open returns.

U. In each of the scenarios of assertions R to T, the converging instance's first round over the view SHALL have re-derived every aggregate it planned within those 60 seconds.

V. In each of the scenarios of assertions R to T, once the serving loop stops, the view SHALL be current for the converging instance within 30 seconds.

## Rationale

**Why transactions bounded by time (assertions A and B)?** A convergence transaction holds back the appends it is ordered against until it commits. Bounding each transaction by elapsed time rather than by a count of units ties the wait an append can see to the bound the deployment needs, whatever the size of an aggregate or the speed of the database; performing at least one unit keeps a round moving however slow the database is. A unit is not split: a re-derived aggregate's row is replaced in one transaction, so an aggregate whose events alone take longer than the bound to fold holds its transaction, and the appends waiting on it, for that long. The progress report records a round's longest transaction (assertion O), so such an aggregate is visible to an operator.

**Why does a convergence transaction lock the sequence counter's table (assertions C and D)?** Every Postgres transaction of the library runs serializable. An append that loses a serialization race re-runs holding a lock on the table holding the sequence counter, which every competing append writes, so its re-run cannot lose to another append (EVS-PRD-event-log/E). A convergence transaction that took no such lock would not be ordered by it: a chunk reads many events and writes view rows and pair records that appends read, so an append's re-run could lose to one chunk after another until its retries ran out, or the long chunk could lose to the short appends on nearly every attempt and never complete. Locking the table first, in the mode the boot takes, before the fence fixes the snapshot, waits for the appends that have written the table and then holds later appends' writes to it back until the chunk commits: the chunk's snapshot includes every append that committed before it, so the chunk does not lose a race to an append, and an append whose snapshot predates the chunk waits at its write of the counter, fails at most once when the chunk commits, and re-runs behind it holding the same lock, so it succeeds on that re-run. The boot takes the same lock, and every writer of the pair records the boot reads -- a transaction that stores events, a convergence transaction, `rebuildView`'s recording, the boot -- either writes the table or takes the lock, so all of them are ordered by it (EVS-DEV-event-store-open). A convergence transaction writes nothing to the table itself, except the transaction that completes a promotion and appends its audit event. The cost is that every append on the database waits for the convergence transaction in flight: at most the 200 ms bound and one unit, which assertion E keeps to a share of the database's time. Pacing is per instance and the lock is per database, so where N instances converge different views at once an append may queue behind each of them, a wait of up to about N times that bound; the measured assertions have one converging instance.

**Why pace every backend (assertion E)?** A convergence transaction holds back every append while it runs -- on Postgres through the table lock, and on Sembast, which runs a database's transactions one at a time and in the browser takes one write lock for every tab, because nothing runs beside it. A converger that started each transaction as the last ended would hold the database for most of its time; waiting as long as the previous transaction ran leaves the appends at least half of it, so an append waits for at most one convergence transaction.

**Why persist progress in every transaction (assertions F and G)?** A round over a large view spans many transactions, and a process may stop, lose its lease or fail between any two. Progress written in the transaction that did the work is exactly as durable as the work, so a resumed round neither repeats committed work nor skips uncommitted work, whichever instance resumes it.

**Why a lease per view, and why is it not a trusted input (assertions H to J)?** Two instances converging one view would contend on the same rows and redo each other's work, and the view's progress would have no single owner, so one instance at a time converges a view. The lease is per view, not per database, because builds sharing a database need not register the same views: a canary that adds a view converges it while the serving instance, which does not register it, converges the others. The lease rides the mechanisms the library already uses for its generation guard and drain lock -- a session lock on the Postgres lock session, a Web Lock, an isolate-local registry -- and no guarantee rests on it: every convergence transaction checks the round stamp before it writes (EVS-DEV-view-convergence/K), every round removes a gap only where its token is unchanged, and rows are re-derived deterministically from the log, so two instances that overlap after a lost lock session cost duplicated work, and one of them stops. A hidden page releases its lease because a browser throttles a hidden page's timers, which would stall the view for every tab; a lone hidden page therefore converges nothing until it is visible again, which is why every wait on convergence takes a deadline (EVS-DEV-view-convergence/Z, EVS-DEV-converging-view-reads/K and M). An instance holds a lease only while it has work, so an instance for which no gap counts (a build below a promotion gap's round version) never keeps the lease from one for which it does.

**Why keep the store open when a round fails (assertions K to M)?** A failure while re-deriving -- a promoter that throws on a stored payload, a storage failure -- concerns one view. The store stays open for everything else, the failure is logged and reported with its error so an operator sees which view is stuck and why, the view stays converging so its unsettled rows are never served, and the round retries with a growing delay, since a failure the build itself causes repeats until the build changes.

**Why do the library's own views go first (assertion N)?** The authorization policy decides from the role-assignment and permission-grant views, which are table views that serve no rows while they converge, and it refuses while they converge (EVS-DEV-converging-view-reads/J); the default destination-wedges view reports delivery state operators act on. Converging them first shortens the window in which submissions are refused.

**Why report progress per transaction (assertion O)?** A deployment serves while views converge, so the question it asks is which views are not yet current and how far they have come, beyond whether the process has booted. Reporting after each commit reports only work that happened; the units counted grow as planning finds them, so a round's total is known once its planning ends.

**What do the measured assertions state (assertions R to V)?** They are the bounds the library is built to: beside a serving instance, a second instance that adds a view or promotes one, and an instance that promotes a view while it serves, hold no serving append for more than a second -- which includes that no serving append fails after exhausting its retries -- and still converge the view within the minute. The window is a fixed 60 seconds so that it takes in the incremental rounds that follow the first (in R and S the loop's appends of the view's entry type keep recording gaps, as a build that lacks the view or folds an older minor) and the loop's appends into aggregates a round still covers, which re-derive them inline (EVS-DEV-view-convergence/S). The view becomes current only once such recordings stop, so assertion V measures that after the loop stops. They are measured on real Postgres because the bounds depend on how Postgres orders a convergence transaction against an append, which no other backend reproduces. The measured database is the size at which re-deriving the view inside a boot transaction holds appends back for tens of seconds.

## Changelog

- 2026-09-25 | d2ed7726 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-V: convergence transactions bounded by 200 ms, ordered against appends by the sequence counter's table lock and paced on every backend, resumable progress, a per-view lease, retry after failure without closing the store, library views first, progress per transaction, a clean stop at close, and the measured 1 s serving-append bound and convergence time on Postgres

*End* *Scheduling of view convergence* | **Hash**: d2ed7726
