# EVS-DEV-view-convergence-scheduling: Scheduling of view convergence

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-event-log, EVS-PRD-materializer

## Purpose

How the library runs the convergence transactions of view convergence (EVS-DEV-view-convergence) beside a serving database: bounded in time, ordered against every append, one at a time per database and paced so appends keep most of the database, retried after a failure without closing the store, reported as they progress, and stopped with the store. The bound on how long a serving append waits, and how long convergence takes, is stated as measured assertions on Postgres. Every time and count here is a fixed library constant.

Terms used below: a **convergence transaction** is as defined in EVS-DEV-view-convergence. A **unit** is the reading of up to 500 events of the log, the re-derivation of one aggregate's row, the fold of one event into a table view, or the deletion of up to 500 rows of a table view. The **measured database** is a Postgres database whose log holds 20,000 events of 2,000 aggregates, ten each, all folded into one aggregate view of 2,000 rows; the **serving loop** is an instance appending on it continuously, alternating events outside the view's interest and events of the view's entry type into the view's aggregates. The **measured scenarios** are three: beside the serving loop, a second instance of the same data-format major opens registering the view's entry type in an added view; beside the serving loop, a second instance of the same data-format major opens registering a newer minor of the view's entry type; and the instance running the serving loop is itself one that opened registering a newer minor of the view's entry type. A scenario's **window** is the 60 seconds from the moment the converging instance begins to open.

## Assertions

A. Each convergence transaction SHALL begin no further unit once it has run for 200 ms.

B. Each convergence transaction SHALL perform at least one unit.

C. On a `PostgresBackend`, each convergence transaction SHALL, as its first statement and before the generation fence fixes its snapshot, lock the table holding the log's sequence counter in the mode the boot of `EventStore.open` takes.

D. Within the scope its storage backend supports, the library SHALL let at most one instance at a time run convergence transactions on a database, through a convergence lease per database: on Postgres a session lock held on the backend's lock session, on the web a Web Lock, and on Sembast outside the browser an isolate-local registry per open database handle.

E. An instance SHALL hold the convergence lease for at most one convergence transaction at a time.

F. An instance SHALL release the convergence lease no sooner after the end of its convergence transaction than that transaction ran.

G. An instance SHALL request the convergence lease only while a view it registers carries a gap that counts for it.

H. On the web, a page SHALL request the convergence lease only while it is visible.

I. Convergence interrupted by a crash, a close, a lost lease or a failed transaction SHALL continue, in whichever instance next works the gap, from the state of the gap its last committed convergence transaction left.

J. When a convergence transaction throws, the library SHALL log the failure and report it with its error in the view's convergence progress.

K. After a convergence transaction of a view throws, the library SHALL retry the view's convergence after a delay that starts at 1 s and doubles with each consecutive failure up to 5 minutes.

L. A failed convergence transaction of one view SHALL NOT close the event store or delay the convergence of other views.

M. An instance SHALL converge the views the library registers itself before the views the consumer registers.

N. The library SHALL report to the event store's convergence observers, after each convergence transaction commits, the view, the units done since the view began converging for the instance, the log position its re-derivation has reached and the log's latest position, the time since the view began converging, the longest convergence transaction of the view, and its last failure.

O. After `EventStore.close` is called, the library SHALL begin no convergence transaction.

P. `EventStore.close` SHALL return only once the convergence transaction in flight, if any, has committed or rolled back.

Q. In each measured scenario, every append of the serving loop SHALL commit within 1 second of its call throughout the window.

R. In each measured scenario, the converging instance SHALL have re-derived the row of every aggregate of the measured database at least once within the window.

S. In each measured scenario, once the serving loop stops, the view SHALL be current for the converging instance within 30 seconds.

## Rationale

**Why transactions bounded by time (assertions A and B)?** A convergence transaction holds back the appends it is ordered against until it commits. Bounding each transaction by elapsed time rather than by a count of units ties the wait an append can see to the bound the deployment needs, whatever the size of an aggregate or the speed of the database; performing at least one unit keeps convergence moving however slow the database is. A unit is not split: an aggregate's row is re-derived in one transaction, so an aggregate whose events alone take longer than the bound to fold holds its transaction, and the appends waiting on it, for that long. The progress report records the longest transaction (assertion N), so such an aggregate is visible to an operator. The bounds are library constants because a deployment that could raise them could also break the measured one-second bound.

**Why does a convergence transaction lock the sequence counter's table (assertion C)?** Every Postgres transaction of the library runs serializable. An append that loses a serialization race re-runs holding a lock on the table holding the sequence counter, which every competing append writes, so its re-run cannot lose to another append (EVS-PRD-event-log). A convergence transaction that took no such lock would not be ordered by it: it reads many events and writes view rows and gaps that appends read and write, so an append's re-run could lose to one convergence transaction after another until its retries ran out, or the long convergence transaction could lose to the short appends on nearly every attempt. Locking the table first, in the boot's mode, before the fence fixes the snapshot, waits for the appends that have written the table and then holds later appends' writes to it back until the convergence transaction commits: its snapshot includes every append that committed before it, so it does not lose a race to an append, and an append whose snapshot predates it fails at most once and succeeds on its re-run. The boot takes the same lock, and every writer of the gaps and targets the boot reads either writes the table or takes the lock, so all of them are ordered by it (EVS-DEV-event-store-open). That mode conflicts with itself, so two convergence transactions on one database also run one after the other.

**Why one convergence lease per database, held for one transaction (assertions D to G)?** Every append on the database waits for the convergence transaction in flight. With one converging instance per database that wait is at most the 200 ms bound and one unit; with several, their transactions would queue at the table lock ahead of an append, which would then wait for each of them. The lease keeps one instance at a time in a convergence transaction, and holding it through the pause after the transaction paces the database as a whole: whichever instances converge, appends keep at least half the database's time. Releasing it after each transaction lets an instance with other views to converge -- a canary that added a view beside a serving instance that converges its own -- take its turn. An instance requests the lease only while it has a gap that counts for it, so an instance below a promotion's round version never holds it. The lease rides mechanisms the library already uses for its generation guard and drain lock, and no guarantee rests on it: the database orders every convergence transaction against every other writer of the gaps (EVS-DEV-view-convergence), so two instances that overlap after a lost lock session cost duplicated work, not wrong rows.

**Why is a hidden page excluded (assertion H)?** A browser throttles a hidden page's timers, and a page holding the lease while throttled would stall convergence for every tab. A lone hidden page therefore converges nothing until it is visible again, which is why every wait on convergence takes a deadline (EVS-DEV-view-convergence, EVS-DEV-converging-view-reads).

**Why is resumption free (assertion I)?** The progress of convergence is the state of its gaps, written in the transaction that did the work, so it is exactly as durable as the work: a transaction that rolls back leaves the gap as the last commit left it, and any instance for which the gap counts continues from there.

**Why keep the store open when convergence fails (assertions J to L)?** A failure while re-deriving -- a promoter that throws on a stored payload, a storage failure -- concerns one view. The store stays open for everything else, the failure is logged and reported with its error so an operator sees which view is stuck and why, the view stays converging so its unsettled rows are never served, and the retry backs off, since a failure the build itself causes repeats until the build changes. The view stays as it is until a person changes the build or the data.

**Why do the library's own views go first (assertion M)?** The authorization policy decides from the role-assignment and permission-grant views, which are table views that serve no rows while they converge, and it refuses while they converge (EVS-DEV-converging-view-reads); the default destination-wedges view reports delivery state operators act on. Converging them first shortens the window in which submissions are refused.

**Why report progress per transaction (assertion N)?** A deployment serves while views converge, so the question it asks is which views are not yet current and how far they have come. Reporting after each commit reports only work that happened. The position a scan has reached against the log's latest position is how far a whole-log re-derivation has come.

**What do the measured assertions state (assertions Q to S)?** They are the bounds the library is built to: beside a serving instance, a second instance that adds a view or promotes one, and an instance that promotes a view while it serves, hold no serving append for more than a second -- which includes that no serving append fails after exhausting its retries -- and still re-derive every row within the minute. The loop's appends of the view's entry type keep recording gaps in the first two scenarios, as a build that lacks the view or folds an older minor, so the view becomes current only once such recordings stop, which assertion S measures after the loop stops. They are measured on real Postgres because the bounds depend on how Postgres orders a convergence transaction against an append, which no other backend reproduces. The measured database is the size at which re-deriving the view inside a boot transaction holds appends back for tens of seconds.

## Changelog

- 2026-09-25 | ef9b5185 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rewrite A-S (all letters reassigned; no code or test references them): one convergence lease per database held for one transaction and released after a pause as long as the transaction, replacing the per-view lease and per-instance pacing; progress is the state of the gaps, so the persisted-round assertion and the sequence-counter write rule are removed; the three measured scenarios share one latency, one re-derivation and one convergence assertion; all times and counts are fixed library constants
- 2026-09-25 | d2ed7726 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-V: convergence transactions bounded by 200 ms, ordered against appends by the sequence counter's table lock and paced on every backend, resumable progress, a per-view lease, retry after failure without closing the store, library views first, progress per transaction, a clean stop at close, and the measured 1 s serving-append bound and convergence time on Postgres

*End* *Scheduling of view convergence* | **Hash**: ef9b5185
