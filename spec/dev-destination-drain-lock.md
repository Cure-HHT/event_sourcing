# EVS-DEV-destination-drain-lock: Delivery cycle mechanics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations

## Purpose

This requirement holds the implementation mechanics of the library's delivery cycle -- the process that fills each destination's queue from the event log and drains it through the destination's delivery implementation -- and of the test seams through which the library's own tests observe the library's log lines and inject failures into the operations that change a destination's persisted state.

## Assertions

F. The library's test seams SHALL have no effect when assertions are disabled.

## Rationale

**Why gate the test seams on assertions (assertion F)?** The seams let the library's tests observe the library's log lines and make a destination-registry operation, a fill, a drain outcome or the drainer's wedge fail at a named point inside its transaction, or make the drainer's wedge report failure after its transaction committed; they also let the tests observe each run of `EventStore.open`'s boot transaction and make the boot fail after its library-version event, hold the incompatible-generation guard's boot lock at a named point, replace the timer that drives the Postgres lock session's probe, and make a generation registration, a probe, the ending of a lost lock session, the close of a lost lock session, a provisioning or the browser's lock manager fail, or the lock session's check fail as a transaction-mode pooler would. Two seams are input substitutions: one replaces the package and data-format versions the boot decides with and records, and one replaces the Postgres migration list (and so the schema version and its minimum) of the backends and provisionings started under it, so that one test process can play two builds of the library against one database; with either installed, the checks decide and record as a build of those versions would. The library reads the installed seams only inside an assertion, so a release build, a program run without assertions, or a compiled executable never reads them: whatever a caller installs is ignored, and the seams are not an input that participates in any decision the library makes in production. They therefore add no entry to the library's trust boundaries. A seam may observe, delay, make an operation fail at a named point, replace a timer, or substitute the build's declared versions or schema declaration; it cannot make an operation succeed that would otherwise fail except as the build it declares would, an observing seam that throws does not change what the library does next, and a seam never receives a database handle or a transaction. The residual is stated as it is: with assertions enabled (a test run, or a Flutter debug build) code that imports the internal seam file from the library's `src/` directory can install seams, the substituted build and schema declarations included, which then decide the boot or a provisioning and are recorded in the log or the database; that path is not one of the library's operations, so it lies outside the precondition of EVS-PRD-destinations/L, and the analyzer reports it as a use of an internal member.

## Changelog

- 2026-09-23 | 66b0a2e5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of F names the boot seams and the build-declaration input substitution
- 2026-09-23 | 66b0a2e5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of F names every operation a seam can make fail
- 2026-09-23 | 66b0a2e5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Add F: test seams are inert without assertions

*End* *Delivery cycle mechanics* | **Hash**: 66b0a2e5
