# EVS-DEV-destination-drain-lock: Delivery cycle mechanics

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations

## Purpose

This requirement holds the implementation mechanics of the library's delivery cycle -- the process that fills each destination's queue from the event log and drains it through the destination's delivery implementation -- and of the test seams through which the library's own tests observe the library's log lines and inject failures into the operations that change a destination's persisted state.

## Assertions

F. The library's test seams SHALL have no effect when assertions are disabled.

## Rationale

**Why gate the test seams on assertions (assertion F)?** The seams let the library's tests observe the library's log lines and make a destination-registry operation, a fill, a drain outcome or the drainer's wedge fail at a named point inside its transaction, or make the drainer's wedge report failure after its transaction committed. The library reads the installed seams only inside an assertion, so a release build, a program run without assertions, or a compiled executable never reads them: whatever a caller installs is ignored, and the seams are not an input that participates in any decision the library makes in production. They therefore add no entry to the library's trust boundaries. A seam may observe or make an operation fail at a named point; it cannot make an operation succeed that would otherwise fail, an observing seam that throws does not change what the library does next, and a seam never receives a database handle or a transaction. The residual is stated as it is: with assertions enabled (a test run, or a Flutter debug build) code that imports the internal seam file from the library's `src/` directory can install seams; that path is not one of the library's operations, so it lies outside the precondition of EVS-PRD-destinations/L, and the analyzer reports it as a use of an internal member.

## Changelog

- 2026-09-23 | 66b0a2e5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of F names every operation a seam can make fail
- 2026-09-23 | 66b0a2e5 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-23 | - | - | Michael Lewis (<michael@anspar.org>) | Add F: test seams are inert without assertions

*End* *Delivery cycle mechanics* | **Hash**: 66b0a2e5
