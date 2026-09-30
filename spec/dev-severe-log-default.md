# EVS-DEV-severe-log-default: Severe log records reach standard error

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-destinations

## Purpose

This requirement fixes where the library's severe log records from the fill and the drain go when the application has installed nothing to receive them.

## Assertions

A. The library SHALL write each log record the fill or the drain emits at severe level or above to the process's standard error (the console in a browser) unless the application has turned that default off.

## Rationale

**Why standard error by default?** A fill or drain failure the library logs at severe level is one an operator must see: a transform that throws, a transaction that failed to commit an outcome, a delivery cycle that refused its policy. Log records reach nothing unless something listens, and an application that installs no listener would otherwise lose them without a trace, so a failure that stops delivery would be invisible until someone noticed the destination had gone quiet. Writing them to standard error by default puts them in the process's own output, which every deployment captures, without wiring. An application that routes the library's log records elsewhere turns the default off so that each record is not written twice.

## Changelog

- 2026-09-25 | 2019159d | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-26 | - | - | Michael Lewis (<michael@anspar.org>) | Add A: severe fill and drain log records reach standard error by default

*End* *Severe log records reach standard error* | **Hash**: 2019159d
