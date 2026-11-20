# EVS-DEV-destination-key-hold-guards: Guards on a keyed destination's holds

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-DEV-destination-key-hold, EVS-PRD-destination-key-hold

## Purpose

This requirement holds the guards that keep a keyed destination's per-key order when the processes that serve it disagree, and when a hold or a recovery meets a queue or a log it cannot act on. A keying is the pair a registration records in the destination's persisted schedule: whether the destination is keyed, and, for a keyed destination, the name of its key scheme, the rule its block keys are computed by. A process's keying is the keying of the destination that process registers.

## Assertions

A. Every registration of a destination SHALL record its keying in the destination's persisted schedule and in its registration event.

B. The destination registry SHALL refuse, before writing anything, to register a keyed destination that names no key scheme or an empty one.

C. While a destination has a key list, the destination registry SHALL refuse, writing nothing but the database-wide check record, a registration whose keying differs from the keying the persisted schedule records.

D. While the persisted schedule of a destination records a keying other than the process's keying, the fill, every replay and every release step of that process SHALL write nothing for that destination.

E. If an attempt reports a permanent failure scoped to its item's block key while the persisted schedule records a keying that is not keyed or that differs from the keying of the process that drains, then the drainer SHALL wedge the head with cause `permanent_refusal`.

F. If a hold cannot be committed because the queue or the log is not as the hold requires, then the drainer SHALL roll the hold back and wedge the head with cause `permanent_refusal`.

G. If an operator recovery would remove a released item that it cannot return to its key's list, then the destination registry SHALL refuse the recovery, writing nothing but the database-wide check record.

H. Every hold of a key SHALL remove that key's release failure record in the hold's transaction.

I. When a destination is registered over a persisted schedule that records no keying, the destination registry SHALL record the registration's keying, whether or not the destination has a key list.

## Rationale

**Why record the keying (assertions A to D, and I)?** While a key has a key list, the fill routes that key's new events into the list by comparing the key it computes for each event with the listed keys. A process that computes keys differently -- one that registers the destination unkeyed, or keyed under a changed key function -- matches no list and queues a held subject's new events ahead of its held ones, breaking `EVS-PRD-destination-key-hold/C`. A rolling deploy runs two builds side by side, and a rollback or another process sharing the database does the same, so the registration of one process says nothing about the keying of another. The persisted schedule therefore records the keying of the latest registration, and the registration event records it in the log (assertion A). A keyed destination names its key scheme (`Destination.keyScheme`, for example `participant-v1`) because the library cannot compare two key functions: the consumer changes the name whenever a change to `blockKeyOf` may give one subject's events another key, and a keyed destination without a name is refused (assertion B). Registration refuses a change of keying while key lists exist (assertion C), turning the stop-then-start a keying change needs into a check the library enforces; without key lists a change of keying is accepted and recorded, the latest registration's keying in effect as for the hard-delete opt-in. A schedule that records no keying -- one written before keyings were recorded, as on every database migrated in place to the schema that adds keyed holds -- takes the registration's, as a first registration does (assertion I). A process registered before another recorded a different keying keeps running, so its fill, its replays and its release steps check the recorded keying too and write nothing under another (assertion D); the schedule is part of the fill's compare-and-set (`EVS-DEV-destination-drain/G`), so a keying recorded while a fill builds its items makes its commit write nothing as well.

**Why a mismatched keying makes a key-scoped refusal a wedge (assertion E)?** A hold creates a key list that the process under the recorded keying would not route into. Holding nothing and wedging the head stops every key until an operator acts, which is the destination-wide behaviour every refusal had before keys; the drain keeps delivering otherwise, since a queue already built stays in order whatever the keying. A drainer that honours a halt for a destination it does not register has no keying of its own, and holds only when the recorded keying is keyed.

**Why a hold that throws becomes a wedge (assertion F)?** The hold reads the head, the key's pending items and their events, and refuses a queue or log it cannot hold (an item behind the head that carries attempts, an event missing from the log). Were the failure to end the pass with the attempt alone, every later pass would reach the same hold and fail again: nothing records a wedge, and a recovery refuses a pending head, so only a deletion would move the destination. Rolling the hold back and wedging the head puts the destination on the recovery path every other unrecoverable head takes. Each trigger needs an inconsistent queue or log, so this is a fail-safe rather than an expected path.

**Why a recovery refuses an unreturnable released item (assertion G)?** A recovery returns each removed released item's events to its key's list (`EVS-DEV-destination-key-hold/X`). When an item carries no block key, one of its events is missing from the log, or the log holds no hold event for the hold a missing list must be created under, the return cannot be built. The recovery decides this before it writes and refuses with its check record, as every other refused registry operation does, so the refusal is on record and the queue is left as it was; the remedy is deletion.

**Why a hold removes the key's release failure record (assertion H)?** A release failure record times a key's failing release and spends its retry budget (`EVS-DEV-destination-retry-budget`). A refusal of a released item holds the key again under a new hold id; a record left from the previous release would make the next release start with its budget partly spent and its backoff already running, so it could hold the key again early. Each hold therefore starts its key's release with no record.

## Changelog

- 2026-11-18 | 0c871cf6 | - | muhammadU-anspar (<muhammad@anspar.org>) | Auto-fix: update hash
- 2026-11-18 | - | - | muhammadU-anspar (<muhammad@anspar.org>) | Add A-I: the keying a registration records, a keyed destination's key scheme, refusal of a keying change while key lists exist, the fill and the hold under another keying, a hold that throws wedging the head, a recovery refusing an unreturnable released item, a hold removing its key's release failure record, and a registration over a schedule that records no keying

*End* *Guards on a keyed destination's holds* | **Hash**: 0c871cf6
