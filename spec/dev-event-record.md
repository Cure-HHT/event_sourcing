# EVS-DEV-event-record: The event record as read, stored and sent

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-hash-chain-integrity, EVS-PRD-ingest

## Purpose

An event is stored and sent as a record: a JSON object whose fields the event hash covers as the record spells them. This requirement fixes which client timestamps a record may carry, so that every storage backend and every host reads the same instant from one, and how a build treats the parts of a record it does not read, so that a record written by a later release of the same data-format major verifies, stores and relays unchanged through an earlier one.

## Assertions

A. The library SHALL accept a record's client timestamp only as an ISO 8601 date-time with a four-digit year, a month, day, hour, minute and second each within its calendar range, and an explicit UTC offset -- `Z`, or a sign, hours and optional minutes of at most 23 hours and 59 minutes -- and SHALL refuse a record carrying any other client timestamp as malformed, naming the field: on every ingest entry point before any write, and on every append and read.

B. When the library reads, stores or forwards a record, it SHALL keep every key of the record's initiator and of its entry-type and data-format version maps, and every top-level key it does not read, exactly as the record carries them.

## Rationale

**Why an explicit offset (assertion A)?** A timestamp without an offset names a wall-clock time, not an instant: the Dart parser reads it in the local zone of the host that parses it. The Postgres backend writes the instant it parsed into a `TIMESTAMPTZ` column, which window queries filter on, while the event it returns is parsed again from the stored string on the reading host; the Sembast backend parses the string on each query; and the fold stamps a row's times from the parsed instant. An offsetless timestamp would name a different instant on hosts in different zones, so a window query could filter an event by one instant and return it with another, and two hosts folding the same log would disagree. The rule is the one provenance entries apply to `received_at`.

**Why a four-digit year and calendar ranges (assertion A)?** The Dart parser accepts a signed year of up to six digits, and on the native runtime a year beyond the range of its time value wraps to a different instant; the Postgres `TIMESTAMPTZ` range begins in 4714 BC. A four-digit year keeps every accepted timestamp inside the range both backends store and every runtime reads alike, with an offset shifting it at most a day past either end. The same parser rolls an out-of-range field over -- 30 February to 2 March, hour 24 to the next day, a 60th second to the next minute -- so a record naming an impossible time would be read as a different, possible one. Refusing it leaves each accepted timestamp one instant, the one its fields name.

**Why refuse on append and read as well as at ingest (assertion A)?** The library writes every time it stamps in UTC, and an event built in code with a local time is written in UTC too, so every record the library writes carries an accepted timestamp. A stored record that does not was not written by this data format, and reading it would give the host-dependent instant the rule exists to prevent. At ingest the refusal comes before any write, like the other decode failures, so a malformed record leaves the log as it was.

**Why keep the keys a build does not read (assertion B)?** Within a data-format major, a later release may add optional fields (EVS-DEV-version-compatibility, Rationale for assertions A to D). The initiator and the two version maps are hashed as the record spells them, so a build that dropped a key it does not model would change the record's hash, and the copy it stored or relayed would no longer verify. Keeping them verbatim lets a record of a later minor verify, store and relay unchanged through an earlier build; the build reads the numbers and the fields it models and carries the rest. A top-level key outside the record's declared fields is not covered by the event hash (EVS-PRD-hash-chain-integrity, Rationale). It is kept, stored and forwarded as it arrived, so a field a later release adds there survives a relay, but nothing attests it: a hop can change or drop it undetected. A field that must be attested belongs inside a hashed structure -- the data, the metadata or the initiator -- which every build of the major hashes whole.

## Changelog

- 2026-09-24 | f24e0c04 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-B: a client timestamp carries a four-digit year, in-range calendar fields and an explicit offset; the initiator, the version maps and unread top-level keys are kept as the record carries them

*End* *The event record as read, stored and sent* | **Hash**: f24e0c04
