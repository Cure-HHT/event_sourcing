# EVS-DEV-event-record: The event record as read, stored and sent

**Level**: DEV | **Status**: Active | **Implements**: -
**Refines**: EVS-PRD-hash-chain-integrity, EVS-PRD-ingest

## Purpose

An event is stored and sent as a record: a JSON object whose fields the event hash covers as the record spells them. This requirement fixes four things:

- which client timestamps and provenance receipt times a record may carry, so that every storage backend and every host reads the same instant from one;
- which database identity and library version every provenance entry the library stamps records, so that the log states which database and which build appended or stored each event at each hop;
- the causal record every event carries, kept unchanged at every hop and covered by the event hash;
- how a build treats the parts of a record it does not read, so that a record written by a later release of the same data-format major verifies, stores and relays unchanged through an earlier one.

## Assertions

A. The library SHALL accept a record's client timestamp only as an ISO 8601 date-time with a four-digit year, a month, day, hour, minute and second each within its calendar range, and an explicit UTC offset -- `Z`, or a sign, hours and optional minutes of at most 23 hours and 59 minutes -- and SHALL refuse a record carrying any other client timestamp as malformed, naming the field: on every ingest entry point before any write, and on every append and read.

B. When the library reads, stores or forwards a record, it SHALL keep every key of the record's initiator and of its entry-type and data-format version maps, and every top-level key it does not read, exactly as the record carries them.

C. The library SHALL accept the `received_at` of a provenance entry only in the timestamp form assertion A states: the `provenance` package's entry decoder SHALL refuse any other `received_at`, naming the field, and the library SHALL refuse a record whose provenance carries one as malformed, naming the field, on every ingest entry point before any write, and on every append and read.

D. The library SHALL record, as `database_id` in every provenance entry it stamps (the originator entry of each event it appends and the entry it adds to each event it ingests, recovers or restores), the identity of the database that stamps it.

E. Every provenance entry the library stamps -- the originator entry of every event it appends, the events its boot and its reserved operations append included, and the receiver entry it stamps at ingest, recovery or restore -- SHALL carry `library_version`, as a non-empty string.

F. In a build with assertions disabled, the `library_version` the library stamps SHALL equal the package version compiled into the library.

G. When the library reads, stores or forwards a record, it SHALL keep every provenance entry the record carries, and every key of each entry, exactly as the record carries them, and at ingest, recovery or restore SHALL add only its own entry after them.

H. The library SHALL treat as malformed, naming the field, a record any of whose provenance entries lacks `database_id` or `library_version`, or carries one that is not a non-empty string: every ingest entry point, recovery and restore SHALL store no event for it, and every append and read SHALL refuse it.

I. The library SHALL include the `library_version` of every provenance entry in the content from which the event's hash is derived.

J. The library SHALL keep an event's `causal` object unchanged whenever it stores, delivers, ingests, recovers or restores the event.

K. The library SHALL include an event's `causal` object in the content from which the event hash is derived.

## Rationale

**Why an explicit offset (assertion A)?** A timestamp without an offset names a wall-clock time, not an instant: the Dart parser reads it in the local zone of the host that parses it. The Postgres backend writes the instant it parsed into a `TIMESTAMPTZ` column, which window queries filter on, while the event it returns is parsed again from the stored string on the reading host; the Sembast backend parses the string on each query; and the fold stamps a row's times from the parsed instant. An offsetless timestamp would name a different instant on hosts in different zones, so a window query could filter an event by one instant and return it with another, and two hosts folding the same log would disagree. One parser in the `provenance` package reads the form for both the client timestamp and each provenance entry's `received_at` (assertion C), so the two times of a record follow one rule.

**Why a four-digit year and calendar ranges (assertion A)?** The Dart parser accepts a signed year of up to six digits, and on the native runtime a year beyond the range of its time value wraps to a different instant; the Postgres `TIMESTAMPTZ` range begins in 4714 BC. A four-digit year keeps every accepted timestamp inside the range both backends store and every runtime reads alike, with an offset shifting it at most a day past either end. The same parser rolls an out-of-range field over -- 30 February to 2 March, hour 24 to the next day, a 60th second to the next minute -- so a record naming an impossible time would be read as a different, possible one. Refusing it leaves each accepted timestamp one instant, the one its fields name.

**Why refuse on append and read as well as at ingest (assertion A)?** The library writes every time it stamps in UTC, and an event built in code with a local time is written in UTC too, so every record the library writes carries an accepted timestamp. A stored record that does not was not written by this data format, and reading it would give the host-dependent instant the rule exists to prevent. At ingest the refusal comes before any write, like the other decode failures, so a malformed record leaves the log as it was.

**Why keep the keys a build does not read (assertion B)?** Within a data-format major, a later release may add optional fields (EVS-DEV-version-compatibility, Rationale for assertions A to D). The initiator and the two version maps are hashed as the record spells them, so a build that dropped a key it does not model would change the record's hash, and the copy it stored or relayed would no longer verify. Keeping them verbatim lets a record of a later minor verify, store and relay unchanged through an earlier build; the build reads the numbers and the fields it models and carries the rest. A top-level key outside the record's declared fields is not covered by the event hash (EVS-PRD-hash-chain-integrity, Rationale). It is kept, stored and forwarded as it arrived, so a field a later release adds there survives a relay, but nothing attests it: a hop can change or drop it undetected. A field that must be attested belongs inside a hashed structure -- the data, the metadata or the initiator -- which every build of the major hashes whole.

**Why the same form for `received_at` (assertion C)?** A provenance entry's `received_at` is the time a hop received the event, which audit review reads across hops; the same host-dependence and rollover that assertion A excludes for the client timestamp would make one hop's receipt time name different instants on different hosts. The entry decoder refuses it wherever an entry is read, and the event store refuses a record carrying one at the points assertion A names, so a malformed receipt time never enters the log.

**Why the database identity in every provenance entry (assertion D)?** A delivery channel's sender is the database, and the library decides by it which events a database authored (a destination enqueues only those), which ingest records as the receiver's own, which a recovery may admit as the sender's own, which origin chain an event belongs to, and which database a receiver's stamp belongs to. The `Source` identifier the entry already carries is attribution the application supplies and may reuse across databases. The identity is minted inside the database and checked at every open (EVS-DEV-event-store-open/F), so an entry that records it names the database that stamped it; the entry is covered by the event hash, so the name cannot be changed afterwards without the change being detected.

**Why the library version on every entry the library stamps (assertions E and F)?** Builds of one data-format major share a database: a canary, a rolling deploy, a rollback. The library-version events record only which builds opened it, in order, so the latest one before an event names the last open, not the build that appended the event. Recording the package version on the originator entry of each event it appends, and on the entry it stamps at ingest, recovery or restore, states per event which build appended it and which build stored it at each hop.

The application's own version stays in `software_version`. The data-format version the appending build declared is the record's `lib_format_version`. The recorded value is what the stamping build declares. In a build with assertions disabled that is the version compiled into it, since the test seam that substitutes a build declaration is never read there (EVS-DEV-destination-drain-lock/F). A build patched without changing its version records the version it claims.

**Why keep every provenance entry verbatim (assertion G)?** The receiver's hash, and every later hop's, covers the whole provenance list, and Chain 1 verification reconstructs each hop's hash from the list as that hop sealed it. An entry re-encoded from a decoded value can drop a key a later release added, add a null key, or re-spell a receipt time, and the reconstructed hash then fails; the `provenance` package's entry codec keeps every key it decodes for the same reason (EVS-PRD-provenance/E). Keeping entries as the record carries them lets later releases of the same data-format major add fields to an entry, and lets the library add its own entry without touching the ones before it. The storage-chain fields every stored event's last entry records (`EVS-DEV-chain-verification/C`) and the delivery stamp a receiver records (`EVS-DEV-delivery-receiver/H`) live in such entries.

**Why refuse a record without them (assertion H)?** Every build of this data format stamps both fields, so a record lacking either was not appended or ingested by one. It was written by an earlier data format, or outside the library. Storing no event for it at ingest, recovery or restore (the record is kept in full in a security finding, `EVS-DEV-security-findings`, and the rest of its delivery is admitted), and refusing it on append and read like the timestamp rules, keeps "every event in the log names the database and the build that stamped each of its entries" a checked property rather than a convention. A database an earlier data format wrote therefore fails its open's first read, and is refused as one that must be reset (EVS-DEV-event-store-open/F).

**Why hashed (assertion I)?** The event hash is an unkeyed digest. Covering the field makes a change to it that is made without recomputing the hash break the event's hash. A change made after a later hop, or a later event of the same origin chain, sealed a hash over the record breaks that later hash too. A hop that rewrites the field and recomputes every hash it forwards is not detected by the hash alone; it is detected only where a receiver already holds a hash sealed over the record before the change, such as the delivery chain or a later event's predecessor link. Assertions E and I are separate facts: omitting the field when stamping breaks no hash, and changing it after sealing breaks the hash. A record from which the field was removed is refused as malformed (assertion H) before its hash is checked, so a verification that the hash covers the field presents a changed value, not a missing field.

**Why keep the causal record unchanged, and hash it (assertions J and K)?** The causal record states which versions of its aggregate an event follows, as its author's database held them when it appended the event (EVS-DEV-causal-parents). Every holder resolves the parents against sealed hashes, so the record means the same everywhere only if every holder carries it as the author wrote it. It is a top-level field, and a top-level key outside the hashed fields is not attested (EVS-PRD-hash-chain-integrity, Rationale). So the record is added to the hash input: a hop that changed or dropped it would break the event's hash. The field is part of the data-format major step every change of this data format belongs to (EVS-DEV-version-compatibility/C), because a build whose hash input lacks it computes a different hash for the same record.

**Why a data-format major step for these fields.** The entry fields and the causal record are required, and a record without them is refused, so a build of an earlier minor of the same major would refuse, or re-encode without, what a later minor wrote. They therefore ride the data-format major step (EVS-DEV-version-compatibility/N).

## Changelog

- 2026-09-25 | d66285f6 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | H: ingest, recovery and restore store no event for a record whose provenance lacks database_id or library_version, keeping it in a security finding, instead of refusing the delivery; append and read still refuse it. No code or test references H
- 2026-09-25 | 33428cca | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of I: a record without `library_version` is refused as malformed before its hash is checked, so a verification of the hash's coverage changes the value
- 2026-09-25 | 33428cca | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: sync changelog hash
- 2026-09-25 | - | - | Michael Lewis (<michael@anspar.org>) | Rationale of G: the receiver's delivery stamp is stated by the receiver mechanics
- 2026-09-25 | 33428cca | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add D-K: every provenance entry the library stamps records the stamping database's identity and library_version, equal to the compiled package version with assertions disabled; every provenance entry and its keys are kept verbatim; a record whose entries lack either field is refused as malformed; library_version is hashed; the causal record is kept unchanged across storage, delivery, ingest, recovery and restore, and covered by the event hash. Purpose: the four things the record requirement fixes
- 2026-09-24 | 67431a47 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add C: a provenance entry's received_at takes the timestamp form of A
- 2026-09-24 | f24e0c04 | - | Michael Lewis (<michael@anspar.org>) | Auto-fix: update hash
- 2026-09-24 | - | - | Michael Lewis (<michael@anspar.org>) | Add A-B: a client timestamp carries a four-digit year, in-range calendar fields and an explicit offset; the initiator, the version maps and unread top-level keys are kept as the record carries them

*End* *The event record as read, stored and sent* | **Hash**: d66285f6
