## 0.2.0

* `ProvenanceEntry` models the optional `delivery` (`ProvenanceDelivery`): the channel (`sender_database_id`, `destination_id`, `registration_id`, `generation`) and the `delivery_number` a receiver ingested the event from. `fromJson` refuses a delivery object without exactly those keys, or with a generation or number below 1. It takes part in `==` and `hashCode`; an entry without it encodes no `delivery` key.
* `ProvenanceEntry` models the optional `libraryVersion` (`library_version`), the version of the library that stamped the entry, and `databaseId` (`database_id`), the identity of the database that stamped it. `fromJson` refuses either one when present but not a String, naming the field. Both take part in `==` and `hashCode`.
* An entry decoded with `fromJson` keeps its source JSON, and `toJson` emits exactly the decoded keys with every value unchanged: a key the type does not model survives, `transform_version` is not added when it was absent, and `received_at` keeps its original spelling. An entry built with the constructor encodes as before, plus the two new fields when set.
* `parseIso8601Instant(text)` parses an ISO 8601 date-time to the UTC instant it names, and throws `FormatException` unless it has a four-digit year (0000-9999), a month, day, hour, minute and second each within its calendar range (no 30 February, hour 24 or 60th second, which `DateTime.parse` rolls over), and an explicit offset (`Z` or `+/-HH[:]MM`) of at most 23:59.
* `ProvenanceEntry.fromJson` reads `received_at` with `parseIso8601Instant`: besides a `received_at` without an offset, it refuses one with a year outside 0000-9999 or a calendar field out of range, with a `FormatException` naming `received_at`.

## 0.1.0

First functional release. Implements REQ-d00115 (ProvenanceEntry Schema and Append Rules).

* `ProvenanceEntry` immutable value type with fields `hop`, `receivedAt`, `identifier`, `softwareVersion`, and optional `transformVersion`. JSON serialization uses snake_case keys. `fromJson` raises `FormatException` for missing or wrong-typed required fields, and rejects offsetless ISO 8601 `received_at` strings (the timezone-offset requirement is enforced to preserve ALCOA+ Contemporaneous across the audit chain). `toJson` emits a `transform_version: null` key rather than omitting the key when unset — wire consumers can distinguish *absent-because-null* from *absent-because-missing*. Value equality via `==` and `hashCode`. (REQ-d00115-C,D,E,F)
* `appendHop(chain, entry)` pure function returning a new `List.unmodifiable` with `entry` appended. Input chain is never mutated; returned list rejects further modification. (REQ-d00115-A,B)

This is the first release. Subsequent phases of CUR-1154 consume these types from within the event-sourcing pipeline; the package itself remains pure Dart with no Flutter dependency so the same code can be reused on any downstream server.
