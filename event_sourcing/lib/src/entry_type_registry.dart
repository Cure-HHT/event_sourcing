// Implements: EVS-PRD-event-log/A — EntryTypeRegistry is the substrate's
//   authoritative mapping from entry_type id to EntryTypeDefinition; it
//   enforces that every event appended to the append-only log corresponds
//   to a known, registered entry type.
// Implements: EVS-DEV-append-stamps-registered-version/A — byId(entryType)
//   is the lookup path that EventStore.append calls to retrieve the
//   registeredVersion it stamps on every appended event.

import 'package:event_sourcing/src/entry_type_definition.dart';

/// Process-wide registry mapping `entry_type` ids to `EntryTypeDefinition`.
///
/// `EntryService.record` validates the incoming `entryType` through this
/// registry before opening a write transaction; widgets and
/// materialization paths also read through it to resolve per-type metadata
/// (`effective_date_path`, `widget_id`). Centralizing registrations on one
/// object is how an app keeps "the set of known entry types" a single
/// authority rather than a ledger scattered across widgets.
///
/// This minimal surface — `register`, `byId`, `isRegistered`, `all` — covers
/// all entry-type validation needs for the event store.
class EntryTypeRegistry {
  /// Register [defn]. Duplicate id is a configuration bug — silent
  /// shadowing would let an app declare two competing definitions for the
  /// same entry type and the later one would silently win — so it is
  /// surfaced loudly via `ArgumentError`.
  void register(EntryTypeDefinition defn) {
    if (_defs.containsKey(defn.id)) {
      throw ArgumentError.value(
        defn.id,
        'defn.id',
        'EntryTypeDefinition "${defn.id}" already registered',
      );
    }
    _defs[defn.id] = defn;
  }

  /// Returns the `EntryTypeDefinition` registered under [id], or `null`
  /// when no registration matches. Callers distinguish "unknown type"
  /// from "registered" with a null check.
  EntryTypeDefinition? byId(String id) => _defs[id];

  /// True iff a definition is registered under [id]. Convenience
  /// wrapper over `byId != null` for call sites that only need the
  /// yes/no answer.
  bool isRegistered(String id) => _defs.containsKey(id);

  /// Every currently-registered `EntryTypeDefinition`, in registration
  /// order. Returned list is unmodifiable so callers cannot mutate the
  /// registry by mutating the view.
  List<EntryTypeDefinition> all() =>
      List<EntryTypeDefinition>.unmodifiable(_defs.values);

  final Map<String, EntryTypeDefinition> _defs =
      <String, EntryTypeDefinition>{};
}
