// Implements: EVS-PRD-event-log/A
// EntryTypeDefinition is the static schema
//   metadata the substrate uses to classify each event appended to the
//   append-only log.
// Implements: EVS-DEV-append-stamps-registered-version/A
// the
//   registeredVersion field is the source value that EventStore.append
//   stamps onto every appended event's entryTypeVersion field.
// Implements: EVS-DEV-append-stamps-registered-version/C
// registeredVersion
//   is owned by this definition and the EntryTypeRegistry; it does not
//   appear on the public append/appendInTxn signatures.

/// Metadata describing one entry type supported by the event store.
///
/// An `EntryTypeDefinition` is pure data (no storage, no Flutter dependency)
/// that participates in the Event Type Registry. It identifies the entry type
/// by `id`, binds it to a registered schema version (`registeredVersion`),
/// and controls whether a materializer runs for events of this type
/// (`isMaterialized`).
///
/// JSON serialization uses snake_case keys:
/// `id`, `registered_version`, `name`, `materialize`.
///
class EntryTypeDefinition {
  const EntryTypeDefinition({
    required this.id,
    required this.registeredVersion,
    required this.name,
    this.isMaterialized = true,
  });

  /// Matches `event.entry_type` for every event of this entry type.
  final String id;

  /// Highest `entry_type_version` this lib build's registry accepts on
  /// `EventStore.ingestBatch`. Currently a single version per entry type;
  /// ingest rejects events whose `entry_type_version` exceeds this value.
  final int registeredVersion;

  /// Display name used by operational tooling.
  final String name;

  /// When `false`, no materializer runs for events of this entry type.
  /// Used by reserved system entry types (e.g., `security_context_redacted`)
  /// that must land in the event log as immutable audit rows but write no
  /// view state. Defaults to `true`.
  final bool isMaterialized;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is EntryTypeDefinition &&
          id == other.id &&
          registeredVersion == other.registeredVersion &&
          name == other.name &&
          isMaterialized == other.isMaterialized;

  @override
  int get hashCode => Object.hash(id, registeredVersion, name, isMaterialized);

  @override
  String toString() =>
      'EntryTypeDefinition('
      'id: $id, registeredVersion: $registeredVersion, name: $name, '
      'isMaterialized: $isMaterialized)';
}
