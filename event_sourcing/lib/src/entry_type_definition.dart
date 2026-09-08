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
/// by `id` and binds it to a registered schema version
/// (`registeredVersion`).
///
/// A definition does not decide which views an entry type reaches. That is a
/// projection's own declaration, made through its interest filter, so the
/// same entry type can feed one view and be absent from another.
///
/// Definitions are not compared to one another. Whether the registry has
/// changed between boots is answered by the bootstrap audit event, whose
/// content hash covers the canonicalized id-to-version map — a cryptographic
/// comparison at the level that matters, rather than a field-wise one here.
///
/// JSON serialization uses snake_case keys:
/// `id`, `registered_version`, `name`, `materialize`.
///
class EntryTypeDefinition {
  const EntryTypeDefinition({
    required this.id,
    required this.registeredVersion,
    required this.name,
  });

  /// Matches `event.entry_type` for every event of this entry type.
  final String id;

  /// Highest `entry_type_version` this lib build's registry accepts on
  /// `EventStore.ingestBatch`. Currently a single version per entry type;
  /// ingest rejects events whose `entry_type_version` exceeds this value.
  final int registeredVersion;

  /// Display name used by operational tooling.
  final String name;

  @override
  String toString() =>
      'EntryTypeDefinition('
      'id: $id, registeredVersion: $registeredVersion, name: $name)';
}
