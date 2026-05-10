/// Metadata describing one entry type supported by the event store.
///
/// An `EntryTypeDefinition` is pure data (no storage, no Flutter dependency)
/// that participates in the Event Type Registry. It identifies the entry type
/// by `id`, binds it to a registered schema version (`registeredVersion`),
/// and controls whether a materializer runs for events of this type
/// (`materialize`).
///
/// JSON serialization uses snake_case keys:
/// `id`, `registered_version`, `name`, `materialize`.
///
// Implements: REQ-d00116-A+B+C — value type carrying the three core fields.
class EntryTypeDefinition {
  const EntryTypeDefinition({
    required this.id,
    required this.registeredVersion,
    required this.name,
    this.materialize = true,
  });

  // Implements: REQ-d00116-A+B+C — decode from snake_case JSON; reject
  // payloads missing any of the three required fields or with wrong types.
  factory EntryTypeDefinition.fromJson(Map<String, Object?> json) {
    final id = _requireString(json, 'id');
    final registeredVersion = _requireInt(json, 'registered_version');
    final name = _requireString(json, 'name');

    final materializeRaw = json['materialize'];
    if (materializeRaw != null && materializeRaw is! bool) {
      throw const FormatException(
        'EntryTypeDefinition: "materialize" must be a bool when present',
      );
    }

    return EntryTypeDefinition(
      id: id,
      registeredVersion: registeredVersion,
      name: name,
      materialize: (materializeRaw as bool?) ?? true,
    );
  }

  /// Matches `event.entry_type` for every event of this entry type.
  final String id;

  /// Highest `entry_type_version` this lib build's registry accepts on
  /// `EventStore.ingestBatch`. Today (single-version world) it's the only
  /// value; Phase 4.21 may expand to a `Set<int>` for multi-sponsor concurrency.
  // Implements: REQ-d00116-B.
  final int registeredVersion;

  /// Display name used by operational tooling.
  final String name;

  /// When `false`, no materializer runs for events of this entry type.
  /// Used by reserved system entry types (e.g., `security_context_redacted`)
  /// that must land in the event log as immutable audit rows but write no
  /// view state. Defaults to `true`.
  // Implements: REQ-d00140-C — def.materialize=false skips all materializers.
  final bool materialize;

  Map<String, Object?> toJson() => <String, Object?>{
    'id': id,
    'registered_version': registeredVersion,
    'name': name,
    'materialize': materialize,
  };

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is EntryTypeDefinition &&
          id == other.id &&
          registeredVersion == other.registeredVersion &&
          name == other.name &&
          materialize == other.materialize;

  @override
  int get hashCode => Object.hash(id, registeredVersion, name, materialize);

  @override
  String toString() =>
      'EntryTypeDefinition('
      'id: $id, registeredVersion: $registeredVersion, name: $name, '
      'materialize: $materialize)';
}

String _requireString(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is! String) {
    throw FormatException('EntryTypeDefinition: missing or non-string "$key"');
  }
  return value;
}

int _requireInt(Map<String, Object?> json, String key) {
  final value = json[key];
  if (value is! int) {
    throw FormatException('EntryTypeDefinition: missing or non-int "$key"');
  }
  return value;
}
