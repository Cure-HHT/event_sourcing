/// Constructor-time identity of the process writing events. Stamps
/// `metadata.provenance[0]` on every event written through `EventStore`.
///
/// `identifier` is the per-installation unique identity.
/// Production callers MUST persist a globally-unique value (UUIDv4
/// recommended) on first install and pass the same value on every
/// subsequent bootstrap. The library does NOT validate the format at
/// runtime — callers that violate the global-uniqueness requirement get
/// correct lib behavior on each install in isolation but produce data
/// that collides on receivers when bridged. System audit aggregate_ids
/// equal `source.identifier`, so two installs that share an identifier share
/// a system aggregate on any receiver they both bridge to.
///
/// `hopId` is the role-class string (e.g. `'mobile-device'`,
/// `'relay-server'`). Two installations of the same
/// role class are distinct originators — discrimination on
/// `EventStore.isLocallyOriginated` compares
/// `identifier`, not `hopId`.
// Implements: EVS-PRD-event-log/C — per-aggregate-per-authority order; Source
//   identifies the authority whose events must stay ordered within an aggregate.
// Implements: EVS-PRD-portability/C — pure Dart value; no platform dependency.
class Source {
  const Source({
    required this.hopId,
    required this.identifier,
    required this.softwareVersion,
  });

  final String hopId;
  final String identifier;
  final String softwareVersion;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      (other is Source &&
          hopId == other.hopId &&
          identifier == other.identifier &&
          softwareVersion == other.softwareVersion);

  @override
  int get hashCode => Object.hash(hopId, identifier, softwareVersion);

  @override
  String toString() =>
      'Source(hopId: $hopId, identifier: $identifier, '
      'softwareVersion: $softwareVersion)';
}
