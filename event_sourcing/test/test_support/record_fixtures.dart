// Test helper: the fields a well-formed hand-built event record carries
// beside its own: the causal object, and the database identity and library
// version of each provenance entry. This file declares no tests, so it
// carries no citation.
import 'package:event_sourcing/event_sourcing.dart';

/// The database identity a hand-built peer record names as the database
/// that stamped its provenance entries.
const String kPeerDatabaseId = 'peer-database';

/// The library version a hand-built record names as the version that
/// stamped its provenance entries.
const String kPeerLibraryVersion = '0.0.0-peer';

/// The causal object of an aggregate's first version: an eligible version
/// with no parents.
const Map<String, Object?> kRootVersionCausalJson = <String, Object?>{
  'kind': 'version',
  'eligible': true,
  'parents': <Object?>[],
};

/// [kRootVersionCausalJson] decoded.
final CausalRecord kRootVersionCausal = CausalRecord.fromJson(
  kRootVersionCausalJson,
);

/// A provenance entry map [entry] with `database_id` and `library_version`
/// added, each defaulting to the peer's.
Map<String, Object?> stampedEntryJson(
  Map<String, Object?> entry, {
  String databaseId = kPeerDatabaseId,
  String libraryVersion = kPeerLibraryVersion,
}) => <String, Object?>{
  ...entry,
  'database_id': databaseId,
  'library_version': libraryVersion,
};

/// [record] as a build of data format 2.0 stored or sent it: its
/// `lib_format_version` is 2.0, its provenance entries carry no
/// `database_id` and no `library_version`, and it carries no `causal`
/// object. Its `event_hash` is left as it was.
Map<String, Object?> dataFormat2Record(Map<String, Object?> record) {
  final metadata = Map<String, Object?>.from(
    (record['metadata'] as Map?) ?? const <String, Object?>{},
  );
  final provenance = metadata['provenance'];
  if (provenance is List) {
    metadata['provenance'] = <Object?>[
      for (final entry in provenance)
        if (entry is Map)
          Map<String, Object?>.from(entry)
            ..remove('database_id')
            ..remove('library_version')
        else
          entry,
    ];
  }
  return Map<String, Object?>.from(record)
    ..remove('causal')
    ..['lib_format_version'] = const DataFormatVersion(2, 0).toJson()
    ..['metadata'] = metadata;
}
