// Implements: EVS-DEV-destination-drain/F
// the declared configuration of a destination and its fingerprint: the
//   declared fields that shape the destination's queue items, with every set
//   sorted, and the configuration version the delivery cycle was started
//   with; the fingerprint is the SHA-256 of its canonical JSON.
import 'dart:convert';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:crypto/crypto.dart';
import 'package:event_sourcing/src/destinations/destination.dart';

/// The declared configuration of [destination] under [configurationVersion]:
/// the fields of the destination that shape its queue items and that the
/// library can read, as a JSON map. Every set is sorted, so two equal
/// filters built in different insertion orders declare equal maps.
///
/// Its keys are `id`, `wire_format`, `serializes_natively`,
/// `max_accumulate_time_micros`, `filter` (`entry_types`, `event_types` and
/// `aggregate_types`, each sorted or null; `include_system_events`;
/// `has_predicate`) and `configuration_version`. The hard-delete opt-in is
/// not an input: it shapes no queue item.
///
/// Code the library cannot read -- the transform, the filter's predicate,
/// the batching rule -- is not in the map. A deployment that changes such
/// code changes [configurationVersion] with it (a build or revision
/// identifier does).
Map<String, Object?> declaredConfiguration(
  Destination destination,
  String? configurationVersion,
) {
  final filter = destination.filter;
  List<String>? sorted(Set<String>? values) =>
      values == null ? null : (values.toList()..sort());
  return <String, Object?>{
    'id': destination.id,
    'wire_format': destination.wireFormat,
    'serializes_natively': destination.serializesNatively,
    'max_accumulate_time_micros': destination.maxAccumulateTime.inMicroseconds,
    'filter': <String, Object?>{
      'entry_types': sorted(filter.entryTypes),
      'event_types': sorted(filter.eventTypes),
      'aggregate_types': sorted(filter.aggregateTypes),
      'include_system_events': filter.includeSystemEvents,
      'has_predicate': filter.predicate != null,
    },
    'configuration_version': configurationVersion,
  };
}

/// The fingerprint of a declared [configuration]: the lowercase hex SHA-256
/// of its RFC 8785 canonical JSON.
String configurationFingerprint(Map<String, Object?> configuration) =>
    sha256.convert(utf8.encode(canonicalize(configuration))).toString();
