// Implements: EVS-PRD-cross-process-event-transport/A — JSON codec for
//   the Update<T> family (Snapshot, Delta, Tombstone, EndOfReplay) with
//   field-level round-trip preservation.
// Implements: EVS-PRD-cross-process-event-transport/B — each envelope
//   carries sequence + subscriptionId in the encoded shape.
// Implements: EVS-PRD-cross-process-event-transport/G — ships raw rows
//   as Map<String, Object?>; consumer-supplied mapper applies client-
//   side so Layer-2 conventions stay equivalent across Local/Remote.

import 'package:event_sourcing/event_sourcing.dart';

import 'envelope.dart';

/// JSON codec for the substrate's [Update<T>] family. The wire ships
/// rows as opaque `Map<String, Object?>`; the client applies its
/// consumer-supplied mapper after decode.
///
/// Wire shape per variant (all include "type", "subscriptionId",
/// "sequence"):
///   snapshot      + "value" (nullable map; the materialized row, or
///                            null when the substrate emitted a null
///                            value)
///   delta         + "value" (map) + "cause" (event id that caused
///                            this delta)
///   tombstone     + "aggregateId"
///   end_of_replay (no extra fields)
///
/// [Snapshot.value] and [Delta.value] carry the mapped row, which by
/// `AggregateProjectionSpec` convention contains the aggregateId as a
/// column — so the wire does not duplicate aggregateId for those
/// variants. [Tombstone] has no row, so it ships aggregateId
/// explicitly.
class UpdateCodec {
  const UpdateCodec._();

  static Map<String, Object?> encode(
    Update<Map<String, Object?>> u, {
    required String subscriptionId,
  }) {
    if (u is Snapshot<Map<String, Object?>>) {
      return {
        'type': 'snapshot',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
        'value': u.value,
      };
    } else if (u is Delta<Map<String, Object?>>) {
      return {
        'type': 'delta',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
        'value': u.value,
        'cause': u.cause,
      };
    } else if (u is Tombstone<Map<String, Object?>>) {
      return {
        'type': 'tombstone',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
        'aggregateId': u.aggregateId,
      };
    } else if (u is EndOfReplay<Map<String, Object?>>) {
      return {
        'type': 'end_of_replay',
        'subscriptionId': subscriptionId,
        'sequence': u.sequence,
      };
    } else {
      throw FormatException('unknown Update<T> type: ${u.runtimeType}');
    }
  }

  static Update<Map<String, Object?>> decode(Map<String, Object?> json) {
    final type = readType(json);
    final sequence = requireInt(json, 'sequence');
    switch (type) {
      case 'snapshot':
        return Snapshot<Map<String, Object?>>(
          value: _readNullableMap(json, 'value'),
          sequence: sequence,
        );
      case 'delta':
        return Delta<Map<String, Object?>>(
          value: requireMap(json, 'value'),
          sequence: sequence,
          cause: requireString(json, 'cause'),
        );
      case 'tombstone':
        return Tombstone<Map<String, Object?>>(
          aggregateId: requireString(json, 'aggregateId'),
          sequence: sequence,
        );
      case 'end_of_replay':
        return EndOfReplay<Map<String, Object?>>(sequence: sequence);
      default:
        throw FormatException('unknown update type: $type');
    }
  }

  /// Peeks at the subscriptionId without fully decoding the envelope.
  /// Used by RemoteConnection to route incoming envelopes.
  static String subscriptionIdOf(Map<String, Object?> json) =>
      requireString(json, 'subscriptionId');

  /// Reads a nullable map field. Returns null when the key is present
  /// with a null value (the explicit-null case for [Snapshot.value]).
  /// Throws on a present-but-non-map value.
  static Map<String, Object?>? _readNullableMap(
    Map<String, Object?> json,
    String key,
  ) {
    if (!json.containsKey(key)) {
      throw FormatException('missing "$key"');
    }
    final v = json[key];
    if (v == null) return null;
    if (v is! Map) {
      throw FormatException('non-map "$key": $v');
    }
    return Map<String, Object?>.from(v);
  }
}
