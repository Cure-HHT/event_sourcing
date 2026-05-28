// Implements: EVS-PRD-cross-process-event-transport/A — JSON codec for
//   SubscriptionFilter (carried inside SubscribeMsg envelopes).

import 'package:event_sourcing/event_sourcing.dart';

/// JSON codec for [SubscriptionFilter]. Encodes the three allow-list
/// fields plus the `includeSystemEvents` flag. The `sources` field is
/// omitted in v1 per the single-source commitment. The `predicate`
/// field is a Dart closure and cannot be serialized; it is dropped on
/// encode (decoded remote filters always have `predicate == null`).
/// See `spec/reaction-remote.md` Future work for the named-predicate
/// registry path if cross-process predicates ever become needed.
///
/// Wire shape (all fields optional; absent means "no filter for this dim"):
///   {"entryTypes": ["..."],
///    "eventTypes": ["..."],
///    "aggregateTypes": ["..."],
///    "includeSystemEvents": true}   // omitted when false (the default)
class FilterCodec {
  const FilterCodec._();

  static Map<String, Object?> encode(SubscriptionFilter f) {
    final out = <String, Object?>{};
    if (f.entryTypes != null) {
      out['entryTypes'] = f.entryTypes!.toList()..sort();
    }
    if (f.eventTypes != null) {
      out['eventTypes'] = f.eventTypes!.toList()..sort();
    }
    if (f.aggregateTypes != null) {
      out['aggregateTypes'] = f.aggregateTypes!.toList()..sort();
    }
    if (f.includeSystemEvents) {
      out['includeSystemEvents'] = true;
    }
    return out;
  }

  static SubscriptionFilter decode(Map<String, Object?> json) {
    Set<String>? readSet(String key) {
      final v = json[key];
      if (v == null) return null;
      if (v is! List) {
        throw FormatException('non-list "$key": $v');
      }
      return v.cast<String>().toSet();
    }

    final ise = json['includeSystemEvents'];
    return SubscriptionFilter(
      entryTypes: readSet('entryTypes'),
      eventTypes: readSet('eventTypes'),
      aggregateTypes: readSet('aggregateTypes'),
      includeSystemEvents: ise == true,
    );
  }
}
