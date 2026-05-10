// event_sourcing/lib/src/projections/primitives/row_key.dart
import 'package:event_sourcing/src/storage/stored_event.dart';

sealed class RowKeyExtractor {
  const RowKeyExtractor();
  Object extract(StoredEvent event);
}

class AggregateIdKey extends RowKeyExtractor {
  const AggregateIdKey();
  @override
  Object extract(StoredEvent event) => event.aggregateId;
}

class CompositeKey extends RowKeyExtractor {
  /// Each path is dotted; the first segment is one of `data`, `metadata`,
  /// or a top-level field on the StoredEvent. The remaining segments
  /// index into the value.
  final List<String> paths;
  const CompositeKey(this.paths);

  @override
  Object extract(StoredEvent event) {
    final parts = <String>[];
    for (final path in paths) {
      final v = _resolve(event, path);
      if (v == null) {
        throw StateError(
          'CompositeKey: required path "$path" did not resolve on event '
          '${event.eventId}',
        );
      }
      parts.add(v.toString());
    }
    return parts.join('|');
  }

  static Object? _resolve(StoredEvent event, String path) {
    final segs = path.split('.');
    if (segs.isEmpty) return null;
    Object? root;
    switch (segs.first) {
      case 'data':
        root = event.data;
      case 'aggregateId':
        return event.aggregateId;
      case 'eventType':
        return event.eventType;
      default:
        root = event.data;
      // Fall through and treat full path as data.X
    }
    final rest = segs.first == 'data' ? segs.skip(1) : segs;
    Object? cur = root;
    for (final seg in rest) {
      if (cur is! Map) return null;
      if (!cur.containsKey(seg)) return null;
      cur = cur[seg];
    }
    return cur;
  }
}
