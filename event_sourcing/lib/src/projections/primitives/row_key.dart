// event_sourcing/lib/src/projections/primitives/row_key.dart
//
// Implements: EVS-PRD-materializer/A — RowKeyExtractor (AggregateIdKey,
//   CompositeKey) and ListedRowKeys are library-supplied primitives that
//   determine how the TableProjectionSpec fold identifies rows; they are
//   part of the library's materializer rule set.
// Implements: EVS-PRD-materializer/B
// each extractor is a pure function of
//   the StoredEvent; same event always yields the same key.
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
  const CompositeKey(this.paths);

  /// Each path is dotted; the first segment is one of `data`, `metadata`,
  /// or a top-level field on the StoredEvent. The remaining segments
  /// index into the value.
  final List<String> paths;

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

/// The keys of the rows one event names in a list: for each element of the
/// list in the event's data field [listField], the key [paths] resolve,
/// joined with `|` as [CompositeKey] joins them. A path beginning with
/// `item.` resolves inside the element; every other path resolves on the
/// event as a [CompositeKey] path does.
///
/// An event that carries no [listField], or null there, names no row.
class ListedRowKeys {
  const ListedRowKeys({
    required this.eventTypes,
    required this.listField,
    required this.paths,
  });

  /// The event types whose events name rows this way.
  final Set<String> eventTypes;

  /// The data field holding the list, each element an object.
  final String listField;

  /// The key paths, `item.`-prefixed ones resolving in each element.
  final List<String> paths;

  /// The keys of the rows [event] names, in list order; throws [StateError]
  /// when the list is not a list of objects or a path does not resolve.
  List<String> extract(StoredEvent event) {
    final list = event.data[listField];
    if (list == null) return const <String>[];
    if (list is! List) {
      throw StateError(
        'ListedRowKeys: field "$listField" on event ${event.eventId} is not '
        'a List (got ${list.runtimeType})',
      );
    }
    final keys = <String>[];
    for (final element in list) {
      if (element is! Map) {
        throw StateError(
          'ListedRowKeys: an element of "$listField" on event '
          '${event.eventId} is not an object',
        );
      }
      final parts = <String>[];
      for (final path in paths) {
        final Object? v;
        if (path.startsWith('item.')) {
          final field = path.substring('item.'.length);
          v = element.containsKey(field) ? element[field] : null;
        } else {
          v = CompositeKey._resolve(event, path);
        }
        if (v == null) {
          throw StateError(
            'ListedRowKeys: required path "$path" did not resolve on an '
            'element of "$listField" on event ${event.eventId}',
          );
        }
        parts.add(v.toString());
      }
      keys.add(parts.join('|'));
    }
    return keys;
  }
}
