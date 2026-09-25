// The view-queue invariant of the default destination-wedges view
// (EVS-PRD-destinations, assertions S and T): the view holds a row for a
// destination of the store's own database exactly while that destination's
// queue head is wedged, and its rows are the ones a replay of the whole log
// derives. It runs after the operations of the wedge, recovery, deletion
// and view scenario files on both backends, whose tests carry the
// citations; a path that ends (or starts) a wedge without appending its
// event fails every file that runs it.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kDestinationAuditAggregateType, kReservedEventShapes;
import 'package:flutter_test/flutter_test.dart';

/// Every row of the default destination-wedges view, keyed by row key.
Future<Map<String, Map<String, Object?>>> wedgesViewRows(
  StorageBackend backend,
) async => <String, Map<String, Object?>>{
  for (final row in await backend.findViewRows(
    defaultDestinationWedgesSpec.viewName,
  ))
    row['aggregateId']! as String: Map<String, Object?>.of(row),
};

/// The three entry types the default view folds, at [store]'s registered
/// versions: the target map a rebuild of the view takes.
Map<String, EntryTypeVersion> wedgesViewTargets(EventStore store) =>
    <String, EntryTypeVersion>{
      for (final id in <String>[
        kDestinationWedgedEntryType,
        kDestinationWedgeRecoveredEntryType,
        kDestinationDeletedEntryType,
      ])
        id: store.entryTypes.byId(id)!.registeredVersion,
    };

/// Asserts the view-queue invariant on [store]:
///
/// 1. the view's rows whose `database_id` is [store]'s own, as
///    (destination, item) pairs, equal the wedged queue heads `wedgedFifos`
///    reports;
/// 2. the view's rows, peer rows included, equal the rows a rebuild of the
///    view from the whole log derives;
/// 3. in [store]'s log, every recovery event naming [store]'s own database
///    follows a wedge event naming the same database, destination and item.
Future<void> expectWedgesViewMatchesQueue(EventStore store) async {
  final backend = store.backend;
  final wedgedItems = <(String, String)>{};
  for (final event in await backend.findAllEvents()) {
    if (event.entryType != kDestinationWedgedEntryType &&
        event.entryType != kDestinationWedgeRecoveredEntryType) {
      continue;
    }
    if (event.data['database_id'] != store.databaseId) continue;
    final item = (
      event.data['id']! as String,
      (event.data['row_id'] ?? '') as String,
    );
    if (event.entryType == kDestinationWedgedEntryType) {
      wedgedItems.add(item);
    } else if (event.entryType == kDestinationWedgeRecoveredEntryType) {
      expect(
        wedgedItems,
        contains(item),
        reason:
            'recovery ${event.eventId} follows a wedge event naming the same '
            'database, destination and item',
      );
    }
  }
  final before = await wedgesViewRows(backend);
  final local = <(String, String)>{
    for (final row in before.values)
      if (row['database_id'] == store.databaseId)
        (row['id']! as String, row['row_id']! as String),
  };
  final wedged = <(String, String)>{
    for (final summary in await backend.wedgedFifos())
      (summary.destinationId, summary.headEntryId),
  };
  expect(
    local,
    wedged,
    reason:
        'the local rows of the default destination-wedges view name '
        'exactly the wedged queue heads',
  );
  for (final entry in before.entries) {
    expect(
      entry.key,
      '${entry.value['database_id']}|${entry.value['id']}',
      reason: 'a row is keyed by its database identity and destination',
    );
  }
  await rebuildView(
    store: store,
    viewName: defaultDestinationWedgesSpec.viewName,
    targetVersionByEntryType: wedgesViewTargets(store),
  );
  expect(
    await wedgesViewRows(backend),
    before,
    reason: 'a replay of the whole log derives the same rows',
  );
}

/// Asserts every event of a reserved entry type in [store]'s log was
/// appended with the aggregate type and an event type the library declares
/// for its entry type, and every destination audit carries a destination
/// identifier and a database identity.
Future<void> expectReservedShapes(EventStore store) async {
  for (final event in await store.backend.findAllEvents()) {
    if (!kReservedSystemEntryTypeIds.contains(event.entryType)) continue;
    final shape = kReservedEventShapes[event.entryType];
    expect(shape, isNotNull, reason: event.entryType);
    expect(event.aggregateType, shape!.aggregateType, reason: event.eventId);
    expect(shape.eventTypes, contains(event.eventType), reason: event.eventId);
    if (event.aggregateType == kDestinationAuditAggregateType) {
      expect(event.data['id'], isA<String>(), reason: event.eventId);
      expect(event.data['database_id'], isA<String>(), reason: event.eventId);
    }
  }
}
