// Whether a library-generated event reaches a view is decided by the
// projection's own interest filter, not by anything on the entry type. A
// projection that does not opt in sees none of them; one that opts in sees
// them. Nothing in the substrate makes an event unviewable — an audit view is
// a legitimate consumer of exactly these events.
//
// Verifies: EVS-PRD-event-log/F
// Verifies: EVS-PRD-event-log/A
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'install-view-admission-test',
  softwareVersion: 'pkg@0.0.1',
);

AggregateProjectionSpec _spec({required bool includeSystemEvents}) =>
    AggregateProjectionSpec(
      viewName: 'audit',
      interest: SubscriptionFilter(includeSystemEvents: includeSystemEvents),
      tombstoneEventTypes: const <String>{},
    );

Future<List<Map<String, dynamic>>> _bootAndRead({
  required bool includeSystemEvents,
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'view-admission-$includeSystemEvents.db',
  );
  final backend = SembastBackend(database: db);
  try {
    await bootstrapEventStore(
      backend: backend,
      source: _source,
      entryTypes: const <EntryTypeDefinition>[],
      destinations: const <Destination>[],
      projections: ProjectionRegistry()
        ..register(_spec(includeSystemEvents: includeSystemEvents)),
    );
    // Bootstrap itself emits library-generated events (lib-version boot,
    // entry-type registry snapshot), so no user append is needed.
    return backend.findViewRows('audit');
  } finally {
    await backend.close();
  }
}

void main() {
  group('library-generated events reach a view only when it opts in', () {
    test(
      'a projection that does not opt in materializes none of them',
      () async {
        final rows = await _bootAndRead(includeSystemEvents: false);
        expect(rows, isEmpty);
      },
    );

    test('a projection that opts in materializes them', () async {
      final rows = await _bootAndRead(includeSystemEvents: true);
      expect(
        rows,
        isNotEmpty,
        reason:
            'an audit view opting in must receive library-generated '
            'events; the substrate does not make them unviewable',
      );
    });

    // The reserved id set is what a filter discriminates on, so its size is a
    // deliberate decision rather than an incidental one. A change here means a
    // library-generated entry type was added; confirm it belongs in the set
    // that filters gate.
    test('the reserved id set is the discriminator and has 14 members', () {
      expect(kReservedSystemEntryTypeIds, hasLength(14));
    });

    test('reserved ids are registered so a filter can resolve them', () {
      final byId = {for (final d in kSystemEntryTypes) d.id: d};
      for (final id in <String>['ingest-audit', 'view_snapshot_promoted']) {
        expect(byId.containsKey(id), isTrue, reason: '$id must be registered');
        expect(byId[id]!.registeredVersion, 1);
      }
    });
  });
}
