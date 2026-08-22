// Verifies: EVS-PRD-provenance/A
// ProvenanceEntry records the hop's
//   identifier; isLocallyOriginated compares provenance[0].identifier to
//   the store's source.identifier, not the hop class.  Two installs of the
//   same hop class with different identifiers are distinguished correctly.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<EventStore> _bootstrap({
  required String identifier,
  String hopId = 'mobile-device',
}) async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'is-locally-originated-${DateTime.now().microsecondsSinceEpoch}-'
    '$identifier.db',
  );
  final backend = SembastBackend(database: db);
  final ds = await bootstrapEventStore(
    backend: backend,
    source: Source(
      hopId: hopId,
      identifier: identifier,
      softwareVersion: 'my_app@1.0.0',
    ),
    entryTypes: <EntryTypeDefinition>[
      const EntryTypeDefinition(
        id: 'epistaxis_event',
        registeredVersion: 1,
        name: 'Epistaxis Event',
      ),
    ],
    destinations: const <Destination>[],
  );
  return ds.eventStore;
}

void main() {
  // because its provenance[0].identifier matches the EventStore's
  // source.identifier.
  test('locally-appended event is recognized as local', () async {
    final eventStore = await _bootstrap(identifier: 'install-A');
    final appended = await eventStore.append(
      entryType: 'epistaxis_event',
      aggregateId: 'agg-1',
      aggregateType: 'note',
      eventType: 'finalized',
      data: const <String, Object?>{
        'answers': <String, Object?>{'severity': 'mild'},
      },
      initiator: const UserInitiator('u1'),
    );
    expect(appended, isNotNull);
    expect(eventStore.isLocallyOriginated(appended!), isTrue);
  });

  // a different install of the same hop class is NOT locally originated. The
  // comparison is on install identity, not hop class.
  test('different install identifier is NOT locally originated', () async {
    // Local install is install-A; ingested event came from install-B (a
    // different mobile device).
    final localStore = await _bootstrap(identifier: 'install-A');
    final foreignStore = await _bootstrap(identifier: 'install-B');

    final foreign = await foreignStore.append(
      entryType: 'epistaxis_event',
      aggregateId: 'agg-foreign',
      aggregateType: 'note',
      eventType: 'finalized',
      data: const <String, Object?>{
        'answers': <String, Object?>{'severity': 'mild'},
      },
      initiator: const UserInitiator('u2'),
    );
    expect(foreign, isNotNull);
    // Both originate on hopId 'mobile-device' but distinct installs:
    // foreign event must NOT be considered local on install-A's store.
    expect(localStore.isLocallyOriginated(foreign!), isFalse);
  });
}
