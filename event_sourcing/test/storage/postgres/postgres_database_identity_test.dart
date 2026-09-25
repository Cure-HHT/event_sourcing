// Concurrent opens of one Postgres database: first opens record one
// identity in one initialization, and concurrent opens of a build that
// changes the recorded version record one change; gated on PG_TEST_URL.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart'
    show LibVersionEvents;
import 'package:test/test.dart';

import '../../test_support/lib_version_seed.dart';
import 'test_postgres_url.dart';

Future<EventStore> _open(PostgresBackend backend, String identifier) {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      PostgresSecurityContextStore(backend: backend),
    ),
    entryTypes: registry,
    source: Source(
      hopId: 'identity-hop',
      identifier: identifier,
      softwareVersion: 'identity-test',
    ),
  );
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment();
  if (db != null) tearDownAll(db.drop);
  final backends = <PostgresBackend>[];

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL unset');
      return;
    }
    await db.reset();
    // Opened one after the other, so the schema DDL runs once.
    for (var i = 0; i < 2; i++) {
      backends.add(await db.open(provision: true));
    }
  });

  tearDown(() async {
    for (final backend in backends) {
      await backend.close();
    }
    backends.clear();
  });

  // Verifies: EVS-DEV-event-store-open/E
  // Verifies: EVS-DEV-event-store-open/F
  test('two concurrent first opens record one identity in exactly one '
      'initialization', () async {
    if (db == null) return;
    final stores = await Future.wait(<Future<EventStore>>[
      _open(backends[0], 'install-a'),
      _open(backends[1], 'install-b'),
    ]);
    expect(stores[1].databaseId, stores[0].databaseId);
    final initialized = await backends[0].findAllEvents(
      entryType: LibVersionEvents.initialized,
    );
    expect(initialized, hasLength(1));
    expect(initialized.single.data['database_id'], stores[0].databaseId);
    expect(
      await backends[0].findAllEvents(entryType: LibVersionEvents.changed),
      isEmpty,
    );
  });

  // Verifies: EVS-DEV-event-store-open/C
  // Verifies: EVS-DEV-event-store-open/E
  test('two concurrent opens of a build that changes the recorded version '
      'append exactly one change', () async {
    if (db == null) return;
    await seedLibVersionEventForTest(
      backends[0],
      version: '0.4.9',
      dataFormat: LibVersion.dataFormat,
    );
    final stores = await Future.wait(<Future<EventStore>>[
      _open(backends[0], 'install-a'),
      _open(backends[1], 'install-b'),
    ]);
    expect(stores[1].databaseId, stores[0].databaseId);
    final changes = await backends[0].findAllEvents(
      entryType: LibVersionEvents.changed,
    );
    expect(changes, hasLength(1));
    expect(changes.single.data['fromVersion'], '0.4.9');
    expect(changes.single.data['toVersion'], LibVersion.version);
  });
}
