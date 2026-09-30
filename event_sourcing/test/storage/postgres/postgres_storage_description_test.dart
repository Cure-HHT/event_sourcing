// Verifies: EVS-PRD-storage-barrier/A
// Verifies: EVS-PRD-storage-barrier/H
// Verifies: EVS-PRD-storage-barrier/I
// Verifies: EVS-DEV-storage-capability/A
//
// A Postgres description: the library opens the backend from the
// description's connection, lock-session and wait settings, and closes it
// when the event store closes, when the boot refuses after the backend
// opened, when bootstrapEventStore fails after the open, and when the open
// itself is refused. After each, the server holds no session of the pool's
// or the lock session's role.
//
// Gated on PG_TEST_URL, whose role must be able to create roles; files
// that reset the schema run one at a time.

@TestOn('vm')
library;

import 'dart:async';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

const EntryTypeDefinition _noteDef = EntryTypeDefinition(
  id: 'description_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'description_note',
);

const Source _source = Source(
  hopId: 'description-test',
  identifier: 'aaaa0001-0000-4000-8000-00000000d35c',
  softwareVersion: '0.0.0-test',
);

EntryTypeRegistry _entryTypes() => EntryTypeRegistry()..register(_noteDef);

/// The server sessions [roles] hold, other than the one asking.
Future<int> _sessionsOf(PostgresTestDatabase db, Set<String> roles) =>
    db.asAdmin((admin) async {
      final r = await admin.execute(
        Sql.named(
          'SELECT count(*) FROM pg_stat_activity '
          'WHERE usename = ANY(@roles) AND pid <> pg_backend_pid()',
        ),
        parameters: <String, Object?>{'roles': roles.toList()},
      );
      return r.first[0]! as int;
    });

/// Waits, a bounded while, for [roles] to hold no server session: a closed
/// connection's server process ends shortly after its socket closes.
Future<int> _settledSessionsOf(
  PostgresTestDatabase db,
  Set<String> roles,
) async {
  var sessions = await _sessionsOf(db, roles);
  for (var i = 0; i < 40 && sessions > 0; i++) {
    await Future<void>.delayed(const Duration(milliseconds: 50));
    sessions = await _sessionsOf(db, roles);
  }
  return sessions;
}

class _NoopDestination extends Destination {
  _NoopDestination(this.id);

  @override
  final String id;

  @override
  SubscriptionFilter get filter =>
      const SubscriptionFilter(entryTypes: <String>{'never'});

  @override
  String get wireFormat => 'noop-v1';

  @override
  Duration get maxAccumulateTime => Duration.zero;

  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      currentBatch.isEmpty;

  @override
  Future<WirePayload> transform(List<StoredEvent> batch) =>
      throw UnimplementedError();

  @override
  Future<SendResult> send(WirePayload payload) async => const SendOk();
}

void main() {
  final db = PostgresTestDatabase.fromEnvironment(tag: 'desc');

  PostgresStorage describe({String? url}) => PostgresStorage(
    url: url ?? db!.runtimeUrl,
    schema: db!.schema,
    lockUrl: db.lockUrl,
    sslMode: SslMode.disable,
  );

  Set<String> libraryRoles() => <String>{db!.runtime, db.lock};

  setUp(() async {
    if (db == null) {
      markTestSkipped('PG_TEST_URL not set');
      return;
    }
    await db.reset(provision: true);
  });

  tearDownAll(() async => db?.drop());

  test('the library opens the described backend, and closing the store '
      'leaves no session of its pool or lock session', () async {
    if (db == null) return;
    final store = await EventStore.open(
      storage: describe(),
      entryTypes: _entryTypes(),
      source: _source,
    );
    await store.append(
      entryType: _noteDef.id,
      aggregateId: 'n-1',
      aggregateType: 'note',
      eventType: 'written',
      data: const <String, Object?>{'text': 'hello'},
      initiator: const UserInitiator('description-user'),
    );
    expect(await _sessionsOf(db, <String>{db.runtime}), greaterThan(0));
    expect(await _sessionsOf(db, <String>{db.lock}), 1);

    await store.close();

    expect(await _settledSessionsOf(db, libraryRoles()), 0);
  });

  test('a boot refusal after the backend opened closes it before the error '
      'reaches the caller', () async {
    if (db == null) return;
    final first = await EventStore.open(
      storage: describe(),
      entryTypes: _entryTypes(),
      source: _source,
    );
    await first.close();
    await db.asAdmin((admin) async {
      await admin.execute(
        'UPDATE ${quoteIdent(db.schema)}.backend_state '
        "SET value = to_jsonb('tampered-identity'::text) "
        "WHERE key = 'database_id'",
      );
    });

    await expectLater(
      EventStore.open(
        storage: describe(),
        entryTypes: _entryTypes(),
        source: _source,
      ),
      throwsA(isA<DatabaseIdentityMismatchError>()),
    );

    expect(await _settledSessionsOf(db, libraryRoles()), 0);
  });

  test('a failure inside bootstrapEventStore after the open closes the '
      'backend before the error reaches the caller', () async {
    if (db == null) return;
    await expectLater(
      bootstrapEventStore(
        storage: describe(),
        source: _source,
        entryTypes: const <EntryTypeDefinition>[_noteDef],
        destinations: <Destination>[
          _NoopDestination('dup'),
          _NoopDestination('dup'),
        ],
      ),
      throwsA(anything),
    );

    expect(await _settledSessionsOf(db, libraryRoles()), 0);
  });

  test('an open refused before the backend is returned leaves no session '
      'of the refused role', () async {
    if (db == null) return;
    await expectLater(
      EventStore.open(
        storage: describe(url: db.ownerUrl),
        entryTypes: _entryTypes(),
        source: _source,
      ),
      throwsA(isA<PostgresRoleRefusedException>()),
    );

    expect(
      await _settledSessionsOf(db, <String>{db.owner, db.runtime, db.lock}),
      0,
    );
  });
}
