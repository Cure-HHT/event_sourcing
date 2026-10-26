// test/permissions/seed_one_transaction_test.dart
//
// The permission seed appends every grant one run emits in one transaction
// of the substrate (EVS-DEV-bootstrap-action-permissions/E). A backend that
// counts its transactions shows it: a seed of seventy grants costs the store
// one writing transaction, not seventy, and the grants commit together.
//
// Why it matters: on Postgres every transaction that appends waits for the
// lock the view catch-up holds (EVS-DEV-view-convergence/L). A first boot
// on an empty log that waited once per grant overran its budget on a cloud
// database; one that waits once does not grow with the seed.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' hide Transaction;

/// An in-memory backend that counts the transactions the store runs on it,
/// and the ones that appended at least one event.
class _CountingBackend extends SembastBackend {
  _CountingBackend({required super.database});

  int transactions = 0;
  int writingTransactions = 0;

  @override
  Future<T> transaction<T>(Future<T> Function(Transaction txn) body) {
    transactions++;
    // Counted inside the transaction, where its own writes are visible and
    // another transaction's commit cannot be mistaken for this one's.
    return super.transaction<T>((txn) async {
      final before = await _logHeadInTxn(txn);
      final result = await body(txn);
      if (await _logHeadInTxn(txn) > before) writingTransactions++;
      return result;
    });
  }

  Future<int> _logHeadInTxn(Transaction txn) async {
    final events = await findAllEventsInTxn(txn, afterSequence: 0);
    return events.isEmpty ? 0 : events.last.sequenceNumber;
  }
}

const int _kGrantCount = 70;

String _seedYaml(int grants) {
  final buffer = StringBuffer('roles:\n  - admin\ngrants:\n  admin:\n');
  for (var i = 0; i < grants; i++) {
    buffer.writeln('    - perm.$i');
  }
  return buffer.toString();
}

Set<Permission> _declared(int grants) => <Permission>{
  for (var i = 0; i < grants; i++) Permission('perm.$i'),
};

Future<(EventStore, _CountingBackend)> _open() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'seed-one-transaction-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = _CountingBackend(database: db);
  final registry = EntryTypeRegistry()
    ..register(
      const EntryTypeDefinition(
        id: 'role_permission_grant',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'Role-permission grant',
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: 'user_role_scope',
        registeredVersion: EntryTypeVersion(1, 0),
        name: 'User-role-scope assignment',
      ),
    );
  final store = await EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
    entryTypes: registry,
    source: const Source(
      hopId: 'test-server',
      identifier: 'seed-one-transaction',
      softwareVersion: 'event_sourcing_test@0.0.0',
    ),
    projections: ProjectionRegistry()
      ..register(rolePermissionGrantsSpec)
      ..register(userRoleScopesSpec),
  );
  return (store, backend);
}

void main() {
  late EventStore store;
  late _CountingBackend backend;

  setUp(() async {
    (store, backend) = await _open();
  });

  tearDown(() async {
    await store.close();
  });

  // Verifies: EVS-DEV-bootstrap-action-permissions/E
  // Verifies: EVS-DEV-bootstrap-action-permissions/A
  test(
    'a seed of $_kGrantCount grants appends them all in one transaction',
    () async {
      backend
        ..transactions = 0
        ..writingTransactions = 0;

      final result = await bootstrapActionPermissions(
        eventStore: store,
        declaredPermissions: _declared(_kGrantCount),
        yamlSource: _seedYaml(_kGrantCount),
      );

      expect(result, isA<PolicyReady>());
      expect(
        backend.writingTransactions,
        1,
        reason: 'the seed appends every grant of one run in one transaction',
      );
      // The wait for the view and the read of its rows are a handful of
      // read-only transactions; a per-grant seed would have run at least
      // $_kGrantCount more.
      expect(backend.transactions, lessThan(_kGrantCount));

      final rows = await store.reader.findViewRows('role_permission_grants');
      expect(rows.state, ViewConvergenceState.current);
      expect(rows.rows, hasLength(_kGrantCount));
      final events = await backend.findAllEvents(afterSequence: 0, limit: 1000);
      expect(
        events.where((e) => e.eventType == 'permission_granted'),
        hasLength(_kGrantCount),
      );
    },
  );

  // Verifies: EVS-DEV-bootstrap-action-permissions/D+E
  test('a second run of the same seed opens no writing transaction', () async {
    await bootstrapActionPermissions(
      eventStore: store,
      declaredPermissions: _declared(_kGrantCount),
      yamlSource: _seedYaml(_kGrantCount),
    );
    backend.writingTransactions = 0;

    await bootstrapActionPermissions(
      eventStore: store,
      declaredPermissions: _declared(_kGrantCount),
      yamlSource: _seedYaml(_kGrantCount),
    );

    expect(backend.writingTransactions, 0);
  });

  // Verifies: EVS-DEV-bootstrap-action-permissions/E
  test(
    'a run that adds grants to a seeded log appends only those, together',
    () async {
      await bootstrapActionPermissions(
        eventStore: store,
        declaredPermissions: _declared(10),
        yamlSource: _seedYaml(10),
      );
      backend.writingTransactions = 0;

      final result = await bootstrapActionPermissions(
        eventStore: store,
        declaredPermissions: _declared(25),
        yamlSource: _seedYaml(25),
      );

      expect(result, isA<PolicyReady>());
      expect(backend.writingTransactions, 1);
      final rows = await store.reader.findViewRows('role_permission_grants');
      expect(rows.rows, hasLength(25));
    },
  );
}
