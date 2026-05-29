// Verifies: EVS-PRD-event-log/A — deleteInTxn on a security-context row does
//   not touch the immutable event-log store, confirming the one-way FK design.
// Verifies: EVS-PRD-regulatory-alignment — findOlderThanInTxn and
//   findUnredactedOlderThanInTxn correctly select rows for compact/purge sweeps,
//   exercising the retention-window query paths required for ALCOA+ Enduring.
import 'package:event_sourcing/src/security/event_security_context.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<(SembastBackend, SembastSecurityContextStore)> _setup() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'sec-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  final backend = SembastBackend(database: db);
  final store = SembastSecurityContextStore(backend: backend);
  return (backend, store);
}

void main() {
  group('SembastSecurityContextStore', () {
    test('read on missing returns null', () async {
      final (backend, store) = await _setup();
      expect(await store.read('nope'), isNull);
      await backend.close();
    });

    test('writeInTxn + read round-trip', () async {
      final (backend, store) = await _setup();
      final row = EventSecurityContext(
        eventId: 'e-1',
        recordedAt: DateTime.utc(2026, 4, 22),
        ipAddress: '198.51.100.2',
      );
      await backend.transaction((txn) async {
        await store.writeInTxn(txn, row);
      });
      expect(await store.read('e-1'), row);
      await backend.close();
    });

    test('deleteInTxn on security row does not touch event log', () async {
      final (backend, store) = await _setup();
      await backend.transaction((txn) async {
        await store.writeInTxn(
          txn,
          EventSecurityContext(
            eventId: 'e-2',
            recordedAt: DateTime.utc(2026, 4, 22),
          ),
        );
      });
      await backend.transaction((txn) async {
        await store.deleteInTxn(txn, 'e-2');
      });
      expect(await store.read('e-2'), isNull);
      expect(await backend.findAllEvents(), isEmpty);
      await backend.close();
    });

    test(
      'findOlderThanInTxn / findUnredactedOlderThanInTxn select by recordedAt',
      () async {
        final (backend, store) = await _setup();
        final old = EventSecurityContext(
          eventId: 'old',
          recordedAt: DateTime.utc(2020, 1, 1),
        );
        final recent = EventSecurityContext(
          eventId: 'recent',
          recordedAt: DateTime.utc(2030, 1, 1),
        );
        final redacted = EventSecurityContext(
          eventId: 'redacted',
          recordedAt: DateTime.utc(2020, 1, 1),
          redactedAt: DateTime.utc(2020, 6, 1),
          redactionReason: 'gdpr',
        );
        await backend.transaction((txn) async {
          await store.writeInTxn(txn, old);
          await store.writeInTxn(txn, recent);
          await store.writeInTxn(txn, redacted);
        });
        final cutoff = DateTime.utc(2025, 1, 1);
        final older = await backend.transaction(
          (txn) async => store.findOlderThanInTxn(txn, cutoff),
        );
        expect(older.map((r) => r.eventId).toSet(), {'old', 'redacted'});
        final unredacted = await backend.transaction(
          (txn) async => store.findUnredactedOlderThanInTxn(txn, cutoff),
        );
        expect(unredacted.map((r) => r.eventId).toSet(), {'old'});
        await backend.close();
      },
    );
  });
}
