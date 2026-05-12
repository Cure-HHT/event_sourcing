// Verifies: EVS-DEV-event-store-open/C/D
// Verifies: EVS-DEV-entry-type-downgrade-refusal
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'vc-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

StoredEvent _versionEvent(
  String type,
  Map<String, Object?> data, {
  required int sequenceNumber,
}) => StoredEvent.synthetic(
  eventId: 'lvi-$sequenceNumber',
  aggregateId: '_lib',
  aggregateType: '_lib',
  entryType: type,
  eventType: type,
  sequenceNumber: sequenceNumber,
  eventHash: 'h-$sequenceNumber',
  initiator: const AutomationInitiator(service: 'event_sourcing'),
  clientTimestamp: DateTime.utc(2026, 5, 9),
  data: Map<String, dynamic>.from(data),
);

Future<void> _appendVersionEvent(
  SembastBackend backend,
  String type,
  Map<String, Object?> data,
) async {
  await backend.transaction((txn) async {
    final seq = await backend.nextSequenceNumber(txn);
    await backend.appendEvent(
      txn,
      _versionEvent(type, data, sequenceNumber: seq),
    );
  });
}

void main() {
  group('VersionCheck.findMostRecent', () {
    test('returns null when no version events exist', () async {
      final backend = await _openBackend();
      final result = await VersionCheck.findMostRecent(backend);
      expect(result, isNull);
      await backend.close();
    });

    test('returns the most recent lib_version_initialized event', () async {
      final backend = await _openBackend();
      await _appendVersionEvent(backend, LibVersionEvents.initialized, {
        'version': '0.4.0',
        'initializedAt': '2026-05-09T00:00:00Z',
      });
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.recordedVersion, '0.4.0');
      await backend.close();
    });

    test(
      'returns the most recent lib_version_changed when newer than initialized',
      () async {
        final backend = await _openBackend();
        await _appendVersionEvent(backend, LibVersionEvents.initialized, {
          'version': '0.4.0',
          'initializedAt': '2026-05-09T00:00:00Z',
        });
        await _appendVersionEvent(backend, LibVersionEvents.changed, {
          'fromVersion': '0.4.0',
          'toVersion': '0.4.1',
          'changedAt': '2026-05-15T00:00:00Z',
        });
        final result = await VersionCheck.findMostRecent(backend);
        expect(result?.recordedVersion, '0.4.1');
        await backend.close();
      },
    );
  });
}
