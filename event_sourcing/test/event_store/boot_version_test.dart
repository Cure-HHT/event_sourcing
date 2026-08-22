// Verifies: EVS-DEV-event-store-open/B/C/D
// EventStore.open boot flow:
//   emits lib_version_initialized on first boot (B), emits lib_version_changed
//   on upgrade (C), refuses to construct on downgrade throwing
//   DowngradeRefusedError (D); also verifies no-op on same-version reboot
//   (no additional event emitted).

import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

const _kTestSource = Source(
  hopId: 'boot-version-test',
  identifier: 'boot-version-test',
  softwareVersion: '0.0.0-test',
);

Future<SembastBackend> _openBackend() async {
  final db = await newDatabaseFactoryMemory().openDatabase(
    'bv-${DateTime.now().microsecondsSinceEpoch}.db',
  );
  return SembastBackend(database: db);
}

/// Append a synthetic version event directly to [backend], simulating a
/// previous boot at [version]. Uses the Transaction-based API with an explicit
/// nextSequenceNumber reservation, matching the pattern from version_check_test.
Future<void> _appendSyntheticVersionEvent(
  SembastBackend backend, {
  required String version,
  required String eventType,
  String? fromVersion,
}) async {
  await backend.transaction((txn) async {
    final seq = await backend.nextSequenceNumber(txn);
    final data = eventType == LibVersionEvents.initialized
        ? <String, Object?>{
            'version': version,
            'initializedAt': '2026-04-01T00:00:00Z',
          }
        : <String, Object?>{
            'fromVersion': fromVersion ?? '0.0.0',
            'toVersion': version,
            'changedAt': '2026-04-01T00:00:00Z',
          };
    await backend.appendEvent(
      txn,
      StoredEvent.synthetic(
        eventId: 'synth-$eventType-$seq',
        aggregateId: '_lib',
        aggregateType: '_lib',
        entryType: eventType,
        eventType: eventType,
        sequenceNumber: seq,
        eventHash: 'h-$seq',
        initiator: const AutomationInitiator(service: 'event_sourcing'),
        clientTimestamp: DateTime.utc(2026, 4, 1),
        data: Map<String, dynamic>.from(data),
      ),
    );
  });
}

void main() {
  group('EventStore.open boot version flow', () {
    test('emits lib_version_initialized on first boot', () async {
      final backend = await _openBackend();
      final store = await EventStore.open(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: _kTestSource,
        securityContexts: SembastSecurityContextStore(backend: backend),
      );
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.recordedVersion, LibVersion.version);
      expect(result?.eventType, LibVersionEvents.initialized);
      await store.close();
    });

    test('no-op when recorded version equals current', () async {
      final backend = await _openBackend();
      // first boot writes init
      await EventStore.open(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: _kTestSource,
        securityContexts: SembastSecurityContextStore(backend: backend),
      );
      final beforeSecond = await VersionCheck.findMostRecent(backend);
      // second boot at same version
      await EventStore.open(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: _kTestSource,
        securityContexts: SembastSecurityContextStore(backend: backend),
      );
      final afterSecond = await VersionCheck.findMostRecent(backend);
      expect(afterSecond?.sequenceNumber, beforeSecond?.sequenceNumber);
    });

    test('emits lib_version_changed on upgrade', () async {
      final backend = await _openBackend();
      // Simulate an older recorded version by appending a synthetic init event.
      await _appendSyntheticVersionEvent(
        backend,
        version: '0.3.0',
        eventType: LibVersionEvents.initialized,
      );
      final store = await EventStore.open(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: _kTestSource,
        securityContexts: SembastSecurityContextStore(backend: backend),
      );
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.eventType, LibVersionEvents.changed);
      expect(result?.recordedVersion, LibVersion.version);
      await store.close();
    });

    test('refuses to boot on downgrade by default', () async {
      final backend = await _openBackend();
      // Simulate a future version recorded.
      await _appendSyntheticVersionEvent(
        backend,
        version: '99.0.0',
        eventType: LibVersionEvents.initialized,
      );
      expect(
        () => EventStore.open(
          storage: backend,
          entryTypes: EntryTypeRegistry(),
          source: _kTestSource,
          securityContexts: SembastSecurityContextStore(backend: backend),
        ),
        throwsA(isA<DowngradeRefusedError>()),
      );
    });

    test('allowDowngrade: true bypasses downgrade refusal', () async {
      final backend = await _openBackend();
      await _appendSyntheticVersionEvent(
        backend,
        version: '99.0.0',
        eventType: LibVersionEvents.initialized,
      );
      final store = await EventStore.open(
        storage: backend,
        entryTypes: EntryTypeRegistry(),
        source: _kTestSource,
        securityContexts: SembastSecurityContextStore(backend: backend),
        allowDowngrade: true,
      );
      // No new lib_version event should be emitted on downgrade — the existing
      // record stays authoritative until the next upgrade re-passes through.
      final result = await VersionCheck.findMostRecent(backend);
      expect(result?.recordedVersion, '99.0.0');
      await store.close();
    });
  });
}
