// EventStore.open boot flow on Sembast: the library-version event it
// appends at the first open, after an upgrade and after a downgrade within
// one data-format major, none on a same-version reopen, and the refusal of
// another data-format major.

import 'package:event_sourcing/src/entry_type_registry.dart';
import 'package:event_sourcing/src/event_store.dart';
import 'package:event_sourcing/src/lifecycle/boot_errors.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/version_check.dart';
import 'package:event_sourcing/src/security/sembast_security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/source.dart';
import 'package:event_sourcing/src/versions.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/lib_version_seed.dart';

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

Future<EventStore> _open(SembastBackend backend) => EventStore.open(
  storage: backend,
  entryTypes: EntryTypeRegistry(),
  source: _kTestSource,
  securityContexts: SembastSecurityContextStore(backend: backend),
);

Future<RecordedLibVersion?> _latest(SembastBackend backend) async =>
    (await backend.transaction(
      (txn) => VersionCheck.readLocalInTxn(backend, txn),
    )).latest;

void main() {
  group('EventStore.open boot version flow', () {
    // Verifies: EVS-DEV-event-store-open/B
    test('emits lib_version_initialized on first boot', () async {
      final backend = await _openBackend();
      final store = await _open(backend);
      final result = await _latest(backend);
      expect(result?.packageVersion, LibVersion.version);
      expect(result?.dataFormat, LibVersion.dataFormat);
      expect(result?.event.eventType, LibVersionEvents.initialized);
      await store.close();
    });

    // Verifies: EVS-DEV-event-store-open/C
    test('no-op when recorded version equals current', () async {
      final backend = await _openBackend();
      await _open(backend);
      final beforeSecond = await _latest(backend);
      await _open(backend);
      final afterSecond = await _latest(backend);
      expect(
        afterSecond?.event.sequenceNumber,
        beforeSecond?.event.sequenceNumber,
      );
    });

    // Verifies: EVS-DEV-event-store-open/C
    test('emits lib_version_changed on upgrade', () async {
      final backend = await _openBackend();
      await seedLibVersionEventForTest(
        backend,
        version: '0.3.0',
        dataFormat: LibVersion.dataFormat,
      );
      final store = await _open(backend);
      final result = await _latest(backend);
      expect(result?.event.eventType, LibVersionEvents.changed);
      expect(result?.packageVersion, LibVersion.version);
      expect(result?.event.data['fromVersion'], '0.3.0');
      await store.close();
    });

    // Verifies: EVS-DEV-event-store-open/C
    test('emits lib_version_changed when the recorded version is newer '
        'within the data-format major', () async {
      final backend = await _openBackend();
      await seedLibVersionEventForTest(
        backend,
        version: '99.0.0',
        dataFormat: LibVersion.dataFormat.nextMinor,
      );
      final store = await _open(backend);
      final result = await _latest(backend);
      expect(result?.event.eventType, LibVersionEvents.changed);
      expect(result?.event.data['fromVersion'], '99.0.0');
      expect(
        result?.event.data['fromDataFormat'],
        LibVersion.dataFormat.nextMinor.toJson(),
      );
      expect(result?.packageVersion, LibVersion.version);
      expect(result?.dataFormat, LibVersion.dataFormat);
      await store.close();
    });

    // Verifies: EVS-DEV-event-store-open/D
    test('refuses a database of another data-format major', () async {
      final backend = await _openBackend();
      await seedLibVersionEventForTest(
        backend,
        version: '99.0.0',
        dataFormat: DataFormatVersion(LibVersion.dataFormat.major + 1, 0),
      );
      await expectLater(
        _open(backend),
        throwsA(isA<DataFormatIncompatibleError>()),
      );
      final result = await _latest(backend);
      expect(result?.packageVersion, '99.0.0');
    });
  });
}
