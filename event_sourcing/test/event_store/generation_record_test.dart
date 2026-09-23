// The data generation of a build, the live comparison between two
// generations, and the database's generation record, which every boot
// merges and which refuses a build it does not admit before any write. The
// stop-then-start sequences run on one Sembast database file reopened in
// turn, where no live guard exists and the record decides alone.
@TestOn('vm')
library;

import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;
import 'package:sembast/sembast_io.dart' show databaseFactoryIo;

const _kX = 'gen_x';

GenerationDescriptor _descriptor(
  Map<String, EntryTypeVersion> types, {
  int dataFormatMajor = 2,
}) => GenerationDescriptor(
  packageVersion: '0.5.0',
  dataFormat: DataFormatVersion(dataFormatMajor, 0),
  entryTypes: types,
);

Future<EventStore> _open(
  String path, {
  EntryTypeVersion x = const EntryTypeVersion(1, 0),
}) async {
  final backend = SembastBackend(
    database: await databaseFactoryIo.openDatabase(path),
  );
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry.register(
    EntryTypeDefinition(id: _kX, registeredVersion: x, name: _kX),
  );
  try {
    return await EventStore.open(
      storage: backend,
      entryTypes: registry,
      source: const Source(
        hopId: 'gen-hop',
        identifier: 'gen-install',
        softwareVersion: 'gen-test',
      ),
      securityContexts: SembastSecurityContextStore(backend: backend),
    );
  } catch (_) {
    await backend.close();
    rethrow;
  }
}

Future<void> _append(EventStore store) => store.append(
  entryType: _kX,
  aggregateId: 'agg-1',
  aggregateType: 'note',
  eventType: 'finalized',
  data: const <String, Object?>{'title': 't'},
  initiator: const UserInitiator('gen-user'),
);

/// The database file's bytes: a refused open writes nothing to it.
Future<String> _contents(String path) => File(path).readAsString();

void main() {
  group('GenerationDescriptor', () {
    // Verifies: EVS-DEV-version-compatibility/F
    test('components name the data-format major and each entry-type '
        'major', () {
      expect(
        _descriptor(const {
          'b': EntryTypeVersion(1, 3),
          'a': EntryTypeVersion(2, 0),
        }).components,
        <String>['data_format:2', 'entry_type:a:2', 'entry_type:b:1'],
      );
    });

    // Verifies: EVS-DEV-version-compatibility/F
    test('two generations conflict on another data-format major or another '
        'major of an entry type both register, not on minors or on an entry '
        'type only one registers', () {
      final a = _descriptor(const {
        'x': EntryTypeVersion(1, 0),
        'y': EntryTypeVersion(1, 0),
      });
      expect(
        a.conflictsWith(
          _descriptor(const {
            'x': EntryTypeVersion(1, 4),
            'z': EntryTypeVersion(3, 0),
          }),
        ),
        isFalse,
      );
      expect(
        a.conflictingComponents(
          _descriptor(const {'x': EntryTypeVersion(2, 0)}),
        ),
        <String>['entry_type:x:2'],
      );
      expect(
        a.conflictingComponents(
          _descriptor(const {'x': EntryTypeVersion(1, 0)}, dataFormatMajor: 3),
        ),
        <String>['data_format:3'],
      );
    });
  });

  group('GenerationRecord', () {
    final record = GenerationRecord(
      dataFormatMajor: 2,
      entryTypeMajors: const <String, int>{'x': 2, 'y': 1},
    );

    // Verifies: EVS-DEV-version-compatibility/I
    test('admits the same majors, a higher major (the bump), and an entry '
        'type it does not record', () {
      expect(
        record.admits(
          _descriptor(const {
            'x': EntryTypeVersion(2, 7),
            'y': EntryTypeVersion(3, 0),
            'z': EntryTypeVersion(1, 0),
          }),
        ),
        isTrue,
      );
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test('refuses another data-format major and a lower entry-type '
        'major', () {
      expect(
        record.refusedComponent(
          _descriptor(const {'x': EntryTypeVersion(2, 0)}, dataFormatMajor: 3),
        ),
        'data_format:2',
      );
      expect(
        record.refusedComponent(
          _descriptor(const {'x': EntryTypeVersion(1, 9)}),
        ),
        'entry_type:x:2',
      );
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test('a merge keeps the highest major of each entry type', () {
      expect(
        record.merge(
          _descriptor(const {
            'x': EntryTypeVersion(2, 0),
            'y': EntryTypeVersion(4, 0),
            'z': EntryTypeVersion(1, 0),
          }),
        ),
        GenerationRecord(
          dataFormatMajor: 2,
          entryTypeMajors: const <String, int>{'x': 2, 'y': 4, 'z': 1},
        ),
      );
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test('round-trips its stored shape and refuses a malformed one', () {
      expect(GenerationRecord.fromJson(record.toJson()), record);
      expect(
        () => GenerationRecord.fromJson(<String, Object?>{
          'data_format_major': 0,
          'entry_type_majors': <String, Object?>{},
        }),
        throwsFormatException,
      );
    });
  });

  group('stop-then-start on one Sembast database file', () {
    late Directory dir;
    late String path;

    setUp(() async {
      dir = await Directory.systemTemp.createTemp('generation_record_');
      path = p.join(dir.path, 'gen.db');
    });

    tearDown(() => dir.delete(recursive: true));

    // Verifies: EVS-DEV-version-compatibility/I
    // Verifies: EVS-DEV-entry-type-downgrade-refusal/A
    test('a major bump opens after the old build stopped, and the old build '
        'is then refused although no view names the entry type', () async {
      final v1 = await _open(path);
      await _append(v1);
      await v1.close();
      final v2 = await _open(path, x: const EntryTypeVersion(2, 0));
      await v2.close();
      final before = await _contents(path);
      await expectLater(
        _open(path, x: const EntryTypeVersion(1, 3)),
        throwsA(
          isA<EntryTypeVersionDowngradeError>()
              .having((e) => e.entryType, 'entryType', _kX)
              .having((e) => e.fromVersion.major, 'recorded major', 2)
              .having((e) => e.recordedByOpen, 'recordedByOpen', isTrue),
        ),
      );
      expect(await _contents(path), before);
    });

    // Verifies: EVS-DEV-version-compatibility/I
    // Verifies: EVS-DEV-event-store-open/D
    test('after a build of another data-format major opened, the compiled '
        'build is refused', () async {
      await runWithDeliveryTestHooks(
        const DeliveryTestHooks(
          buildDeclaration: (
            version: '9.0.0',
            dataFormat: DataFormatVersion(3, 0),
          ),
        ),
        () async => (await _open(path)).close(),
      );
      final before = await _contents(path);
      await expectLater(
        _open(path),
        throwsA(isA<DataFormatIncompatibleError>()),
      );
      expect(await _contents(path), before);
    });

    // Verifies: EVS-DEV-version-compatibility/I
    // Verifies: EVS-DEV-version-compatibility/H
    test('two builds of conflicting majors open in turn (no live guard '
        'outside the browser), and the later older open is refused by the '
        'record', () async {
      await (await _open(path)).close();
      await (await _open(path, x: const EntryTypeVersion(2, 0))).close();
      await expectLater(
        _open(path),
        throwsA(isA<EntryTypeVersionDowngradeError>()),
      );
    });

    // Verifies: EVS-DEV-version-compatibility/I
    test('openForTest refuses what the record does not admit', () async {
      await (await _open(path, x: const EntryTypeVersion(2, 0))).close();
      final backend = SembastBackend(
        database: await databaseFactoryIo.openDatabase(path),
      );
      addTearDown(backend.close);
      final registry = EntryTypeRegistry()
        ..register(
          const EntryTypeDefinition(
            id: _kX,
            registeredVersion: EntryTypeVersion(1, 0),
            name: _kX,
          ),
        );
      await expectLater(
        EventStore.openForTest(
          storage: backend,
          entryTypes: registry,
          source: const Source(
            hopId: 'gen-hop',
            identifier: 'gen-install',
            softwareVersion: 'gen-test',
          ),
          securityContexts: SembastSecurityContextStore(backend: backend),
        ),
        throwsA(isA<EntryTypeVersionDowngradeError>()),
      );
    });
  });
}
