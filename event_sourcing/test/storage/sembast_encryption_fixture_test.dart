// A database the library's earlier release wrote unencrypted (data format
// 3.0, test/fixtures/production_era_sembast/) opens through EventStore.open
// with an encryption key: it is converted in place, holds every event it
// held with its hash and its view's rows unchanged, its chain verifies, and
// the file stores no readable record.
@TestOn('vm')
library;

import 'dart:convert';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

// The definitions the fixture was written with
// (test/fixtures/production_era_sembast/generator/bin/generate.dart),
// repeated unchanged so the boot sees the same build configuration.
const Source _source = Source(
  hopId: 'fixture-device',
  identifier: '00000000-0000-4000-8000-00000000f1c5',
  softwareVersion: 'fixture-app@1.0.0',
);

const EntryTypeDefinition _noteType = EntryTypeDefinition(
  id: 'fixture_note',
  registeredVersion: EntryTypeVersion(1, 0),
  name: 'Fixture note',
  declarations: <EventTypeDeclaration>[
    EventTypeDeclaration(
      eventType: 'written',
      kind: CausalKind.version,
      eligible: true,
    ),
    EventTypeDeclaration(
      eventType: 'removed',
      kind: CausalKind.version,
      eligible: true,
    ),
  ],
);

AggregateProjectionSpec _notesView() => AggregateProjectionSpec(
  viewName: 'fixture_notes',
  interest: SubscriptionFilter(entryTypes: <String>{_noteType.id}),
  tombstoneEventTypes: const <String>{'removed'},
);

const _fixtureDir = 'test/fixtures/production_era_sembast';

void main() {
  late Directory dir;
  setUp(() async {
    dir = await Directory.systemTemp.createTemp('evs-fixture-');
  });
  tearDown(() async {
    await dir.delete(recursive: true);
  });

  // Verifies: EVS-PRD-storage-barrier/M, EVS-DEV-sembast-encryption/F
  test('a database the earlier release wrote unencrypted opens encrypted '
      'with every event', () async {
    final expected =
        json.decode(File('$_fixtureDir/expected.json').readAsStringSync())
            as Map<String, Object?>;
    final expectedEvents = (expected['events']! as List)
        .cast<Map<String, Object?>>();
    expect(expected['data_format'], '3.0');
    expect(expectedEvents, isNotEmpty);

    final path = p.join(dir.path, 'diary.db');
    File('$_fixtureDir/diary.db').copySync(path);
    expect(
      await SembastStorage.inspectFile(path),
      SembastFileEncryption.unencrypted,
    );

    final key = StorageEncryptionKey.generate();
    final store = await EventStore.open(
      storage: SembastStorage.file(path, encryptionKey: key),
      entryTypes: EntryTypeRegistry()..register(_noteType),
      projections: ProjectionRegistry()..register(_notesView()),
      source: _source,
    );
    addTearDown(store.close);

    expect(store.databaseId, expected['database_id']);
    final events = await store.reader.findAllEvents();
    // The open by this build appends its library-version event after the
    // events the fixture holds.
    expect(events.length, greaterThan(expectedEvents.length));
    for (var i = 0; i < expectedEvents.length; i++) {
      final want = expectedEvents[i];
      final got = events[i];
      expect(got.sequenceNumber, want['sequence_number']);
      expect(got.eventId, want['event_id']);
      expect(got.entryType, want['entry_type']);
      expect(got.eventType, want['event_type']);
      expect(got.aggregateId, want['aggregate_id']);
      expect(got.eventHash, want['event_hash']);
      expect(
        json.decode(json.encode(got.data)),
        want['data'],
        reason: 'event ${want['event_id']}',
      );
    }
    final added = events.skip(expectedEvents.length).toList();
    expect(added.map((e) => e.entryType), ['lib_version_changed']);
    expect((await store.reader.verifyChains()).findings, isEmpty);

    final rows = await store.reader.findViewRows('fixture_notes');
    expect(json.decode(json.encode(rows.rows)), expected['fixture_notes_rows']);

    expect(
      await SembastStorage.inspectFile(path),
      SembastFileEncryption.encrypted,
    );
    final text = File(path).readAsStringSync();
    expect(text, isNot(contains('nosebleed')));
    expect(text, isNot(contains('event_id')));
  });
}
