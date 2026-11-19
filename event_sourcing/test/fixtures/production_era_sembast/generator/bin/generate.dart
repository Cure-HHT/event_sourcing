// Writes the unencrypted Sembast database fixture, and the list of the
// events it holds, with the library at the commit pubspec.yaml pins.
//
//   cd test/fixtures/production_era_sembast/generator
//   flutter pub get
//   dart run bin/generate.dart ..
//
// The definitions below are repeated, unchanged, in the test that opens the
// fixture (test/storage/sembast_encryption_fixture_test.dart).
import 'dart:convert';
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';

const Source source = Source(
  hopId: 'fixture-device',
  identifier: '00000000-0000-4000-8000-00000000f1c5',
  softwareVersion: 'fixture-app@1.0.0',
);

const EntryTypeDefinition noteType = EntryTypeDefinition(
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

AggregateProjectionSpec notesView() => AggregateProjectionSpec(
  viewName: 'fixture_notes',
  interest: SubscriptionFilter(entryTypes: <String>{noteType.id}),
  tombstoneEventTypes: const <String>{'removed'},
);

class FixtureDestination extends Destination {
  @override
  String get id => 'fixture-outbound';
  @override
  SubscriptionFilter get filter =>
      SubscriptionFilter(entryTypes: <String>{noteType.id});
  @override
  String get wireFormat => 'fixture-json-1';
  @override
  Duration get maxAccumulateTime => Duration.zero;
  @override
  bool canAddToBatch(List<StoredEvent> currentBatch, StoredEvent candidate) =>
      true;
  @override
  Future<WirePayload> transform(List<StoredEvent> batch) async => WirePayload(
    bytes: utf8.encode(jsonEncode(<String>[for (final e in batch) e.eventId])),
    contentType: 'application/json',
    transformVersion: 'fixture-1',
  );
  @override
  Future<SendResult> send(WirePayload payload) async => const SendOk();
}

Future<void> main(List<String> args) async {
  final outDir = args.isEmpty ? '..' : args.single;
  final dbPath = '$outDir/diary.db';
  final dbFile = File(dbPath);
  if (dbFile.existsSync()) dbFile.deleteSync();

  final projections = ProjectionRegistry()..register(notesView());
  final bundle = await bootstrapEventStore(
    storage: SembastStorage.file(dbPath),
    source: source,
    entryTypes: const <EntryTypeDefinition>[noteType],
    destinations: <Destination>[FixtureDestination()],
    projections: projections,
  );
  final store = bundle.eventStore;
  await bundle.destinations.setStartDate(
    'fixture-outbound',
    DateTime.utc(2026, 1, 1),
    initiator: const UserInitiator('fixture-user'),
  );
  for (var i = 1; i <= 6; i++) {
    await store.append(
      entryType: noteType.id,
      aggregateId: 'note-$i',
      aggregateType: 'FixtureNote',
      eventType: 'written',
      data: <String, Object?>{
        'text': 'fixture nosebleed record $i',
        'severity': i,
      },
      initiator: const UserInitiator('fixture-user'),
      security: const SecurityDetails(ipAddress: '10.0.0.1'),
    );
  }
  await store.append(
    entryType: noteType.id,
    aggregateId: 'note-2',
    aggregateType: 'FixtureNote',
    eventType: 'removed',
    data: const <String, Object?>{},
    initiator: const UserInitiator('fixture-user'),
    security: const SecurityDetails(ipAddress: '10.0.0.1'),
  );

  final events = await store.reader.findAllEvents();
  final rows = await store.reader.findViewRows('fixture_notes');
  final expected = <String, Object?>{
    'library_version': LibVersion.version,
    'data_format': LibVersion.dataFormat.toString(),
    'database_id': store.databaseId,
    'events': <Map<String, Object?>>[
      for (final event in events)
        <String, Object?>{
          'sequence_number': event.sequenceNumber,
          'event_id': event.eventId,
          'entry_type': event.entryType,
          'event_type': event.eventType,
          'aggregate_id': event.aggregateId,
          'event_hash': event.eventHash,
          'data': event.data,
        },
    ],
    'fixture_notes_rows': <Map<String, Object?>>[
      for (final row in rows.rows) row,
    ],
  };
  await store.close();
  File('$outDir/expected.json').writeAsStringSync(
    '${const JsonEncoder.withIndent('  ').convert(expected)}\n',
  );
}
