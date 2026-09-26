// Verifies: EVS-PRD-destinations/E
import 'dart:typed_data';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing_demo/demo_types.dart';
import 'package:event_sourcing_demo/downstream_bridge.dart';
import 'package:event_sourcing_demo/synthetic_ingest.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<EventStoreBundle> _bootstrapHub(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  final backend = SembastBackend(database: db);
  return bootstrapEventStore(
    storage: ApplicationSuppliedStorage(
      backend,
      SembastSecurityContextStore(backend: backend),
    ),
    source: const Source(
      hopId: 'hub-server',
      identifier: '11111111-1111-4111-8111-111111111111',
      softwareVersion: 'event_sourcing_demo@0.1.0+1',
    ),
    entryTypes: allDemoEntryTypes,
    destinations: const <Destination>[],
  );
}

/// The security findings [store] holds.
Future<List<Map<String, Object?>>> _findings(EventStore store) async =>
    <Map<String, Object?>>[
      for (final e in await store.reader.findAllEvents(
        entryType: kSecurityFindingEntryType,
      ))
        Map<String, Object?>.from(e.data),
    ];

/// [envelope] carrying [events] in place of its own.
BatchEnvelope _withEvents(
  BatchEnvelope envelope,
  List<Map<String, Object?>> events,
) => BatchEnvelope(
  batchFormatVersion: envelope.batchFormatVersion,
  batchId: envelope.batchId,
  senderHop: envelope.senderHop,
  senderIdentifier: envelope.senderIdentifier,
  senderSoftwareVersion: envelope.senderSoftwareVersion,
  sentAt: envelope.sentAt,
  events: events,
);

WirePayload _wirePayload(Uint8List bytes) => WirePayload(
  bytes: bytes,
  contentType: BatchEnvelope.wireFormat,
  transformVersion: null,
);

void main() {
  var pathCounter = 0;
  String nextPath() => 'bridge-${++pathCounter}.db';

  group('DownstreamBridge.deliver', () {
    test('valid esd/batch@2 envelope returns SendOk and the hub admits '
        'its event', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final envelope = SyntheticBatchBuilder().buildSingleEventBatch();
      final eventId = envelope.events.single['event_id']! as String;
      expect(await hub.eventStore.reader.findEventById(eventId), isNull);
      final result = await bridge.deliver(_wirePayload(envelope.encode()));
      expect(result, isA<SendOk>());
      final admitted = await hub.eventStore.reader.findEventById(eventId);
      expect(admitted, isNotNull, reason: 'the hub log holds the event');
      expect(admitted!.aggregateId, 'remote-aggregate-1');
    });

    test('garbage bytes return SendPermanent (decode failure)', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final result = await bridge.deliver(
        _wirePayload(Uint8List.fromList(<int>[0, 1, 2, 3])),
      );
      expect(result, isA<SendPermanent>());
    });

    test('unsupported wireFormat returns SendPermanent', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final envelope = SyntheticBatchBuilder().buildSingleEventBatch();
      final payload = WirePayload(
        bytes: envelope.encode(),
        contentType: 'application/x-unknown',
        transformVersion: null,
      );
      final result = await bridge.deliver(payload);
      expect(result, isA<SendPermanent>());
    });

    test('thrown StateError maps to SendTransient', () async {
      final bridge = DownstreamBridge(_ThrowingEventStore(StateError('boom')));
      final envelope = SyntheticBatchBuilder().buildSingleEventBatch();
      final result = await bridge.deliver(_wirePayload(envelope.encode()));
      expect(result, isA<SendTransient>());
    });

    test('IngestDataFormatIncompatible -> SendPermanent', () async {
      final stub = _ThrowingEventStore(
        const IngestDataFormatIncompatible(
          eventId: 'e-1',
          wireFormat: DataFormatVersion(3, 0),
          receiverFormat: DataFormatVersion(2, 0),
        ),
      );
      final bridge = DownstreamBridge(stub);
      final result = await bridge.deliver(
        _wirePayload(Uint8List.fromList(<int>[1])),
      );
      expect(result, isA<SendPermanent>());
    });

    test('IngestEntryTypeVersionAhead -> SendPermanent', () async {
      final stub = _ThrowingEventStore(
        const IngestEntryTypeVersionAhead(
          eventId: 'e-1',
          entryType: 'demo_note',
          wireVersion: EntryTypeVersion(5, 0),
          receiverVersion: EntryTypeVersion(2, 0),
        ),
      );
      final bridge = DownstreamBridge(stub);
      final result = await bridge.deliver(
        _wirePayload(Uint8List.fromList(<int>[1])),
      );
      expect(result, isA<SendPermanent>());
    });

    test('IngestEntryTypeVersionUnpromotable -> SendPermanent', () async {
      final stub = _ThrowingEventStore(
        const IngestEntryTypeVersionUnpromotable(
          eventId: 'e-1',
          entryType: 'demo_note',
          viewName: 'demo_notes',
          wireVersion: EntryTypeVersion(1, 3),
          receiverVersion: EntryTypeVersion(2, 0),
          reason: 'no major step is registered from major 1',
        ),
      );
      final bridge = DownstreamBridge(stub);
      final result = await bridge.deliver(
        _wirePayload(Uint8List.fromList(<int>[1])),
      );
      expect(result, isA<SendPermanent>());
    });

    test(
      'a record whose hash does not recompute returns SendOk; the hub '
      'stores it as received with a hash_mismatch security finding',
      () async {
        final hub = await _bootstrapHub(nextPath());
        final bridge = DownstreamBridge(hub.eventStore);
        final sealed = SyntheticBatchBuilder().buildSingleEventBatch();
        final tampered = Map<String, Object?>.from(sealed.events.single)
          ..['event_hash'] = 'f' * 64;
        final eventId = tampered['event_id']! as String;
        final result = await bridge.deliver(
          _wirePayload(_withEvents(sealed, [tampered]).encode()),
        );
        expect(result, isA<SendOk>());
        expect(
          await hub.eventStore.reader.findEventById(eventId),
          isNotNull,
          reason: 'the suspect event is stored as received',
        );
        final findings = await _findings(hub.eventStore);
        expect(findings.map((f) => f['kind']), <String>['hash_mismatch']);
        expect(findings.single['aggregates'], <String>['remote-aggregate-1']);
      },
    );

    test('a declared reserved entry type under an aggregate type the library '
        'does not declare for it returns SendOk; the hub keeps the record in '
        'an event_malformed security finding', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final sealed = SyntheticBatchBuilder().buildSingleEventBatch(
        entryType: kSecurityFindingEntryType,
      );
      final eventId = sealed.events.single['event_id']! as String;
      final result = await bridge.deliver(_wirePayload(sealed.encode()));
      expect(result, isA<SendOk>());
      expect(await hub.eventStore.reader.findEventById(eventId), isNull);
      final findings = await _findings(hub.eventStore);
      expect(findings.map((f) => f['kind']), <String>['event_malformed']);
      final evidence = findings.single['evidence']! as Map;
      expect(evidence['reason'], 'reserved_type_undeclared');
      expect((evidence['record']! as Map)['event_id'], eventId);
    });
  });
}

class _ThrowingEventStore implements EventStore {
  _ThrowingEventStore(this._toThrow);
  final Object _toThrow;
  @override
  Future<IngestBatchResult> ingestBatch(
    Uint8List bytes, {
    required String wireFormat,
  }) {
    // ignore: only_throw_errors
    throw _toThrow;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
