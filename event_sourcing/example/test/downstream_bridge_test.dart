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

WirePayload _wirePayload(Uint8List bytes) => WirePayload(
  bytes: bytes,
  contentType: DeliveryEnvelope.wireFormat,
  transformVersion: null,
);

void main() {
  var pathCounter = 0;
  String nextPath() => 'bridge-${++pathCounter}.db';

  group('DownstreamBridge.deliver', () {
    test("a synthetic sender's deliveries are numbered and linked, and the "
        'hub accepts each', () async {
      final hub = await _bootstrapHub(nextPath());
      final sender = SyntheticSender();
      final first = await sender.deliverOne(hub.eventStore);
      final second = await sender.deliverOne(hub.eventStore);
      expect(
        first,
        isA<ReceiverAcknowledgement>().having(
          (a) => a.outcome,
          'outcome',
          AcknowledgementOutcome.accepted,
        ),
      );
      expect(
        second,
        isA<ReceiverAcknowledgement>()
            .having(
              (a) => a.outcome,
              'outcome',
              AcknowledgementOutcome.accepted,
            )
            .having((a) => a.record.deliveryNumber, 'number', 2),
      );
    });

    test(
      'two synthetic senders delivering to one hub are each accepted',
      () async {
        final hub = await _bootstrapHub(nextPath());
        for (final sender in <SyntheticSender>[
          SyntheticSender(),
          SyntheticSender(),
        ]) {
          expect(
            await sender.deliverOne(hub.eventStore),
            isA<ReceiverAcknowledgement>()
                .having(
                  (a) => a.outcome,
                  'outcome',
                  AcknowledgementOutcome.accepted,
                )
                .having((a) => a.record.deliveryNumber, 'number', 1),
          );
        }
      },
    );

    // Verifies: EVS-PRD-ingest/A
    test('a valid native delivery returns an acknowledgement and the hub '
        'admits its event', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final delivery = SyntheticSender().nextDelivery(<Map<String, Object?>>[
        SyntheticSender().buildEvent(),
      ]);
      final eventId = delivery.events.single['event_id']! as String;
      expect(await hub.eventStore.reader.findEventById(eventId), isNull);
      final result = await bridge.deliver(_wirePayload(delivery.encode()));
      expect(result, isA<SendAnswered>());
      final admitted = await hub.eventStore.reader.findEventById(eventId);
      expect(admitted, isNotNull, reason: 'the hub log holds the event');
      expect(admitted!.aggregateId, 'remote-aggregate-1');
    });

    test('a native esd/batch@3 delivery goes to the hub receiver endpoint '
        'and returns the acknowledgement carrying its record', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final record = SyntheticSender().buildEvent();
      final originator =
          ((record['metadata']! as Map)['provenance']! as List).first as Map;
      final delivery = DeliveryEnvelope.seal(
        batchId: 'bridge-delivery-1',
        senderHop: 'mobile',
        senderIdentifier: 'mobile-install',
        senderSoftwareVersion: 'event_sourcing_demo@0.1.0+1',
        sentAt: DateTime.utc(2026, 9, 1),
        channel: DeliveryChannel(
          senderDatabaseId: originator['database_id']! as String,
          destinationId: 'Native',
          registrationId: 'registration-1',
          generation: 1,
        ),
        deliveryNumber: 1,
        previousDeliveryHash: null,
        events: <Map<String, Object?>>[record],
      );

      final result = await bridge.deliver(
        WirePayload(
          bytes: delivery.encode(),
          contentType: DeliveryEnvelope.wireFormat,
          transformVersion: null,
        ),
      );

      expect(
        result,
        isA<SendAnswered>().having(
          (a) => a.response,
          'response',
          ReceiverAcknowledgement(
            channel: delivery.channel,
            receiverDatabaseId: hub.eventStore.databaseId,
            record: DeliveryRecord(
              deliveryNumber: 1,
              deliveryHash: delivery.deliveryHash,
            ),
            outcome: AcknowledgementOutcome.accepted,
          ),
        ),
      );
      expect(
        await hub.eventStore.reader.findEventById(
          record['event_id']! as String,
        ),
        isNotNull,
      );
    });

    test('garbage bytes return SendPermanent (decode failure)', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final result = await bridge.deliver(
        _wirePayload(Uint8List.fromList(<int>[0, 1, 2, 3])),
      );
      expect(result, isA<SendPermanent>());
    });

    test(
      'another wire format returns SendPermanent and writes nothing',
      () async {
        final hub = await _bootstrapHub(nextPath());
        final bridge = DownstreamBridge(hub.eventStore);
        final before = (await hub.eventStore.reader.findAllEvents()).length;
        final delivery = SyntheticSender().nextDelivery(<Map<String, Object?>>[
          SyntheticSender().buildEvent(),
        ]);
        for (final format in <String>['application/x-unknown', 'esd/batch@2']) {
          final payload = WirePayload(
            bytes: delivery.encode(),
            contentType: format,
            transformVersion: null,
          );
          expect(await bridge.deliver(payload), isA<SendPermanent>());
        }
        expect((await hub.eventStore.reader.findAllEvents()).length, before);
      },
    );

    test('thrown StateError maps to SendTransient', () async {
      final bridge = DownstreamBridge(_ThrowingEventStore(StateError('boom')));
      final delivery = SyntheticSender().nextDelivery(<Map<String, Object?>>[
        SyntheticSender().buildEvent(),
      ]);
      final result = await bridge.deliver(_wirePayload(delivery.encode()));
      expect(result, isA<SendTransient>());
    });

    test('a delivery the hub refuses as rejected maps to SendPermanent naming '
        'the reason', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final record = SyntheticSender().buildEvent()
        ..['lib_format_version'] = DataFormatVersion(
          LibVersion.dataFormat.major + 1,
          0,
        ).toJson();
      record['event_hash'] = canonicalEventHash(record);
      final delivery = SyntheticSender().nextDelivery(<Map<String, Object?>>[
        record,
      ]);
      final result = await bridge.deliver(_wirePayload(delivery.encode()));
      expect(
        result,
        isA<SendPermanent>().having(
          (p) => p.error,
          'error',
          contains(IngestDataFormatIncompatible.refusalReason),
        ),
      );
      expect(
        await hub.eventStore.reader.findEventById(
          record['event_id']! as String,
        ),
        isNull,
      );
    });

    // Verifies: EVS-PRD-ingest/D
    test(
      'a record whose hash does not recompute is accepted; the hub '
      'stores it as received with a hash_mismatch security finding',
      () async {
        final hub = await _bootstrapHub(nextPath());
        final bridge = DownstreamBridge(hub.eventStore);
        final tampered = SyntheticSender().buildEvent()
          ..['event_hash'] = 'f' * 64;
        final eventId = tampered['event_id']! as String;
        final result = await bridge.deliver(
          _wirePayload(
            SyntheticSender().nextDelivery(<Map<String, Object?>>[
              tampered,
            ]).encode(),
          ),
        );
        expect(result, isA<SendAnswered>());
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

    // Verifies: EVS-PRD-ingest/G
    test('a declared reserved entry type under an aggregate type the library '
        'does not declare for it is accepted; the hub keeps the record in '
        'an event_malformed security finding', () async {
      final hub = await _bootstrapHub(nextPath());
      final bridge = DownstreamBridge(hub.eventStore);
      final record = SyntheticSender().buildEvent(
        entryType: kSecurityFindingEntryType,
      );
      final eventId = record['event_id']! as String;
      final result = await bridge.deliver(
        _wirePayload(
          SyntheticSender().nextDelivery(<Map<String, Object?>>[
            record,
          ]).encode(),
        ),
      );
      expect(result, isA<SendAnswered>());
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
  ReceiverEndpoint get receiverEndpoint {
    // ignore: only_throw_errors
    throw _toThrow;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}
