import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('HaltPurpose', () {
    // Verifies: EVS-DEV-destination-drain/Q
    // the halt request's purpose is recorded as `pause` or `reconfigure`.
    test('has the two recorded purposes', () {
      expect(
        <String, HaltPurpose>{for (final p in HaltPurpose.values) p.wire: p},
        <String, HaltPurpose>{
          'pause': HaltPurpose.pause,
          'reconfigure': HaltPurpose.reconfigure,
        },
      );
      for (final purpose in HaltPurpose.values) {
        expect(HaltPurpose.fromWire(purpose.wire), purpose);
      }
    });

    // Verifies: EVS-DEV-destination-drain/L
    // a purpose this build does not know is carried verbatim, equal to
    //   itself and to no purpose it knows.
    test('carries an unknown purpose verbatim', () {
      final future = HaltPurpose.fromWire('future_purpose');
      expect(future.wire, 'future_purpose');
      expect(future.isKnown, isFalse);
      expect(future, HaltPurpose.fromWire('future_purpose'));
      expect(future.hashCode, HaltPurpose.fromWire('future_purpose').hashCode);
      expect(HaltPurpose.fromWire('Pause'), isNot(HaltPurpose.pause));
      expect(HaltPurpose.values, isNot(contains(future)));
      for (final purpose in HaltPurpose.values) {
        expect(purpose.isKnown, isTrue);
        expect(HaltPurpose.fromWire(purpose.wire), same(purpose));
      }
    });
  });

  group('HaltRequest', () {
    // Verifies: EVS-DEV-destination-drain/L
    // a stored halt request whose purpose this build does not know reads
    //   back with the purpose verbatim.
    test('reads a request of an unknown purpose back verbatim', () {
      final json = <String, Object?>{
        'request_event_id': 'e-1',
        'requested_at': '2026-04-01T02:03:04.005006Z',
        'purpose': 'future_purpose',
        'requested_by': const UserInitiator('operator').toJson(),
      };
      final read = HaltRequest.fromJson(json);
      expect(read.purpose.wire, 'future_purpose');
      expect(read.toJson(), json);
    });

    final request = HaltRequest(
      requestEventId: 'e-1',
      requestedAt: DateTime.utc(2026, 4, 1, 2, 3, 4, 5, 6),
      purpose: HaltPurpose.reconfigure,
      requestedBy: const UserInitiator('operator').toJson(),
    );

    test('round-trips every field', () {
      expect(HaltRequest.fromJson(request.toJson()), request);
      expect(request.toJson(), <String, Object?>{
        'request_event_id': 'e-1',
        'requested_at': '2026-04-01T02:03:04.005006Z',
        'purpose': 'reconfigure',
        'requested_by': <String, Object?>{
          'type': 'user',
          'user_id': 'operator',
        },
      });
      final other = HaltRequest(
        requestEventId: 'e-1',
        requestedAt: request.requestedAt,
        purpose: HaltPurpose.reconfigure,
        requestedBy: const UserInitiator('someone-else').toJson(),
      );
      expect(other == request, isFalse);
      expect(HaltRequest.fromJson(request.toJson()).hashCode, request.hashCode);
    });

    test('refuses a malformed record', () {
      final valid = request.toJson();
      for (final broken in <Map<String, Object?>>[
        {...valid}..remove('request_event_id'),
        {...valid, 'request_event_id': ''},
        {...valid, 'requested_at': 5},
        {...valid, 'purpose': null},
        {...valid, 'requested_by': 'operator'},
      ]) {
        expect(
          () => HaltRequest.fromJson(broken),
          throwsFormatException,
          reason: '$broken',
        );
      }
    });
  });

  group('SendFence', () {
    final fence = SendFence(
      entryId: 'row-1',
      attemptCount: 2,
      at: DateTime.utc(2026, 4, 1, 2, 3, 4),
    );

    test('round-trips every field', () {
      expect(SendFence.fromJson(fence.toJson()), fence);
      expect(fence.toJson(), <String, Object?>{
        'entry_id': 'row-1',
        'attempt_count': 2,
        'at': '2026-04-01T02:03:04.000Z',
      });
    });

    test('refuses a malformed record', () {
      final valid = fence.toJson();
      for (final broken in <Map<String, Object?>>[
        {...valid}..remove('entry_id'),
        {...valid, 'attempt_count': '2'},
        {...valid, 'at': null},
      ]) {
        expect(
          () => SendFence.fromJson(broken),
          throwsFormatException,
          reason: '$broken',
        );
      }
    });

    // Verifies: EVS-DEV-delivery-channel/I
    // the send fence of a delivery on a channel names its delivery number
    //   and delivery hash; a fence of a destination that is no channel names
    //   neither.
    test('names the delivery number and hash of a delivery', () {
      final delivery = SendFence(
        entryId: 'row-1',
        attemptCount: 0,
        at: DateTime.utc(2026, 4, 1, 2, 3, 4),
        deliveryNumber: 5,
        deliveryHash: 'h5',
      );
      expect(delivery.toJson(), <String, Object?>{
        'entry_id': 'row-1',
        'attempt_count': 0,
        'at': '2026-04-01T02:03:04.000Z',
        'delivery_number': 5,
        'delivery_hash': 'h5',
      });
      expect(SendFence.fromJson(delivery.toJson()), delivery);
      expect(fence.deliveryNumber, isNull);
      expect(fence.deliveryHash, isNull);
      expect(
        delivery,
        isNot(
          SendFence(
            entryId: 'row-1',
            attemptCount: 0,
            at: DateTime.utc(2026, 4, 1, 2, 3, 4),
            deliveryNumber: 5,
            deliveryHash: 'other',
          ),
        ),
      );
      for (final broken in <Map<String, Object?>>[
        {...delivery.toJson(), 'delivery_number': '5'},
        {...delivery.toJson(), 'delivery_hash': 5},
        {...delivery.toJson()}..remove('delivery_hash'),
      ]) {
        expect(
          () => SendFence.fromJson(broken),
          throwsFormatException,
          reason: '$broken',
        );
      }
    });
  });

  group('SenderChannelRecord', () {
    // Verifies: EVS-DEV-delivery-channel/D
    // the record a registration starts with is generation 1, number 0, a
    //   null hash and no receiver identity; every field round-trips.
    test('starts at generation 1, number 0, no hash and no receiver', () {
      expect(SenderChannelRecord.initial.generation, 1);
      expect(SenderChannelRecord.initial.receiverRecord, DeliveryRecord.none);
      expect(SenderChannelRecord.initial.receiverDatabaseId, isNull);
      expect(SenderChannelRecord.initial.toJson(), <String, Object?>{
        'generation': 1,
        'delivery_number': 0,
        'delivery_hash': null,
        'receiver_database_id': null,
      });
      const advanced = SenderChannelRecord(
        generation: 3,
        receiverRecord: DeliveryRecord(deliveryNumber: 9, deliveryHash: 'h9'),
        receiverDatabaseId: 'receiver-db',
      );
      expect(SenderChannelRecord.fromJson(advanced.toJson()), advanced);
      expect(
        SenderChannelRecord.fromJson(SenderChannelRecord.initial.toJson()),
        SenderChannelRecord.initial,
      );
      expect(advanced, isNot(SenderChannelRecord.initial));
    });

    test('refuses a malformed record', () {
      const valid = <String, Object?>{
        'generation': 2,
        'delivery_number': 1,
        'delivery_hash': 'h1',
        'receiver_database_id': 'r',
      };
      for (final broken in <Map<String, Object?>>[
        {...valid}..remove('generation'),
        {...valid, 'generation': 0},
        {...valid, 'generation': '2'},
        {...valid, 'delivery_number': -1},
        {...valid, 'delivery_hash': null},
        {...valid, 'delivery_number': 0},
        {...valid, 'receiver_database_id': 7},
      ]) {
        expect(
          () => SenderChannelRecord.fromJson(broken),
          throwsFormatException,
          reason: '$broken',
        );
      }
    });
  });
}
