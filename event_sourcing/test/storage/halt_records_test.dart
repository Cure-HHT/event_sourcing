// Verifies: EVS-DEV-destination-drain/Q
// this build appends a halt request with one of two purposes, each with its
//   recorded string.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('HaltPurpose', () {
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
  });
}
