// Verifies: EVS-PRD-event-log/A — StoredEvent fields are immutable; fromMap/
//   toMap round-trips preserve every field; malformed records throw
//   FormatException.
// Verifies: EVS-PRD-portability/C — toMap/fromMap produce identical results
//   regardless of platform; pure-Dart serialisation.
// Verifies: EVS-DEV-flow-token/D - flowToken is an opaque nullable String that round-trips; a non-string flow_token is rejected.
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';

Map<String, Object?> _minimalMap({Object? initiator, Object? flowToken}) => {
  'event_id': 'e',
  'aggregate_id': 'a',
  'aggregate_type': 'note',
  'entry_type': 'epistaxis_event',
  'entry_type_version': 1,
  'lib_format_version': 1,
  'event_type': 'finalized',
  'sequence_number': 1,
  'data': const {
    'answers': {'x': 1},
  },
  'metadata': const {'change_reason': 'initial', 'provenance': <Object?>[]},
  'initiator': initiator ?? const {'type': 'user', 'user_id': 'u'},
  'flow_token': flowToken,
  'client_timestamp': '2026-04-22T00:00:00.000Z',
  'event_hash': 'h',
};

Map<String, Object?> _validEventMap() => <String, Object?>{
  'event_id': 'e-1',
  'aggregate_id': 'a-1',
  'aggregate_type': 'note',
  'entry_type': 'demo_note',
  'event_type': 'finalized',
  'sequence_number': 1,
  'data': <String, Object?>{},
  'metadata': <String, Object?>{},
  'initiator': const UserInitiator('u-1').toJson(),
  'flow_token': null,
  'client_timestamp': DateTime.utc(2026, 4, 26).toIso8601String(),
  'event_hash': 'hash-1',
  'previous_event_hash': null,
  'entry_type_version': 1,
  'lib_format_version': 1,
};

void main() {
  group('StoredEvent storage shape', () {
    // round-trips through fromMap/toMap.
    test('initiator round-trips through fromMap/toMap', () {
      final map = _minimalMap();
      final ev = StoredEvent.fromMap(map, 7);
      expect(ev.initiator, const UserInitiator('u'));
      expect(ev.toMap()['initiator'], {'type': 'user', 'user_id': 'u'});
    });

    test('flowToken is nullable and round-trips', () {
      final mapNull = _minimalMap();
      final ev1 = StoredEvent.fromMap(mapNull, 7);
      expect(ev1.flowToken, isNull);
      expect(ev1.toMap()['flow_token'], isNull);

      final mapWithToken = _minimalMap(flowToken: 'invite:ABC');
      final ev2 = StoredEvent.fromMap(mapWithToken, 7);
      expect(ev2.flowToken, 'invite:ABC');
      expect(ev2.toMap()['flow_token'], 'invite:ABC');
    });

    // removed from the serialized map.
    test('top-level user_id / device_id / software_version fields '
        'are not emitted', () {
      final ev = StoredEvent.fromMap(_minimalMap(), 7);
      final map = ev.toMap();
      expect(map.containsKey('user_id'), isFalse);
      expect(map.containsKey('device_id'), isFalse);
      expect(map.containsKey('software_version'), isFalse);
    });

    test('fromMap throws FormatException on missing initiator', () {
      final map = _minimalMap()..remove('initiator');
      expect(() => StoredEvent.fromMap(map, 7), throwsFormatException);
    });

    test('fromMap throws FormatException on non-string flow_token', () {
      final map = _minimalMap()..['flow_token'] = 42;
      expect(() => StoredEvent.fromMap(map, 7), throwsFormatException);
    });

    test('fromMap accepts AutomationInitiator via JSON', () {
      final map = _minimalMap(
        initiator: const {
          'type': 'automation',
          'service': 'retention-policy',
          'triggering_event_id': null,
        },
      );
      final ev = StoredEvent.fromMap(map, 7);
      expect(
        ev.initiator,
        const AutomationInitiator(service: 'retention-policy'),
      );
    });
  });

  group('StoredEvent.synthetic', () {
    test('constructs a minimally-valid StoredEvent for test fixtures', () {
      final ev = StoredEvent.synthetic(
        eventId: 'x',
        aggregateId: 'a',
        entryType: 't',
        initiator: const UserInitiator('u'),
        eventHash: 'h',
        clientTimestamp: DateTime.utc(2026, 4, 22),
      );
      expect(ev.eventId, 'x');
      expect(ev.aggregateId, 'a');
      expect(ev.initiator, const UserInitiator('u'));
      expect(ev.sequenceNumber, 0);
      expect(ev.data, isEmpty);
      expect(ev.metadata, isEmpty);
      expect(ev.flowToken, isNull);
    });
  });

  group('entry_type_version + lib_format_version fields', () {
    test('toMap includes entry_type_version', () {
      final e = StoredEvent.synthetic(
        eventId: 'e-1',
        aggregateId: 'a-1',
        entryType: 'demo_note',
        initiator: const UserInitiator('u-1'),
        clientTimestamp: DateTime.utc(2026, 4, 26),
        eventHash: 'hash-1',
        entryTypeVersion: 7,
      );
      expect(e.toMap()['entry_type_version'], 7);
    });

    test('toMap includes lib_format_version', () {
      final e = StoredEvent.synthetic(
        eventId: 'e-1',
        aggregateId: 'a-1',
        entryType: 'demo_note',
        initiator: const UserInitiator('u-1'),
        clientTimestamp: DateTime.utc(2026, 4, 26),
        eventHash: 'hash-1',
        libFormatVersion: 3,
      );
      expect(e.toMap()['lib_format_version'], 3);
    });

    test('fromMap rejects missing entry_type_version', () {
      final m = _validEventMap()..remove('entry_type_version');
      expect(() => StoredEvent.fromMap(m, 0), throwsFormatException);
    });

    test('fromMap rejects non-int entry_type_version', () {
      final m = _validEventMap()..['entry_type_version'] = 'not-an-int';
      expect(() => StoredEvent.fromMap(m, 0), throwsFormatException);
    });

    test('fromMap rejects missing lib_format_version', () {
      final m = _validEventMap()..remove('lib_format_version');
      expect(() => StoredEvent.fromMap(m, 0), throwsFormatException);
    });

    test('fromMap rejects non-int lib_format_version', () {
      final m = _validEventMap()..['lib_format_version'] = true;
      expect(() => StoredEvent.fromMap(m, 0), throwsFormatException);
    });

    test('round-trip preserves both fields', () {
      final m = _validEventMap()
        ..['entry_type_version'] = 11
        ..['lib_format_version'] = 1;
      final e = StoredEvent.fromMap(m, 0);
      expect(e.entryTypeVersion, 11);
      expect(e.libFormatVersion, 1);
    });

    test('currentLibFormatVersion is 1', () {
      expect(StoredEvent.currentLibFormatVersion, 1);
    });
  });

  group('StoredEvent.withData', () {
    test('replaces only the data field; all other fields preserved', () {
      final original = StoredEvent(
        key: 1,
        eventId: 'e1',
        aggregateId: 'agg-1',
        aggregateType: 'note',
        entryType: 'note',
        entryTypeVersion: 2,
        libFormatVersion: 1,
        eventType: 'finalized',
        sequenceNumber: 42,
        data: <String, dynamic>{'old_key': 'old_value'},
        metadata: <String, dynamic>{'provenance': <Map<String, Object?>>[]},
        initiator: const UserInitiator('u-1'),
        clientTimestamp: DateTime.utc(2026, 1, 1),
        eventHash: 'h1',
        flowToken: null,
        previousEventHash: null,
      );

      final promoted = original.withData(<String, Object?>{
        'new_key': 'new_value',
      });

      expect(promoted.data, {'new_key': 'new_value'});
      // All other fields preserved.
      expect(promoted.key, original.key);
      expect(promoted.eventId, original.eventId);
      expect(promoted.aggregateId, original.aggregateId);
      expect(promoted.aggregateType, original.aggregateType);
      expect(promoted.entryType, original.entryType);
      expect(promoted.entryTypeVersion, original.entryTypeVersion);
      expect(promoted.libFormatVersion, original.libFormatVersion);
      expect(promoted.eventType, original.eventType);
      expect(promoted.sequenceNumber, original.sequenceNumber);
      expect(promoted.metadata, original.metadata);
      expect(promoted.initiator, original.initiator);
      expect(promoted.clientTimestamp, original.clientTimestamp);
      expect(promoted.eventHash, original.eventHash);
      expect(promoted.flowToken, original.flowToken);
      expect(promoted.previousEventHash, original.previousEventHash);
    });
  });
}
