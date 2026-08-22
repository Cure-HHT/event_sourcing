// Verifies: EVS-PRD-provenance/A (ProvenanceEntry value type shape,
//   required and optional fields, value equality, identity shapes)
// Verifies: EVS-PRD-provenance/C
// (toJson/fromJson round-trip without
//   loss, timezone-offset validation, ingest and origin fields)

import 'package:provenance/provenance.dart';
import 'package:test/test.dart';

void main() {
  group('ProvenanceEntry', () {
    test('construct with all required fields; getters round-trip', () {
      final receivedAt = DateTime.utc(2026, 4, 21, 10, 30, 0);
      final entry = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: receivedAt,
        identifier: 'device-uuid-abc123',
        softwareVersion: 'my_app@1.2.3+45',
      );

      expect(entry.hop, 'mobile-device');
      expect(entry.receivedAt, receivedAt);
      expect(entry.identifier, 'device-uuid-abc123');
      expect(entry.softwareVersion, 'my_app@1.2.3+45');
      expect(entry.transformVersion, isNull);
    });

    test('construct with transformVersion; getter returns value', () {
      final entry = ProvenanceEntry(
        hop: 'control-server',
        receivedAt: DateTime.utc(2026, 4, 21, 11, 0, 0),
        identifier: 'control-instance-7',
        softwareVersion: 'control-functions@0.5.0',
        transformVersion: 'fhir-r4-v1',
      );

      expect(entry.transformVersion, 'fhir-r4-v1');
    });

    test('toJson emits snake_case keys including null transform_version', () {
      final entry = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: DateTime.utc(2026, 4, 21, 10, 30, 0),
        identifier: 'device-uuid-abc123',
        softwareVersion: 'my_app@1.2.3+45',
      );

      expect(entry.toJson(), {
        'hop': 'mobile-device',
        'received_at': '2026-04-21T10:30:00.000Z',
        'identifier': 'device-uuid-abc123',
        'software_version': 'my_app@1.2.3+45',
        'transform_version': null,
      });
    });

    test('toJson emits non-null transform_version when set', () {
      final entry = ProvenanceEntry(
        hop: 'control-server',
        receivedAt: DateTime.utc(2026, 4, 21, 11, 0, 0),
        identifier: 'control-instance-7',
        softwareVersion: 'control-functions@0.5.0',
        transformVersion: 'fhir-r4-v1',
      );

      expect(entry.toJson()['transform_version'], 'fhir-r4-v1');
    });

    test('toJson/fromJson round-trip preserves all fields', () {
      final original = ProvenanceEntry(
        hop: 'relay-server',
        receivedAt: DateTime.utc(2026, 4, 21, 12, 15, 30, 500),
        identifier: 'relay-instance-42',
        softwareVersion: 'relay_functions@0.8.2+101',
        transformVersion: 'v2',
      );

      final roundTripped = ProvenanceEntry.fromJson(original.toJson());

      expect(roundTripped, equals(original));
    });

    test('round-trip preserves null transform_version', () {
      final original = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
        identifier: 'device-xyz',
        softwareVersion: 'my_app@1.0.0',
      );

      final roundTripped = ProvenanceEntry.fromJson(original.toJson());

      expect(roundTripped.transformVersion, isNull);
      expect(roundTripped, equals(original));
    });

    test('received_at serializes with timezone offset (Z for UTC)', () {
      final entry = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: DateTime.utc(2026, 4, 21, 10, 30, 0),
        identifier: 'd',
        softwareVersion: 'my_app@1.0.0',
      );

      final json = entry.toJson();
      expect(json['received_at'], endsWith('Z'));
      expect(json['received_at'], '2026-04-21T10:30:00.000Z');
    });

    group('fromJson validation', () {
      final validJson = {
        'hop': 'mobile-device',
        'received_at': '2026-04-21T10:30:00.000Z',
        'identifier': 'device-uuid',
        'software_version': 'my_app@1.0.0',
        'transform_version': null,
      };

      test('missing hop throws FormatException', () {
        final bad = Map<String, Object?>.of(validJson)..remove('hop');
        expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
      });

      test('missing received_at throws FormatException', () {
        final bad = Map<String, Object?>.of(validJson)..remove('received_at');
        expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
      });

      test('missing identifier throws FormatException', () {
        final bad = Map<String, Object?>.of(validJson)..remove('identifier');
        expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
      });

      test('missing software_version throws FormatException', () {
        final bad = Map<String, Object?>.of(validJson)
          ..remove('software_version');
        expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
      });

      test('non-string hop throws FormatException', () {
        final bad = Map<String, Object?>.of(validJson)..['hop'] = 42;
        expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
      });

      test('non-string transform_version throws FormatException', () {
        final bad = Map<String, Object?>.of(validJson)
          ..['transform_version'] = 12;
        expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
      });

      test('absent transform_version key is treated as null', () {
        final bad = Map<String, Object?>.of(validJson)
          ..remove('transform_version');
        final entry = ProvenanceEntry.fromJson(bad);
        expect(entry.transformVersion, isNull);
      });

      test('malformed received_at throws FormatException', () {
        final bad = Map<String, Object?>.of(validJson)
          ..['received_at'] = 'not-a-date';
        expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
      });

      // DateTime.parse would silently accept this as local time, silently
      // breaking the ALCOA+ Contemporaneous guarantee in an audit chain.
      test(
        'offsetless received_at (no Z, no +/-HH:MM) throws FormatException',
        () {
          final bad = Map<String, Object?>.of(validJson)
            ..['received_at'] = '2026-04-21T10:30:00';
          expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
        },
      );

      test('received_at with +HH:MM offset is accepted', () {
        final input = Map<String, Object?>.of(validJson)
          ..['received_at'] = '2026-04-21T10:30:00+05:30';
        final entry = ProvenanceEntry.fromJson(input);
        expect(entry.receivedAt.isUtc, isTrue);
      });

      test('received_at with -HHMM (no colon) offset is accepted', () {
        final input = Map<String, Object?>.of(validJson)
          ..['received_at'] = '2026-04-21T10:30:00-0430';
        final entry = ProvenanceEntry.fromJson(input);
        expect(entry.receivedAt.isUtc, isTrue);
      });
    });

    group('identity shapes', () {
      test('accepts a mobile-device hop with a device UUID identifier', () {
        final entry = ProvenanceEntry(
          hop: 'mobile-device',
          receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
          identifier: '550e8400-e29b-41d4-a716-446655440000',
          softwareVersion: 'my_app@1.2.3+45',
        );
        expect(
          entry.identifier,
          matches(RegExp(r'^[0-9a-f-]{36}$', caseSensitive: false)),
        );
      });

      test('accepts a server hop with a server instance identifier', () {
        final entry = ProvenanceEntry(
          hop: 'relay-server',
          receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
          identifier: 'relay-instance-42',
          softwareVersion: 'relay_functions@0.8.2',
        );
        expect(entry.identifier, startsWith('relay-instance-'));
      });

      test('software_version round-trips package@semver+build verbatim', () {
        const target = 'my_app@1.2.3+45';
        final entry = ProvenanceEntry(
          hop: 'mobile-device',
          receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
          identifier: 'd',
          softwareVersion: target,
        );
        expect(entry.softwareVersion, target);
        expect(entry.toJson()['software_version'], target);
      });
    });

    group('value equality', () {
      test('equal fields produce equal entries and equal hashCodes', () {
        final a = ProvenanceEntry(
          hop: 'mobile-device',
          receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
          identifier: 'd',
          softwareVersion: 'my_app@1.0.0',
        );
        final b = ProvenanceEntry(
          hop: 'mobile-device',
          receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
          identifier: 'd',
          softwareVersion: 'my_app@1.0.0',
        );

        expect(a, equals(b));
        expect(a.hashCode, b.hashCode);
      });

      test('any field differing breaks equality', () {
        final base = ProvenanceEntry(
          hop: 'mobile-device',
          receivedAt: DateTime.utc(2026, 4, 21, 10, 0, 0),
          identifier: 'd',
          softwareVersion: 'my_app@1.0.0',
        );

        expect(
          base,
          isNot(
            equals(
              ProvenanceEntry(
                hop: 'relay-server',
                receivedAt: base.receivedAt,
                identifier: base.identifier,
                softwareVersion: base.softwareVersion,
              ),
            ),
          ),
        );
        expect(
          base,
          isNot(
            equals(
              ProvenanceEntry(
                hop: base.hop,
                receivedAt: DateTime.utc(2026, 4, 22, 10, 0, 0),
                identifier: base.identifier,
                softwareVersion: base.softwareVersion,
              ),
            ),
          ),
        );
      });
    });
  });

  group('ProvenanceEntry ingest fields', () {
    test('defaults to null for all four ingest fields', () {
      final entry = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'device-abc',
        softwareVersion: 'my_app@1.0.0',
      );
      expect(entry.arrivalHash, isNull);
      expect(entry.previousIngestHash, isNull);
      expect(entry.ingestSequenceNumber, isNull);
      expect(entry.batchContext, isNull);
    });

    test('non-null ingest fields round-trip through JSON', () {
      final entry = ProvenanceEntry(
        hop: 'control-server',
        receivedAt: DateTime.parse('2026-04-24T12:00:01Z'),
        identifier: 'control-1',
        softwareVersion: 'control@0.1.0',
        arrivalHash: 'abc123',
        previousIngestHash: 'def456',
        ingestSequenceNumber: 42,
        batchContext: const BatchContext(
          batchId: 'batch-xyz',
          batchPosition: 3,
          batchSize: 5,
          batchWireBytesHash: 'hhh',
          batchWireFormat: 'esd/batch@1',
        ),
      );
      final json = entry.toJson();
      final back = ProvenanceEntry.fromJson(json);
      expect(back, equals(entry));
      expect(back.arrivalHash, equals('abc123'));
      expect(back.previousIngestHash, equals('def456'));
      expect(back.ingestSequenceNumber, equals(42));
      expect(back.batchContext, isNotNull);
      expect(back.batchContext!.batchId, equals('batch-xyz'));
    });

    test('json omits ingest fields when all null', () {
      final entry = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'device-abc',
        softwareVersion: 'my_app@1.0.0',
      );
      final json = entry.toJson();
      expect(json.containsKey('arrival_hash'), isFalse);
      expect(json.containsKey('previous_ingest_hash'), isFalse);
      expect(json.containsKey('ingest_sequence_number'), isFalse);
      expect(json.containsKey('batch_context'), isFalse);
    });

    test('equality and hashCode include the four new fields', () {
      final a = ProvenanceEntry(
        hop: 'h',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'i',
        softwareVersion: 's@1',
        arrivalHash: 'x',
      );
      final b = ProvenanceEntry(
        hop: 'h',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'i',
        softwareVersion: 's@1',
        arrivalHash: 'y', // differs only here
      );
      expect(a, isNot(equals(b)));
      expect(a.hashCode, isNot(equals(b.hashCode)));
    });
  });

  group('ProvenanceEntry origin_sequence_number', () {
    test('originSequenceNumber defaults to null on originator entries', () {
      final entry = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'device-abc',
        softwareVersion: 'my_app@1.0.0',
      );
      expect(entry.originSequenceNumber, isNull);
    });

    test('non-null originSequenceNumber round-trips through JSON', () {
      final entry = ProvenanceEntry(
        hop: 'relay-server',
        receivedAt: DateTime.parse('2026-04-24T12:00:01Z'),
        identifier: 'relay-instance-7',
        softwareVersion: 'relay_functions@0.8.2',
        arrivalHash: 'aaa',
        previousIngestHash: 'bbb',
        ingestSequenceNumber: 42,
        originSequenceNumber: 17,
      );
      final json = entry.toJson();
      expect(json['origin_sequence_number'], equals(17));
      final back = ProvenanceEntry.fromJson(json);
      expect(back, equals(entry));
      expect(back.originSequenceNumber, equals(17));
    });

    test('toJson omits origin_sequence_number when null', () {
      final entry = ProvenanceEntry(
        hop: 'mobile-device',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'device-abc',
        softwareVersion: 'my_app@1.0.0',
      );
      final json = entry.toJson();
      expect(json.containsKey('origin_sequence_number'), isFalse);
    });

    test('absent origin_sequence_number key decodes to null', () {
      final input = <String, Object?>{
        'hop': 'mobile-device',
        'received_at': '2026-04-24T12:00:00Z',
        'identifier': 'device-abc',
        'software_version': 'my_app@1.0.0',
        'transform_version': null,
      };
      final entry = ProvenanceEntry.fromJson(input);
      expect(entry.originSequenceNumber, isNull);
    });

    test('non-int origin_sequence_number throws FormatException', () {
      final bad = <String, Object?>{
        'hop': 'relay-server',
        'received_at': '2026-04-24T12:00:00Z',
        'identifier': 'relay-instance-1',
        'software_version': 'relay_functions@0.8.2',
        'transform_version': null,
        'origin_sequence_number': '17',
      };
      expect(() => ProvenanceEntry.fromJson(bad), throwsFormatException);
    });

    test('equality and hashCode include originSequenceNumber', () {
      final a = ProvenanceEntry(
        hop: 'h',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'i',
        softwareVersion: 's@1',
        originSequenceNumber: 1,
      );
      final b = ProvenanceEntry(
        hop: 'h',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'i',
        softwareVersion: 's@1',
        originSequenceNumber: 2, // differs only here
      );
      expect(a, isNot(equals(b)));
      expect(a.hashCode, isNot(equals(b.hashCode)));
    });

    test('toString includes originSequenceNumber', () {
      final entry = ProvenanceEntry(
        hop: 'relay-server',
        receivedAt: DateTime.parse('2026-04-24T12:00:00Z'),
        identifier: 'relay-instance-1',
        softwareVersion: 'relay_functions@0.8.2',
        originSequenceNumber: 99,
      );
      expect(entry.toString(), contains('originSequenceNumber: 99'));
    });
  });
}
