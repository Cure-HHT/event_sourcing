// Verifies: EVS-DEV-destination-drain/Y
// the transform failure record's persisted form round-trips its failure
//   times and the batch's sequence range, and refuses a malformed record.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  group('TransformFailureRecord', () {
    test('round-trips every field', () {
      final record = TransformFailureRecord(
        failureTimes: [
          DateTime.utc(2026, 9, 1, 12),
          DateTime.utc(2026, 9, 1, 12, 5),
        ],
        sequenceRange: (firstSeq: 3, lastSeq: 7),
      );
      expect(TransformFailureRecord.fromJson(record.toJson()), record);
      expect(record.toJson(), <String, Object?>{
        'failure_times': [
          '2026-09-01T12:00:00.000Z',
          '2026-09-01T12:05:00.000Z',
        ],
        'first_seq': 3,
        'last_seq': 7,
      });
    });

    test('round-trips a single failure', () {
      final record = TransformFailureRecord(
        failureTimes: [DateTime.utc(2026, 1, 1)],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
      );
      expect(TransformFailureRecord.fromJson(record.toJson()), record);
    });

    test('two records with different failure times are unequal', () {
      final a = TransformFailureRecord(
        failureTimes: [DateTime.utc(2026, 1, 1)],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
      );
      final b = TransformFailureRecord(
        failureTimes: [DateTime.utc(2026, 1, 2)],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
      );
      expect(a == b, isFalse);
    });

    test('refuses a malformed record', () {
      final valid = TransformFailureRecord(
        failureTimes: [DateTime.utc(2026, 1, 1)],
        sequenceRange: (firstSeq: 1, lastSeq: 1),
      ).toJson();
      for (final broken in <Map<String, Object?>>[
        {...valid}..remove('failure_times'),
        {...valid, 'failure_times': 'not-a-list'},
        {
          ...valid,
          'failure_times': [1, 2],
        },
        {...valid}..remove('first_seq'),
        {...valid, 'first_seq': 'not-an-int'},
        {...valid}..remove('last_seq'),
        {...valid, 'last_seq': 'not-an-int'},
      ]) {
        expect(
          () => TransformFailureRecord.fromJson(broken),
          throwsFormatException,
          reason: '$broken',
        );
      }
    });
  });
}
