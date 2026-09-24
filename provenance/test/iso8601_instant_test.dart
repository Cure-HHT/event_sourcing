// Verifies: EVS-DEV-event-record/A+C
// (the shared timestamp form: a four-digit year, calendar fields within
//   their ranges and an explicit offset)

import 'package:provenance/provenance.dart';
import 'package:test/test.dart';

import 'timestamp_cases.dart';

void main() {
  group('parseIso8601Instant', () {
    // Verifies: EVS-DEV-event-record/A+C
    test('accepts a four-digit year, calendar fields in range and an '
        'explicit offset, returning the instant DateTime.parse reads', () {
      for (final entry in acceptedTimestamps.entries) {
        final parsed = parseIso8601Instant(entry.value);
        expect(
          parsed.isAtSameMomentAs(DateTime.parse(entry.value)),
          isTrue,
          reason: entry.key,
        );
        expect(parsed.isUtc, isTrue, reason: entry.key);
      }
    });

    // Verifies: EVS-DEV-event-record/A+C
    test('refuses a timestamp without an offset, outside the four-digit '
        'years, or with a field out of its calendar range', () {
      for (final entry in refusedTimestamps.entries) {
        expect(
          () => parseIso8601Instant(entry.value),
          throwsFormatException,
          reason: entry.key,
        );
      }
    });

    // Verifies: EVS-DEV-event-record/A+C
    test('names the field that is out of range', () {
      expect(
        () => parseIso8601Instant('2026-02-30T00:00:00Z'),
        throwsA(
          isA<FormatException>().having(
            (e) => e.message,
            'message',
            contains('day 30'),
          ),
        ),
      );
    });
  });
}
