// event_sourcing/test/projections/primitives/row_data_test.dart
//
// Verifies: EVS-PRD-materializer/A
// WholePayload, PayloadField, and
//   SelectedFields are library-supplied materializer primitives (row data
//   extractors for TableProjectionSpec).
// Verifies: EVS-PRD-materializer/B
// each extractor is a pure function of
//   the StoredEvent; tests confirm deterministic extraction and error cases.
import 'package:event_sourcing/src/projections/primitives/row_data.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';

StoredEvent _event(Map<String, Object?> data) => StoredEvent.synthetic(
  eventId: 'e-1',
  aggregateId: 'a',
  aggregateType: 'X',
  entryType: 'x',
  eventType: 'evt',
  initiator: const UserInitiator('u'),
  clientTimestamp: DateTime.utc(2026, 5, 9),
  eventHash: 'h',
  data: data,
);

void main() {
  group('WholePayload', () {
    test('returns the entire data map', () {
      const e = WholePayload();
      final result = e.extract(_event({'a': 1, 'b': 2}));
      expect(result, {'a': 1, 'b': 2});
    });
  });

  group('PayloadField', () {
    test('returns the named field as a Map', () {
      const e = PayloadField('answers');
      final result = e.extract(
        _event({
          'answers': {'q1': 'yes'},
          'meta': 'x',
        }),
      );
      expect(result, {'q1': 'yes'});
    });

    test('returns empty map when field missing', () {
      const e = PayloadField('missing');
      expect(e.extract(_event({})), <String, Object?>{});
    });

    test('throws when field present but not a Map', () {
      const e = PayloadField('answers');
      expect(
        () => e.extract(_event({'answers': 'not-a-map'})),
        throwsStateError,
      );
    });
  });

  group('SelectedFields', () {
    test(
      'returns a Map of just the selected fields, missing fields omitted',
      () {
        const e = SelectedFields(['a', 'c']);
        final result = e.extract(_event({'a': 1, 'b': 2, 'c': 3}));
        expect(result, {'a': 1, 'c': 3});
      },
    );
  });
}
