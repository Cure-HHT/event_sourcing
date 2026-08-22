// event_sourcing/test/projections/interpreter/aggregate_fold_test.dart
//
// Verifies: EVS-PRD-materializer/A
// AggregateFold provides the fold engine
//   that the library's materializer uses for AggregateProjectionSpec views.
// Verifies: EVS-PRD-materializer/B
// merge + metadata-stamp + derived-field
//   sequence is a pure function of (prior, event); tests confirm first-event
//   creation, incremental merge, and tombstone deletion are deterministic.
import 'package:event_sourcing/src/projections/interpreter/aggregate_fold.dart';
import 'package:event_sourcing/src/projections/primitives/derived_field.dart';
import 'package:event_sourcing/src/projections/projection_spec.dart';
import 'package:event_sourcing/src/projections/subscription_filter.dart';
import 'package:event_sourcing/src/storage/initiator.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

Future<SembastBackend> _backend() async => SembastBackend(
  database: await newDatabaseFactoryMemory().openDatabase(
    'af-${DateTime.now().microsecondsSinceEpoch}.db',
  ),
);

StoredEvent _ev(
  String aggId,
  String type,
  Map<String, Object?> data, {
  DateTime? ts,
  int? sequenceNumber,
}) => StoredEvent.synthetic(
  eventId: 'e-$aggId-$type-${ts?.microsecondsSinceEpoch ?? 0}',
  aggregateId: aggId,
  entryType: 'epistaxis_event',
  eventType: type,
  initiator: const UserInitiator('u'),
  clientTimestamp: ts ?? DateTime.utc(2026, 5, 9),
  eventHash: 'h',
  sequenceNumber: sequenceNumber ?? 1,
  data: data,
);

final _spec = AggregateProjectionSpec(
  viewName: 'diary_entries',
  interest: SubscriptionFilter(aggregateTypes: const {'note'}),
  tombstoneEventTypes: const {'tombstone'},
  derivedFields: const [
    DerivedField(
      'effective_date',
      DottedPathLookup(
        'answers.date_of_event',
        fallback: FirstEventTimestamp(),
      ),
    ),
  ],
);

void main() {
  group('AggregateFold.applyEvent', () {
    test(
      'first event creates row with merged data + metadata + derivation',
      () async {
        final backend = await _backend();
        await backend.transaction((txn) async {
          await AggregateFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: _spec,
            event: _ev('e1', 'finalized', {
              'answers': {'q1': 'yes', 'date_of_event': '2026-04-01'},
            }, ts: DateTime.utc(2026, 5, 9)),
          );
        });
        final row = await backend.transaction(
          (txn) async => backend.readViewRowInTxn(txn, 'diary_entries', 'e1'),
        );
        expect(row?['answers'], {'q1': 'yes', 'date_of_event': '2026-04-01'});
        expect(row?['effective_date'], '2026-04-01');
        expect(row?['updatedAt'], isNotNull);
        expect(row?['latestEventId'], isNotNull);
      },
    );

    test(
      'second event merges into existing row (null clears, absent preserves)',
      () async {
        final backend = await _backend();
        await backend.transaction((txn) async {
          await AggregateFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: _spec,
            event: _ev('e1', 'checkpoint', {
              'answers': {'q1': 'yes', 'q2': 'no'},
            }),
          );
          await AggregateFold.applyEvent(
            txn: txn,
            backend: backend,
            spec: _spec,
            event: _ev('e1', 'checkpoint', {
              'answers': {'q2': null},
            }),
          );
        });
        final row = await backend.transaction(
          (txn) async => backend.readViewRowInTxn(txn, 'diary_entries', 'e1'),
        );
        expect((row?['answers'] as Map)['q1'], 'yes'); // preserved
        expect((row?['answers'] as Map)['q2'], isNull); // cleared
      },
    );

    test('tombstone event deletes the row', () async {
      final backend = await _backend();
      await backend.transaction((txn) async {
        await AggregateFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          event: _ev('e1', 'finalized', {
            'answers': {'q1': 'yes'},
          }),
        );
        await AggregateFold.applyEvent(
          txn: txn,
          backend: backend,
          spec: _spec,
          event: _ev('e1', 'tombstone', {}),
        );
      });
      final row = await backend.transaction(
        (txn) async => backend.readViewRowInTxn(txn, 'diary_entries', 'e1'),
      );
      expect(row, isNull);
    });
  });
}
