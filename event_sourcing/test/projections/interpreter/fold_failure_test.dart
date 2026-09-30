// Checks the FoldFailure type for the fold-failure vocabulary
// (`EVS-DEV-view-convergence` Terms): which computation site each reason
// names, that only those four sites are wrapped, and that no row is
// written for a fold the wrap catches.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/projections/interpreter/fold_failure.dart';
import 'package:event_sourcing/src/projections/interpreter/projection_interpreter.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' show databaseFactoryMemory;

import '../../test_support/record_fixtures.dart' show kRootVersionCausal;

var _dbCounter = 0;
const _kNoteEntryType = 'note';

Future<SembastBackend> _openBackend() async {
  final db = await databaseFactoryMemory.openDatabase(
    'fold_failure_${_dbCounter++}.db',
  );
  return SembastBackend(database: db);
}

/// A backend whose table-row write throws a plain (non-[FoldFailure])
/// error, standing in for a backend failure unrelated to computing the
/// row.
class _WriteThrowsBackend extends SembastBackend {
  _WriteThrowsBackend({required super.database});

  @override
  Future<void> upsertTableViewRowInTxn(
    Transaction txn,
    String copyId,
    String key,
    Map<String, dynamic> row, {
    required String sourceAggregateId,
  }) {
    throw Exception('storage unavailable');
  }
}

StoredEvent _event({
  required int seq,
  required Map<String, Object?> data,
  EntryTypeVersion entryTypeVersion = const EntryTypeVersion(1, 0),
  String eventType = 'finalized',
}) {
  return StoredEvent(
    key: seq,
    eventId: 'e$seq',
    aggregateId: 'agg-1',
    aggregateType: 'note',
    entryType: _kNoteEntryType,
    entryTypeVersion: entryTypeVersion,
    libFormatVersion: LibVersion.dataFormat,
    eventType: eventType,
    sequenceNumber: seq,
    data: data,
    metadata: <String, dynamic>{'provenance': <Map<String, Object?>>[]},
    initiator: const UserInitiator('test-user'),
    clientTimestamp: DateTime.utc(2026, 1, 1),
    eventHash: 'h$seq',
    flowToken: null,
    previousEventHash: null,
    causal: kRootVersionCausal,
  );
}

void main() {
  group('FoldFailure typing', () {
    // Verifies: EVS-DEV-security-findings/R
    test('a promoter that cannot chain yields promoter_failed', () async {
      final backend = await _openBackend();
      const spec = AggregateProjectionSpec(
        viewName: 'notes',
        interest: SubscriptionFilter(entryTypes: <String>{_kNoteEntryType}),
        tombstoneEventTypes: <String>{},
      );
      final promoters = PromoterRegistry(); // no chain registered
      final copyId = await backend.transaction(
        (txn) => backend.createViewCopyInTxn(txn, 'notes', 'fp', 0),
      );

      await backend.transaction((txn) async {
        await expectLater(
          ProjectionInterpreter.foldIntoView(
            txn: txn,
            backend: backend,
            spec: spec,
            promoters: promoters,
            event: _event(
              seq: 1,
              data: const {'body': 'hello'},
              entryTypeVersion: const EntryTypeVersion(1, 0),
            ),
            // A version above the event's own forces promotion, and no
            // chain is registered from (1,0) to (2,0).
            version: const EntryTypeVersion(2, 0),
            copyId: copyId,
          ),
          throwsA(
            isA<FoldFailure>().having(
              (f) => f.reason,
              'reason',
              FoldFailureReason.promoterFailed,
            ),
          ),
        );
        final row = await backend.readViewRowInTxn(txn, copyId, 'agg-1');
        expect(row, isNull, reason: 'no row is written on a failed fold');
      });
    });

    // Verifies: EVS-DEV-security-findings/R
    test('a table row key that cannot extract yields row_key_failed', () async {
      final backend = await _openBackend();
      const spec = TableProjectionSpec(
        viewName: 'unkeyable',
        interest: SubscriptionFilter(aggregateTypes: {'note'}),
        insertEventTypes: {'finalized'},
        removeEventTypes: {'removed'},
        rowKey: CompositeKey(<String>['data.k']),
        rowData: WholePayload(),
      );
      final promoters = PromoterRegistry();
      final copyId = await backend.transaction(
        (txn) => backend.createViewCopyInTxn(txn, 'unkeyable', 'fp', 0),
      );

      await backend.transaction((txn) async {
        await expectLater(
          ProjectionInterpreter.foldIntoView(
            txn: txn,
            backend: backend,
            spec: spec,
            promoters: promoters,
            event: _event(seq: 1, data: const {'title': 'no key'}),
            version: const EntryTypeVersion(1, 0),
            copyId: copyId,
          ),
          throwsA(
            isA<FoldFailure>().having(
              (f) => f.reason,
              'reason',
              FoldFailureReason.rowKeyFailed,
            ),
          ),
        );
      });
    });

    // Verifies: EVS-DEV-security-findings/R
    test('a table row data extractor that cannot extract yields '
        'row_data_failed', () async {
      final backend = await _openBackend();
      const spec = TableProjectionSpec(
        viewName: 'unextractable',
        interest: SubscriptionFilter(aggregateTypes: {'note'}),
        insertEventTypes: {'finalized'},
        removeEventTypes: {'removed'},
        rowKey: AggregateIdKey(),
        rowData: PayloadField('answers'),
      );
      final promoters = PromoterRegistry();
      final copyId = await backend.transaction(
        (txn) => backend.createViewCopyInTxn(txn, 'unextractable', 'fp', 0),
      );

      await backend.transaction((txn) async {
        await expectLater(
          ProjectionInterpreter.foldIntoView(
            txn: txn,
            backend: backend,
            spec: spec,
            promoters: promoters,
            // 'answers' is present but not a Map, so PayloadField.extract
            // throws.
            event: _event(seq: 1, data: const {'answers': 'not-a-map'}),
            version: const EntryTypeVersion(1, 0),
            copyId: copyId,
          ),
          throwsA(
            isA<FoldFailure>().having(
              (f) => f.reason,
              'reason',
              FoldFailureReason.rowDataFailed,
            ),
          ),
        );
        final row = await backend.readViewRowInTxn(txn, copyId, 'agg-1');
        expect(row, isNull, reason: 'no row is written on a failed fold');
      });
    });

    // No shipped DerivedFieldComputation throws (DottedPathLookup always
    // falls back), so this reason cannot be produced through the public
    // primitives today; it verifies the same wrapper every computation
    // site shares.
    // Verifies: EVS-DEV-security-findings/R
    test('guardFold wraps a throw as derived_field_failed', () {
      expect(
        () => guardFold<int>(FoldFailureReason.derivedFieldFailed, () {
          throw StateError('boom');
        }),
        throwsA(
          isA<FoldFailure>()
              .having(
                (f) => f.reason,
                'reason',
                FoldFailureReason.derivedFieldFailed,
              )
              .having((f) => f.cause, 'cause', isA<StateError>()),
        ),
      );
    });

    test('guardFold never double-wraps a FoldFailure', () {
      final inner = FoldFailure(
        FoldFailureReason.rowKeyFailed,
        StateError('x'),
        StackTrace.current,
      );
      expect(
        () => guardFold<int>(FoldFailureReason.promoterFailed, () {
          throw inner;
        }),
        throwsA(same(inner)),
      );
    });

    test('a throw from the backend row write is not a FoldFailure', () async {
      final backend = _WriteThrowsBackend(
        database: await databaseFactoryMemory.openDatabase(
          'fold_failure_write_${_dbCounter++}.db',
        ),
      );
      const spec = TableProjectionSpec(
        viewName: 'writes',
        interest: SubscriptionFilter(aggregateTypes: {'note'}),
        insertEventTypes: {'finalized'},
        removeEventTypes: {'removed'},
        rowKey: AggregateIdKey(),
        rowData: WholePayload(),
      );
      final promoters = PromoterRegistry();
      final copyId = await backend.transaction(
        (txn) => backend.createViewCopyInTxn(txn, 'writes', 'fp', 0),
      );

      await backend.transaction((txn) async {
        await expectLater(
          ProjectionInterpreter.foldIntoView(
            txn: txn,
            backend: backend,
            spec: spec,
            promoters: promoters,
            event: _event(seq: 1, data: const {'ok': true}),
            version: const EntryTypeVersion(1, 0),
            copyId: copyId,
          ),
          throwsA(isNot(isA<FoldFailure>())),
        );
      });
    });
  });
}
