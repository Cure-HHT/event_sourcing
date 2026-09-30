// Runs the scenarios of delivery_receiver_conformance.dart on Postgres, and
// races two presentations of one delivery through two event stores over
// one database; gated on PG_TEST_URL.
//
// The scenarios' assertions are cited on their own tests in
// test_support/delivery_receiver_conformance.dart.

@TestOn('vm')
library;

import 'dart:convert';
import 'dart:math' show Random;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/permissions/wait_for_current_views.dart'
    show waitForViewsCurrent;
import 'package:event_sourcing/src/projections/view_fingerprint.dart'
    show viewFingerprint;
import 'package:event_sourcing/src/storage/chain_coordinates.dart'
    show ChainCoordinates;
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:flutter_test/flutter_test.dart';

import '../../test_support/delivery_receiver_conformance.dart';
import '../../test_support/ingest_record_findings_conformance.dart'
    show asReceived, sealedRecord;
import 'postgres_scenario_database.dart';
import 'test_postgres_url.dart';

/// A table view keyed on `data.k`, so a record carrying U+0000 that ingest
/// keeps in a finding (never storing it as an event) folds into no row,
/// while the delivery's other records fold normally.
const String _kNulByteView = 'nul_byte_delivery_notes';

const TableProjectionSpec _kNulByteTableSpec = TableProjectionSpec(
  viewName: _kNulByteView,
  interest: SubscriptionFilter(entryTypes: <String>{kDeliveryNoteType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: CompositeKey(<String>['data.k']),
  rowData: WholePayload(),
);

/// A table view keyed on `data.k`, so a key several kilobytes long reaches a
/// real `view_rows` btree-index write the server refuses with SQLSTATE
/// 54000 (`program_limit_exceeded`, index row too large): a fold failure
/// of reason `row_write_failed` (`EVS-DEV-view-convergence` Terms), not a
/// storage failure -- the row key extracts cleanly, so nothing throws
/// before the write itself.
const String _kBigKeyView = 'big_key_delivery_notes';

const TableProjectionSpec _kBigKeyTableSpec = TableProjectionSpec(
  viewName: _kBigKeyView,
  interest: SubscriptionFilter(entryTypes: <String>{kDeliveryNoteType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: CompositeKey(<String>['data.k']),
  rowData: WholePayload(),
);

/// A deterministic pseudo-random string of [length] characters, spread
/// over a 62-symbol alphabet: too irregular for the server's storage
/// compression to shrink it back under the btree index's row-size limit,
/// unlike a repeated character (which compresses to almost nothing).
String _incompressibleKey(int length) {
  final random = Random(1234567);
  const alphabet =
      'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  return String.fromCharCodes(
    List<int>.generate(
      length,
      (_) => alphabet.codeUnitAt(random.nextInt(alphabet.length)),
    ),
  );
}

void main() {
  final pg = PostgresTestDatabase.fromEnvironment(tag: 'delrecv');
  if (pg != null) tearDownAll(pg.drop);
  runDeliveryReceiverScenarios(
    openDatabase: () => PostgresScenarioDatabase.fresh(pg),
    backendLabel: 'postgres',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
  );

  // Verifies: EVS-DEV-delivery-receiver/B
  // Verifies: EVS-PRD-delivery-channel/C
  test(
    'two instances presented the same next delivery at once accept it once '
    'and acknowledge the other presentation as represented',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
    () async {
      final db = (await PostgresScenarioDatabase.fresh(pg))!;
      final first = await openReceiverStore(db);
      final second = await openReceiverStore(db);
      addTearDown(() async {
        await second.close();
        await first.close();
        await db.close();
      });
      final senders = <String>{deliveryChannel().senderDatabaseId};
      var previous = sealedDelivery();
      await first.receiverEndpoint.accept(
        previous.encode(),
        senderDatabaseIds: senders,
      );

      for (var number = 2; number <= 11; number++) {
        final next = sealedDelivery(
          number: number,
          link: previous.deliveryHash,
        );
        final bytes = next.encode();
        final answers = await Future.wait(<Future<ReceiverResponse>>[
          first.receiverEndpoint.accept(bytes, senderDatabaseIds: senders),
          second.receiverEndpoint.accept(bytes, senderDatabaseIds: senders),
        ]);
        final outcomes = <AcknowledgementOutcome>[
          for (final a in answers) (a as ReceiverAcknowledgement).outcome,
        ]..sort((a, b) => a.index.compareTo(b.index));
        expect(outcomes, <AcknowledgementOutcome>[
          AcknowledgementOutcome.accepted,
          AcknowledgementOutcome.represented,
        ], reason: 'delivery $number');
        for (final a in answers) {
          expect(a.record, recordAfter(next));
        }
        previous = next;
      }

      final audits = await authoredDeliveryAudits(first);
      expect(
        <Object?>[for (final a in audits) a.data['delivery_number']],
        <int>[for (var n = 1; n <= 11; n++) n],
        reason: 'exactly one audit per delivery',
      );
    },
  );

  // Verifies: EVS-PRD-ingest/G
  // Verifies: EVS-DEV-security-findings/O
  // Verifies: EVS-DEV-security-findings/R
  // Verifies: EVS-DEV-security-findings/U
  // Verifies: EVS-DEV-event-record/L
  test(
    'a delivered record carrying U+0000, which no backend the library ships '
    'can store as an event, is kept in one event_malformed finding '
    '(unstorable_character); the rest of the delivery is admitted and '
    'folds normally',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
    () async {
      final db = (await PostgresScenarioDatabase.fresh(pg))!;
      final registry = ProjectionRegistry()..register(_kNulByteTableSpec);
      final store = await openReceiverStore(db, projections: registry);
      addTearDown(() async {
        await store.close();
        await db.close();
      });
      final senders = <String>{deliveryChannel().senderDatabaseId};

      await waitForViewsCurrent(store, <String>{
        _kNulByteView,
      }, DateTime.now().add(const Duration(seconds: 10)));

      final ok1 = sealedRecord(data: <String, Object?>{'k': 'a'});
      final nul = sealedRecord(
        data: <String, Object?>{'k': 'b', 'note': 'x\u0000y'},
      );
      final ok2 = sealedRecord(data: <String, Object?>{'k': 'c'});
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[ok1, nul, ok2],
      );

      final response = await store.receiverEndpoint.accept(
        delivery.encode(),
        senderDatabaseIds: senders,
      );
      expect(
        response,
        isA<ReceiverAcknowledgement>().having(
          (r) => r.outcome,
          'outcome',
          AcknowledgementOutcome.accepted,
        ),
        reason:
            'a record no backend can store as an event does not block the '
            'rest of the delivery',
      );

      expect(
        await store.reader.findEventById(ok1['event_id']! as String),
        isNotNull,
        reason: 'the rest of the delivery is admitted',
      );
      expect(
        await store.reader.findEventById(ok2['event_id']! as String),
        isNotNull,
        reason: 'the rest of the delivery is admitted',
      );
      expect(
        await store.reader.findEventById(nul['event_id']! as String),
        isNull,
        reason:
            'no backend the library ships can store a record carrying '
            'U+0000 as an event',
      );

      final findings = await authoredFindings(store);
      expect(findings, hasLength(1));
      final finding = findings.single;
      expect(finding['kind'], 'event_malformed');
      final evidence = finding['evidence']! as Map;
      expect(evidence['reason'], 'unstorable_character');
      final encoded = evidence['record'];
      expect(
        encoded,
        isA<String>(),
        reason: 'a record carrying U+0000 is kept encoded, not verbatim',
      );
      // The encoding round-trips: base64 -> UTF-8 -> canonical JSON decodes
      // back to the record as it was sent.
      final decoded = jsonDecode(
        utf8.decode(base64.decode(encoded! as String)),
      );
      expect(decoded, asReceived(nul));

      await waitForViewsCurrent(store, <String>{
        _kNulByteView,
      }, DateTime.now().add(const Duration(seconds: 10)));
      final rows = await store.reader.findViewRows(_kNulByteView);
      expect(
        rows.rows.map((r) => r['k']).toSet(),
        <String>{'a', 'c'},
        reason:
            'the record kept in a finding folds into no row; the '
            "delivery's other records fold normally",
      );
    },
  );

  // Verifies: EVS-DEV-view-convergence/E
  // Verifies: EVS-DEV-security-findings/R
  // Verifies: EVS-DEV-security-findings/S
  test(
    'a view-row write the server rejects with SQLSTATE 54000 is a '
    'fold_failed finding of reason row_write_failed; the delivery is '
    'accepted, the copy stays current and its other row folds normally',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
    () async {
      final db = (await PostgresScenarioDatabase.fresh(pg))!;
      final registry = ProjectionRegistry()..register(_kBigKeyTableSpec);
      final store = await openReceiverStore(db, projections: registry);
      addTearDown(() async {
        await store.close();
        await db.close();
      });
      final senders = <String>{deliveryChannel().senderDatabaseId};

      await waitForViewsCurrent(store, <String>{
        _kBigKeyView,
      }, DateTime.now().add(const Duration(seconds: 10)));

      final huge = sealedRecord(
        data: <String, Object?>{'k': _incompressibleKey(4000)},
      );
      final ok = sealedRecord(data: <String, Object?>{'k': 'ok'});
      final delivery = sealedDelivery(
        records: <Map<String, Object?>>[huge, ok],
      );

      final response = await store.receiverEndpoint.accept(
        delivery.encode(),
        senderDatabaseIds: senders,
      );
      expect(
        response,
        isA<ReceiverAcknowledgement>().having(
          (r) => r.outcome,
          'outcome',
          AcknowledgementOutcome.accepted,
        ),
        reason:
            'a view-row write the server rejects for its value is a fold '
            'failure, never a storage failure: it never refuses the '
            'delivery',
      );

      for (final record in <Map<String, Object?>>[huge, ok]) {
        expect(
          await store.reader.findEventById(record['event_id']! as String),
          isNotNull,
          reason: 'every event of an accepted delivery is stored',
        );
      }

      await waitForViewsCurrent(store, <String>{
        _kBigKeyView,
      }, DateTime.now().add(const Duration(seconds: 10)));
      final progress = (await store.reader.viewProgress()).singleWhere(
        (p) => p.viewName == _kBigKeyView,
      );
      expect(
        progress.state,
        ViewConvergenceState.current,
        reason: 'a copy that passes over a failed fold stays current',
      );

      final rows = await store.reader.findViewRows(_kBigKeyView);
      expect(
        rows.rows.map((r) => r['k']).toList(),
        <String>['ok'],
        reason:
            'no row is written for the event whose row write the server '
            "rejected; the delivery's other event still folds",
      );

      final findings = await authoredFindings(store);
      expect(findings, hasLength(1));
      expect(findings.single['kind'], 'fold_failed');
      final expectedFingerprint = viewFingerprint(
        _kBigKeyTableSpec,
        store.entryTypes,
        store.promoters,
      );
      final hugeEvent = (await store.reader.findEventById(
        huge['event_id']! as String,
      ))!;
      expect(findings.single['evidence'], <String, Object?>{
        'view': _kBigKeyView,
        'definition_fingerprint': expectedFingerprint,
        'event_id': hugeEvent.eventId,
        'sealed_hash': ChainCoordinates.of(hugeEvent).sealedHash,
        'reason': 'row_write_failed',
      });
    },
  );

  // Verifies: EVS-DEV-view-convergence/E
  test(
    "a genuine storage failure (SQLSTATE 40001) raised inside the fold's "
    'savepoint is not caught as a fold failure: it refuses the whole '
    'delivery, leaving nothing behind',
    skip: pg == null ? 'PG_TEST_URL is not set' : null,
    () async {
      final db = (await PostgresScenarioDatabase.fresh(pg))!;
      final registry = ProjectionRegistry()..register(_kBigKeyTableSpec);
      final store = await openReceiverStore(db, projections: registry);
      addTearDown(() async {
        await store.close();
        await db.close();
      });
      final senders = <String>{deliveryChannel().senderDatabaseId};

      await waitForViewsCurrent(store, <String>{
        _kBigKeyView,
      }, DateTime.now().add(const Duration(seconds: 10)));

      final record = sealedRecord(data: <String, Object?>{'k': 'x'});
      final delivery = sealedDelivery(records: <Map<String, Object?>>[record]);

      await runWithDeliveryTestHooks(
        DeliveryTestHooks(
          failFoldSavepointWithSerializationFailure: () => true,
        ),
        () => expectLater(
          store.receiverEndpoint.accept(
            delivery.encode(),
            senderDatabaseIds: senders,
          ),
          // Every attempt of the storing transaction meets the injected
          // 40001, so the transaction's own bounded retry (a serialization
          // conflict is retried whatever raised it) exhausts and surfaces
          // this typed failure rather than the bare ServerException: it is
          // still never caught as a fold failure, so it still refuses the
          // whole delivery.
          throwsA(
            isA<TransactionRetryExhaustedException>().having(
              (e) => e.lastError.code,
              'lastError.code',
              '40001',
            ),
          ),
          reason:
              'a storage failure inside the savepoint is rethrown '
              'unchanged, refusing the whole storing transaction',
        ),
      );

      expect(
        await store.reader.findEventById(record['event_id']! as String),
        isNull,
        reason: 'a storage failure rolls back the whole delivery',
      );
      expect(
        await store.reader.findAllEvents(entryType: kSecurityFindingEntryType),
        isEmpty,
        reason: 'a storage failure records no fold_failed finding',
      );
      expect(
        await authoredDeliveryAudits(store),
        isEmpty,
        reason: 'no delivery audit is left behind by the refused delivery',
      );
    },
  );
}
