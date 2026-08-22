// Verifies: EVS-PRD-destinations/C
// (FIFO order — drain attempts rows in
//   sequence_in_queue order; SendOk advances to the next head; a wedged head
//   halts the pass; trail rows are never sent ahead of a wedged head)
// Verifies: EVS-PRD-destinations/D
// (durable queue — drain reads from
//   StorageBackend; queued rows survive across drain call boundaries)
// Verifies: EVS-PRD-destinations/E
// (pluggable delivery — every test calls
//   drain() via FakeDestination.send, the application-supplied transport)
import 'dart:convert';

import 'package:event_sourcing/src/destinations/batch_envelope_metadata.dart';
import 'package:event_sourcing/src/destinations/wire_payload.dart';
import 'package:event_sourcing/src/ingest/batch_envelope.dart';
import 'package:event_sourcing/src/storage/attempt_result.dart';
import 'package:event_sourcing/src/storage/final_status.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/send_result.dart';
import 'package:event_sourcing/src/sync/drain.dart';
import 'package:event_sourcing/src/sync/sync_policy.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast.dart' as sembast;
import 'package:sembast/sembast_memory.dart';

import '../test_support/fake_destination.dart';
import '../test_support/fifo_entry_helpers.dart';

/// Fixture — a fresh in-memory SembastBackend per test.
Future<SembastBackend> _openBackend(String path) async {
  final db = await newDatabaseFactoryMemory().openDatabase(path);
  return SembastBackend(database: db);
}

/// Enqueue a single-event row via the batch-aware `enqueueFifo`. The
/// backend mints a v4-UUID `entry_id` at enqueue time (independent of
/// the event id); callers that need to look the row up later capture the
/// returned `FifoEntry.entryId`.
Future<String> _enqueueRow(
  SembastBackend backend,
  String destId, {
  required String eventId,
  required int sequenceNumber,
}) async {
  final entry = await enqueueSingle(
    backend,
    destId,
    eventId: eventId,
    sequenceNumber: sequenceNumber,
    wirePayload: <String, Object?>{'event_id': eventId},
    wireFormat: 'fake-v1',
    transformVersion: 'fake-v1',
  );
  return entry.entryId;
}

void main() {
  group('drain()', () {
    late SembastBackend backend;
    var dbCounter = 0;

    setUp(() async {
      dbCounter += 1;
      backend = await _openBackend('drain-$dbCounter.db');
    });

    tearDown(() async {
      await backend.close();
    });

    test('empty FIFO returns without calling send', () async {
      final dest = FakeDestination();
      await drain(dest, backend: backend);
      expect(dest.sent, isEmpty);
    });

    test('SendOk marks head sent and advances to the next head', () async {
      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = FakeDestination(script: [const SendOk()]);

      await drain(dest, backend: backend);

      expect(dest.sent, hasLength(1));
      // After markFinal sent, the head is gone; readFifoHead returns null.
      expect(await backend.readFifoHead('fake'), isNull);
    });

    test('drain loops across multiple SendOks in one call', () async {
      var seq = 0;
      for (final id in ['e1', 'e2', 'e3']) {
        seq += 1;
        await _enqueueRow(backend, 'fake', eventId: id, sequenceNumber: seq);
      }
      final dest = FakeDestination(
        script: [const SendOk(), const SendOk(), const SendOk()],
      );

      await drain(dest, backend: backend);
      expect(dest.sent, hasLength(3));
      expect(await backend.readFifoHead('fake'), isNull);
    });

    // When the head row's final_status is FinalStatus.wedged, drain
    // SHALL return without calling Destination.send; the row is NOT
    // re-attempted, and its trail rows are NOT attempted either.
    // Recovery from a wedged head is tombstoneAndRefill.
    test('drain halts when head is wedged, does not call send', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      await backend.markFinal('fake', e1RowId, FinalStatus.wedged);
      // Script would throw StateError if send() were invoked (see
      // FakeDestination.send); absence of such a throw confirms
      // drain did not call send. We script SendOk defensively so a
      // regression that DID call send would surface as a hasLength(1)
      // mismatch rather than an exhausted-script StateError.
      final dest = FakeDestination(script: [const SendOk()]);

      await drain(dest, backend: backend);

      expect(dest.sent, isEmpty);
      // The wedged row remains wedged, unchanged.
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);
    });

    // On the next loop iteration, drain reads the newly-wedged row and
    // halts at the top-of-loop check. Concretely: drain attempts e1
    // exactly once, e1 becomes wedged, e2 (the trail row) is NEVER
    // attempted, and e1 remains at the head of readFifoHead.
    test('SendPermanent marks head wedged; drain halts on '
        'next iteration; trail row is NOT attempted', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      final dest = FakeDestination(
        script: [const SendPermanent(error: 'schema-skew')],
      );

      await drain(dest, backend: backend);
      // Exactly one send call — e1. e2 (trail) was NOT attempted.
      expect(dest.sent, hasLength(1));

      // e1 is wedged; e2 is still pre-terminal (final_status null).
      // readFifoHead returns the wedged e1 because wedged is a
      // returnable-but-halting final_status.
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);

      // e2 is still pre-terminal.
      final e2 = await backend.readFifoRow('fake', e2RowId);
      expect(e2, isNotNull);
      expect(e2!.finalStatus, isNull);
    });

    // SendTransient at maxAttempts marks the head wedged; drain halts
    // on the next iteration; the trail row is NOT attempted. Uses a
    // tiny maxAttempts policy (=1) with Duration.zero backoffs so a
    // single SendTransient trips the cap.
    test('SendTransient at maxAttempts marks head wedged; '
        'drain halts on next iteration; trail row is NOT attempted', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      final e2RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e2',
        sequenceNumber: 2,
      );
      const oneAttemptPolicy = SyncPolicy(
        initialBackoff: Duration.zero,
        backoffMultiplier: 1.0,
        maxBackoff: Duration.zero,
        jitterFraction: 0.0,
        maxAttempts: 1,
        periodicInterval: Duration(minutes: 15),
      );
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      await drain(
        dest,
        backend: backend,
        clock: () => DateTime.utc(2026, 4, 22, 11),
        policy: oneAttemptPolicy,
      );
      // Exactly one send call — e1 tripped the cap. e2 was NOT attempted.
      expect(dest.sent, hasLength(1));

      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);

      // e2 remains pre-terminal.
      final e2 = await backend.readFifoRow('fake', e2RowId);
      expect(e2, isNotNull);
      expect(e2!.finalStatus, isNull);
    });

    // A transient attempt is appended; entry remains pending; backoff
    // gates the next drain.
    test('SendTransient appends attempt; next drain honors '
        'backoff and does not call send again', () async {
      final firstAttemptAt = DateTime.utc(2026, 4, 22, 10, 0, 5);

      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      // First drain: uses scripted "now" = firstAttemptAt.
      await drain(dest, backend: backend, clock: () => firstAttemptAt);
      expect(dest.sent, hasLength(1));
      // Entry is still pending with one attempt.
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.attempts, hasLength(1));
      expect(head.finalStatus, isNull);

      // Re-drain immediately after (clock = firstAttemptAt + 1s). Backoff
      // is 60s from the last attempt; 1s after is well inside the window.
      await drain(
        dest,
        backend: backend,
        clock: () => firstAttemptAt.add(const Duration(seconds: 1)),
      );
      expect(dest.sent, hasLength(1)); // no new send call
    });

    test('after backoff elapses, drain calls send again', () async {
      final firstAttemptAt = DateTime.utc(2026, 4, 22, 10, 0, 5);
      // SyncPolicy.backoffFor(1) is roughly 300s (60 * 5).
      final afterBackoff = firstAttemptAt.add(
        const Duration(seconds: 300 * 2),
      ); // 10 minutes — well past

      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = FakeDestination(
        script: [
          const SendTransient(error: 'HTTP 503', httpStatus: 503),
          const SendOk(),
        ],
      );

      await drain(dest, backend: backend, clock: () => firstAttemptAt);
      expect(dest.sent, hasLength(1));

      await drain(dest, backend: backend, clock: () => afterBackoff);
      expect(dest.sent, hasLength(2));
      expect(await backend.readFifoHead('fake'), isNull); // sent
    });

    // Every attempted row's final_status is either null (still
    // pre-terminal), sent, or wedged by the time drain returns. This
    // test uses three successful SendOk results so all three rows are
    // visited without triggering a halt; each send call must append
    // exactly one AttemptResult to its row.
    test('every send call appends an AttemptResult', () async {
      final rowIds = <String>{};
      var seq = 0;
      for (final id in ['e1', 'e2', 'e3']) {
        seq += 1;
        rowIds.add(
          await _enqueueRow(backend, 'fake', eventId: id, sequenceNumber: seq),
        );
      }
      final dest = FakeDestination(
        script: [const SendOk(), const SendOk(), const SendOk()],
      );

      await drain(
        dest,
        backend: backend,
        clock: () => DateTime.utc(2026, 4, 22, 11),
      );

      // Inspect the raw store: each row has exactly 1 attempt.
      final db = backend.databaseForTesting;
      final raw = await StoreRef<int, Map<String, Object?>>(
        'fifo_fake',
      ).find(db);
      expect(raw, hasLength(3));
      for (final r in raw) {
        expect(rowIds.contains(r.value['entry_id']), isTrue);
        expect((r.value['attempts']! as List).length, 1);
      }
    });

    // drain attempts rows in sequence_in_queue order. Three successful
    // SendOks prove the ordering: the payloads land in the destination
    // in the same order the rows were enqueued.
    test('strict FIFO — drain attempts e1, e2, e3 in enqueue order', () async {
      var seq = 0;
      for (final id in ['e1', 'e2', 'e3']) {
        seq += 1;
        await _enqueueRow(backend, 'fake', eventId: id, sequenceNumber: seq);
      }
      final dest = FakeDestination(
        script: [const SendOk(), const SendOk(), const SendOk()],
      );

      await drain(
        dest,
        backend: backend,
        clock: () => DateTime.utc(2026, 4, 22, 11),
      );
      // Three send calls, in the order e1, e2, e3. The WirePayload
      // content reflects the row's event_id JSON encoding; decode it
      // to confirm the drain called send in FIFO order.
      expect(dest.sent, hasLength(3));
      final orderedEventIds = dest.sent
          .map(
            (p) =>
                (jsonDecode(utf8.decode(p.bytes))
                        as Map<String, Object?>)['event_id']
                    as String,
          )
          .toList();
      expect(orderedEventIds, ['e1', 'e2', 'e3']);
    });

    // Verifies: multi-destination independence — d1 wedged, d2 drains
    // normally. (Orchestrated via sync_cycle in Task 8; here we exercise
    // the drain-loop half of the claim by calling drain separately per
    // destination.)
    test(
      'multi-destination independence: wedge on d1 does not block d2',
      () async {
        final clockTime = DateTime.utc(2026, 4, 22, 10);
        final d1RowId = await _enqueueRow(
          backend,
          'd1',
          eventId: 'e1',
          sequenceNumber: 1,
        );
        await _enqueueRow(backend, 'd2', eventId: 'e2', sequenceNumber: 2);
        final d1 = FakeDestination(
          id: 'd1',
          script: [const SendPermanent(error: 'HTTP 400')],
        );
        final d2 = FakeDestination(id: 'd2', script: [const SendOk()]);

        await drain(d1, backend: backend, clock: () => clockTime);
        await drain(d2, backend: backend, clock: () => clockTime);

        expect(d1.sent, hasLength(1));
        expect(d2.sent, hasLength(1));
        // d1's row is wedged (SendPermanent); readFifoHead returns the
        // wedged row so UI surfaces can observe the wedge via this one
        // entry point.
        final d1Head = await backend.readFifoHead('d1');
        expect(d1Head, isNotNull);
        expect(d1Head!.entryId, d1RowId);
        expect(d1Head.finalStatus, FinalStatus.wedged);
        // d2's only row was sent (terminal-passable); no more rows.
        expect(await backend.readFifoHead('d2'), isNull);
      },
    );

    // drain consults the injected policy (not the defaults). Pre-seed
    // attempts[] to one below the injected cap; the next transient
    // attempt should wedge the entry.
    test('drain honors injected policy.maxAttempts', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );

      const smallPolicy = SyncPolicy(
        initialBackoff: Duration(seconds: 60),
        backoffMultiplier: 5.0,
        maxBackoff: Duration(hours: 2),
        jitterFraction: 0.1,
        maxAttempts: 3, // smaller cap than defaults.maxAttempts (20)
        periodicInterval: Duration(minutes: 15),
      );
      // Pre-load attempts: smallPolicy.maxAttempts - 1 transient records.
      for (var i = 0; i < smallPolicy.maxAttempts - 1; i++) {
        await backend.appendAttempt('fake', e1RowId, _attemptResultFactory(i));
      }
      // Clock well past any backoff window.
      final longAfter = DateTime.utc(2027, 1, 1);
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      await drain(
        dest,
        backend: backend,
        clock: () => longAfter,
        policy: smallPolicy,
      );
      expect(dest.sent, hasLength(1));
      // With a cap of 3 and 3 total attempts, the entry is wedged.
      // readFifoHead returns the wedged row (it is a halt signal to
      // drain, not a skip-past).
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.entryId, e1RowId);
      expect(head.finalStatus, FinalStatus.wedged);
    });

    // Sanity-check that omitting `policy` reads the defaults (20 attempts).
    test('null policy falls back to SyncPolicy.defaults', () async {
      final e1RowId = await _enqueueRow(
        backend,
        'fake',
        eventId: 'e1',
        sequenceNumber: 1,
      );
      // Pre-load 2 attempts: well below the default cap of 20, so a
      // transient should leave the entry pending (head still present).
      for (var i = 0; i < 2; i++) {
        await backend.appendAttempt('fake', e1RowId, _attemptResultFactory(i));
      }
      final longAfter = DateTime.utc(2027, 1, 1);
      final dest = FakeDestination(
        script: [const SendTransient(error: 'HTTP 503', httpStatus: 503)],
      );

      await drain(dest, backend: backend, clock: () => longAfter);
      expect(dest.sent, hasLength(1));
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.finalStatus, isNull);
    });

    // Verifies: drain treats a thrown exception from send() as SendTransient
    // and continues rather than crashing the caller.
    test('drain treats a thrown exception as SendTransient and records an '
        'attempt', () async {
      await _enqueueRow(backend, 'fake', eventId: 'e1', sequenceNumber: 1);
      final dest = _ThrowingDestination();

      await drain(
        dest,
        backend: backend,
        clock: () => DateTime.utc(2026, 4, 22, 11),
      );

      // Entry is still pending, with one attempt whose outcome is
      // "transient".
      final head = await backend.readFifoHead('fake');
      expect(head, isNotNull);
      expect(head!.attempts, hasLength(1));
      expect(head.attempts.first.outcome, 'transient');
    });

    // A native row reconstructs wire bytes from `envelope_metadata` +
    // `event_ids`-resolved events through `BatchEnvelope.encode`. The
    // re-encode is JCS-canonical and therefore byte-identical across
    // retries: a transient first attempt and a successful second attempt
    // hand `Destination.send` the exact same bytes, captured here at
    // `FakeDestination.sent`.
    test('drain on native row re-encodes deterministically '
        'across retries', () async {
      // Write the event into the origin event store so findEventById
      // resolves it at re-encode time.
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        // Re-mint the fixture with the reserved sequence number so the
        // append-side guard (event.sequenceNumber == reserved) holds.
        await backend.appendEvent(
          txn,
          storedEventFixture(eventId: 'e1', sequenceNumber: seq),
        );
      });

      // Enqueue a native esd/batch@1 row directly via the
      // nativeEnvelope: path. Drain reconstructs the wire bytes from
      // envelope_metadata + event_ids-resolved events on each attempt.
      final envelope = BatchEnvelopeMetadata(
        batchFormatVersion: '1',
        batchId: 'batch-x',
        senderHop: 'mobile-1',
        senderIdentifier: 'device-uuid',
        senderSoftwareVersion: 'diary@1.2.3',
        sentAt: DateTime.utc(2026, 4, 25, 12),
      );
      await backend.enqueueFifo('fake', [event], nativeEnvelope: envelope);

      // First drain: scripted SendTransient leaves the row pending and
      // captures the bytes the destination saw on attempt #1.
      const oneAttemptCapPolicy = SyncPolicy(
        initialBackoff: Duration.zero,
        backoffMultiplier: 1.0,
        maxBackoff: Duration.zero,
        jitterFraction: 0.0,
        maxAttempts: 5, // well above 2 — keeps the row pending across both
        periodicInterval: Duration(minutes: 15),
      );
      final dest = FakeDestination(
        script: [
          const SendTransient(error: 'HTTP 503', httpStatus: 503),
          const SendOk(),
        ],
      );
      await drain(
        dest,
        backend: backend,
        clock: () => DateTime.utc(2026, 4, 25, 13),
        policy: oneAttemptCapPolicy,
      );
      expect(dest.sent, hasLength(1));
      final firstBytes = dest.sent.last.bytes;
      // The reconstructed payload must be tagged with the native wire
      // format and its bytes must decode back to the original envelope.
      expect(dest.sent.last.contentType, BatchEnvelope.wireFormat);
      final firstDecoded =
          jsonDecode(utf8.decode(firstBytes)) as Map<String, Object?>;
      expect(firstDecoded['batch_id'], 'batch-x');
      expect((firstDecoded['events']! as List).length, 1);

      // Second drain: clock past the zero-backoff window; SendOk lands
      // the row. Capture bytes again and assert byte-for-byte equality.
      await drain(
        dest,
        backend: backend,
        clock: () => DateTime.utc(2026, 4, 25, 14),
        policy: oneAttemptCapPolicy,
      );
      expect(dest.sent, hasLength(2));
      final secondBytes = dest.sent.last.bytes;
      expect(
        secondBytes,
        firstBytes,
        reason:
            'native re-encode MUST be byte-deterministic across retries '
            '(RFC 8785 JCS)',
      );
    });

    // A native row whose `event_ids` reference a missing event throws
    // StateError. Models the integrity-violation case where the FIFO row
    // outlives its underlying event log entry; drain refuses to send a
    // partial / incorrect re-encode.
    test('drain on native row with missing event throws '
        'StateError', () async {
      // Append the event, enqueue the native row, then surgically delete
      // the event from the underlying sembast store. After deletion,
      // findEventById returns null and drain MUST throw.
      final event = storedEventFixture(eventId: 'e1', sequenceNumber: 1);
      await backend.transaction((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        await backend.appendEvent(
          txn,
          storedEventFixture(eventId: 'e1', sequenceNumber: seq),
        );
      });
      final envelope = BatchEnvelopeMetadata(
        batchFormatVersion: '1',
        batchId: 'batch-x',
        senderHop: 'mobile-1',
        senderIdentifier: 'device-uuid',
        senderSoftwareVersion: 'diary@1.2.3',
        sentAt: DateTime.utc(2026, 4, 25, 12),
      );
      await backend.enqueueFifo('fake', [event], nativeEnvelope: envelope);

      // Surgically delete the event from the origin event store
      // (bypasses the append-only API; test-only mutation that simulates
      // a torn / corrupted event log). The `sembast.Finder` prefix
      // disambiguates from `flutter_test`'s widget-tree `Finder`.
      final db = backend.databaseForTesting;
      final eventStore = intMapStoreFactory.store('events');
      final record = (await eventStore.find(
        db,
        finder: sembast.Finder(
          filter: sembast.Filter.equals('event_id', 'e1'),
          limit: 1,
        ),
      )).single;
      await eventStore.record(record.key).delete(db);

      final dest = FakeDestination(script: [const SendOk()]);
      expect(() => drain(dest, backend: backend), throwsA(isA<StateError>()));
    });
  });
}

/// Scripted AttemptResult for pre-loading transient history.
AttemptResult _attemptResultFactory(int i) => AttemptResult(
  attemptedAt: DateTime.utc(2026, 1, 1).add(Duration(minutes: i)),
  outcome: 'transient',
  errorMessage: 'pre-seeded transient #$i',
  httpStatus: 503,
);

class _ThrowingDestination extends FakeDestination {
  _ThrowingDestination() : super(id: 'fake');

  @override
  Future<SendResult> send(WirePayload payload) async {
    sent.add(payload);
    throw StateError('boom');
  }
}
