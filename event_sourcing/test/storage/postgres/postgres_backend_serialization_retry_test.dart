// Verifies: EVS-PRD-event-log/E
// concurrent appends against the Postgres
// backend make progress: the per-transaction reserve-and-increment of the
// global sequence counter under SERIALIZABLE isolation provokes SQLSTATE 40001
// (serialization_failure) on the losers of each race, and PostgresBackend
// .transaction's bounded retry re-runs each loser to completion so NO 40001
// escapes to the caller and every append is assigned a distinct, gapless
// sequence number. Gated on PG_TEST_URL; inert where no Postgres is available.
// Verifies: EVS-DEV-postgres-backend/C
// transaction<T> runs at SERIALIZABLE
//   isolation (conflicting concurrent txns retry/serialize); rollback on throw,
//   commit on return, handle invalidated after body.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import 'test_postgres_url.dart';

void main() {
  final url = testPostgresUrl();
  if (url == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }

  group('PostgresBackend serialization-conflict retry', () {
    late PostgresBackend backend;

    setUp(() async {
      // Clean slate so the counter starts at 0 and the assertions on the
      // final counter / contiguous sequence numbers are exact.
      final conn = await Connection.open(
        PostgresBackend.endpointFromUrl(url),
        settings: const ConnectionSettings(sslMode: SslMode.disable),
      );
      await conn.execute('DROP SCHEMA public CASCADE');
      await conn.execute('CREATE SCHEMA public');
      await conn.close();
      backend = await PostgresBackend.open(url: url, sslMode: SslMode.disable);
    });

    tearDown(() => backend.close());

    test('concurrent appends all succeed (no 40001 escapes) and get '
        'distinct, gapless sequence numbers', () async {
      const concurrency = 12;

      // Each future opens its own SERIALIZABLE transaction that reserves a
      // sequence number and appends an event. Because every transaction
      // read-modify-writes the single global counter row, Postgres aborts the
      // losers with 40001 — which the backend's retry loop must absorb. A
      // micro-yield between the reserve and the append widens the conflict
      // window so the race is reliably exercised.
      Future<int> appendOne(int i) => backend.transaction<int>((txn) async {
        final seq = await backend.nextSequenceNumber(txn);
        await Future<void>.delayed(Duration.zero);
        await backend.appendEvent(txn, _event('e$i-$seq', seq));
        return seq;
      });

      final seqs = await Future.wait([
        for (var i = 0; i < concurrency; i++) appendOne(i),
      ]);

      // No 40001 escaped (Future.wait would have rejected). Every reservation
      // is distinct and the set is exactly 1..concurrency — contiguous, no
      // gaps, no reuse — proving each retried transaction re-reserved cleanly.
      expect(
        seqs.toSet(),
        hasLength(concurrency),
        reason: 'sequence numbers must be distinct across concurrent appends',
      );
      expect(
        seqs.toSet(),
        equals({for (var n = 1; n <= concurrency; n++) n}),
        reason: 'reservations must be contiguous 1..N with no gaps',
      );

      // The durable counter and the persisted event count both reflect every
      // append having actually committed.
      expect(await backend.readSequenceCounter(), equals(concurrency));
      final all = await backend.findAllEvents();
      expect(all, hasLength(concurrency));
    });
  });
}

StoredEvent _event(String eventId, int sequenceNumber) => StoredEvent(
  key: 0,
  eventId: eventId,
  aggregateId: 'agg-1',
  aggregateType: 'note',
  entryType: 'epistaxis_event',
  entryTypeVersion: 1,
  libFormatVersion: 1,
  eventType: 'Event',
  sequenceNumber: sequenceNumber,
  data: const <String, dynamic>{},
  metadata: const <String, dynamic>{},
  initiator: const UserInitiator('u'),
  clientTimestamp: DateTime.utc(2026, 4, 22),
  eventHash: 'hash-$eventId',
);
