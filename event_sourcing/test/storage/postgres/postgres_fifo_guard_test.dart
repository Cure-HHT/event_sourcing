// The Postgres schema's guard on the queue table: a CHECK on the status
// values and the `fifo_entries_guard` trigger, exercised with raw SQL on a
// connection of the role that provisioned the schema, so the library's own
// checks are not in the way. Gated on PG_TEST_URL; each test runs against a
// freshly reset and provisioned `public` schema.

@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

import '../../test_support/fake_destination.dart';
import '../../test_support/queue_test_support.dart';
import '../../test_support/wedges_view_invariant.dart';
import 'test_postgres_url.dart';

Future<Connection> _connect(String url) => Connection.open(
  PostgresBackend.endpointFromUrl(url),
  settings: const ConnectionSettings(sslMode: SslMode.disable),
);

/// The statuses a queue item can hold; null is pending.
const List<String?> _statuses = <String?>[null, 'sent', 'wedged', 'tombstoned'];

/// The status changes the guard admits.
const Set<(String?, String?)> _legalChanges = <(String?, String?)>{
  (null, null),
  (null, 'sent'),
  (null, 'wedged'),
  ('wedged', 'tombstoned'),
};

/// The columns a queue item is enqueued with, which the guard holds
/// unchanged, each with an assignment that changes it.
const Map<String, String> _immutableColumns = <String, String>{
  'destination_id': "destination_id = 'other'",
  'entry_id': "entry_id = entry_id || '-other'",
  'event_ids': 'event_ids = \'["other"]\'::jsonb',
  'event_id_first_seq': 'event_id_first_seq = event_id_first_seq + 100',
  'event_id_last_seq': 'event_id_last_seq = event_id_last_seq + 100',
  'sequence_in_queue': 'sequence_in_queue = sequence_in_queue + 100',
  'wire_format': "wire_format = 'other-v1'",
  'transform_version': "transform_version = 'tv-other'",
  'wire_payload': 'wire_payload = NULL',
  'envelope_metadata': 'envelope_metadata = \'{"b":2}\'::jsonb',
  'enqueued_at': "enqueued_at = enqueued_at + interval '1 day'",
};

/// A recorded attempt, as the drainer writes one.
Map<String, Object?> _attempt(int n) => <String, Object?>{
  'attempted_at': DateTime.utc(2026, 1, 1, 0, n).toIso8601String(),
  'outcome': 'transient',
  'error_message': 'attempt $n',
  'http_status': 503,
};

/// Matches the refusal of the guard trigger.
final Matcher _refusedByGuard = throwsA(
  isA<ServerException>()
      .having((e) => e.code, 'code', '23514')
      .having((e) => e.message, 'message', contains('fifo_entries_guard')),
);

void main() {
  final url = testPostgresUrl();
  if (url == null) {
    test('skipped — PG_TEST_URL unset', () {
      markTestSkipped('PG_TEST_URL unset; skipping Postgres tests');
    });
    return;
  }

  late Connection c;
  var nextSeq = 0;

  setUp(() async {
    final admin = await _connect(url);
    await admin.execute('DROP SCHEMA public CASCADE');
    await admin.execute('CREATE SCHEMA public');
    await admin.close();
    await PostgresBackend.provision(url, sslMode: SslMode.disable);
    c = await _connect(url);
    nextSeq = 0;
  });

  tearDown(() async {
    await c.close();
  });

  /// Inserts a pending item the way the library's enqueue does, and returns
  /// its entry id.
  Future<String> insertPending({
    String dest = 'd',
    List<Map<String, Object?>> attempts = const <Map<String, Object?>>[],
  }) async {
    final seq = ++nextSeq;
    final entryId = 'e$seq';
    await c.execute(
      Sql.named('''
        INSERT INTO fifo_entries (
          destination_id, sequence_in_queue, entry_id,
          event_ids, event_id_first_seq, event_id_last_seq,
          wire_format, transform_version, enqueued_at,
          attempts, final_status, sent_at,
          wire_payload, envelope_metadata
        ) VALUES (
          @dest, @seq, @entryId,
          @eventIds:jsonb, @seq, @seq,
          'fake-v1', NULL, @at:timestamptz,
          '[]'::jsonb, NULL, NULL,
          @payload:jsonb, NULL
        )
      '''),
      parameters: <String, Object?>{
        'dest': dest,
        'seq': seq,
        'entryId': entryId,
        'eventIds': <String>['ev$seq'],
        'at': DateTime.utc(2026, 1, 1),
        'payload': <String, Object?>{'a': 1},
      },
    );
    for (final attempt in attempts) {
      await c.execute(
        Sql.named(
          'UPDATE fifo_entries SET attempts = attempts || @a:jsonb '
          'WHERE entry_id = @e',
        ),
        parameters: <String, Object?>{
          'a': <Object?>[attempt],
          'e': entryId,
        },
      );
    }
    return entryId;
  }

  /// Moves [entryId] to [status] with the one update the library makes for
  /// that change.
  Future<void> setStatus(String entryId, String status) => c.execute(
    Sql.named(
      'UPDATE fifo_entries SET final_status = @s:text, '
      "sent_at = CASE WHEN @s:text = 'sent' THEN now() ELSE sent_at END "
      'WHERE entry_id = @e',
    ),
    parameters: <String, Object?>{'s': status, 'e': entryId},
  );

  /// Inserts an item and brings it to [status] through the legal changes.
  Future<String> itemAt(String? status) async {
    final entryId = await insertPending(
      attempts: <Map<String, Object?>>[_attempt(1)],
    );
    switch (status) {
      case null:
        break;
      case 'sent':
        await setStatus(entryId, 'sent');
      case 'wedged':
        await setStatus(entryId, 'wedged');
      case 'tombstoned':
        await setStatus(entryId, 'wedged');
        await setStatus(entryId, 'tombstoned');
    }
    return entryId;
  }

  /// Every column of [entryId]'s row, as text.
  Future<Map<String, Object?>> row(String entryId) async {
    final r = await c.execute(
      Sql.named('SELECT to_jsonb(f) FROM fifo_entries f WHERE entry_id = @e'),
      parameters: <String, Object?>{'e': entryId},
    );
    return r.isEmpty
        ? const <String, Object?>{}
        : Map<String, Object?>.from(r.first[0]! as Map);
  }

  Future<int> count() async =>
      (await c.execute('SELECT count(*) FROM fifo_entries')).first[0]! as int;

  // Verifies: EVS-DEV-destination-drain/S
  // a status change other than pending to pending, pending to sent,
  //   pending to wedged and wedged to tombstoned is refused, a repeated
  //   terminal status included, and the refused row is unchanged.
  test('only the four legal status changes pass', () async {
    for (final from in _statuses) {
      for (final to in _statuses) {
        final entryId = await itemAt(from);
        final before = await row(entryId);
        final change = c.execute(
          Sql.named(
            'UPDATE fifo_entries SET final_status = @to:text, '
            "sent_at = CASE WHEN @to:text = 'sent' AND final_status IS NULL "
            'THEN now() ELSE sent_at END WHERE entry_id = @e',
          ),
          parameters: <String, Object?>{'to': to, 'e': entryId},
        );
        if (_legalChanges.contains((from, to))) {
          await change;
          expect(
            (await row(entryId))['final_status'],
            to,
            reason: '$from->$to',
          );
        } else {
          await expectLater(change, _refusedByGuard, reason: '$from->$to');
          expect(await row(entryId), before, reason: '$from->$to');
        }
      }
    }
  });

  // Verifies: EVS-DEV-destination-drain/S
  // within every legal status change, a change to a column the queue item
  //   is enqueued with is refused (nullable columns included, in both
  //   directions of null).
  test('an immutable column changed within a legal status change is '
      'refused', () async {
    for (final (from, to) in _legalChanges) {
      for (final MapEntry(key: column, value: assignment)
          in _immutableColumns.entries) {
        final entryId = await itemAt(from);
        final before = await row(entryId);
        final sentAt = (from == null && to == 'sent')
            ? ', sent_at = now()'
            : '';
        await expectLater(
          c.execute(
            Sql.named(
              'UPDATE fifo_entries SET final_status = @to:text$sentAt, '
              '$assignment WHERE entry_id = @e',
            ),
            parameters: <String, Object?>{'to': to, 'e': entryId},
          ),
          _refusedByGuard,
          reason: '$column on $from->$to',
        );
        expect(await row(entryId), before, reason: '$column on $from->$to');
      }
    }
  });

  // Verifies: EVS-DEV-destination-drain/S
  // attempts change only by appending one attempt, on a pending item or in
  //   the change that marks it sent or wedged; a wedged item's attempts are
  //   kept when it is tombstoned.
  test('attempts change only by appending one attempt', () async {
    Future<void> refused(String entryId, String set, String why) async {
      final before = await row(entryId);
      await expectLater(
        c.execute(
          Sql.named('UPDATE fifo_entries SET $set WHERE entry_id = @e'),
          parameters: <String, Object?>{'e': entryId},
        ),
        _refusedByGuard,
        reason: why,
      );
      expect(await row(entryId), before, reason: why);
    }

    final two = await insertPending(
      attempts: <Map<String, Object?>>[_attempt(1), _attempt(2)],
    );
    await refused(two, 'attempts = attempts - 1', 'truncated when pending');
    await refused(
      two,
      "attempts = jsonb_set(attempts, '{0,error_message}', '\"other\"')",
      'first element replaced, same length',
    );
    await refused(
      two,
      'attempts = attempts || \'[{"n":3},{"n":4}]\'::jsonb',
      'two attempts appended at once',
    );
    await refused(
      two,
      'attempts = \'[{"n":0}]\'::jsonb || attempts',
      'one attempt prepended',
    );
    await refused(two, "attempts = '{}'::jsonb", 'not an array');
    await refused(
      two,
      "final_status = 'sent', sent_at = now(), attempts = '[]'::jsonb",
      'emptied when marked sent',
    );
    await refused(
      two,
      "final_status = 'wedged', attempts = attempts - 0",
      'first attempt dropped when marked wedged',
    );

    final wedged = await itemAt('wedged');
    await refused(
      wedged,
      "final_status = 'tombstoned', "
          'attempts = attempts || \'[{"n":9}]\'::jsonb',
      'appended when tombstoned',
    );
    await refused(
      wedged,
      "final_status = 'tombstoned', attempts = '[]'::jsonb",
      'emptied when tombstoned',
    );

    // One attempt appended passes on each change that records one; a wedge
    // with its attempts unchanged (an operator halt) passes.
    final pending = await insertPending();
    await c.execute(
      Sql.named(
        'UPDATE fifo_entries SET attempts = attempts || \'[{"n":1}]\'::jsonb '
        'WHERE entry_id = @e',
      ),
      parameters: <String, Object?>{'e': pending},
    );
    await c.execute(
      Sql.named(
        "UPDATE fifo_entries SET final_status = 'sent', sent_at = now(), "
        'attempts = attempts || \'[{"n":2}]\'::jsonb WHERE entry_id = @e',
      ),
      parameters: <String, Object?>{'e': pending},
    );
    expect(((await row(pending))['attempts']! as List).length, 2);
    final halted = await insertPending();
    await setStatus(halted, 'wedged');
    expect((await row(halted))['attempts'], isEmpty);
    final exhausted = await insertPending(
      attempts: <Map<String, Object?>>[_attempt(1)],
    );
    await c.execute(
      Sql.named(
        "UPDATE fifo_entries SET final_status = 'wedged', "
        'attempts = attempts || \'[{"n":2}]\'::jsonb WHERE entry_id = @e',
      ),
      parameters: <String, Object?>{'e': exhausted},
    );
    expect((await row(exhausted))['final_status'], 'wedged');
  });

  // Verifies: EVS-DEV-destination-drain/S
  // the delivery time is set only in the change that marks an item sent.
  test('sent_at changes only when an item is marked sent', () async {
    for (final (from, to) in <(String?, String)>[
      (null, 'NULL'),
      (null, "'wedged'"),
      ('wedged', "'tombstoned'"),
    ]) {
      final entryId = await itemAt(from);
      final before = await row(entryId);
      await expectLater(
        c.execute(
          Sql.named(
            'UPDATE fifo_entries SET final_status = $to, sent_at = now() '
            'WHERE entry_id = @e',
          ),
          parameters: <String, Object?>{'e': entryId},
        ),
        _refusedByGuard,
        reason: '$from->$to',
      );
      expect(await row(entryId), before, reason: '$from->$to');
    }
  });

  // Verifies: EVS-DEV-destination-drain/S
  // deleting a sent, wedged or tombstoned item is refused; deleting a
  //   pending item is allowed; truncating a table that holds a terminal item
  //   is refused.
  test('terminal items are never deleted; pending items are', () async {
    for (final status in <String>['sent', 'wedged', 'tombstoned']) {
      final entryId = await itemAt(status);
      await expectLater(
        c.execute(
          Sql.named('DELETE FROM fifo_entries WHERE entry_id = @e'),
          parameters: <String, Object?>{'e': entryId},
        ),
        _refusedByGuard,
        reason: status,
      );
      expect((await row(entryId))['final_status'], status);
    }
    final pending = await itemAt(null);
    await c.execute(
      Sql.named('DELETE FROM fifo_entries WHERE entry_id = @e'),
      parameters: <String, Object?>{'e': pending},
    );
    expect(await row(pending), isEmpty);
    expect(await count(), 3);
    await expectLater(c.execute('TRUNCATE fifo_entries'), _refusedByGuard);
    expect(await count(), 3);
  });

  // Verifies: EVS-DEV-destination-drain/S
  // every truncation of the queue table is refused, of an empty table and
  //   of one holding only pending items included: a truncation cannot see
  //   the terminal items other transactions commit after its snapshot, so
  //   none is admitted.
  test('the queue table is never truncated', () async {
    await expectLater(c.execute('TRUNCATE fifo_entries'), _refusedByGuard);
    await itemAt(null);
    await itemAt(null);
    await expectLater(c.execute('TRUNCATE fifo_entries'), _refusedByGuard);
    expect(await count(), 2);
  });

  // Verifies: EVS-DEV-destination-drain/S
  // the guard fires in a session whose replication role is replica, where a
  //   plainly enabled trigger is skipped: an illegal status change, the
  //   deletion of a terminal item and a truncation are still refused.
  test('the guard holds in a replica session', () async {
    final sent = await itemAt('sent');
    Future<void> inReplica(String sql) => c.runTx<void>((tx) async {
      await tx.execute('SET LOCAL session_replication_role = replica');
      await tx.execute(sql);
    });
    await expectLater(
      inReplica(
        "UPDATE fifo_entries SET final_status = 'wedged' "
        "WHERE entry_id = '$sent'",
      ),
      _refusedByGuard,
    );
    await expectLater(
      inReplica("DELETE FROM fifo_entries WHERE entry_id = '$sent'"),
      _refusedByGuard,
    );
    await expectLater(inReplica('TRUNCATE fifo_entries'), _refusedByGuard);
    expect((await row(sent))['final_status'], 'sent');
  });

  // Verifies: EVS-DEV-destination-drain/S
  // provisioning leaves the status check and both guard triggers in place,
  //   each trigger enabled in every session replication role, so a schema
  //   missing any of them is visible here.
  test('provisioning installs the check and both triggers, always '
      'enabled', () async {
    final triggers = await c.execute(
      'SELECT tgname, tgenabled::text FROM pg_trigger '
      "WHERE tgrelid = 'fifo_entries'::regclass AND NOT tgisinternal "
      'ORDER BY tgname',
    );
    expect(
      <String, String>{
        for (final r in triggers) r[0]! as String: r[1]! as String,
      },
      <String, String>{
        'fifo_entries_guard': 'A',
        'fifo_entries_truncate_guard': 'A',
      },
    );
    final checks = await c.execute(
      'SELECT conname FROM pg_constraint '
      "WHERE conrelid = 'fifo_entries'::regclass AND contype = 'c'",
    );
    expect(
      <String>[for (final r in checks) r[0]! as String],
      <String>['fifo_entries_final_status_check'],
    );
  });

  // Verifies: EVS-DEV-destination-drain/S
  // the guard names every column of the queue table: the enqueue-time
  //   columns it holds unchanged, plus attempts, final_status and sent_at,
  //   whose changes it rules on; a column added to the table without a
  //   decision in the guard fails here.
  test('the guard covers every column of the queue table', () async {
    final columns = await c.execute(
      'SELECT column_name FROM information_schema.columns '
      "WHERE table_schema = current_schema() AND table_name = 'fifo_entries'",
    );
    expect(
      <String>{for (final r in columns) r[0]! as String},
      <String>{
        ..._immutableColumns.keys,
        'attempts',
        'final_status',
        'sent_at',
      },
    );
  });

  // Verifies: EVS-DEV-destination-drain/S
  // the guard checks the shape of a change, not who makes it: a
  //   hand-written change of a legal shape passes (a pending item wedged
  //   with no wedge event, marked sent with no delivery, a wedged item
  //   tombstoned with no recovery, a pending head deleted), so those rest on
  //   the storage precondition, not on the guard.
  test('a hand-written change of a legal shape passes the guard', () async {
    final wedged = await insertPending();
    await setStatus(wedged, 'wedged');
    expect((await row(wedged))['final_status'], 'wedged');
    await setStatus(wedged, 'tombstoned');
    expect((await row(wedged))['final_status'], 'tombstoned');
    final sent = await insertPending();
    await setStatus(sent, 'sent');
    expect((await row(sent))['final_status'], 'sent');
    final head = await insertPending();
    await c.execute(
      Sql.named('DELETE FROM fifo_entries WHERE entry_id = @e'),
      parameters: <String, Object?>{'e': head},
    );
    expect(await row(head), isEmpty);
    final events = await c.execute('SELECT count(*) FROM events');
    expect(events.first[0], 0);
  });

  // Verifies: EVS-DEV-destination-drain/S
  // a status value outside sent, wedged and tombstoned violates the
  //   schema's CHECK, which holds even with the trigger disabled.
  test('an unknown status value violates the status check', () async {
    final entryId = await itemAt(null);
    await expectLater(
      c.execute(
        Sql.named(
          "UPDATE fifo_entries SET final_status = 'bogus' WHERE entry_id = @e",
        ),
        parameters: <String, Object?>{'e': entryId},
      ),
      throwsA(isA<ServerException>().having((e) => e.code, 'code', '23514')),
    );
    await expectLater(
      c.runTx<void>((tx) async {
        await tx.execute(
          'ALTER TABLE fifo_entries DISABLE TRIGGER fifo_entries_guard',
        );
        await tx.execute(
          Sql.named(
            "UPDATE fifo_entries SET final_status = 'bogus' "
            'WHERE entry_id = @e',
          ),
          parameters: <String, Object?>{'e': entryId},
        );
      }),
      throwsA(
        isA<ServerException>()
            .having((e) => e.code, 'code', '23514')
            .having(
              (e) => e.constraintName,
              'constraint',
              'fifo_entries_final_status_check',
            ),
      ),
    );
    expect((await row(entryId))['final_status'], isNull);
  });

  // Verifies: EVS-DEV-destination-drain/S
  // an item inserted sent, wedged or tombstoned, carrying attempts, or with
  //   a delivery time is refused, so no item is inserted wedged; the queue
  //   reads report nothing wedged afterwards.
  test('only a pending item with no attempts and no delivery time is '
      'inserted', () async {
    Future<void> insert(String attempts, String status, String sentAt) =>
        c.execute(
          'INSERT INTO fifo_entries (destination_id, sequence_in_queue, '
          'entry_id, event_ids, event_id_first_seq, event_id_last_seq, '
          'wire_format, enqueued_at, attempts, final_status, sent_at) '
          "VALUES ('d', 900, 'inserted', '[\"ev\"]'::jsonb, 1, 1, "
          "'fake-v1', now(), $attempts, $status, $sentAt)",
        );

    // The pending form the library's enqueue writes passes.
    await insert("'[]'::jsonb", 'NULL', 'NULL');
    await c.execute("DELETE FROM fifo_entries WHERE entry_id = 'inserted'");

    final cases = <String, (String, String, String)>{
      'sent': ("'[]'::jsonb", "'sent'", 'NULL'),
      'wedged': ("'[]'::jsonb", "'wedged'", 'NULL'),
      'tombstoned': ("'[]'::jsonb", "'tombstoned'", 'NULL'),
      'attempts': ('\'[{"n":1}]\'::jsonb', 'NULL', 'NULL'),
      'attempts not an array': ("'{}'::jsonb", 'NULL', 'NULL'),
      'sent_at': ("'[]'::jsonb", 'NULL', 'now()'),
      'sent with sent_at and an attempt': (
        '\'[{"n":1}]\'::jsonb',
        "'sent'",
        'now()',
      ),
    };
    for (final MapEntry(key: why, value: (attempts, status, sentAt))
        in cases.entries) {
      await expectLater(
        insert(attempts, status, sentAt),
        _refusedByGuard,
        reason: why,
      );
      expect(await count(), 0, reason: why);
    }
    final backend = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
    );
    addTearDown(backend.close);
    expect(await backend.hasFifoWedged(), isFalse);
    expect(await backend.wedgedFifos(), isEmpty);
  });

  // Verifies: EVS-DEV-destination-drain/S
  // the library's own enqueue, attempt, delivery and wedge pass the guard.
  test("the library's own fill and drain pass the guard", () async {
    final w = await _World.open(url);
    addTearDown(w.close);
    final d = FakeDestination(id: 'x');
    await w.activate(d);
    await w.note('n1');
    await w.note('n2');
    await w.fillAll(d);
    expect(await w.statuses('x'), <FinalStatus?>[null, null]);
    await drainForTest(
      FakeDestination(
        id: 'x',
        script: <SendResult>[
          const SendTransient(error: 'busy'),
          const SendOk(),
        ],
      ),
      registry: w.registry,
      clock: () => DateTime.utc(2100),
    );
    await drainForTest(
      FakeDestination(
        id: 'x',
        script: <SendResult>[const SendOk(), const SendOk()],
      ),
      registry: w.registry,
      clock: () => DateTime.utc(2101),
    );
    expect(await w.statuses('x'), <FinalStatus?>[
      FinalStatus.sent,
      FinalStatus.sent,
    ]);
    await w.note('n3');
    await w.fillAll(d);
    await wedgeHeadForTest(w.registry, 'x');
    expect(await w.statuses('x'), <FinalStatus?>[
      FinalStatus.sent,
      FinalStatus.sent,
      FinalStatus.wedged,
    ]);
    await expectWedgesViewMatchesQueue(w.store);
  });

  // Verifies: EVS-PRD-destinations/O
  // deleting a destination whose queue holds sent and tombstoned items, a
  //   wedged head and pending items behind it succeeds under the guard and
  //   retains every terminal item.
  // Verifies: EVS-DEV-destination-drain/A
  // the deletion tombstones the wedged head and deletes only the pending
  //   items.
  test('deletion under the guard retains the terminal items', () async {
    final w = await _World.open(url);
    addTearDown(w.close);
    final d = FakeDestination(id: 'x', allowHardDelete: true);
    await w.activate(d);
    await w.note('s1');
    await w.fillAll(d);
    await drainForTest(
      FakeDestination(id: 'x', script: <SendResult>[const SendOk()]),
      registry: w.registry,
    );
    await w.note('t1');
    await w.fillAll(d);
    final first = await wedgeHeadForTest(w.registry, 'x');
    await w.registry.tombstoneAndRefill('x', first, initiator: _init);
    await w.note('p1');
    await w.note('p2');
    await w.fillAll(d);
    final second = await wedgeHeadForTest(w.registry, 'x');
    expect(await w.statuses('x'), <FinalStatus?>[
      FinalStatus.sent,
      FinalStatus.tombstoned,
      FinalStatus.wedged,
      null,
      null,
    ]);
    final terminalBefore = <String, FinalStatus?>{
      for (final r in await w.backend.listFifoEntries('x'))
        if (r.finalStatus != null) r.entryId: r.finalStatus,
    };

    await w.registry.deleteDestination('x', initiator: _init);

    final after = await w.backend.listFifoEntries('x');
    expect(
      <String, FinalStatus?>{for (final r in after) r.entryId: r.finalStatus},
      <String, FinalStatus?>{...terminalBefore, second: FinalStatus.tombstoned},
    );
    expect(terminalBefore[first], FinalStatus.tombstoned);
    await expectWedgesViewMatchesQueue(w.store);
  });
}

const Initiator _init = AutomationInitiator(service: 'fifo-guard');
const String _noteType = 'guard_note';
const Source _source = Source(
  hopId: 'mobile-device',
  identifier: 'guard-install',
  softwareVersion: 'test@1.0.0',
);

/// One process over the test database: a backend, an event store and a
/// destination registry.
class _World {
  _World(this.backend, this.store, this.registry);

  final PostgresBackend backend;
  final EventStore store;
  final DestinationRegistry registry;

  static Future<_World> open(String url) async {
    final backend = await PostgresBackend.open(
      url: url,
      sslMode: SslMode.disable,
    );
    final entryTypes = EntryTypeRegistry();
    for (final d in kSystemEntryTypes) {
      entryTypes.register(d);
    }
    entryTypes.register(
      const EntryTypeDefinition(
        id: _noteType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _noteType,
      ),
    );
    final store = await EventStore.openForTest(
      storage: backend,
      entryTypes: entryTypes,
      source: _source,
      securityContexts: PostgresSecurityContextStore(backend: backend),
      clock: () => DateTime.utc(2026, 3, 1),
    );
    return _World(backend, store, DestinationRegistry(eventStore: store));
  }

  Future<void> activate(Destination d) async {
    await registry.addDestination(d, initiator: _init);
    await registry.setStartDate(
      d.id,
      DateTime.utc(2026, 1, 1),
      initiator: _init,
    );
  }

  Future<void> note(String id) => store.append(
    entryType: _noteType,
    aggregateId: id,
    aggregateType: 'note',
    eventType: 'noted',
    data: <String, Object?>{'id': id},
    initiator: const UserInitiator('u'),
  );

  Future<void> fillAll(Destination d) async {
    for (var i = 0; i < 20; i++) {
      final before = (await backend.listFifoEntries(d.id)).length;
      final cursor = await backend.readFillCursor(d.id);
      await fillForTest(
        d,
        backend: backend,
        source: _source,
        clock: () => DateTime.utc(2027, 1, 1),
      );
      if ((await backend.listFifoEntries(d.id)).length == before &&
          await backend.readFillCursor(d.id) == cursor) {
        return;
      }
    }
  }

  Future<List<FinalStatus?>> statuses(String destId) async => <FinalStatus?>[
    for (final r in await backend.listFifoEntries(destId)) r.finalStatus,
  ];

  Future<void> close() async {
    await store.close();
    await backend.close();
  }
}
