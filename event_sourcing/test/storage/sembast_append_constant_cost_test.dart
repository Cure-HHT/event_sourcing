// A guard against a Sembast append reading the whole event store or the
// whole view: seeds a Sembast memory database to 1,000 events and to
// 40,000 events, each with several aggregates, eligible and annotation
// events, one ingested event, a registered TableProjectionSpec view the
// eligible-version events fold into, and one held position_reused and one
// held fork_unrecorded finding about the database's own chain, then times
// a fixed batch of ordinary appends at each size.
// [SembastBackend.readLatestEventHash] and
// [SembastBackend.readLatestEligibleVersionInTxn] are each served from a
// backend-owned index keyed by the value they answer with, and so are the
// marks fold's own lookups once a finding is held
// ([SembastBackend.readAggregateAuthorshipInTxn] and
// [SembastBackend.readLowestOriginPositionByPredecessorInTxn], plus the
// held-finding-sequences record `findSecurityFindingsInTxn` reads) — never
// a Finder over the whole store (`EVS-DEV-causal-parents/H`,
// `EVS-PRD-materializer/E`, Rationale). Each measured append also dedupes
// by content against a fresh aggregate, so
// [SembastBackend.findEventsForAggregateInTxn] is served from its own
// per-aggregate index (`EVS-PRD-event-log/C`) independently of the marks
// refresh. Because the held findings are about the measured database's
// own chain, every timed append also marks its aggregate and refreshes the
// registered table view's rows for it, which
// [SembastBackend.findTableRowsBySourceAggregateInTxn] serves from a
// backend-owned per-copy index (`EVS-PRD-materializer/E`,
// `EVS-PRD-materializer/G`) rather than a scan of the whole view copy. So
// neither the event-store side nor the view side of an append's cost
// grows with the store's size. A linear scan would make the 40k batch run
// about 40x the 1k batch; this test allows headroom for the ambient cost
// that does grow with the store (a longer sequence counter, more index
// records) and bounds the ratio well under that.
@TestOn('vm')
library;

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart'
    show recordFindingInTxnForTest;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart' hide Transaction;

import '../test_support/deliveries.dart';

const _kTableViewName = 'append_cost_table';

/// A row per eligible-version event, keyed by a field unique to that event
/// (not by aggregate id), so the seeded store holds one distinct table row
/// per seeded event rather than one per aggregate: the view-copy side of the
/// guard needs a copy whose row count grows with the store's size, not one
/// that stays pinned at [_kAggregateCount] rows.
const TableProjectionSpec _kTableSpec = TableProjectionSpec(
  viewName: _kTableViewName,
  interest: SubscriptionFilter(entryTypes: <String>{_kVersionType}),
  insertEventTypes: <String>{'finalized'},
  removeEventTypes: <String>{},
  rowKey: CompositeKey(<String>['data.n']),
  rowData: WholePayload(),
);

const _kVersionType = 'append_cost_version';
const _kAnnotationType = 'append_cost_annotation';
const _kAggregateCount = 20;

/// Individual, one-transaction-each appends timed at each store size: the
/// shape an ordinary caller uses, not a batched seeding transaction.
const _kMeasuredAppends = 500;

/// The ratio a linear-in-store-size scan would produce, at these sizes.
const _kLinearRatio = 40;

/// The ratio a constant-time lookup allows, generous enough to absorb the
/// ambient per-event cost that does grow with the store (a longer sequence
/// counter record, more per-aggregate index records to write) without
/// masking a reintroduced scan.
const _kAllowedRatio = 4;

EntryTypeRegistry _registry() {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  registry
    ..register(
      const EntryTypeDefinition(
        id: _kVersionType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kVersionType,
      ),
    )
    ..register(
      const EntryTypeDefinition(
        id: _kAnnotationType,
        registeredVersion: EntryTypeVersion(1, 0),
        name: _kAnnotationType,
        declarations: <EventTypeDeclaration>[
          EventTypeDeclaration(
            eventType: 'noted',
            kind: CausalKind.annotation,
            eligible: false,
          ),
        ],
      ),
    );
  return registry;
}

var _dbCounter = 0;

Future<EventStore> _openStore({
  String hopId = 'guard-hop',
  String identifier = 'guard-install',
}) async {
  _dbCounter += 1;
  final db = await newDatabaseFactoryMemory().openDatabase(
    'append-cost-$_dbCounter.db',
  );
  final backend = SembastBackend(database: db);
  return EventStore.openForTest(
    storage: backend,
    entryTypes: _registry(),
    projections: ProjectionRegistry()..register(_kTableSpec),
    source: Source(
      hopId: hopId,
      identifier: identifier,
      softwareVersion: 'guard-test',
    ),
    securityContexts: SembastSecurityContextStore(backend: backend),
  );
}

/// Seeds [store], already holding [already] events, up to [target] events
/// across [_kAggregateCount] aggregates, alternating eligible-version and
/// ineligible-annotation events, in batches of 200 so seeding a large store
/// stays fast. Each event carries a `n` field unique within the store (the
/// seeding loop's own counter), so every eligible-version event's insert
/// into [_kTableSpec]'s view produces a distinct row: the seeded view copy
/// grows with the store's size rather than staying pinned at
/// [_kAggregateCount] rows.
Future<void> _seedTo(EventStore store, int target, {int already = 0}) async {
  const chunk = 200;
  var written = already;
  var n = 0;
  while (written < target) {
    final inThisChunk = (target - written).clamp(0, chunk);
    await store.runTransaction((txn, collector) async {
      for (var i = 0; i < inThisChunk; i++) {
        final aggregateId = 'agg-${n % _kAggregateCount}';
        final annotation = n.isOdd;
        await store.appendInTxn(
          txn,
          entryType: annotation ? _kAnnotationType : _kVersionType,
          aggregateId: aggregateId,
          aggregateType: 'guard',
          eventType: annotation ? 'noted' : 'finalized',
          data: <String, Object?>{'v': 1, 'n': n},
          initiator: const UserInitiator('guard-user'),
          flowToken: null,
          metadata: null,
          security: null,
          checkpointReason: null,
          changeReason: null,
          dedupeByContent: false,
          collector: collector,
        );
        n++;
      }
    });
    written += inThisChunk;
  }
}

/// Delivers one event, authored by a peer store, into [store] as an
/// ingested event, so the seeded log holds a received copy alongside its
/// own appends.
Future<void> _seedOneIngestedEvent(EventStore store) async {
  final peer = await _openStore(hopId: 'guard-peer-hop', identifier: 'peer');
  final authored = await peer.append(
    entryType: _kVersionType,
    aggregateId: 'agg-ingested',
    aggregateType: 'guard',
    eventType: 'finalized',
    data: const <String, Object?>{'v': 1, 'n': 1000000},
    initiator: const UserInitiator('guard-user'),
  );
  await deliverEventsOrThrow(store, <StoredEvent>[authored!]);
}

/// Records one held `position_reused` and one held `fork_unrecorded`
/// finding about [store]'s own database identity, so
/// `holdsSecurityFindingInTxn` is true for the rest of the store's life and
/// every later local append marks its own aggregate: the `position_reused`
/// finding's threshold (`origin_sequence_number: 1`) is at or below every
/// held event's own origin position, so the marks fold's `chainDb == origin`
/// branch adds the appended event's aggregate as a refresh candidate on
/// every append, exercising both the event-store side of the guard
/// (`authorshipOf`, the predecessor-position threshold, the held-finding
/// list and, once an aggregate is a candidate,
/// [SembastBackend.findEventsForAggregateInTxn]) and the view side (the
/// [_kTableSpec] row refresh via
/// [SembastBackend.findTableRowsBySourceAggregateInTxn]).
Future<void> _seedHeldFindings(EventStore store) async {
  final ownDatabaseId = store.databaseId;
  await store.runTransaction((txn, collector) async {
    await recordFindingInTxnForTest(
      store,
      txn,
      collector,
      role: FindingRole.walk,
      kind: FindingKind.positionReused,
      evidence: <String, Object?>{
        'database_id': ownDatabaseId,
        'origin_sequence_number': 1,
      },
      aggregates: const <String>[],
    );
  });
  await store.runTransaction((txn, collector) async {
    await recordFindingInTxnForTest(
      store,
      txn,
      collector,
      role: FindingRole.walk,
      kind: FindingKind.forkUnrecorded,
      evidence: <String, Object?>{
        'database_id': ownDatabaseId,
        'previous_event_hash': null,
      },
      aggregates: const <String>[],
    );
  });
}

/// A handful of ordinary appends, discarded from the measurement, so the
/// timed batch does not absorb one-time warm-up cost (JIT, first-write page
/// faults) that would otherwise bias whichever store is timed first.
Future<void> _warmUp(EventStore store) async {
  for (var i = 0; i < 50; i++) {
    await store.append(
      entryType: _kVersionType,
      aggregateId: 'agg-warmup-$i',
      aggregateType: 'guard',
      eventType: 'finalized',
      data: <String, Object?>{'v': 1, 'n': 2000000 + i},
      initiator: const UserInitiator('guard-user'),
    );
  }
}

/// Times [_kMeasuredAppends] ordinary, one-transaction-each appends against
/// [store], each to a fresh aggregate so none of them dedupes or competes
/// with the seeded aggregates.
Future<Duration> _timeMeasuredAppends(EventStore store, String label) async {
  final clock = Stopwatch()..start();
  for (var i = 0; i < _kMeasuredAppends; i++) {
    await store.append(
      entryType: _kVersionType,
      aggregateId: 'agg-measured-$label-$i',
      aggregateType: 'guard',
      eventType: 'finalized',
      data: <String, Object?>{'v': 1, 'n': 3000000 + i},
      initiator: const UserInitiator('guard-user'),
      // Each measured aggregate is fresh, so this never skips the append;
      // it exercises SembastBackend.findEventsForAggregateInTxn (the
      // dedupe-by-content read) on every measured append, independent of
      // the marks refresh, which no longer calls it now that the table
      // refresh reads its own source-aggregate index instead.
      dedupeByContent: true,
    );
  }
  return clock.elapsed;
}

void main() {
  // Verifies: EVS-DEV-causal-parents/H
  // Verifies: EVS-PRD-event-log/C
  // Verifies: EVS-PRD-materializer/E
  // Verifies: EVS-PRD-materializer/G
  test('ordinary append cost at 40k stored events is well under '
      '${_kAllowedRatio}x its cost at 1k (a linear scan would be about '
      '${_kLinearRatio}x)', () async {
    final small = await _openStore();
    await _seedTo(small, 1000);
    await _seedOneIngestedEvent(small);
    await _seedHeldFindings(small);
    await _warmUp(small);
    final smallElapsed = await _timeMeasuredAppends(small, 'small');

    final large = await _openStore();
    await _seedTo(large, 40000);
    await _seedOneIngestedEvent(large);
    await _seedHeldFindings(large);
    await _warmUp(large);
    final largeElapsed = await _timeMeasuredAppends(large, 'large');

    // ignore: avoid_print, the measurement is the point of this file
    print(
      'append cost: 1k store -> $smallElapsed for $_kMeasuredAppends '
      'appends; 40k store -> $largeElapsed for $_kMeasuredAppends '
      'appends',
    );

    final smallMicros = smallElapsed.inMicroseconds.clamp(
      1,
      1 << 62,
    ); // guard against a zero-duration small run
    final ratio = largeElapsed.inMicroseconds / smallMicros;
    // ignore: avoid_print, the measurement is the point of this file
    print('ratio: ${ratio.toStringAsFixed(2)}x');
    expect(
      ratio,
      lessThan(_kAllowedRatio),
      reason:
          'an append reading the whole event store grows with the '
          "store's size; a backend-owned index does not",
    );
  }, timeout: const Timeout(Duration(minutes: 3)));
}
