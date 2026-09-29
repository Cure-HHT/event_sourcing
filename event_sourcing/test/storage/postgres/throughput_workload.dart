// The throughput workload the current tree runs in-process against
// Postgres for the throughput guard (EVS-DEV-chain-verification/T). This
// file declares no tests, so it carries no citation.
//
// It appends events to one fresh database and, separately, ingests events
// a second fresh database on the same server authored, over the owner /
// runtime / lock role fixture every other Postgres-gated test opens
// through. Each measured operation runs a warm-up pass, then three timed
// passes; the reported rate is the median of the three.

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/event_store.dart' show PublishCollector;

import '../../test_support/deliveries.dart' show ingestEventForTest;
import 'test_postgres_url.dart';

/// One throughput measurement: how many appends, and how many ingested
/// events, a workload completed per second.
class ThroughputMeasurement {
  const ThroughputMeasurement({
    required this.appendPerSec,
    required this.ingestPerSec,
  });

  final double appendPerSec;
  final double ingestPerSec;
}

/// Events per timed pass, and how many distinct aggregates they spread
/// over: large enough that a short GC pause or scheduling hiccup does not
/// swing the measured rate, while a warm-up pass plus three timed passes,
/// for both append and ingest, still finish in a few seconds against a
/// local Postgres.
const _kEventsPerRun = 500;
const _kAggregatesPerRun = 20;
const _kChunk = 25;
const _kEntryType = 'throughput_note';
const _kRuns = 3;

EntryTypeRegistry _entryTypes() {
  final registry = EntryTypeRegistry();
  for (final definition in kSystemEntryTypes) {
    registry.register(definition);
  }
  return registry..register(
    const EntryTypeDefinition(
      id: _kEntryType,
      registeredVersion: EntryTypeVersion(1, 0),
      name: _kEntryType,
    ),
  );
}

Future<EventStore> _openStore(PostgresTestDatabase db, String hopId) async {
  final backend = await db.open(provision: true);
  return EventStore.open(
    storage: ApplicationSuppliedStorage(
      backend,
      PostgresSecurityContextStore(backend: backend),
    ),
    entryTypes: _entryTypes(),
    source: Source(
      hopId: hopId,
      identifier: '$hopId-install',
      softwareVersion: 'throughput-guard@current',
    ),
  );
}

Future<StoredEvent> _appendOne(
  EventStore store,
  Transaction txn,
  PublishCollector collector,
  String aggregateId,
  int i,
) async {
  final event = await store.appendInTxn(
    txn,
    entryType: _kEntryType,
    aggregateId: aggregateId,
    aggregateType: 'throughput',
    eventType: 'measured',
    data: <String, Object?>{'i': i},
    initiator: const AutomationInitiator(service: 'throughput-guard'),
    flowToken: null,
    metadata: null,
    security: null,
    checkpointReason: null,
    changeReason: null,
    dedupeByContent: false,
    collector: collector,
  );
  return event!;
}

/// Appends [_kEventsPerRun] events over [_kAggregatesPerRun] aggregates
/// named with [runIndex] (so distinct runs never share an aggregate),
/// in transactions of [_kChunk] events, and returns how long the whole
/// run took.
Future<Duration> _timedAppendRun(EventStore store, int runIndex) async {
  final clock = Stopwatch()..start();
  for (var start = 0; start < _kEventsPerRun; start += _kChunk) {
    await store.runTransaction((txn, collector) async {
      for (var i = start; i < start + _kChunk; i++) {
        final aggregateId = 'thr-append-r$runIndex-a${i % _kAggregatesPerRun}';
        await _appendOne(store, txn, collector, aggregateId, i);
      }
    });
  }
  clock.stop();
  return clock.elapsed;
}

/// Has [peer] author [_kEventsPerRun] events (untimed: only the ingest
/// itself is measured), then ingests each into [target] one at a time,
/// exactly as the ingest path handles a delivered record, and returns how
/// long the ingest passes took.
Future<Duration> _timedIngestRun(
  EventStore peer,
  EventStore target,
  int runIndex,
) async {
  final events = <StoredEvent>[];
  for (var i = 0; i < _kEventsPerRun; i++) {
    final aggregateId = 'thr-ingest-r$runIndex-a${i % _kAggregatesPerRun}';
    final event = await peer.append(
      entryType: _kEntryType,
      aggregateId: aggregateId,
      aggregateType: 'throughput',
      eventType: 'measured',
      data: <String, Object?>{'i': i},
      initiator: const AutomationInitiator(service: 'throughput-guard-peer'),
    );
    events.add(event!);
  }
  final clock = Stopwatch()..start();
  for (final event in events) {
    await ingestEventForTest(target, event);
  }
  clock.stop();
  return clock.elapsed;
}

/// The median of three measurements, sorted first.
double _median(List<double> values) {
  final sorted = List<double>.of(values)..sort();
  return sorted[sorted.length ~/ 2];
}

/// Runs the current tree's throughput workload against [pgUrl] and returns
/// its measurement: the median of three timed append passes (after a
/// warm-up pass) against a fresh database, and the median of three timed
/// ingest passes (after a warm-up pass) of events a second fresh database
/// authored.
Future<ThroughputMeasurement> runThroughputWorkload(String pgUrl) async {
  final mainDb = PostgresTestDatabase(pgUrl, tag: 'thrmain');
  final peerDb = PostgresTestDatabase(pgUrl, tag: 'thrpeer');
  await mainDb.reset();
  await peerDb.reset();
  final mainStore = await _openStore(mainDb, 'throughput-main');
  final peerStore = await _openStore(peerDb, 'throughput-peer');
  try {
    // Warm-up passes: connection and query-plan caching settle before the
    // timed passes run.
    await _timedAppendRun(mainStore, -1);
    await _timedIngestRun(peerStore, mainStore, -1);

    final appendRates = <double>[];
    for (var run = 0; run < _kRuns; run++) {
      final elapsed = await _timedAppendRun(mainStore, run);
      appendRates.add(_kEventsPerRun / elapsed.inMicroseconds * 1e6);
    }

    final ingestRates = <double>[];
    for (var run = 0; run < _kRuns; run++) {
      final elapsed = await _timedIngestRun(peerStore, mainStore, run);
      ingestRates.add(_kEventsPerRun / elapsed.inMicroseconds * 1e6);
    }

    return ThroughputMeasurement(
      appendPerSec: _median(appendRates),
      ingestPerSec: _median(ingestRates),
    );
  } finally {
    await mainStore.close();
    await peerStore.close();
    await mainDb.drop();
    await peerDb.drop();
  }
}
