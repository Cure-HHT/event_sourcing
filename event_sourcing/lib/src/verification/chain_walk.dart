import 'package:collection/collection.dart' show DeepCollectionEquality;
import 'package:event_sourcing/src/causal_record.dart';
import 'package:event_sourcing/src/security/security_finding.dart';
import 'package:event_sourcing/src/storage/chain_coordinates.dart';
import 'package:event_sourcing/src/storage/event_hash.dart'
    show canonicalEventHash;
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/verification/chain_verification_verdict.dart';
import 'package:meta/meta.dart' show internal;

/// The `hash_mismatch` evidence for every hash [event] carries that does
/// not recompute: its own `event_hash`, recomputed over [wireRecord] when
/// given (the record as a delivery carried it) and over the stored record
/// otherwise, then each receiver entry's `arrival_hash`, from the last
/// entry back to the second. Each is `event_id`, `carried_hash` and
/// `recomputed_hash`; an arrival hash the entry does not carry is carried
/// as null. Never throws.
@internal
List<Map<String, Object?>> hashMismatchEvidence(
  StoredEvent event, {
  Map<String, Object?>? wireRecord,
}) {
  final found = <Map<String, Object?>>[];
  Map<String, Object?> mismatch(String? carried, String recomputed) =>
      <String, Object?>{
        'event_id': event.eventId,
        'carried_hash': carried,
        'recomputed_hash': recomputed,
      };
  // The event's own hash, over the record exactly as it is held or as the
  // delivery carried it.
  final recomputedTail = canonicalEventHash(wireRecord ?? event.toMap());
  if (recomputedTail != event.eventHash) {
    found.add(mismatch(event.eventHash, recomputedTail));
  }
  final raw = event.metadata['provenance'];
  if (raw is! List) return found;
  final provenance = <Map<String, Object?>>[
    for (final entry in raw)
      if (entry is Map) Map<String, Object?>.from(entry),
  ];
  if (provenance.length != raw.length) return found;
  // Each receiver entry's arrival hash is the hash of the record as the
  // entry before it stored it: the provenance cut to that entry, and the
  // sequence number that store gave it (the origin position for the
  // originator, recorded on the second entry; a receiver's own
  // ingest_sequence_number for a later one).
  for (var k = provenance.length - 1; k > 0; k--) {
    final carried = provenance[k]['arrival_hash'];
    final seqBefore = k == 1
        ? provenance[k]['origin_sequence_number']
        : provenance[k - 1]['ingest_sequence_number'];
    final recomputed = _hashWithProvenanceSlice(
      event,
      provenance.sublist(0, k),
      sequenceNumberOverride: seqBefore is int ? seqBefore : null,
    );
    if (carried != recomputed) {
      found.add(mismatch(carried is String ? carried : null, recomputed));
    }
  }
  return found;
}

/// The hash [event] has with its provenance cut to [provenanceSlice] and,
/// when given, its `sequence_number` set to [sequenceNumberOverride]: the
/// hash an earlier store sealed it under.
String _hashWithProvenanceSlice(
  StoredEvent event,
  List<Map<String, Object?>> provenanceSlice, {
  int? sequenceNumberOverride,
}) {
  final recordMap = Map<String, Object?>.from(event.toMap());
  recordMap['metadata'] = <String, Object?>{
    ...event.metadata,
    'provenance': provenanceSlice,
  };
  if (sequenceNumberOverride != null) {
    recordMap['sequence_number'] = sequenceNumberOverride;
  }
  recordMap.remove('event_hash');
  return canonicalEventHash(recordMap);
}

/// Throws [ArgumentError] for a range the chain verification refuses: a
/// negative bound, or a lower bound above the upper.
// Implements: EVS-DEV-chain-verification/J
// a range with a negative bound or a lower bound above its upper bound is
//   refused before any event is read.
@internal
void checkChainRange({int? from, int? to}) {
  if (from != null && from < 0) {
    throw ArgumentError.value(from, 'from', 'must not be negative');
  }
  if (to != null && to < 0) {
    throw ArgumentError.value(to, 'to', 'must not be negative');
  }
  if (from != null && to != null && from > to) {
    throw ArgumentError.value(from, 'from', 'must not be above to ($to)');
  }
}

/// The reads the chain verification makes of one database's log. Every
/// list is in ascending local sequence number.
@internal
abstract interface class ChainWalkSource {
  /// The holding database's identity, or null when it has none.
  Future<String?> databaseId();

  /// The highest local sequence number stored, or 0 for an empty log.
  Future<int> highestSequence();

  /// Up to [limit] events stored above local sequence number [after].
  Future<List<StoredEvent>> page({required int after, required int limit});

  /// The event stored at local sequence number [sequenceNumber], or null.
  Future<StoredEvent?> at(int sequenceNumber);

  /// The held event [eventId], or null.
  Future<StoredEvent?> byId(String eventId);

  /// The held events of the aggregate [aggregateId].
  Future<List<StoredEvent>> byAggregate(String aggregateId);

  /// The held events sealed under [sealedHash].
  Future<List<StoredEvent>> bySealedHash(String sealedHash);

  /// The held events of [databaseId] carrying [previousEventHash].
  Future<List<StoredEvent>> byPredecessor(
    String databaseId,
    String? previousEventHash,
  );

  /// The held events of [databaseId] at origin position [originPosition].
  Future<List<StoredEvent>> byOriginPosition(
    String databaseId,
    int originPosition,
  );
}

/// The events the chain verification reads per page.
@internal
const int kChainWalkPageSize = 256;

/// The chain verification of [backend]'s log: refuses a bad range before
/// reading, then walks the log inside [StorageBackend.nonBlockingRead].
/// Records nothing.
@internal
Future<ChainVerificationVerdict> verifyChainsOver(
  StorageBackend backend, {
  int? from,
  int? to,
  int pageSize = kChainWalkPageSize,
  Future<void> Function()? afterPage,
}) async {
  checkChainRange(from: from, to: to);
  return backend.nonBlockingRead(
    (reads) => walkChains(
      _BackendSource(backend, reads),
      from: from,
      to: to,
      pageSize: pageSize,
      afterPage: afterPage,
    ),
  );
}

/// The aggregates the security finding recording [finding] names, read
/// inside [txn], the transaction that records it: for a fork or a reused
/// origin position, those of every event of the named database carrying the
/// named predecessor hash or sitting at the named origin position that the
/// database holds then, events stored after the walk read included; for
/// every other kind, those the walk read, since the events its evidence
/// names stay held.
// Implements: EVS-DEV-security-findings/B
// a fork or reuse finding names the aggregates of the events of the named
//   database carrying the predecessor or at the position that the detecting
//   database holds when it records the finding.
@internal
Future<List<String>> recordedAggregatesInTxn(
  StorageBackend backend,
  Transaction txn,
  ChainVerificationFinding finding,
) async {
  final evidence = finding.evidence;
  final List<StoredEvent> held;
  if (finding.kind == FindingKind.forkUnrecorded) {
    held = await backend.findEventsByPredecessorInTxn(
      txn,
      originatingDatabaseId: evidence['database_id']! as String,
      previousEventHash: evidence['previous_event_hash'] as String?,
    );
  } else if (finding.kind == FindingKind.positionReused) {
    held = await backend.findEventsByOriginPositionInTxn(
      txn,
      originatingDatabaseId: evidence['database_id']! as String,
      originPosition: evidence['origin_sequence_number']! as int,
    );
  } else {
    return finding.aggregates;
  }
  return held.map((e) => e.aggregateId).toSet().toList()..sort();
}

/// The verdict of the chain verification over [source], from [from] (the
/// first local sequence number when null or 0) to [to] (the highest stored
/// when the walk starts, when null or above it), reading [pageSize] events
/// at a time and awaiting [afterPage] after each page it walked.
///
/// Every read is bounded by the upper bound it fixes first: an event
/// stored at a higher local sequence number is neither walked nor counted
/// as held.
// Implements: EVS-DEV-chain-verification/S
// the upper bound of the range is fixed when the walk starts; nothing
//   stored after it is walked or read as held.
// Implements: EVS-PRD-hash-chain-integrity/C+F
// the walk verifies, from the stored log alone, the storage chain and every
//   origin-chain link it can resolve.
@internal
Future<ChainVerificationVerdict> walkChains(
  ChainWalkSource source, {
  int? from,
  int? to,
  int pageSize = kChainWalkPageSize,
  Future<void> Function()? afterPage,
}) async {
  checkChainRange(from: from, to: to);
  if (pageSize < 1) {
    throw ArgumentError.value(pageSize, 'pageSize', 'must be at least 1');
  }
  final holder = await source.databaseId();
  final highest = await source.highestSequence();
  // Implements: EVS-DEV-chain-verification/J
  // an omitted lower bound, or 0, is the database's first stored event.
  final first = (from == null || from == 0) ? 1 : from;
  final last = (to == null || to > highest) ? highest : to;
  final walk = _Walk(source, holder: holder, last: last);

  // Implements: EVS-DEV-chain-verification/D
  // the event before the range is read from outside it, so the first event
  //   of the range is checked like every other.
  var previous = first > 1 && first <= last ? await walk.at(first - 1) : null;
  var next = first;
  var cursor = first - 1;
  while (cursor < last) {
    final page = await source.page(after: cursor, limit: pageSize);
    final inRange = <StoredEvent>[
      for (final e in page)
        if (e.sequenceNumber <= last) e,
    ];
    for (final event in inRange) {
      walk.missing(next, event.sequenceNumber);
      await walk.check(
        event,
        preceding: previous?.sequenceNumber == event.sequenceNumber - 1
            ? previous
            : null,
      );
      previous = event;
      next = event.sequenceNumber + 1;
      cursor = event.sequenceNumber;
    }
    if (inRange.isNotEmpty && afterPage != null) await afterPage();
    if (inRange.length < page.length || page.length < pageSize) break;
  }
  walk.missing(next, last + 1);
  return ChainVerificationVerdict(
    from: first,
    to: last,
    findings: List<ChainVerificationFinding>.unmodifiable(walk.findings),
    unresolvedPredecessors: walk.unresolvedPredecessors,
  );
}

/// The state of one walk: the findings so far, the forks and reused
/// positions already reported, and the aggregates' events already read.
final class _Walk {
  _Walk(this._source, {required this.holder, required this.last});

  final ChainWalkSource _source;

  /// The holding database's identity.
  final String? holder;

  /// The upper bound of the walk.
  final int last;

  final List<ChainVerificationFinding> findings = <ChainVerificationFinding>[];
  int unresolvedPredecessors = 0;
  final Set<String> _reportedForks = <String>{};
  final Set<String> _reportedPositions = <String>{};
  final Map<String, List<StoredEvent>> _aggregates =
      <String, List<StoredEvent>>{};

  List<StoredEvent> _bounded(List<StoredEvent> events) => <StoredEvent>[
    for (final e in events)
      if (e.sequenceNumber <= last) e,
  ];

  StoredEvent? _boundedOne(StoredEvent? event) =>
      event == null || event.sequenceNumber > last ? null : event;

  Future<StoredEvent?> at(int sequenceNumber) async =>
      _boundedOne(await _source.at(sequenceNumber));

  void _add(
    FindingKind kind,
    Map<String, Object?> evidence,
    Iterable<String> aggregates,
  ) {
    findings.add(
      ChainVerificationFinding(
        kind: kind,
        evidence: Map<String, Object?>.unmodifiable(evidence),
        aggregates: List<String>.unmodifiable(
          aggregates.toSet().toList()..sort(),
        ),
      ),
    );
  }

  /// Reports every local sequence number from [from] up to, not including,
  /// [until] as holding no event.
  // Implements: EVS-DEV-chain-verification/E
  // each local sequence number of the range, up to the highest stored, at
  //   which no event is stored is reported.
  void missing(int from, int until) {
    for (var s = from; s < until; s++) {
      _add(FindingKind.sequenceMissing, <String, Object?>{
        'local_sequence_number': s,
      }, const <String>[]);
    }
  }

  /// Checks [event], stored at its local sequence number, whose storage
  /// link names [preceding], the event at the local sequence number before
  /// it, or null when none is stored there.
  Future<void> check(StoredEvent event, {StoredEvent? preceding}) async {
    _checkHashes(event);
    _checkStorageLink(event, preceding);
    final at = ChainCoordinates.of(event);
    await _checkPredecessor(event, at);
    await _checkPosition(at);
    await _checkFork(at);
    await _checkParents(event);
    await _checkStamping(event, at);
  }

  // Implements: EVS-DEV-chain-verification/D
  // every stored event's own hash and every receiver entry's arrival hash
  //   are recomputed as ingest recomputes them.
  void _checkHashes(StoredEvent event) {
    for (final evidence in hashMismatchEvidence(event)) {
      _add(FindingKind.hashMismatch, evidence, <String>[event.aggregateId]);
    }
  }

  // Implements: EVS-DEV-chain-verification/D
  // the last provenance entry's ingest_sequence_number equals the local
  //   sequence number, and its previous_ingest_hash the stored event_hash
  //   of the event at the preceding local sequence number, null for the
  //   first; a preceding number holding no event is reported as missing
  //   instead.
  // Implements: EVS-PRD-hash-chain-integrity/E
  // each stored event records the hash of the event stored before it.
  void _checkStorageLink(StoredEvent event, StoredEvent? preceding) {
    final raw = event.metadata['provenance'];
    final entry = raw is List && raw.isNotEmpty && raw.last is Map
        ? raw.last as Map
        : const <String, Object?>{};
    void broken(String field, Object? expected, Object? actual) => _add(
      FindingKind.storageLinkBreak,
      <String, Object?>{
        'local_sequence_number': event.sequenceNumber,
        'event_id': event.eventId,
        'field': field,
        'expected': expected,
        'actual': actual,
      },
      <String>[event.aggregateId],
    );
    final ingestSequence = entry['ingest_sequence_number'];
    if (ingestSequence != event.sequenceNumber) {
      broken('ingest_sequence_number', event.sequenceNumber, ingestSequence);
    }
    final String? expectedLink;
    if (event.sequenceNumber == 1) {
      expectedLink = null;
    } else if (preceding != null) {
      expectedLink = preceding.eventHash;
    } else {
      return;
    }
    final link = entry['previous_ingest_hash'];
    if (link != expectedLink) {
      broken('previous_ingest_hash', expectedLink, link);
    }
  }

  // Implements: EVS-DEV-chain-verification/F
  // a predecessor held but authored by another database, or at an origin
  //   position not below the event's, is a break, and so is a dangling
  //   predecessor of an event held as authored; every predecessor naming no
  //   held event is counted.
  // Implements: EVS-DEV-chain-verification/A
  // predecessors resolve against sealed hashes and origin positions.
  Future<void> _checkPredecessor(StoredEvent event, ChainCoordinates at) async {
    final previous = at.previousEventHash;
    if (previous == null) return;
    final predecessors = _bounded(await _source.bySealedHash(previous));
    final bool broken;
    if (predecessors.isEmpty) {
      unresolvedPredecessors += 1;
      broken = holder != null && at.heldAsAuthoredBy == holder;
    } else {
      broken = predecessors.any((held) {
        final p = ChainCoordinates.of(held);
        if (p.originatingDatabaseId != at.originatingDatabaseId) return true;
        final heldPosition = p.originPosition;
        final position = at.originPosition;
        return heldPosition != null &&
            position != null &&
            heldPosition >= position;
      });
    }
    final db = at.originatingDatabaseId;
    final sealed = at.sealedHash;
    if (!broken || db == null || sealed == null) return;
    _add(
      FindingKind.predecessorBreak,
      <String, Object?>{
        'database_id': db,
        'event_hash': sealed,
        'previous_event_hash': previous,
      },
      <String>[event.aggregateId, for (final p in predecessors) p.aggregateId],
    );
  }

  // Implements: EVS-DEV-chain-verification/G
  // each reused origin position an event of the range sits at is reported
  //   once.
  // Implements: EVS-PRD-hash-chain-integrity/G
  // every origin position two events of one database sit at is reported
  //   once.
  Future<void> _checkPosition(ChainCoordinates at) async {
    final db = at.originatingDatabaseId;
    final position = at.originPosition;
    if (db == null || position == null) return;
    final key = '$db|$position';
    if (_reportedPositions.contains(key)) return;
    final sitting = _bounded(await _source.byOriginPosition(db, position));
    if (!sitting.any((e) => e.eventId != at.eventId)) return;
    _reportedPositions.add(key);
    _add(FindingKind.positionReused, <String, Object?>{
      'database_id': db,
      'origin_sequence_number': position,
    }, sitting.map((e) => e.aggregateId));
  }

  // Implements: EVS-DEV-chain-verification/G
  // each fork an event of the range takes part in is reported once, and a
  //   fork whose successors all sit at one origin position only as that
  //   reused position.
  // Implements: EVS-PRD-hash-chain-integrity/G
  // every fork is reported once, a fork at one origin position as that
  //   reused position.
  Future<void> _checkFork(ChainCoordinates at) async {
    final db = at.originatingDatabaseId;
    if (db == null) return;
    final previous = at.previousEventHash;
    final key = '$db|${previous ?? ''}|${previous == null}';
    if (_reportedForks.contains(key)) return;
    final successors = _bounded(await _source.byPredecessor(db, previous));
    final positions = <int?>{
      for (final e in successors) ChainCoordinates.of(e).originPosition,
    };
    if (successors.length < 2 || positions.length < 2) return;
    _reportedForks.add(key);
    _add(FindingKind.forkUnrecorded, <String, Object?>{
      'database_id': db,
      'previous_event_hash': previous,
    }, successors.map((e) => e.aggregateId));
  }

  // Implements: EVS-DEV-causal-parents/K
  // a held parent named under another sealed hash, of another aggregate,
  //   an annotation or ineligible is reported as an invalid parent, once
  //   per parent, by the first of those reasons that applies.
  // Implements: EVS-PRD-hash-chain-integrity/I
  // parents naming an annotation, an ineligible event or an event of
  //   another aggregate are reported.
  Future<void> _checkParents(StoredEvent event) async {
    final causal = event.causal;
    if (causal == null) return;
    for (final parent in causal.parents) {
      final held = _boundedOne(await _source.byId(parent.eventId));
      if (held == null) continue;
      final heldCausal = held.causal;
      final String? reason;
      if (ChainCoordinates.of(held).sealedHash != parent.eventHash) {
        reason = 'held_under_other_hash';
      } else if (held.aggregateId != event.aggregateId) {
        reason = 'other_aggregate';
      } else if (heldCausal == null || heldCausal.kind != CausalKind.version) {
        reason = 'annotation';
      } else if (!heldCausal.eligible) {
        reason = 'ineligible';
      } else {
        reason = null;
      }
      if (reason == null) continue;
      _add(
        FindingKind.parentInvalid,
        <String, Object?>{
          'local_sequence_number': event.sequenceNumber,
          'event_id': event.eventId,
          'parent': parent.toJson(),
          'reason': reason,
        },
        <String>[event.aggregateId, held.aggregateId],
      );
    }
  }

  // Implements: EVS-DEV-causal-parents/L
  // an event held as authored whose parents differ from the latest
  //   eligible version of its aggregate among the events stored before it,
  //   read from outside the range where they precede it, is reported.
  // Implements: EVS-PRD-hash-chain-integrity/I
  // an authored event whose parents differ from the stamping rule's is
  //   reported.
  Future<void> _checkStamping(StoredEvent event, ChainCoordinates at) async {
    final causal = event.causal;
    if (causal == null || holder == null || at.heldAsAuthoredBy != holder) {
      return;
    }
    final events = _aggregates[event.aggregateId] ??= _bounded(
      await _source.byAggregate(event.aggregateId),
    );
    StoredEvent? latest;
    for (final e in events) {
      if (e.sequenceNumber >= event.sequenceNumber) break;
      final c = e.causal;
      if (c != null &&
          c.kind == CausalKind.version &&
          c.eligible &&
          ChainCoordinates.of(e).sealedHash != null) {
        latest = e;
      }
    }
    final expected = <Map<String, Object?>>[
      if (latest != null)
        CausalRef(
          eventId: latest.eventId,
          eventHash: ChainCoordinates.of(latest).sealedHash!,
        ).toJson(),
    ];
    final actual = <Map<String, Object?>>[
      for (final p in causal.parents) p.toJson(),
    ];
    if (const DeepCollectionEquality().equals(expected, actual)) return;
    _add(
      FindingKind.parentsNotStamped,
      <String, Object?>{
        'local_sequence_number': event.sequenceNumber,
        'event_id': event.eventId,
        'expected': expected,
        'actual': actual,
      },
      <String>[event.aggregateId],
    );
  }
}

/// The chain verification's reads of [_backend], through the handle
/// [_reads] of its non-blocking read.
final class _BackendSource implements ChainWalkSource {
  _BackendSource(this._backend, this._reads);

  final StorageBackend _backend;
  final Transaction _reads;

  @override
  Future<String?> databaseId() => _backend.readDatabaseIdTxn(_reads);

  @override
  Future<int> highestSequence() async {
    await for (final e in _backend.readEventsReverseInTxn(_reads)) {
      return e.sequenceNumber;
    }
    return 0;
  }

  @override
  Future<List<StoredEvent>> page({required int after, required int limit}) =>
      _backend.findAllEventsInTxn(_reads, afterSequence: after, limit: limit);

  @override
  Future<StoredEvent?> at(int sequenceNumber) async {
    final found = await _backend.findAllEventsInTxn(
      _reads,
      afterSequence: sequenceNumber - 1,
      limit: 1,
    );
    return found.isNotEmpty && found.first.sequenceNumber == sequenceNumber
        ? found.first
        : null;
  }

  @override
  Future<StoredEvent?> byId(String eventId) =>
      _backend.findEventByIdInTxn(_reads, eventId);

  @override
  Future<List<StoredEvent>> byAggregate(String aggregateId) =>
      _backend.findEventsForAggregateInTxn(_reads, aggregateId);

  @override
  Future<List<StoredEvent>> bySealedHash(String sealedHash) =>
      _backend.findEventsBySealedHashInTxn(_reads, sealedHash);

  @override
  Future<List<StoredEvent>> byPredecessor(
    String databaseId,
    String? previousEventHash,
  ) => _backend.findEventsByPredecessorInTxn(
    _reads,
    originatingDatabaseId: databaseId,
    previousEventHash: previousEventHash,
  );

  @override
  Future<List<StoredEvent>> byOriginPosition(
    String databaseId,
    int originPosition,
  ) => _backend.findEventsByOriginPositionInTxn(
    _reads,
    originatingDatabaseId: databaseId,
    originPosition: originPosition,
  );
}
