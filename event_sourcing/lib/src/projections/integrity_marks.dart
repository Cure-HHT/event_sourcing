// Implements: EVS-PRD-materializer/D
// an aggregate is marked by every held finding naming it that the holder
//   holds as authored, and by a received finding naming it only when its
//   events were authored by the finding's originating database or a database
//   of that database's succession lineage.
// Implements: EVS-PRD-materializer/E
// an aggregate is marked by a held position_reused or fork_unrecorded
//   finding about a database X when it holds an event of X at or above the
//   finding's origin position (for a fork, the lowest origin position among
//   the held events of X carrying the named predecessor), counting findings
//   held as authored and received findings whose originating database is X
//   or has X in its succession lineage.
// Implements: EVS-PRD-materializer/B
// the marks are a function of the events the transaction reads, and nothing
//   else, so a rebuild derives the marks the incremental fold did.
import 'package:event_sourcing/src/security/security_finding.dart';
import 'package:event_sourcing/src/security/system_entry_types.dart';
import 'package:event_sourcing/src/storage/chain_coordinates.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:meta/meta.dart' show immutable, internal;

/// The reserved row key under which every row of a default view carries
/// the security findings that mark it.
@internal
const String kIntegrityRowKey = r'$integrity';

/// The key, inside [kIntegrityRowKey], of the ascending finding ids.
@internal
const String kIntegritySecurityFindingsKey = 'security_findings';

/// The `$integrity` value of a row marked by [findingIds], which are in
/// ascending order.
@internal
Map<String, Object?> integrityValue(List<String> findingIds) =>
    Map<String, Object?>.unmodifiable(<String, Object?>{
      kIntegritySecurityFindingsKey: List<String>.unmodifiable(findingIds),
    });

/// The finding ids a row's `$integrity` holds, or null when the row
/// carries none in the shape the fold writes.
@internal
List<String>? integrityFindingIdsOf(Map<String, Object?> row) {
  final value = row[kIntegrityRowKey];
  if (value is! Map) return null;
  final ids = value[kIntegritySecurityFindingsKey];
  if (ids is! List) return null;
  return <String>[for (final id in ids) id.toString()];
}

/// The databases whose authorship a received finding of [originDatabaseId]
/// speaks for: the database itself, and the databases of its succession
/// lineage. While no succession is recorded, the lineage is the database
/// alone.
Set<String> _lineageOf(String originDatabaseId) => <String>{originDatabaseId};

/// One held security finding, as the marks read it.
@immutable
final class _HeldFinding {
  const _HeldFinding({
    required this.eventId,
    required this.findingId,
    required this.kind,
    required this.evidence,
    required this.aggregates,
    required this.originDatabaseId,
    required this.heldAsAuthored,
  });

  /// Reads the finding [event] records, or null when its data does not
  /// carry a finding identity.
  static _HeldFinding? of(StoredEvent event, String? holder) {
    final data = event.data;
    final findingId = data['finding_id'];
    if (findingId is! String) return null;
    final kind = data['kind'];
    final evidence = data['evidence'];
    final aggregates = data['aggregates'];
    final coordinates = ChainCoordinates.of(event);
    return _HeldFinding(
      eventId: event.eventId,
      findingId: findingId,
      kind: kind is String ? FindingKind.fromWire(kind) : null,
      evidence: evidence is Map
          ? Map<String, Object?>.from(evidence)
          : const <String, Object?>{},
      aggregates: aggregates is List
          ? <String>{
              for (final a in aggregates)
                if (a is String) a,
            }
          : const <String>{},
      originDatabaseId: coordinates.originatingDatabaseId,
      heldAsAuthored: holder != null && coordinates.heldAsAuthoredBy == holder,
    );
  }

  final String eventId;
  final String findingId;
  final FindingKind? kind;
  final Map<String, Object?> evidence;
  final Set<String> aggregates;
  final String? originDatabaseId;
  final bool heldAsAuthored;

  /// The database whose origin chain a `position_reused` or
  /// `fork_unrecorded` finding is about, when this finding is one that the
  /// holder counts for it; null otherwise.
  String? get chainDatabase {
    if (kind != FindingKind.positionReused &&
        kind != FindingKind.forkUnrecorded) {
      return null;
    }
    final database = evidence['database_id'];
    if (database is! String) return null;
    if (heldAsAuthored) return database;
    final origin = originDatabaseId;
    if (origin == null || !_lineageOf(origin).contains(database)) return null;
    return database;
  }

  /// Whether a received finding speaks for the aggregate of [authorship]:
  /// some of its held events were authored by the finding's originating
  /// database or its lineage.
  bool speaksFor(_Authorship authorship) {
    if (heldAsAuthored) return true;
    final origin = originDatabaseId;
    if (origin == null) return false;
    return _lineageOf(origin).any(authorship.containsKey);
  }
}

/// Who authored the held events of one aggregate: each originating
/// database, with the highest origin position among its events there, or
/// null when none records one.
typedef _Authorship = Map<String, int?>;

/// Adds [event] to [authorship].
void _addAuthorship(_Authorship authorship, StoredEvent event) {
  final c = ChainCoordinates.of(event);
  final origin = c.originatingDatabaseId;
  if (origin == null) return;
  final position = c.originPosition;
  final held = authorship[origin];
  authorship[origin] = position == null
      ? held
      : (held == null || position > held ? position : held);
}

/// Chunk size of the one read of the log a replay makes.
const int _replayChunkSize = 500;

/// The marks state of one transaction: the holder's identity, the findings
/// held, and the lookups made for the event being folded.
final class _TransactionMarks {
  _TransactionMarks(this.holder, this.findings);

  final String? holder;
  final List<_HeldFinding> findings;

  /// Whether the transaction replays a log it does not append to, so the
  /// authorship of every aggregate is read once.
  bool replay = false;

  /// In a replay, the authorship of every aggregate, once read, and the
  /// highest local sequence number it covers.
  Map<String, _Authorship>? authorship;
  int authorshipThrough = 0;

  /// The authorship of [aggregateId]: in a replay from one read of the
  /// log, otherwise from the aggregate's held events.
  Future<_Authorship> authorshipOf(
    Transaction txn,
    StorageBackend backend,
    String aggregateId,
  ) async {
    if (!replay) {
      final authorship = <String, int?>{};
      for (final e in await backend.findEventsForAggregateInTxn(
        txn,
        aggregateId,
      )) {
        _addAuthorship(authorship, e);
      }
      return authorship;
    }
    var all = authorship;
    if (all == null) {
      all = <String, _Authorship>{};
      int? after;
      while (true) {
        final chunk = await backend.findAllEventsInTxn(
          txn,
          afterSequence: after,
          limit: _replayChunkSize,
        );
        for (final e in chunk) {
          _addAuthorship(all.putIfAbsent(e.aggregateId, () => {}), e);
          if (e.sequenceNumber > authorshipThrough) {
            authorshipThrough = e.sequenceNumber;
          }
        }
        if (chunk.length < _replayChunkSize) break;
        after = chunk.last.sequenceNumber;
      }
      authorship = all;
    }
    return all[aggregateId] ?? const <String, int?>{};
  }

  /// Adds [event], stored after the replay's read, to the authorship.
  void noteFolded(StoredEvent event) {
    final all = authorship;
    if (all == null || event.sequenceNumber <= authorshipThrough) return;
    _addAuthorship(all.putIfAbsent(event.aggregateId, () => {}), event);
    authorshipThrough = event.sequenceNumber;
  }

  /// The event the memo below belongs to.
  String? memoEventId;
  EventMarks? memo;
}

final Expando<_TransactionMarks> _byTransaction = Expando<_TransactionMarks>(
  'integrity marks',
);

/// What folding one event does to the outstanding-finding marks: the marks
/// of the event's own aggregate, and the marks of every aggregate whose
/// marks the event may change, whose rows every view refreshes.
@internal
@immutable
final class EventMarks {
  const EventMarks({required this.own, required this.refresh});

  /// No finding is held: every row's list is empty.
  static const EventMarks none = EventMarks(
    own: <String>[],
    refresh: <String, List<String>>{},
  );

  /// The marks of the event's aggregate, in ascending order.
  final List<String> own;

  /// The aggregates whose rows every view refreshes, with their marks.
  final Map<String, List<String>> refresh;
}

/// Computes the outstanding-finding marks of the default views from the
/// log, inside a transaction.
@internal
abstract final class IntegrityMarks {
  /// The marks folding [event], a held event, writes: its aggregate's
  /// marks, and the aggregates whose marks [event] may change.
  ///
  /// The findings held are read once per transaction; a finding folded
  /// later in the same transaction is added as it is folded. The result is
  /// memoized for the last event, so the views folding one event read the
  /// log once.
  static Future<EventMarks> forEvent(
    Transaction txn,
    StorageBackend backend,
    StoredEvent event,
  ) async {
    final state = await _stateOf(txn, backend);
    if (state.memoEventId == event.eventId) return state.memo!;
    state.noteFolded(event);

    if (_isFinding(event) &&
        !state.findings.any((f) => f.eventId == event.eventId)) {
      final f = _HeldFinding.of(event, state.holder);
      if (f != null) state.findings.add(f);
    }

    final EventMarks result;
    if (state.findings.isEmpty) {
      result = EventMarks.none;
    } else {
      result = await _Evaluation(txn, backend, state).forEvent(event);
    }
    state
      ..memoEventId = event.eventId
      ..memo = result;
    return result;
  }

  /// Declares that [txn] replays the log without appending to it (a
  /// rebuild, or the re-derivation at open), so the marks read the
  /// authorship of every aggregate in one read of the log rather than one
  /// read per event. An event folded in [txn] beyond that read is added to
  /// it.
  static Future<void> beginReplay(
    Transaction txn,
    StorageBackend backend,
  ) async {
    await _stateOf(txn, backend);
    _byTransaction[txn]!.replay = true;
  }

  static Future<_TransactionMarks> _stateOf(
    Transaction txn,
    StorageBackend backend,
  ) async {
    final held = _byTransaction[txn];
    if (held != null) return held;
    final holder = await backend.readDatabaseIdTxn(txn);
    final findings = <_HeldFinding>[];
    if (await backend.holdsSecurityFindingInTxn(txn)) {
      for (final e in await backend.findSecurityFindingsInTxn(txn)) {
        final f = _HeldFinding.of(e, holder);
        if (f != null) findings.add(f);
      }
    }
    final state = _TransactionMarks(holder, findings);
    _byTransaction[txn] = state;
    return state;
  }

  static bool _isFinding(StoredEvent event) =>
      event.entryType == kSecurityFindingEntryType &&
      event.eventType == kSecurityFindingRecordedEventType;
}

/// One evaluation of the marks, with the lookups it made.
final class _Evaluation {
  _Evaluation(this.txn, this.backend, this.state);

  final Transaction txn;
  final StorageBackend backend;
  final _TransactionMarks state;
  List<_HeldFinding> get findings => state.findings;
  final Map<String, _Authorship> _authorshipOf = <String, _Authorship>{};
  final Map<String, int?> _thresholdOf = <String, int?>{};

  Future<EventMarks> forEvent(StoredEvent event) async {
    final own = event.aggregateId;
    final coordinates = ChainCoordinates.of(event);
    final origin = coordinates.originatingDatabaseId;
    final candidates = <String>{};

    if (IntegrityMarks._isFinding(event)) {
      for (final f in findings) {
        if (f.eventId == event.eventId) candidates.addAll(await _reachOf(f));
      }
    }
    for (final f in findings) {
      final chainDb = f.chainDatabase;
      if (chainDb != null && chainDb == origin) {
        if (f.kind == FindingKind.forkUnrecorded &&
            coordinates.previousEventHash ==
                f.evidence['previous_event_hash']) {
          // The event may lower the fork's lowest position.
          candidates.addAll(await _reachOf(f));
        } else {
          final threshold = await _threshold(f);
          final position = coordinates.originPosition;
          if (threshold != null && position != null && position >= threshold) {
            candidates.add(own);
          }
        }
      }
      if (!f.heldAsAuthored && f.aggregates.contains(own)) {
        final fOrigin = f.originDatabaseId;
        if (fOrigin != null && _lineageOf(fOrigin).contains(origin)) {
          candidates.add(own);
        }
      }
    }

    final refresh = <String, List<String>>{};
    for (final aggregateId in candidates.toList()..sort()) {
      refresh[aggregateId] = await marksOf(aggregateId);
    }
    return EventMarks(
      own: refresh[own] ?? await marksOf(own),
      refresh: refresh,
    );
  }

  /// The findings that mark [aggregateId], in ascending order of identity.
  ///
  /// Who authored the aggregate's events is read only when a finding needs
  /// it: a received finding naming the aggregate, or a reused position or
  /// a fork counted for a database. A finding held as authored marks the
  /// aggregates it names whoever authored their events.
  Future<List<String>> marksOf(String aggregateId) async {
    final ids = <String>{};
    for (final f in findings) {
      if (f.aggregates.contains(aggregateId) &&
          (f.heldAsAuthored || f.speaksFor(await _authorship(aggregateId)))) {
        ids.add(f.findingId);
        continue;
      }
      final chainDb = f.chainDatabase;
      if (chainDb == null) continue;
      final threshold = await _threshold(f);
      if (threshold == null) continue;
      final highest = (await _authorship(aggregateId))[chainDb];
      if (highest != null && highest >= threshold) ids.add(f.findingId);
    }
    return ids.toList()..sort();
  }

  /// The aggregates finding [f] may mark: those it names, and, for a reused
  /// position or a fork it counts for, those of the held events of its
  /// database at or above its position.
  Future<Set<String>> _reachOf(_HeldFinding f) async {
    final reach = <String>{...f.aggregates};
    final chainDb = f.chainDatabase;
    if (chainDb == null) return reach;
    final threshold = await _threshold(f);
    if (threshold == null) return reach;
    for (final e in await backend.findEventsFromOriginPositionInTxn(
      txn,
      originatingDatabaseId: chainDb,
      fromPosition: threshold,
    )) {
      reach.add(e.aggregateId);
    }
    return reach;
  }

  /// The origin position at or above which finding [f] marks the events of
  /// its database: a reused position's own, a fork's lowest among the held
  /// events carrying its predecessor; null when there is none.
  Future<int?> _threshold(_HeldFinding f) async {
    if (_thresholdOf.containsKey(f.eventId)) return _thresholdOf[f.eventId];
    int? threshold;
    final chainDb = f.chainDatabase;
    if (chainDb != null) {
      if (f.kind == FindingKind.positionReused) {
        final position = f.evidence['origin_sequence_number'];
        threshold = position is int ? position : null;
      } else {
        final predecessor = f.evidence['previous_event_hash'];
        if (predecessor == null || predecessor is String) {
          for (final e in await backend.findEventsByPredecessorInTxn(
            txn,
            originatingDatabaseId: chainDb,
            previousEventHash: predecessor as String?,
          )) {
            final position = ChainCoordinates.of(e).originPosition;
            if (position != null &&
                (threshold == null || position < threshold)) {
              threshold = position;
            }
          }
        }
      }
    }
    _thresholdOf[f.eventId] = threshold;
    return threshold;
  }

  Future<_Authorship> _authorship(String aggregateId) async =>
      _authorshipOf[aggregateId] ??= await state.authorshipOf(
        txn,
        backend,
        aggregateId,
      );
}
