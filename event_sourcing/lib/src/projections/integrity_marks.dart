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
import 'package:event_sourcing/src/ingest/sender_succession.dart'
    show
        SenderSuccessionData,
        lineageFromSuccessions,
        lineageSetOf,
        readSenderSuccessionsInTxn;
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
  /// holder counts for it; null otherwise. [state] supplies the succession
  /// lineage of the finding's originating database, read at most once per
  /// transaction, inside [txn].
  Future<String?> chainDatabase(
    Transaction txn,
    StorageBackend backend,
    _TransactionMarks state,
  ) async {
    if (kind != FindingKind.positionReused &&
        kind != FindingKind.forkUnrecorded) {
      return null;
    }
    final database = evidence['database_id'];
    if (database is! String) return null;
    if (heldAsAuthored) return database;
    final origin = originDatabaseId;
    if (origin == null ||
        !(await state.lineageOf(txn, backend, origin)).contains(database)) {
      return null;
    }
    return database;
  }

  // Implements: EVS-PRD-delivery-channel/T
  // treats an aggregate authored by the finding's originating database's
  //   successor, after that successor's succession event, as authored by
  //   the originating database too, so a finding the predecessor holds
  //   reaches what the successor authors afterward.
  /// Whether a received finding speaks for the aggregate of [authorship]:
  /// some of its held events were authored by the finding's originating
  /// database or its lineage, per [state]'s succession lineage read inside
  /// [txn].
  Future<bool> speaksFor(
    _Authorship authorship,
    Transaction txn,
    StorageBackend backend,
    _TransactionMarks state,
  ) async {
    if (heldAsAuthored) return true;
    final origin = originDatabaseId;
    if (origin == null) return false;
    return (await state.lineageOf(
      txn,
      backend,
      origin,
    )).any(authorship.containsKey);
  }
}

/// Who authored the held events of one aggregate: each originating
/// database, with the highest origin position among its events there, or
/// null when none records one.
typedef _Authorship = Map<String, int?>;

/// The marks state of one transaction: the holder's identity, the findings
/// held, and the lookups made for the event being folded.
final class _TransactionMarks {
  _TransactionMarks(this.holder, this.findings);

  final String? holder;
  final List<_HeldFinding> findings;

  /// Every succession event the transaction's database holds, whether
  /// authored or received, read at most once per transaction and reused by
  /// every lineage lookup the fold makes, so the sync fold path reads the
  /// log for it only once.
  List<SenderSuccessionData>? _successions;

  final Map<String, Set<String>> _lineageCache = <String, Set<String>>{};

  /// The succession lineage of [originDatabaseId], [originDatabaseId]
  /// itself included: the databases whose authorship a received finding of
  /// [originDatabaseId] speaks for, derived solely from the succession
  /// events [backend] holds, read inside [txn] at most once per
  /// transaction.
  Future<Set<String>> lineageOf(
    Transaction txn,
    StorageBackend backend,
    String originDatabaseId,
  ) async {
    final cached = _lineageCache[originDatabaseId];
    if (cached != null) return cached;
    final successions = _successions ??= await readSenderSuccessionsInTxn(
      txn,
      backend,
    );
    final set = lineageSetOf(
      lineageFromSuccessions(successions, originDatabaseId),
      originDatabaseId,
    );
    _lineageCache[originDatabaseId] = set;
    return set;
  }

  /// Discards the succession read: the next [lineageOf] call rereads the
  /// log, so a succession event just folded in this transaction (whether
  /// stored before or after an earlier lineage lookup ran) is picked up
  /// rather than left stale for the rest of the transaction.
  void noteSuccessionFolded() {
    _successions = null;
    _lineageCache.clear();
  }

  /// The authorship of [aggregateId], from the backend's own authorship
  /// index: never a scan of the aggregate's held events, nor of the whole
  /// event store (`EVS-PRD-materializer/E`).
  Future<_Authorship> authorshipOf(
    Transaction txn,
    StorageBackend backend,
    String aggregateId,
  ) => backend.readAggregateAuthorshipInTxn(txn, aggregateId);

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

  static bool _isSuccessionEvent(StoredEvent event) =>
      event.entryType == kDestinationSenderSucceededEntryType &&
      event.eventType == kDestinationSenderSucceededEventType;

  /// Whether a held `fork_unrecorded` or `position_reused` finding treats
  /// [event] as changing outstanding-finding marks other than by [event]
  /// itself being a finding or a succession event: an event of the
  /// finding's chain database sharing a fork's predecessor hash (which can
  /// lower the fork's lowest position, [_Evaluation.forEvent]'s
  /// `forkUnrecorded` branch), or one at or above the finding's threshold
  /// position (the same branch's `else`) (EVS-PRD-materializer/E). A view's
  /// currency scan calls this once a gap event fails the interest and
  /// finding/succession checks, so the read costs no full log scan: the
  /// findings this transaction holds are read once and cached by
  /// [_stateOf], and this walks only that held set.
  static Future<bool> changesOtherMarks(
    Transaction txn,
    StorageBackend backend,
    StoredEvent event,
  ) async {
    final state = await _stateOf(txn, backend);
    if (state.findings.isEmpty) return false;
    final coordinates = ChainCoordinates.of(event);
    final origin = coordinates.originatingDatabaseId;
    if (origin == null) return false;
    final eval = _Evaluation(txn, backend, state);
    for (final f in state.findings) {
      final chainDb = await eval._chainDatabaseOf(f);
      if (chainDb == null || chainDb != origin) continue;
      if (f.kind == FindingKind.forkUnrecorded &&
          coordinates.previousEventHash == f.evidence['previous_event_hash']) {
        return true;
      }
      final threshold = await eval._threshold(f);
      final position = coordinates.originPosition;
      if (threshold != null && position != null && position >= threshold) {
        return true;
      }
    }
    return false;
  }
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

  /// [f]'s chain database (see [_HeldFinding.chainDatabase]), resolved
  /// against this transaction's state.
  Future<String?> _chainDatabaseOf(_HeldFinding f) =>
      f.chainDatabase(txn, backend, state);

  /// Whether [f] speaks for [authorship] (see [_HeldFinding.speaksFor]),
  /// resolved against this transaction's state.
  Future<bool> _speaksForOf(_HeldFinding f, _Authorship authorship) =>
      f.speaksFor(authorship, txn, backend, state);

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
    if (IntegrityMarks._isSuccessionEvent(event)) {
      // A newly folded succession event may extend the lineage a received
      // finding speaks for, so the marks a rebuild derives do not depend
      // on whether the finding or the succession event is stored first.
      state.noteSuccessionFolded();
      for (final f in findings) {
        if (!f.heldAsAuthored) candidates.addAll(await _reachOf(f));
      }
    }
    for (final f in findings) {
      final chainDb = await _chainDatabaseOf(f);
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
        if (fOrigin != null &&
            origin != null &&
            (await state.lineageOf(txn, backend, fOrigin)).contains(origin)) {
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
          (f.heldAsAuthored ||
              await _speaksForOf(f, await _authorship(aggregateId)))) {
        ids.add(f.findingId);
        continue;
      }
      final chainDb = await _chainDatabaseOf(f);
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
    final chainDb = await _chainDatabaseOf(f);
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
    final chainDb = await _chainDatabaseOf(f);
    if (chainDb != null) {
      if (f.kind == FindingKind.positionReused) {
        final position = f.evidence['origin_sequence_number'];
        threshold = position is int ? position : null;
      } else {
        final predecessor = f.evidence['previous_event_hash'];
        if (predecessor == null || predecessor is String) {
          threshold = await backend.readLowestOriginPositionByPredecessorInTxn(
            txn,
            originatingDatabaseId: chainDb,
            previousEventHash: predecessor as String?,
          );
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
