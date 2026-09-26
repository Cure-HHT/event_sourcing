import 'package:event_sourcing/src/security/security_finding.dart';
import 'package:event_sourcing/src/storage/chain_coordinates.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:meta/meta.dart' show immutable, internal;

/// A security finding a chain-structure check detected, before a detection
/// point records it under its own role.
@internal
@immutable
final class DetectedChainFinding {
  /// The finding of [kind] with [evidence], naming [aggregates] in
  /// ascending order.
  const DetectedChainFinding({
    required this.kind,
    required this.evidence,
    required this.aggregates,
  });

  /// `predecessor_break`, `position_reused` or `fork_unrecorded`.
  final FindingKind kind;

  /// The evidence its kind fixes.
  final Map<String, Object?> evidence;

  /// The aggregates of the held events the finding concerns, in ascending
  /// order, each once.
  final List<String> aggregates;
}

/// The chain-structure findings about [stored], an event stored inside
/// [txn] a moment ago by ingest or the restore, read inside [txn] so that
/// the events stored earlier in it count among the held events.
///
/// In this order, each at most once:
///
/// - `predecessor_break` when the event's `previous_event_hash` is the
///   sealed hash of a held event another database originated, or one of the
///   event's originating database at an origin position not below the
///   event's; a predecessor naming no held event is no finding;
/// - `position_reused` when a held event of the same originating database
///   with another identifier sits at the event's origin position;
/// - `fork_unrecorded` when a held event of the same originating database
///   with another identifier at another origin position carries the event's
///   `previous_event_hash` (null counting as one value). A fork whose
///   successors all sit at one origin position is the reuse only.
///
/// The aggregates of a fork or reuse finding are those of every held event
/// of the database carrying the predecessor or at the position, the stored
/// event included. Returns nothing for a copy whose provenance yields no
/// originating database, sealed hash or origin position. Detects only; the
/// caller records each finding under its detector role.
// Implements: EVS-DEV-chain-verification/K
// an incoming event whose predecessor is the sealed hash of a held event
//   another database originated, or of one of its own database at an origin
//   position not below its own, is a predecessor_break, the held events
//   counting those stored earlier in the transaction.
// Implements: EVS-DEV-chain-verification/L
// an incoming event at an origin position a held event of its database with
//   another identifier occupies is a position_reused, and one whose
//   predecessor hash such an event at another origin position carries is a
//   fork_unrecorded, counting the events stored earlier in the transaction.
// Implements: EVS-DEV-chain-verification/A
// predecessors and positions resolve against sealed hashes and origin
//   positions, never a holder's re-stamped event_hash.
// Implements: EVS-DEV-security-findings/M
// a predecessor_break names the originating database, the event's sealed
//   hash and its previous_event_hash.
// Implements: EVS-DEV-security-findings/J+K
// a fork is fixed by its database and shared predecessor hash, a reused
//   position by its database and origin position.
// Implements: EVS-DEV-security-findings/B
// a fork or reuse finding names the aggregates of the held events carrying
//   the predecessor or at the position, counted after the event is stored.
// Implements: EVS-PRD-ingest/I+J
// a predecessor of another database or at a later position, a second
//   successor of one predecessor and a second event at one origin position
//   are each recorded as a finding.
@internal
Future<List<DetectedChainFinding>> chainStructureFindingsInTxn(
  StorageBackend backend,
  Transaction txn,
  StoredEvent stored,
) async {
  final at = ChainCoordinates.of(stored);
  final db = at.originatingDatabaseId;
  final sealed = at.sealedHash;
  final position = at.originPosition;
  if (db == null || sealed == null || position == null) {
    return const <DetectedChainFinding>[];
  }
  final previous = at.previousEventHash;
  final found = <DetectedChainFinding>[];

  if (previous != null) {
    final predecessors = await backend.findEventsBySealedHashInTxn(
      txn,
      previous,
    );
    final broken = predecessors.any((held) {
      final p = ChainCoordinates.of(held);
      if (p.originatingDatabaseId != db) return true;
      final heldPosition = p.originPosition;
      return heldPosition != null && heldPosition >= position;
    });
    if (broken) {
      found.add(
        DetectedChainFinding(
          kind: FindingKind.predecessorBreak,
          evidence: <String, Object?>{
            'database_id': db,
            'event_hash': sealed,
            'previous_event_hash': previous,
          },
          aggregates: _aggregatesOf(<StoredEvent>[stored, ...predecessors]),
        ),
      );
    }
  }

  final atPosition = await backend.findEventsByOriginPositionInTxn(
    txn,
    originatingDatabaseId: db,
    originPosition: position,
  );
  if (atPosition.any((held) => held.eventId != stored.eventId)) {
    found.add(
      DetectedChainFinding(
        kind: FindingKind.positionReused,
        evidence: <String, Object?>{
          'database_id': db,
          'origin_sequence_number': position,
        },
        aggregates: _aggregatesOf(atPosition),
      ),
    );
  }

  final successors = await backend.findEventsByPredecessorInTxn(
    txn,
    originatingDatabaseId: db,
    previousEventHash: previous,
  );
  final forked = successors.any(
    (held) =>
        held.eventId != stored.eventId &&
        ChainCoordinates.of(held).originPosition != position,
  );
  if (forked) {
    found.add(
      DetectedChainFinding(
        kind: FindingKind.forkUnrecorded,
        evidence: <String, Object?>{
          'database_id': db,
          'previous_event_hash': previous,
        },
        aggregates: _aggregatesOf(successors),
      ),
    );
  }
  return found;
}

List<String> _aggregatesOf(Iterable<StoredEvent> events) =>
    events.map((e) => e.aggregateId).toSet().toList()..sort();
