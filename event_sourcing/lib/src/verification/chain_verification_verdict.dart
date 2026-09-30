import 'package:collection/collection.dart' show DeepCollectionEquality;
import 'package:event_sourcing/src/security/security_finding.dart';
import 'package:meta/meta.dart' show immutable;

/// One anomaly the chain verification found: its [kind] and the [evidence]
/// a security finding of that kind carries.
@immutable
final class ChainVerificationFinding {
  /// The finding of [kind] with [evidence], concerning [aggregates].
  const ChainVerificationFinding({
    required this.kind,
    required this.evidence,
    required this.aggregates,
  });

  /// The kind: `hash_mismatch`, `storage_link_break`, `sequence_missing`,
  /// `predecessor_break`, `fork_unrecorded`, `position_reused`,
  /// `parent_invalid` or `parents_not_stamped`.
  final FindingKind kind;

  /// Exactly the evidence a security finding of [kind] carries.
  final Map<String, Object?> evidence;

  /// The aggregates, in ascending order, of the held events the evidence
  /// names (for a fork or a reused position, of every held event of the
  /// named database carrying the named predecessor hash or sitting at the
  /// named origin position), as the verification read them; empty for a
  /// missing sequence number.
  final List<String> aggregates;

  /// The finding as `kind` and `evidence`.
  Map<String, Object?> toJson() => <String, Object?>{
    'kind': kind.wire,
    'evidence': evidence,
  };

  static const DeepCollectionEquality _deep = DeepCollectionEquality();

  @override
  bool operator ==(Object other) =>
      other is ChainVerificationFinding &&
      other.kind == kind &&
      _deep.equals(other.evidence, evidence) &&
      _deep.equals(other.aggregates, aggregates);

  @override
  int get hashCode =>
      Object.hash(kind, _deep.hash(evidence), _deep.hash(aggregates));

  @override
  String toString() => 'ChainVerificationFinding(${kind.wire}, $evidence)';
}

/// What the chain verification found over the local sequence numbers
/// [from] to [to], both inclusive.
@immutable
final class ChainVerificationVerdict {
  /// The verdict over [from] to [to] listing [findings], with
  /// [unresolvedPredecessors] predecessor hashes naming no held event.
  const ChainVerificationVerdict({
    required this.from,
    required this.to,
    required this.findings,
    required this.unresolvedPredecessors,
  });

  /// The first local sequence number of the range walked.
  final int from;

  /// The last local sequence number of the range walked: the requested
  /// upper bound, or the highest the database had stored when the
  /// verification started when that is lower or none was requested.
  final int to;

  /// Every finding, in the order of the local sequence numbers they were
  /// found at.
  final List<ChainVerificationFinding> findings;

  /// The number of events in the range whose `previous_event_hash` names
  /// no held event: the links of an origin chain the holder cannot follow.
  final int unresolvedPredecessors;

  /// True exactly when the verdict lists no finding.
  // Implements: EVS-DEV-chain-verification/I
  // the verdict is valid exactly when it lists no finding.
  bool get isValid => findings.isEmpty;

  static const DeepCollectionEquality _deep = DeepCollectionEquality();

  @override
  bool operator ==(Object other) =>
      other is ChainVerificationVerdict &&
      other.from == from &&
      other.to == to &&
      other.unresolvedPredecessors == unresolvedPredecessors &&
      _deep.equals(other.findings, findings);

  @override
  int get hashCode =>
      Object.hash(from, to, unresolvedPredecessors, _deep.hash(findings));

  @override
  String toString() =>
      'ChainVerificationVerdict($from..$to, findings: $findings, '
      'unresolvedPredecessors: $unresolvedPredecessors)';
}
