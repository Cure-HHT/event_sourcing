// Implements: EVS-PRD-hash-chain-integrity/C — operation by which any holder
//   can verify the chain without privileged access; ChainVerdict is the
//   non-throwing result type returned by verifyEventChain / verifyIngestChain
// Implements: EVS-PRD-ingest/D — chain verification at ingest boundary;
//   ChainFailure captures per-hop failure details surfaced to callers

/// Reason a single chain link failed verification.
enum ChainFailureKind {
  /// `provenance[k].arrival_hash` did not equal the recomputed hash at hop k.
  arrivalHashMismatch,

  /// `provenance[thisHop].previous_ingest_hash` did not equal the stored
  /// `event_hash` of the prior event in Chain 2.
  previousIngestHashMismatch,

  /// An expected provenance entry was missing (e.g., empty provenance on a
  /// non-origin event).
  provenanceMissing,
}

/// A single broken link encountered during a chain walk.
class ChainFailure {
  const ChainFailure({
    required this.position,
    required this.kind,
    required this.expectedHash,
    required this.actualHash,
  });

  /// For Chain 1: the `provenance[]` index of the failing hop.
  /// For Chain 2: the `ingest_sequence_number` of the failing event.
  final int position;
  final ChainFailureKind kind;
  final String expectedHash;
  final String actualHash;
}

/// Non-throwing verdict returned by `verifyEventChain` / `verifyIngestChain`.
class ChainVerdict {
  const ChainVerdict({required this.isValid, required this.failures});
  final bool isValid;
  final List<ChainFailure> failures;

  static const ChainVerdict valid = ChainVerdict(
    isValid: true,
    failures: <ChainFailure>[],
  );
}
