// Implements: EVS-PRD-ingest/F
// idempotency: IngestOutcome.duplicate
//   distinguishes a safe re-presentation from a new admission; the stored
//   subject is not mutated on duplicate
// Implements: EVS-PRD-ingest/A
// ingest path result types (IngestBatchResult
//   is the return value of EventStore.ingestBatch)
// Implements: EVS-PRD-ingest/C
// PerEventIngestOutcome.resultHash carries the
//   hash of the event as stored after the receiver's provenance hop was appended,
//   enabling callers to thread Chain 2 linkage checks

/// Outcome of a single received record's processing inside `ingestBatch`
/// or `ingestEvent`.
enum IngestOutcome {
  /// New event, stored with a fresh receiver provenance entry.
  ingested,

  /// New event, stored as received with a fresh receiver provenance entry,
  /// with the security findings [PerEventIngestOutcome.findingIds] names
  /// recorded about it.
  ingestedWithFinding,

  /// Known event: held under the sealed hash it arrived with, so nothing is
  /// stored for it; a duplicate_received audit event was emitted
  /// separately. [PerEventIngestOutcome.findingIds] names any finding
  /// recorded about it.
  duplicate,

  /// A record the library does not store as an event: no event is stored
  /// for it, and the security finding [PerEventIngestOutcome.findingIds]
  /// names keeps it in full.
  keptInFinding,
}

/// Per-record outcome from a single ingest call.
class PerEventIngestOutcome {
  const PerEventIngestOutcome({
    required this.eventId,
    required this.outcome,
    required this.resultHash,
    this.findingIds = const <String>[],
  });

  /// The record's `event_id`, or null when the record carries none as a
  /// string.
  final String? eventId;
  final IngestOutcome outcome;

  /// The stored `event_hash` after processing: for `ingested` and
  /// `ingestedWithFinding`, this is the hash the receiver computed
  /// post-provenance-append; for `duplicate`, this is the stored copy's
  /// current `event_hash` (unchanged); null for `keptInFinding`, which
  /// stores no event.
  final String? resultHash;

  /// The identities of the security findings ingest recorded about this
  /// record, or found already held with the same identity, in the order it
  /// met them; empty when it met no anomaly.
  final List<String> findingIds;
}

/// Result of `ingestBatch`.
class IngestBatchResult {
  const IngestBatchResult({required this.batchId, required this.events});
  final String batchId;
  final List<PerEventIngestOutcome> events;
}
