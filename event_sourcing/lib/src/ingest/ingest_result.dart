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

/// Outcome of a single subject event's processing inside `ingestBatch`.
enum IngestOutcome {
  /// New event, stored with a fresh receiver provenance entry.
  ingested,

  /// Known event — identity matched, no mutation; a duplicate_received
  /// audit event was emitted separately.
  duplicate,
}

/// Per-event outcome from a single ingest call.
class PerEventIngestOutcome {
  const PerEventIngestOutcome({
    required this.eventId,
    required this.outcome,
    required this.resultHash,
  });

  final String eventId;
  final IngestOutcome outcome;

  /// The stored `event_hash` after processing: for `ingested`, this is the
  /// hash the receiver computed post-provenance-append; for `duplicate`,
  /// this is the stored copy's current `event_hash` (unchanged).
  final String resultHash;
}

/// Result of `ingestBatch`.
class IngestBatchResult {
  const IngestBatchResult({required this.batchId, required this.events});
  final String batchId;
  final List<PerEventIngestOutcome> events;
}
