// Implements: EVS-PRD-event-log/A — EventDraft is the caller-facing input
//   value type for the append-only log's write path; it carries all fields
//   required to produce a StoredEvent (aggregateId, aggregateType,
//   entryType, eventType, data) without the substrate-stamped fields
//   (entryTypeVersion, hash, sequence) that callers must not supply.

/// Input value for `appendWithSecurity`: the event to be persisted.
///
/// The dispatcher fills in `initiator`, `metadata['action_invocation_id']`,
/// and `metadata['action_name']` before persisting. `flowToken` is optional
/// input from the action; the dispatcher uses the call parameter as fallback.
//
// entryType, eventType, data, flowToken, metadata.
class EventDraft {
  const EventDraft({
    required this.aggregateId,
    required this.aggregateType,
    required this.entryType,
    required this.eventType,
    required this.data,
    this.flowToken,
    this.metadata,
  });

  final String aggregateId;
  final String aggregateType;
  final String entryType;
  final String eventType;
  final Map<String, Object?> data;
  final String? flowToken;
  final Map<String, Object?>? metadata;
}
