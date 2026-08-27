// Test-side helper for admitting a single event through the substrate's only
// ingest entry point.
//
// The substrate admits events one way: a wire envelope handed to
// `EventStore.ingestBatch`. Tests that need a single event admitted therefore
// wrap it in a one-event envelope. This helper exists so that arrangement is
// written once rather than in every fixture, and so a test's intent reads as
// "admit this event" rather than as envelope plumbing.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:uuid/uuid.dart';

/// Wrap [events] in a single `esd/batch@1` envelope attributed to the given
/// sender, and admit them through [EventStore.ingestBatch].
///
/// Returns the batch result so callers can assert per-event outcomes.
Future<IngestBatchResult> admitEvents(
  EventStore store,
  List<StoredEvent> events, {
  String senderHop = 'mobile-device',
  String senderIdentifier = 'device-1',
  String senderSoftwareVersion = 'my_app@1.0.0',
  String? batchId,
}) {
  final envelope = BatchEnvelope(
    batchFormatVersion: '1',
    batchId: batchId ?? const Uuid().v4(),
    senderHop: senderHop,
    senderIdentifier: senderIdentifier,
    senderSoftwareVersion: senderSoftwareVersion,
    sentAt: DateTime.now().toUtc(),
    events: [for (final e in events) Map<String, Object?>.from(e.toMap())],
  );
  return store.ingestBatch(
    envelope.encode(),
    wireFormat: BatchEnvelope.wireFormat,
  );
}

/// Admit exactly one [event] and return its per-event outcome.
///
/// The single-event counterpart to [admitEvents]; the batch wrapping is an
/// implementation detail of how the substrate admits anything at all.
Future<PerEventIngestOutcome> admitOne(
  EventStore store,
  StoredEvent event, {
  String senderHop = 'mobile-device',
  String senderIdentifier = 'device-1',
  String senderSoftwareVersion = 'my_app@1.0.0',
}) async {
  final result = await admitEvents(
    store,
    [event],
    senderHop: senderHop,
    senderIdentifier: senderIdentifier,
    senderSoftwareVersion: senderSoftwareVersion,
  );
  return result.events.single;
}
