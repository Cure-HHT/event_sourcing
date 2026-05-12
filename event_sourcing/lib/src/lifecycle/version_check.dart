// Implements: EVS-DEV-event-store-open/B/C/D
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';

/// The result of a [VersionCheck.findMostRecent] scan.
class VersionCheckResult {
  const VersionCheckResult({
    required this.recordedVersion,
    required this.sequenceNumber,
    required this.eventType,
  });

  /// The version string extracted from the most recent version event.
  ///
  /// For [LibVersionEvents.initialized] events this is the `version` field;
  /// for [LibVersionEvents.changed] events this is the `toVersion` field.
  final String recordedVersion;

  /// The `sequence_number` of the event that produced this result.
  final int sequenceNumber;

  /// The `event_type` of the event — one of [LibVersionEvents.initialized]
  /// or [LibVersionEvents.changed].
  final String eventType;
}

/// Scans the event log for the most recent lib_version event.
///
/// Used by the boot flow to determine the version last seen by the lib,
/// so it can decide whether to emit a [LibVersionEvents.initialized] or
/// [LibVersionEvents.changed] event on this run.
class VersionCheck {
  /// Reverse-scans [backend] for the most recent [LibVersionEvents.initialized]
  /// or [LibVersionEvents.changed] event. Returns null when the log contains
  /// no version event (first boot under this library).
  ///
  /// Terminates on the first matching event found in descending
  /// `sequence_number` order; does not page through the entire log.
  static Future<VersionCheckResult?> findMostRecent(
    StorageBackend backend,
  ) async {
    final stream = backend.readEventsReverse(
      eventTypes: {LibVersionEvents.initialized, LibVersionEvents.changed},
    );
    await for (final event in stream) {
      final version = event.eventType == LibVersionEvents.initialized
          ? event.data['version'] as String
          : event.data['toVersion'] as String;
      return VersionCheckResult(
        recordedVersion: version,
        sequenceNumber: event.sequenceNumber,
        eventType: event.eventType,
      );
    }
    return null;
  }
}
