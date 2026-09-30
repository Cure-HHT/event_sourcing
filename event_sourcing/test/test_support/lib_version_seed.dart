// Test helper: seeds a library-version event as a build's open would have
// appended it, with the database identity stored beside it. This file
// declares no tests, so it carries no citation.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/lifecycle/lib_version.dart'
    show LibVersionEvents;

/// Appends one `lib_version_initialized` ([eventType] defaults to it) or
/// `lib_version_changed` event to [backend], as a build of package
/// [version] and data format [dataFormat] would have when it opened the
/// database. The event carries one originator provenance entry and no
/// receiver entry, so the boot reads it as locally appended. The entry
/// names [databaseId], or else the stored database identity
/// (`seeded-database` when none is stored or [recordDatabaseId] is false) and [version] as the library version that
/// stamped it, and the event is an eligible version with no parents.
///
/// An initialization records [databaseId] when given, or else the stored
/// database identity, which the helper mints when none is stored; so a
/// seeded initialization matches the stored identity unless [databaseId]
/// names another. [recordDatabaseId] false leaves the identity out of the
/// event and stores none; [recordDataFormat] false leaves the data format
/// out. A change records [fromVersion] and [fromDataFormat] as the versions
/// it changed from.
///
/// Returns the appended event.
Future<StoredEvent> seedLibVersionEventForTest(
  StorageBackend backend, {
  required String version,
  required DataFormatVersion dataFormat,
  String eventType = LibVersionEvents.initialized,
  String? databaseId,
  bool recordDatabaseId = true,
  bool recordDataFormat = true,
  String fromVersion = '0.0.1',
  DataFormatVersion fromDataFormat = LibVersion.dataFormat,
  DateTime? at,
}) {
  final when = (at ?? DateTime.utc(2026, 4)).toUtc();
  return backend.transaction((txn) async {
    final Map<String, Object?> data;
    final String stampingId;
    if (!recordDatabaseId) {
      stampingId = 'seeded-database';
    } else if (databaseId != null) {
      stampingId = databaseId;
    } else if (eventType == LibVersionEvents.initialized) {
      stampingId = await backend.readOrCreateDatabaseIdTxn(txn);
    } else {
      stampingId = await backend.readDatabaseIdTxn(txn) ?? 'seeded-database';
    }
    if (eventType == LibVersionEvents.initialized) {
      final id = !recordDatabaseId ? null : stampingId;
      data = <String, Object?>{
        'version': version,
        if (recordDataFormat) 'data_format': dataFormat.toJson(),
        'database_id': ?id,
        'initializedAt': when.toIso8601String(),
      };
    } else {
      data = <String, Object?>{
        'fromVersion': fromVersion,
        'toVersion': version,
        if (recordDataFormat) 'fromDataFormat': fromDataFormat.toJson(),
        if (recordDataFormat) 'toDataFormat': dataFormat.toJson(),
        'changedAt': when.toIso8601String(),
      };
    }
    final seq = await backend.nextSequenceNumber(txn);
    final previous = await backend.readLatestEventHash(txn);
    final event = StoredEvent.synthetic(
      eventId: 'seeded-$eventType-$seq',
      aggregateId: '_lib',
      aggregateType: '_lib',
      entryType: eventType,
      eventType: eventType,
      sequenceNumber: seq,
      eventHash: 'seeded-hash-$seq',
      previousEventHash: previous,
      initiator: const AutomationInitiator(service: 'event_sourcing'),
      clientTimestamp: when,
      data: Map<String, dynamic>.from(data),
      metadata: <String, dynamic>{
        'provenance': <Map<String, Object?>>[
          ProvenanceEntry(
            hop: 'event_sourcing',
            receivedAt: when,
            identifier: 'event_sourcing',
            softwareVersion: version,
            databaseId: stampingId,
            libraryVersion: version,
          ).toJson(),
        ],
      },
    );
    await backend.appendEvent(txn, event);
    return event;
  });
}
