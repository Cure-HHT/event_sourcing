// Implements: EVS-DEV-event-store-open/B+C+F
// the boot reads, inside its transaction, only the library-version events
//   this database appended itself; a peer's forwarded library-version event
//   is never read as this database's version or identity.
import 'package:event_sourcing/src/lifecycle/lib_version.dart';
import 'package:event_sourcing/src/lifecycle/local_event.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/stored_event.dart';
import 'package:event_sourcing/src/storage/transaction.dart';
import 'package:event_sourcing/src/versions.dart';

/// One locally appended library-version event, decoded.
class RecordedLibVersion {
  const RecordedLibVersion({
    required this.event,
    required this.packageVersion,
    required this.dataFormat,
    required this.databaseId,
  });

  /// Decodes [event], a `lib_version_initialized` or `lib_version_changed`
  /// event. A field the event does not carry decodes to null; a field it
  /// carries in another shape throws [StateError].
  factory RecordedLibVersion.fromEvent(StoredEvent event) {
    final initialized = event.eventType == LibVersionEvents.initialized;
    final version = event.data[initialized ? 'version' : 'toVersion'];
    final format = event.data[initialized ? 'data_format' : 'toDataFormat'];
    final databaseId = initialized ? event.data['database_id'] : null;
    if (version != null && version is! String) {
      throw StateError(
        'library-version event ${event.eventId} records a non-string '
        'package version; database corrupted',
      );
    }
    if (databaseId != null && (databaseId is! String || databaseId.isEmpty)) {
      throw StateError(
        'library-version event ${event.eventId} records a malformed '
        'database identity; database corrupted',
      );
    }
    DataFormatVersion? dataFormat;
    if (format != null) {
      try {
        dataFormat = DataFormatVersion.fromJson(format);
      } on FormatException catch (e) {
        throw StateError(
          'library-version event ${event.eventId} records a malformed data '
          'format (${e.message}); database corrupted',
        );
      }
    }
    return RecordedLibVersion(
      event: event,
      packageVersion: version as String?,
      dataFormat: dataFormat,
      databaseId: databaseId as String?,
    );
  }

  /// The event.
  final StoredEvent event;

  /// The package version the event records as the one that opened the
  /// database (`version`, or `toVersion` of a change), or null when it
  /// records none.
  final String? packageVersion;

  /// The data format the event records (`data_format`, or `toDataFormat` of
  /// a change), or null when it records none.
  final DataFormatVersion? dataFormat;

  /// The database identity a `lib_version_initialized` event records, or
  /// null (always null for a change).
  final String? databaseId;

  /// True for a `lib_version_initialized` event.
  bool get isInitialized => event.eventType == LibVersionEvents.initialized;
}

/// The library-version events a database appended itself, oldest first.
class LocalLibVersionHistory {
  const LocalLibVersionHistory(this.events);

  /// The events, in ascending sequence order.
  final List<RecordedLibVersion> events;

  /// The latest event, or null when there is none.
  RecordedLibVersion? get latest => events.isEmpty ? null : events.last;

  /// The earliest `lib_version_initialized` event, or null when there is
  /// none.
  RecordedLibVersion? get firstInitialized {
    for (final recorded in events) {
      if (recorded.isInitialized) return recorded;
    }
    return null;
  }
}

/// Reads the library-version history the boot of `EventStore.open`
/// decides on.
class VersionCheck {
  /// Reads, inside [txn], every `lib_version_initialized` and
  /// `lib_version_changed` event [backend]'s database appended itself
  /// (see [isLocallyAppended]); events it ingested from a peer are left
  /// out.
  static Future<LocalLibVersionHistory> readLocalInTxn(
    StorageBackend backend,
    Transaction txn,
  ) async {
    final newestFirst = <RecordedLibVersion>[];
    await for (final event in backend.readEventsReverseInTxn(
      txn,
      eventTypes: const <String>{
        LibVersionEvents.initialized,
        LibVersionEvents.changed,
      },
    )) {
      if (!isLocallyAppended(event)) continue;
      newestFirst.add(RecordedLibVersion.fromEvent(event));
    }
    return LocalLibVersionHistory(newestFirst.reversed.toList());
  }
}
