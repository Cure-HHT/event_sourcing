// Implements: EVS-DEV-event-store-open/F
// an event is locally appended when it carries no receiver provenance entry.
import 'package:event_sourcing/src/storage/stored_event.dart';

/// True when [event] was appended by the database that stores it, false
/// when the database received it through ingest.
///
/// Ingest adds a receiver provenance entry, carrying the `arrival_hash` of
/// the event as it arrived, to every event it stores; an event the database
/// appended itself carries none. The property is carried by the stored
/// event, not by a timestamp or a sequence position, and a peer cannot
/// forge it away: whatever provenance a forwarded event carries, the
/// receiver adds its own receiver entry.
bool isLocallyAppended(StoredEvent event) {
  final raw = event.metadata['provenance'];
  if (raw is! List) return true;
  for (final entry in raw) {
    if (entry is Map && entry['arrival_hash'] != null) return false;
  }
  return true;
}
