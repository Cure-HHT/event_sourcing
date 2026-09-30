// Test support: the backend a test opened an event store over. The event
// store hands out no backend, so a test that stages or inspects stored
// state the library's operations do not expose records the backend it built
// beside the store it opened over it. This file declares no tests, so it
// carries no citation.
import 'package:event_sourcing/event_sourcing.dart';

final Expando<StorageBackend> _backends = Expando<StorageBackend>(
  'the backend a test opened an event store over',
);

/// Records [backend] as the backend [store] was opened over, and returns
/// [store].
EventStore trackTestBackend(EventStore store, StorageBackend backend) {
  _backends[store] = backend;
  return store;
}

/// The backend [store] was opened over, as [trackTestBackend] recorded it.
/// Throws [StateError] when the test did not record one.
StorageBackend testBackendOf(EventStore store) {
  final backend = _backends[store];
  if (backend == null) {
    throw StateError(
      'no backend recorded for this event store: record the backend the '
      'test opened it over with trackTestBackend',
    );
  }
  return backend;
}
