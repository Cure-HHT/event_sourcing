// Implements: EVS-DEV-storage-capability/B
// the library selects the Sembast factory itself: the file factory where
//   dart:io exists, the browser factory under the browser's interop, and the
//   in-memory factory for an in-memory description.
// Implements: EVS-DEV-storage-capability/L
// the isolate-local registry of the Sembast locations an event store of
//   this isolate holds open, which the delete operation consults.
import 'package:event_sourcing/src/storage/sembast_factory_io.dart'
    if (dart.library.js_interop) 'package:event_sourcing/src/storage/sembast_factory_web.dart';
import 'package:meta/meta.dart' show internal;
import 'package:sembast/sembast.dart' show DatabaseFactory;
import 'package:sembast/sembast_memory.dart' show databaseFactoryMemory;

/// Where a Sembast database a description names lives.
@internal
enum SembastLocationKind {
  /// A database file on a native runtime.
  file,

  /// A browser (IndexedDB) database.
  browser,

  /// An in-memory database of this isolate.
  memory,
}

/// The factory the library opens and deletes a database of [kind] with.
@internal
DatabaseFactory sembastFactoryFor(SembastLocationKind kind) => switch (kind) {
  SembastLocationKind.file => platformFileFactory(),
  SembastLocationKind.browser => platformBrowserFactory(),
  SembastLocationKind.memory => databaseFactoryMemory,
};

/// One Sembast location an event store of this isolate holds open: the
/// value the first opener stored (the open of the library's backend over
/// the database) and the number of event stores holding it. A second open of a held
/// location receives the same value, as the factory would hand it the same
/// database handle.
final class _Held {
  _Held(this.value);
  final Object value;
  int holders = 1;
}

/// The Sembast locations event stores of this isolate hold open. A
/// top-level variable is per isolate in Dart.
final Map<(SembastLocationKind, String), _Held> _openLocations =
    <(SembastLocationKind, String), _Held>{};

/// The value stored for [location] when an event store of this isolate
/// holds it open, counting the caller as one more holder; null when none
/// does.
@internal
Object? shareHeldSembastLocation(SembastLocationKind kind, String location) {
  final held = _openLocations[(kind, location)];
  if (held == null) return null;
  held.holders += 1;
  return held.value;
}

/// Records that an event store of this isolate holds [location] open
/// through [value].
@internal
void holdSembastLocation(
  SembastLocationKind kind,
  String location,
  Object value,
) {
  _openLocations[(kind, location)] = _Held(value);
}

/// Records that one holder of [location] closed; returns true when it was
/// the last, so the location is no longer held.
@internal
bool releaseSembastLocation(SembastLocationKind kind, String location) {
  final key = (kind, location);
  final held = _openLocations[key];
  if (held == null) return true;
  held.holders -= 1;
  if (held.holders > 0) return false;
  _openLocations.remove(key);
  return true;
}

/// Whether the caller is the only holder of [location] left.
@internal
bool isLastSembastHolder(SembastLocationKind kind, String location) =>
    (_openLocations[(kind, location)]?.holders ?? 0) <= 1;

/// Whether an event store of this isolate holds [location] open.
@internal
bool isSembastLocationHeld(SembastLocationKind kind, String location) =>
    _openLocations.containsKey((kind, location));
