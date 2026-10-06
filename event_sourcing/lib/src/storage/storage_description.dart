// Implements: EVS-DEV-storage-capability/A
// EventStore.open takes a storage description for the backends the library
//   ships: a Sembast description naming a file path, a browser database
//   name or an in-memory database name, and a Postgres description, which
//   package:event_sourcing/postgres.dart declares as a companion-backend
//   description.
// Implements: EVS-DEV-storage-capability/J
// a backend instance reaches EventStore.open only inside the description
//   that names it application-supplied.
// Implements: EVS-PRD-storage-barrier/A
// the library opens the storage of every database it runs on a backend it
//   ships from the description the application supplies.
// Implements: EVS-PRD-storage-barrier/G
// a backend the application constructed is accepted only through the
//   description that names it application-supplied.
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/sembast_factory.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:meta/meta.dart' show internal;

/// Where and how an event store's storage lives: the input of
/// `EventStore.open` and `bootstrapEventStore`.
///
/// For the backends the library ships, the description is data -- a
/// location, or connection settings -- and the library opens the storage
/// itself, holds it, and closes it when the event store closes (and when an
/// open fails after it opened it). A backend the application constructs
/// enters only through [ApplicationSuppliedStorage], which the application
/// keeps and closes.
sealed class StorageDescription {
  const StorageDescription();
}

/// A Sembast database the library opens: a file on the native runtimes, a
/// browser (IndexedDB) database on the web, or an in-memory database of the
/// calling isolate. The library selects the factory itself; the description
/// carries no code.
final class SembastStorage extends StorageDescription {
  /// The database file at [path], on the native runtimes.
  const SembastStorage.file(
    String path, {
    this.bootLockWait = const Duration(seconds: 60),
  }) : location = path,
       _kind = SembastLocationKind.file;

  /// The browser database named [name], on the web.
  const SembastStorage.browser(
    String name, {
    this.bootLockWait = const Duration(seconds: 60),
  }) : location = name,
       _kind = SembastLocationKind.browser;

  /// The in-memory database named [name] of the calling isolate. It keeps
  /// its data across closes and reopens in the isolate until it is deleted
  /// ([deleteSembastDatabase]).
  const SembastStorage.memory(
    String name, {
    this.bootLockWait = const Duration(seconds: 60),
  }) : location = name,
       _kind = SembastLocationKind.memory;

  /// The file path, browser database name or in-memory database name.
  final String location;

  /// On the web, how long `EventStore.open` waits for another tab's boot of
  /// the same database (see `SembastBackend`). Outside the browser it has no
  /// effect.
  final Duration bootLockWait;

  final SembastLocationKind _kind;

  @override
  bool operator ==(Object other) =>
      other is SembastStorage &&
      other._kind == _kind &&
      other.location == location &&
      other.bootLockWait == bootLockWait;

  @override
  int get hashCode => Object.hash(_kind, location, bootLockWait);

  @override
  String toString() => 'SembastStorage.${_kind.name}($location)';
}

/// Storage on a backend the library ships behind a public library of its
/// own, which declares the description: the Postgres backend, through
/// `package:event_sourcing/postgres.dart`. The library opens the storage
/// from the description, holds it, and closes it when the event store
/// closes.
///
/// This class exists because the Postgres backend lives in its own library:
/// the Postgres driver does not compile for the web, and the main library
/// must load on every runtime. It is the one storage description that a
/// library other than this one can extend, so the family is closed by
/// `@internal` here rather than by the language: the analyzer reports a
/// subclass or a call outside the package, and nothing refuses one at run
/// time. When the driver compiles with dart2js, the Postgres backend can
/// return to the main library, and this class can then be removed, leaving
/// every storage description declared in this library.
@internal
abstract base class CompanionBackendStorage extends StorageDescription {
  @internal
  const CompanionBackendStorage();

  /// Opens the backend this description names and builds the
  /// security-context store over it. `EventStore.open` calls it and holds
  /// what it returns.
  @internal
  Future<(StorageBackend, MutableSecurityContextStore)> openBackend();
}

/// A storage backend the application constructed, with the security-context
/// store over it. The application holds the backend and closes it: the event
/// store over it does not, and the storage barrier does not cover it.
final class ApplicationSuppliedStorage extends StorageDescription {
  const ApplicationSuppliedStorage(this.backend, this.securityContexts);

  /// The application's backend.
  final StorageBackend backend;

  /// The security-context store over [backend].
  final MutableSecurityContextStore securityContexts;
}

/// The storage an event store runs on, as `EventStore.open` opened it from
/// a description: the backend, the security-context store over it, and
/// whether the library opened it (and so closes it).
@internal
final class OpenedStorage {
  OpenedStorage._(
    this.backend,
    this.securityContexts, {
    required bool owned,
    SembastStorage? heldLocation,
  }) : _owned = owned,
       _heldLocation = heldLocation;

  final StorageBackend backend;
  final MutableSecurityContextStore securityContexts;
  final bool _owned;
  final SembastStorage? _heldLocation;
  bool _closed = false;

  /// Whether the library opened this storage.
  bool get owned => _owned;

  /// Closes the storage when the library opened it, then releases its
  /// location; storage the application supplied is left open. Calling it
  /// again does nothing more.
  // Implements: EVS-PRD-storage-barrier/H
  // the storage the library opened is closed with the event store over it;
  //   storage the application supplied is not.
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    if (!_owned) return;
    final held = _heldLocation;
    if (held == null) {
      await backend.close();
      return;
    }
    // The database handle is shared by every event store of this isolate
    // that holds the location; the last one to close it closes it, and the
    // location stays held until that close has completed.
    if (!isLastSembastHolder(held._kind, held.location)) {
      releaseSembastLocation(held._kind, held.location);
      return;
    }
    try {
      await backend.close();
    } finally {
      releaseSembastLocation(held._kind, held.location);
    }
  }
}

/// Opens the storage [description] names. For a Sembast description the
/// library opens the database with the factory it selects and records the
/// location as held by this isolate; a companion-backend description (the
/// Postgres one) opens its backend itself; in both cases the matching
/// security-context store is built over the backend. An
/// application-supplied description is returned as it is, not owned.
@internal
Future<OpenedStorage> openDescribedStorage(
  StorageDescription description,
) async {
  switch (description) {
    case SembastStorage(:final location, _kind: final kind):
      // The location is recorded as held before the database opens, so a
      // concurrent open of it in this isolate shares this open.
      var opening =
          shareHeldSembastLocation(kind, location) as Future<SembastBackend>?;
      if (opening == null) {
        opening = _openSembast(description);
        holdSembastLocation(kind, location, opening);
      }
      final SembastBackend backend;
      try {
        backend = await opening;
      } catch (_) {
        releaseSembastLocation(kind, location);
        rethrow;
      }
      return OpenedStorage._(
        backend,
        SembastSecurityContextStore(backend: backend),
        owned: true,
        heldLocation: description,
      );
    case CompanionBackendStorage():
      final (backend, securityContexts) = await description.openBackend();
      return OpenedStorage._(backend, securityContexts, owned: true);
    case ApplicationSuppliedStorage():
      return OpenedStorage._(
        description.backend,
        description.securityContexts,
        owned: false,
      );
  }
}

Future<SembastBackend> _openSembast(SembastStorage storage) async {
  final db = await sembastFactoryFor(
    storage._kind,
  ).openDatabase(storage.location);
  return SembastBackend(database: db, bootLockWait: storage.bootLockWait);
}

/// Deletes the Sembast database [storage] names, through the factory the
/// library opens it with. A database an earlier data format wrote is
/// refused at open as one that must be reset; this is how the application
/// resets it. Deleting a database that does not exist does nothing.
///
/// Throws [StateError], deleting nothing, while an event store of the
/// calling isolate holds the database open: close it first. An event store
/// whose open failed has already closed its storage.
// Implements: EVS-DEV-storage-capability/L
// the library deletes the Sembast database a description names, and
//   refuses with StateError while an event store of the calling isolate
//   holds it open.
Future<void> deleteSembastDatabase(SembastStorage storage) async {
  if (isSembastLocationHeld(storage._kind, storage.location)) {
    throw StateError(
      'the Sembast database $storage is held open by an event store of this '
      'isolate; close the event store before deleting its database',
    );
  }
  await sembastFactoryFor(storage._kind).deleteDatabase(storage.location);
}
