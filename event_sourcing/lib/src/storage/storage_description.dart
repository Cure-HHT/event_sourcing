// Implements: EVS-DEV-storage-capability/A
// EventStore.open takes a storage description for the backends the library
//   ships: a Sembast description naming a file path, a browser database
//   name or an in-memory database name, and a Postgres description carrying
//   the schema and the connection, lock-session and wait settings.
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
import 'package:event_sourcing/src/storage/postgres/postgres_backend.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/sembast_factory.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:meta/meta.dart' show internal;
import 'package:postgres/postgres.dart' show SslMode;

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

/// A Postgres database the library opens with `PostgresBackend.open`: the
/// library's tables live in [schema], the pool connects to [url], and the
/// lock session to [lockUrl] (or [url]). Provisioning is a separate
/// deployment step run as the owner (`PostgresBackend.provision`).
final class PostgresStorage extends StorageDescription {
  const PostgresStorage({
    required this.url,
    required this.schema,
    this.lockUrl,
    this.sslMode = SslMode.require,
    this.lockQueryTimeout = const Duration(seconds: 5),
    this.lockHeartbeat = const Duration(seconds: 5),
    this.bootLockWait = const Duration(seconds: 60),
  });

  /// The pool's connection URL, as a runtime role the deployment declared.
  final String url;

  /// The schema that holds the library's tables.
  final String schema;

  /// The lock session's connection URL, as a lock role the deployment
  /// declared; the pool's URL when null.
  final String? lockUrl;

  /// The TLS mode of every connection.
  final SslMode sslMode;

  /// Bounds every statement on the lock session and its connect.
  final Duration lockQueryTimeout;

  /// How often the idle lock session is probed.
  final Duration lockHeartbeat;

  /// Bounds each wait of `EventStore.open` for a boot lock.
  final Duration bootLockWait;

  @override
  String toString() => 'PostgresStorage(schema: $schema)';
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
/// location as held by this isolate; for a Postgres description it opens a
/// `PostgresBackend`; in both cases it builds the matching security-context
/// store. An application-supplied description is returned as it is, not
/// owned.
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
    case PostgresStorage():
      final backend = await PostgresBackend.open(
        url: description.url,
        schema: description.schema,
        lockUrl: description.lockUrl,
        sslMode: description.sslMode,
        lockQueryTimeout: description.lockQueryTimeout,
        lockHeartbeat: description.lockHeartbeat,
        bootLockWait: description.bootLockWait,
      );
      return OpenedStorage._(
        backend,
        PostgresSecurityContextStore(backend: backend),
        owned: true,
      );
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
