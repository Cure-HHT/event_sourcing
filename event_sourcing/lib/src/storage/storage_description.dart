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
// Implements: EVS-PRD-storage-barrier/L
// a key that does not decrypt the database, or no key for an encrypted one,
//   is refused before sembast writes anything.
// Implements: EVS-DEV-sembast-encryption/E+K
// the codec-signature refusal surfaces as StorageKeyMismatchError, and a
//   location held open in this isolate is shared only under the same
//   encryption setting.
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/storage/sembast_backend.dart';
import 'package:event_sourcing/src/storage/sembast_factory.dart';
import 'package:event_sourcing/src/storage/sembast_file_io.dart'
    if (dart.library.js_interop) 'package:event_sourcing/src/storage/sembast_file_web.dart';
import 'package:event_sourcing/src/storage/storage_backend.dart';
import 'package:event_sourcing/src/storage/storage_encryption.dart';
import 'package:meta/meta.dart' show internal;
import 'package:sembast/sembast.dart' show Database, DatabaseException;

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
///
/// A file description may carry an [encryptionKey]. The library then stores
/// every line of the database file encrypted with AES-256-GCM under it, and
/// an existing unencrypted database at the path is converted to an
/// encrypted one holding the same records the first time it is opened with
/// a key. A key that does not decrypt the database, or no key for an
/// encrypted one, is refused with [StorageKeyMismatchError], leaving the
/// file unchanged. A description without a key stores the database
/// unencrypted.
final class SembastStorage extends StorageDescription {
  /// The database file at [path], on the native runtimes, stored encrypted
  /// under [encryptionKey] when one is given.
  const SembastStorage.file(
    String path, {
    this.bootLockWait = const Duration(seconds: 60),
    this.encryptionKey,
  }) : location = path,
       _kind = SembastLocationKind.file;

  /// The browser database named [name], on the web.
  const SembastStorage.browser(
    String name, {
    this.bootLockWait = const Duration(seconds: 60),
  }) : location = name,
       encryptionKey = null,
       _kind = SembastLocationKind.browser;

  /// The in-memory database named [name] of the calling isolate. It keeps
  /// its data across closes and reopens in the isolate until it is deleted
  /// ([deleteSembastDatabase]).
  const SembastStorage.memory(
    String name, {
    this.bootLockWait = const Duration(seconds: 60),
  }) : location = name,
       encryptionKey = null,
       _kind = SembastLocationKind.memory;

  /// Whether the Sembast database file at [path] is absent, unencrypted or
  /// encrypted, read from the first line of the file. It writes nothing,
  /// and needs no key.
  ///
  /// A missing file, or one with no line, is [SembastFileEncryption.absent].
  /// When the file is missing and the file sembast compacts it into is
  /// present, that file is the database an open would use, and it is the
  /// one read. Throws [FormatException] when the first line is not the
  /// metadata line of a Sembast database, and [UnsupportedError] in the
  /// browser, which has no database files.
  // Implements: EVS-DEV-sembast-encryption/J
  // reads the first line only and writes nothing.
  static Future<SembastFileEncryption> inspectFile(String path) =>
      inspectSembastFile(path);

  /// The file path, browser database name or in-memory database name.
  final String location;

  /// On the web, how long `EventStore.open` waits for another tab's boot of
  /// the same database (see `SembastBackend`). Outside the browser it has no
  /// effect.
  final Duration bootLockWait;

  /// The key a file description stores its database under; null for an
  /// unencrypted database, and always null for a browser or in-memory
  /// description.
  final StorageEncryptionKey? encryptionKey;

  final SembastLocationKind _kind;

  @override
  bool operator ==(Object other) =>
      other is SembastStorage &&
      other._kind == _kind &&
      other.location == location &&
      other.bootLockWait == bootLockWait &&
      other.encryptionKey == encryptionKey;

  @override
  int get hashCode => Object.hash(_kind, location, bootLockWait, encryptionKey);

  @override
  String toString() => encryptionKey == null
      ? 'SembastStorage.${_kind.name}($location)'
      : 'SembastStorage.${_kind.name}($location, encrypted)';
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
      // concurrent open of it in this isolate shares this open, when it
      // asks for the same encryption.
      final held = shareHeldSembastLocation(kind, location) as _SembastOpening?;
      final Future<SembastBackend> opening;
      if (held == null) {
        opening = _openSembast(description);
        holdSembastLocation(
          kind,
          location,
          _SembastOpening(opening, description.encryptionKey),
        );
      } else {
        _checkSameEncryption(held.key, description);
        opening = held.backend;
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

/// The open of a Sembast location an event store of this isolate holds,
/// with the key it was opened under.
final class _SembastOpening {
  _SembastOpening(this.backend, this.key);
  final Future<SembastBackend> backend;
  final StorageEncryptionKey? key;
}

/// Refuses a second open of a location this isolate holds open under
/// [heldKey] when [description] asks for another encryption: a key that
/// does not decrypt the open database, or no key for an encrypted one,
/// with [StorageKeyMismatchError]; a key for a database held open
/// unencrypted with [StateError], since converting it needs every event
/// store over it closed. The caller was counted as a holder, and is
/// released before the refusal.
void _checkSameEncryption(
  StorageEncryptionKey? heldKey,
  SembastStorage description,
) {
  final key = description.encryptionKey;
  if (heldKey == null && key == null) return;
  if (heldKey != null && heldKey.sameAs(key)) return;
  releaseSembastLocation(description._kind, description.location);
  if (heldKey == null) {
    throw StateError(
      'the Sembast database ${description.location} is held open '
      'unencrypted by an event store of this isolate; close every event '
      'store over it before opening it with an encryption key',
    );
  }
  throw StorageKeyMismatchError(description.location, keyGiven: key != null);
}

Future<SembastBackend> _openSembast(SembastStorage storage) async {
  final key = storage.encryptionKey;
  if (key != null) {
    await prepareEncryptedSembastFile(storage.location, key);
  }
  final factory = sembastFactoryFor(storage._kind);
  final Database db;
  try {
    db = await factory.openDatabase(
      storage.location,
      codec: key == null ? null : encryptingSembastCodec(key),
    );
  } on DatabaseException catch (error) {
    // sembast compares the codec signature recorded on the database's
    // first line with the codec it is given before it writes anything,
    // and refuses a mismatch as an invalid codec.
    if (error.code != DatabaseException.errInvalidCodec) rethrow;
    throw StorageKeyMismatchError(storage.location, keyGiven: key != null);
  }
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
