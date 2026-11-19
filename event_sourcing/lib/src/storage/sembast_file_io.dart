// The file operations of an encrypted Sembast database on the native
// runtimes: reading whether a database file is encrypted, and converting an
// unencrypted one. Compiled only where dart:io exists; the counterpart of
// sembast_file_web.dart, selected by a conditional import in
// storage_description.dart.
// Implements: EVS-PRD-storage-barrier/M
// an unencrypted database named with a key is converted to an encrypted one
//   holding the same records.
// Implements: EVS-DEV-sembast-encryption/F+G+H+I+J+M+N+O
// the copy, read-back, flush and one-rename replacement, so the file holds
//   one complete database at every point; the discarding of a copy an
//   interrupted conversion left; the completion of an interrupted
//   compaction; the first-line inspection that writes nothing; the
//   process-exclusive lock the whole step runs under; and the deletion of
//   every file the conversion leaves beside the database.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:collection/collection.dart' show DeepCollectionEquality;
import 'package:event_sourcing/src/storage/storage_encryption.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:path/path.dart' as p;
import 'package:sembast/sembast.dart' show SembastCodec;
import 'package:sembast/sembast_io.dart' show databaseFactoryIo;

/// The file sembast writes a compacted database to before it replaces the
/// database with it. sembast opens it in place of a missing database file.
String _compactionPath(String path) =>
    p.join(p.dirname(path), '~${p.basename(path)}');

/// The file a conversion writes the encrypted copy of [path] to.
@internal
String sembastConversionCopyPath(String path) => '$path.encrypting';

/// The file whose exclusive lock a process holds while it prepares the
/// database at [path] for an encrypted open.
@internal
String sembastConversionLockPath(String path) => '$path.encryption-lock';

/// Whether the Sembast database at [path] is absent, unencrypted or
/// encrypted, read from the first line of its file. Writes nothing.
///
/// When the file is missing and the file sembast compacts it into is
/// present, that file is the database sembast would open, and it is the
/// one read. A file with no line is reported absent. Throws
/// [FormatException] when the first line is not the metadata line of a
/// Sembast database.
@internal
Future<SembastFileEncryption> inspectSembastFile(String path) async {
  var file = File(path);
  if (!file.existsSync()) {
    final compacted = File(_compactionPath(path));
    if (!compacted.existsSync()) return SembastFileEncryption.absent;
    file = compacted;
  }
  final String? first;
  try {
    first = await utf8.decoder
        .bind(file.openRead())
        .transform(const LineSplitter())
        .cast<String?>()
        .firstWhere((_) => true, orElse: () => null);
  } on FileSystemException {
    // Removed between the existence check and the read.
    return SembastFileEncryption.absent;
  }
  if (first == null || first.isEmpty) return SembastFileEncryption.absent;
  final Object? meta;
  try {
    meta = json.decode(first);
  } on FormatException {
    throw FormatException(
      'the first line of $path is not the metadata line of a Sembast '
      'database',
    );
  }
  if (meta is! Map || meta['version'] == null) {
    throw FormatException(
      'the first line of $path is not the metadata line of a Sembast '
      'database',
    );
  }
  return meta['codec'] == null
      ? SembastFileEncryption.unencrypted
      : SembastFileEncryption.encrypted;
}

/// Prepares the database file at [path] to be opened under [key].
///
/// It first completes what an interrupted run left: a compaction sembast
/// left with the database file missing is moved into place, as sembast's
/// own open would, and an encrypted copy an interrupted conversion left is
/// deleted, since the database file is then still the unencrypted one, or
/// the copy already replaced it. Then, when the database is unencrypted, it
/// converts it: it opens it as an open without a key would, which repairs a
/// last line a crash cut short, writes an encrypted copy of the file line
/// by line, reads the copy back from its file under [key] and compares it
/// with the database line by line, flushes it to the device, and replaces the database file with
/// it by one rename, which also removes the unencrypted file. Until that
/// rename the file holds the unencrypted database, as sembast keeps it; from
/// it on, the complete encrypted copy. A crash at any point therefore
/// leaves one of the two, and the next call finishes or redoes the
/// conversion.
///
/// An absent or encrypted database is left for the open, which creates an
/// encrypted one, or refuses a key that does not decrypt it.
///
/// It runs holding an exclusive operating-system lock on the file
/// [sembastConversionLockPath] names, waiting for it, so the same step in
/// another process runs before or after it, never beside it. The lock is
/// the process's: two isolates of one process are not excluded by it.
@internal
Future<void> prepareEncryptedSembastFile(
  String path,
  StorageEncryptionKey key,
) async {
  final lock = await File(
    sembastConversionLockPath(path),
  ).open(mode: FileMode.append);
  try {
    await lock.lock(FileLock.blockingExclusive);
    await _prepareLocked(path, key);
  } finally {
    // Closing the handle releases the lock.
    await lock.close();
  }
}

Future<void> _prepareLocked(String path, StorageEncryptionKey key) async {
  final file = File(path);
  final compacted = File(_compactionPath(path));
  if (!file.existsSync() && compacted.existsSync()) {
    await compacted.rename(path);
  }
  final copyPath = sembastConversionCopyPath(path);
  await _deleteIfPresent(File(copyPath));
  await _deleteIfPresent(File(_compactionPath(copyPath)));

  if (await inspectSembastFile(path) != SembastFileEncryption.unencrypted) {
    return;
  }

  // A compaction file beside a present database file is one sembast had
  // not yet moved into place when it stopped. It holds the unencrypted
  // records, and sembast deletes it before its next compaction; the
  // conversion deletes it now, so no unencrypted copy outlives the
  // conversion.
  await _deleteIfPresent(compacted);

  final codec = encryptingSembastCodec(key);
  // Opened and closed as an open without a key opens it, so the conversion
  // reads what that open would: a last line a crash cut short is dropped
  // and the file compacted without it, as sembast repairs it there.
  await (await databaseFactoryIo.openDatabase(path)).close();

  await _writeCopy(path, copyPath, codec);
  _failAt(SembastConversionPoint.copyWritten);
  if (!await _copyMatches(path, copyPath, codec)) {
    await _deleteIfPresent(File(copyPath));
    throw StateError(
      'the encrypted copy of the Sembast database at $path does not hold '
      'the lines of the database; the database was left unencrypted',
    );
  }
  final handle = await File(copyPath).open(mode: FileMode.append);
  try {
    await handle.flush();
  } finally {
    await handle.close();
  }
  _failAt(SembastConversionPoint.copyVerified);

  await File(copyPath).rename(path);
  _failAt(SembastConversionPoint.replaced);
}

/// Deletes what an encrypted open leaves beside the database file at
/// [path]: the encrypted copy of a conversion, the files sembast compacts
/// the database and the copy into, and the conversion's lock file.
@internal
Future<void> deleteSembastFileCompanions(String path) async {
  final copyPath = sembastConversionCopyPath(path);
  for (final companion in <String>[
    copyPath,
    _compactionPath(copyPath),
    _compactionPath(path),
    sembastConversionLockPath(path),
  ]) {
    await _deleteIfPresent(File(companion));
  }
}

/// The lines of the database file at [path], as sembast reads them.
Stream<String> _lines(String path) => utf8.decoder
    .bind(File(path).openRead())
    .transform(const LineSplitter())
    .where((line) => line.isNotEmpty);

/// Whether [value], a decoded line of a database file, is a metadata line:
/// sembast reads a line with a record key as a record and any other line
/// with a version as metadata.
bool _isMeta(Object? value) =>
    value is Map && value['key'] == null && value['version'] != null;

/// Writes the database file at [path] to [copyPath] line by line, each
/// record line encoded by [codec] and each metadata line carrying [codec]'s
/// encoded signature: the file sembast writes for the same records under
/// [codec]. One line is held in memory at a time.
Future<void> _writeCopy(
  String path,
  String copyPath,
  SembastCodec codec,
) async {
  final lineCodec = codec.codec!;
  final signature = lineCodec.encode(<String, Object?>{
    'signature': codec.signature,
  });
  final sink = File(copyPath).openWrite();
  try {
    await for (final line in _lines(path)) {
      final value = json.decode(line);
      if (_isMeta(value)) {
        sink.writeln(
          json.encode(<String, Object?>{
            ...(value as Map).cast<String, Object?>(),
            'codec': signature,
          }),
        );
      } else {
        sink.writeln(lineCodec.encode(value));
      }
    }
  } finally {
    await sink.close();
  }
}

/// Whether the copy at [copyPath], read back from its file under [codec],
/// holds line for line what the database file at [path] holds.
Future<bool> _copyMatches(
  String path,
  String copyPath,
  SembastCodec codec,
) async {
  const equality = DeepCollectionEquality();
  final lineCodec = codec.codec!;
  final source = StreamIterator<String>(_lines(path));
  final copy = StreamIterator<String>(_lines(copyPath));
  try {
    while (await source.moveNext()) {
      if (!await copy.moveNext()) return false;
      final expected = json.decode(source.current);
      final Object? actual;
      try {
        if (_isMeta(expected)) {
          final meta = (json.decode(copy.current) as Map)
              .cast<String, Object?>();
          final signature = lineCodec.decode(meta.remove('codec')! as String);
          if (!equality.equals(signature, <String, Object?>{
            'signature': codec.signature,
          })) {
            return false;
          }
          actual = meta;
        } else {
          actual = lineCodec.decode(copy.current);
        }
      } on FormatException {
        return false;
      }
      if (!equality.equals(expected, actual)) return false;
    }
    return !await copy.moveNext();
  } finally {
    await source.cancel();
    await copy.cancel();
  }
}

Future<void> _deleteIfPresent(File file) async {
  if (file.existsSync()) await file.delete();
}

/// Stops the conversion at [point] when the test seam asks for it, as a
/// crash there would: nothing after it runs, and nothing is cleaned up.
void _failAt(SembastConversionPoint point) {
  if (DeliveryTestHooks.current?.failSembastConversion?.call(point) ?? false) {
    throw InjectedFailure('Sembast conversion at ${point.name}');
  }
}
