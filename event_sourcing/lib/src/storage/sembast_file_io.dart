// The file operations of an encrypted Sembast database on the native
// runtimes: reading whether a database file is encrypted, and converting an
// unencrypted one. Compiled only where dart:io exists; the counterpart of
// sembast_file_web.dart, selected by a conditional import in
// storage_description.dart.
// Implements: EVS-PRD-storage-barrier/M
// an unencrypted database named with a key is converted to an encrypted one
//   holding the same records.
// Implements: EVS-DEV-sembast-encryption/F+G+H+I+J+M
// the copy, read-back, flush and one-rename replacement, so the file holds
//   one complete database at every point; the discarding of a copy an
//   interrupted conversion left; the completion of an interrupted
//   compaction; and the first-line inspection that writes nothing.
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:collection/collection.dart' show DeepCollectionEquality;
import 'package:event_sourcing/src/storage/storage_encryption.dart';
import 'package:event_sourcing/src/testing/delivery_test_hooks.dart';
import 'package:meta/meta.dart' show internal;
import 'package:path/path.dart' as p;
import 'package:sembast/sembast_io.dart' show databaseFactoryIo;
import 'package:sembast/utils/sembast_import_export.dart'
    show DatabaseMode, exportDatabase, importDatabase;

/// The file sembast writes a compacted database to before it replaces the
/// database with it. sembast opens it in place of a missing database file.
String _compactionPath(String path) =>
    p.join(p.dirname(path), '~${p.basename(path)}');

/// The file a conversion writes the encrypted copy of [path] to.
@internal
String sembastConversionCopyPath(String path) => '$path.encrypting';

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
/// last line a crash cut short, writes an encrypted copy of every store,
/// reads the copy back from its file under [key] and compares it with the
/// database, flushes it to the device, and replaces the database file with
/// it by one rename, which also removes the unencrypted file. Until that
/// rename the file holds the unencrypted database, as sembast keeps it; from
/// it on, the complete encrypted copy. A crash at any point therefore
/// leaves one of the two, and the next call finishes or redoes the
/// conversion.
///
/// An absent or encrypted database is left for the open, which creates an
/// encrypted one, or refuses a key that does not decrypt it.
@internal
Future<void> prepareEncryptedSembastFile(
  String path,
  StorageEncryptionKey key,
) async {
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
  // Opened as an open without a key opens it, so the conversion reads what
  // that open would: a last line a crash cut short is dropped and the file
  // compacted without it, as sembast repairs it there.
  final source = await databaseFactoryIo.openDatabase(path);
  final Map<String, Object?> exported;
  try {
    exported = await exportDatabase(source);
  } finally {
    await source.close();
  }

  final copy = await importDatabase(
    exported,
    databaseFactoryIo,
    copyPath,
    codec: codec,
  );
  await copy.close();
  _failAt(SembastConversionPoint.copyWritten);

  final reread = await databaseFactoryIo.openDatabase(
    copyPath,
    mode: DatabaseMode.readOnly,
    codec: codec,
  );
  final Map<String, Object?> copied;
  try {
    copied = await exportDatabase(reread);
  } finally {
    await reread.close();
  }
  if (!const DeepCollectionEquality().equals(exported, copied)) {
    await _deleteIfPresent(File(copyPath));
    throw StateError(
      'the encrypted copy of the Sembast database at $path does not hold '
      'the records of the database; the database was left unencrypted',
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
