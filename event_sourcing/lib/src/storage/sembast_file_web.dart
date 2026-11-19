// The file operations of an encrypted Sembast database in the browser,
// where there is no database file: each refuses. The counterpart of
// sembast_file_io.dart, selected by a conditional import in
// storage_description.dart.
import 'package:event_sourcing/src/storage/storage_encryption.dart';
import 'package:meta/meta.dart' show internal;

/// The file a conversion writes the encrypted copy of [path] to.
@internal
String sembastConversionCopyPath(String path) => '$path.encrypting';

/// No database file exists in the browser.
@internal
Future<SembastFileEncryption> inspectSembastFile(String path) =>
    Future<SembastFileEncryption>.error(
      UnsupportedError(
        'a Sembast database file is inspected only on the native runtimes',
      ),
    );

/// No database file exists in the browser.
@internal
Future<void> prepareEncryptedSembastFile(
  String path,
  StorageEncryptionKey key,
) => Future<void>.error(
  UnsupportedError(
    'a Sembast database file is opened only on the native runtimes',
  ),
);
