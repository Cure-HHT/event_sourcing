// Implements: EVS-PRD-storage-barrier/K
// the codec the library ships encrypts every line it writes to a Sembast
//   database under the key the description carries.
// Implements: EVS-DEV-sembast-encryption/A+B+C+D+L
// a 32-byte key, refused at any other length; AES-256-GCM with a 128-bit
//   tag over every line, record lines and the codec signature alike; a fresh
//   96-bit nonce from a secure source per encryption; text forms that omit
//   the key.
import 'dart:convert';
import 'dart:math' show Random;
import 'dart:typed_data';

import 'package:event_sourcing/src/storage/aes_gcm.dart';
import 'package:meta/meta.dart' show internal, visibleForTesting;
import 'package:sembast/sembast.dart' show SembastCodec;

/// The key an encrypted Sembast database is stored under: 32 bytes, used
/// as an AES-256 key.
///
/// The application keeps it where it keeps its secrets and hands it to the
/// library in a `SembastStorage.file` description. The key holds a copy of
/// the bytes it was given, and [bytes] returns another copy, so nothing the
/// caller keeps aliases it. Its text form names no byte of it.
final class StorageEncryptionKey {
  /// A key of the 32 [bytes] given. Throws [ArgumentError] for any other
  /// length.
  StorageEncryptionKey(Uint8List bytes) : _bytes = Uint8List.fromList(bytes) {
    if (bytes.length != keyLength) {
      throw ArgumentError.value(
        '${bytes.length} bytes',
        'bytes',
        'an encryption key is exactly $keyLength bytes',
      );
    }
  }

  /// A new key of 32 bytes from a cryptographically secure random source.
  factory StorageEncryptionKey.generate() {
    final random = Random.secure();
    return StorageEncryptionKey(
      Uint8List.fromList(
        List<int>.generate(keyLength, (_) => random.nextInt(256)),
      ),
    );
  }

  /// The length of a key, in bytes.
  static const int keyLength = 32;

  final Uint8List _bytes;

  /// A copy of the key's bytes.
  Uint8List get bytes => Uint8List.fromList(_bytes);

  /// Whether [other] holds the same bytes. Every byte is compared, whatever
  /// the first difference.
  @internal
  bool sameAs(StorageEncryptionKey? other) {
    if (other == null) return false;
    var difference = 0;
    for (var i = 0; i < keyLength; i++) {
      difference |= _bytes[i] ^ other._bytes[i];
    }
    return difference == 0;
  }

  @override
  bool operator ==(Object other) =>
      other is StorageEncryptionKey && sameAs(other);

  @override
  int get hashCode => Object.hashAll(_bytes);

  @override
  String toString() => 'StorageEncryptionKey(<redacted>)';
}

/// Thrown by `EventStore.open` when the encryption key a Sembast storage
/// description carries does not decrypt the database the description names,
/// or when the description carries no key and the database is encrypted.
/// Nothing is written: the database file is left as it was, and opens again
/// once the right key is given.
class StorageKeyMismatchError extends Error {
  StorageKeyMismatchError(this.location, {required this.keyGiven});

  /// The file path the description names.
  final String location;

  /// Whether the description carried a key.
  final bool keyGiven;

  @override
  String toString() => keyGiven
      ? 'StorageKeyMismatchError: the encryption key given does not decrypt '
            'the Sembast database at $location; it was left unchanged. Open '
            'it with the key it is stored under.'
      : 'StorageKeyMismatchError: the Sembast database at $location is '
            'encrypted and no encryption key was given; it was left '
            'unchanged. Open it with the key it is stored under.';
}

/// Whether a Sembast database file is present, and whether it is encrypted,
/// as `SembastStorage.inspectFile` reads it from its first line.
enum SembastFileEncryption {
  /// No database at the path: no file, or an empty one. Opening it creates
  /// a new database, encrypted when the description carries a key.
  absent,

  /// A database whose lines are stored as written, under no codec. Opening
  /// it with a key converts it to an encrypted one.
  unencrypted,

  /// A database whose lines are stored under a codec, which opens only
  /// with the key it is stored under.
  encrypted,
}

/// The codec signature an encrypted database records. sembast stores it
/// encoded by the codec itself, so only the key the database is stored
/// under decodes it back to this value.
const String _signature = 'evs-aes256gcm-1';

/// Nonce length, in bytes: the 96 bits GCM is specified for.
const int _nonceLength = 12;

/// Authentication tag length, in bits.
const int _tagBits = 128;

/// The sembast codec that stores every line of a database encrypted under
/// [key].
@internal
SembastCodec encryptingSembastCodec(StorageEncryptionKey key) =>
    SembastCodec(signature: _signature, codec: _AesGcmLineCodec(key._bytes));

/// Encrypts [plaintext] under [key] with [nonce]: the ciphertext followed
/// by the authentication tag. Exposed for the known-answer tests.
@visibleForTesting
@internal
Uint8List aesGcmEncrypt(Uint8List key, Uint8List nonce, Uint8List plaintext) =>
    AesGcm(key).seal(nonce, plaintext);

/// A line codec: a JSON-encodable value to one line of base64 holding the
/// nonce, the ciphertext of its JSON text and the tag, and back.
///
/// Every failure to decode a line is a [FormatException]: sembast reads a
/// line it cannot decode as a damaged one only when the decoder throws an
/// [Exception], which is how a database whose last line a crash cut short
/// still opens.
class _AesGcmLineCodec extends Codec<Object?, String> {
  _AesGcmLineCodec(Uint8List key) : _cipher = AesGcm(key);

  final AesGcm _cipher;
  final Random _random = Random.secure();

  @override
  late final Converter<Object?, String> encoder = _Encoder(this);

  @override
  late final Converter<String, Object?> decoder = _Decoder(this);

  String _encode(Object? input) {
    final nonce = Uint8List(_nonceLength);
    for (var i = 0; i < _nonceLength; i++) {
      nonce[i] = _random.nextInt(256);
    }
    final plaintext = Uint8List.fromList(utf8.encode(json.encode(input)));
    final sealed = _cipher.seal(nonce, plaintext);
    final line = Uint8List(_nonceLength + sealed.length)
      ..setAll(0, nonce)
      ..setAll(_nonceLength, sealed);
    return base64.encode(line);
  }

  Object? _decode(String line) {
    final bytes = base64.decode(line);
    if (bytes.length < _nonceLength + _tagBits ~/ 8) {
      throw const FormatException('encrypted line too short');
    }
    final plaintext = _cipher.open(
      Uint8List.sublistView(bytes, 0, _nonceLength),
      Uint8List.sublistView(bytes, _nonceLength),
    );
    return json.decode(utf8.decode(plaintext));
  }
}

class _Encoder extends Converter<Object?, String> {
  const _Encoder(this._codec);
  final _AesGcmLineCodec _codec;

  @override
  String convert(Object? input) => _codec._encode(input);
}

class _Decoder extends Converter<String, Object?> {
  const _Decoder(this._codec);
  final _AesGcmLineCodec _codec;

  @override
  Object? convert(String input) => _codec._decode(input);
}

/// The points of a conversion of an unencrypted database at which the
/// `failSembastConversion` test seam can stop it, as a crash would.
@internal
enum SembastConversionPoint {
  /// The encrypted copy is written and closed, not yet read back.
  copyWritten,

  /// The copy is read back, holds every record, and is flushed to the
  /// device; the unencrypted file is not yet replaced.
  copyVerified,

  /// The copy has replaced the unencrypted file.
  replaced,
}
