// Checks of the library's AES-GCM, called from the VM and the browser test
// files: against the GCM specification's known
// answers and against pointycastle's independent GCM implementation over
// random keys, nonces and lengths, so its GHASH and counter arithmetic is
// right on whichever runtime runs these cases (the VM, and the browser,
// where integers are JavaScript numbers).
import 'dart:math';
import 'dart:typed_data';

import 'package:event_sourcing/src/storage/aes_gcm.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:pointycastle/api.dart' show AEADParameters, KeyParameter;
import 'package:pointycastle/block/aes.dart' show AESEngine;
import 'package:pointycastle/block/modes/gcm.dart' show GCMBlockCipher;

String _hex(List<int> bytes) =>
    bytes.map((b) => b.toRadixString(16).padLeft(2, '0')).join();

Uint8List _reference(Uint8List key, Uint8List nonce, Uint8List plaintext) {
  final cipher = GCMBlockCipher(AESEngine())
    ..init(true, AEADParameters(KeyParameter(key), 128, nonce, Uint8List(0)));
  return cipher.process(plaintext);
}

Uint8List _random(Random random, int length) =>
    Uint8List.fromList(List<int>.generate(length, (_) => random.nextInt(256)));

/// The GCM specification's known answers for a 256-bit key (its test
/// cases 13 and 14) hold.
void checkKnownAnswers() {
  final gcm = AesGcm(Uint8List(32));
  expect(
    _hex(gcm.seal(Uint8List(12), Uint8List(0))),
    '530f8afbc74536b9a963b4f1c4cb738b',
  );
  expect(
    _hex(gcm.seal(Uint8List(12), Uint8List(16))),
    'cea7403d4d606b6e074ec5d3baf39d18d0d1c8a799996bf0265b98b5d48ab919',
  );
}

/// The library's AES-GCM seals as pointycastle's GCM does, and opens what
/// it seals, over random keys, nonces and lengths.
void checkAgainstReference() {
  final random = Random(20261119);
  final lengths = <int>[
    for (var n = 0; n <= 70; n++) n,
    255,
    256,
    257,
    4095,
    4096,
    4097,
    70001,
  ];
  for (final keyLength in <int>[16, 32]) {
    for (final length in lengths) {
      final key = _random(random, keyLength);
      final nonce = _random(random, 12);
      final plaintext = _random(random, length);
      final sealed = AesGcm(key).seal(nonce, plaintext);
      expect(
        sealed,
        _reference(key, nonce, plaintext),
        reason: 'key $keyLength bytes, plaintext $length bytes',
      );
      expect(AesGcm(key).open(nonce, sealed), plaintext);
    }
  }
}

/// An altered ciphertext, tag or nonce, another key and a message shorter
/// than its tag are refused.
void checkRefusals() {
  final random = Random(7);
  final key = _random(random, 32);
  final nonce = _random(random, 12);
  final gcm = AesGcm(key);
  final sealed = gcm.seal(nonce, _random(random, 100));
  for (final at in <int>[0, 50, 99, 100, 115]) {
    final altered = Uint8List.fromList(sealed)..[at] ^= 1;
    expect(() => gcm.open(nonce, altered), throwsFormatException);
  }
  final otherNonce = Uint8List.fromList(nonce)..[0] ^= 1;
  expect(() => gcm.open(otherNonce, sealed), throwsFormatException);
  expect(
    () => AesGcm(_random(random, 32)).open(nonce, sealed),
    throwsFormatException,
  );
  expect(() => gcm.open(nonce, Uint8List(15)), throwsFormatException);
}
