// Implements: EVS-DEV-sembast-encryption/C
// AES-GCM (NIST SP 800-38D) with a 96-bit nonce, no additional data and a
//   128-bit tag: the counter mode over pointycastle's AES block cipher, and
//   GHASH by Shoup's four-bit table method in 32-bit words, so it runs alike
//   on the VM and under dart2js.
import 'dart:typed_data';

import 'package:meta/meta.dart' show internal;
import 'package:pointycastle/api.dart' show KeyParameter;
import 'package:pointycastle/block/aes.dart' show AESEngine;

/// Reductions of the four bits a four-bit shift moves out of a GHASH
/// value, in the top 16 bits of its first word.
const List<int> _last4 = <int>[
  0x0000, 0x1c20, 0x3840, 0x2460, 0x7080, 0x6ca0, 0x48c0, 0x54e0, //
  0xe100, 0xfd20, 0xd940, 0xc560, 0x9180, 0x8da0, 0xa9c0, 0xb5e0,
];

/// AES-GCM under one key, with a 96-bit nonce, no additional authenticated
/// data and a 128-bit tag.
///
/// The key schedule and the GHASH table are computed once per key, so one
/// instance seals and opens any number of messages. A nonce must never be
/// used twice under one key; the caller draws each one at random.
@internal
final class AesGcm {
  /// AES-GCM under [key]: 16, 24 or 32 bytes (AES-128, -192 or -256).
  AesGcm(Uint8List key) : _aes = AESEngine()..init(true, KeyParameter(key)) {
    final h = Uint8List(16);
    _aes.processBlock(Uint8List(16), 0, h, 0);
    _buildTable(h);
  }

  /// Nonce length, in bytes.
  static const int nonceLength = 12;

  /// Tag length, in bytes.
  static const int tagLength = 16;

  final AESEngine _aes;

  /// `_table[4 * i + w]` is word `w` of the product of H and the four-bit
  /// value `i`, in GCM's bit order.
  final Uint32List _table = Uint32List(64);

  void _buildTable(Uint8List h) {
    var v0 = _word(h, 0), v1 = _word(h, 4), v2 = _word(h, 8);
    var v3 = _word(h, 12);
    _set(8, v0, v1, v2, v3);
    for (var i = 4; i > 0; i >>= 1) {
      final t = (v3 & 1) == 1 ? 0xe1000000 : 0;
      v3 = ((v3 >> 1) | (v2 << 31)) & 0xffffffff;
      v2 = ((v2 >> 1) | (v1 << 31)) & 0xffffffff;
      v1 = ((v1 >> 1) | (v0 << 31)) & 0xffffffff;
      v0 = (v0 >> 1) ^ t;
      _set(i, v0, v1, v2, v3);
    }
    for (var i = 2; i <= 8; i *= 2) {
      for (var j = 1; j < i; j++) {
        for (var w = 0; w < 4; w++) {
          _table[4 * (i + j) + w] = _table[4 * i + w] ^ _table[4 * j + w];
        }
      }
    }
  }

  void _set(int i, int v0, int v1, int v2, int v3) {
    _table[4 * i] = v0;
    _table[4 * i + 1] = v1;
    _table[4 * i + 2] = v2;
    _table[4 * i + 3] = v3;
  }

  static int _word(Uint8List b, int o) =>
      (b[o] << 24) | (b[o + 1] << 16) | (b[o + 2] << 8) | b[o + 3];

  /// Replaces the 16 bytes of [x] with their product with H.
  void _multiply(Uint8List x) {
    final t = _table;
    var lo = x[15] & 0xf;
    var z0 = t[4 * lo], z1 = t[4 * lo + 1], z2 = t[4 * lo + 2];
    var z3 = t[4 * lo + 3];
    for (var i = 15; i >= 0; i--) {
      lo = x[i] & 0xf;
      final hi = x[i] >> 4;
      if (i != 15) {
        final rem = z3 & 0xf;
        z3 = ((z3 >> 4) | (z2 << 28)) & 0xffffffff;
        z2 = ((z2 >> 4) | (z1 << 28)) & 0xffffffff;
        z1 = ((z1 >> 4) | (z0 << 28)) & 0xffffffff;
        z0 = (z0 >> 4) ^ (_last4[rem] << 16);
        z0 ^= t[4 * lo];
        z1 ^= t[4 * lo + 1];
        z2 ^= t[4 * lo + 2];
        z3 ^= t[4 * lo + 3];
      }
      final rem = z3 & 0xf;
      z3 = ((z3 >> 4) | (z2 << 28)) & 0xffffffff;
      z2 = ((z2 >> 4) | (z1 << 28)) & 0xffffffff;
      z1 = ((z1 >> 4) | (z0 << 28)) & 0xffffffff;
      z0 = (z0 >> 4) ^ (_last4[rem] << 16);
      z0 ^= t[4 * hi];
      z1 ^= t[4 * hi + 1];
      z2 ^= t[4 * hi + 2];
      z3 ^= t[4 * hi + 3];
    }
    _put(x, 0, z0);
    _put(x, 4, z1);
    _put(x, 8, z2);
    _put(x, 12, z3);
  }

  static void _put(Uint8List b, int o, int w) {
    b[o] = (w >> 24) & 0xff;
    b[o + 1] = (w >> 16) & 0xff;
    b[o + 2] = (w >> 8) & 0xff;
    b[o + 3] = w & 0xff;
  }

  /// GHASH of [data] (no additional data), with its length block.
  Uint8List _ghash(Uint8List data, int length) {
    final y = Uint8List(16);
    for (var offset = 0; offset < length; offset += 16) {
      final end = offset + 16 < length ? offset + 16 : length;
      for (var i = offset; i < end; i++) {
        y[i - offset] ^= data[i];
      }
      _multiply(y);
    }
    // The length block: 64 bits of additional-data length (zero), then 64
    // bits of ciphertext length, in bits.
    final bits = length * 8;
    y[11] ^= (bits ~/ 0x100000000) & 0xff;
    y[12] ^= (bits >> 24) & 0xff;
    y[13] ^= (bits >> 16) & 0xff;
    y[14] ^= (bits >> 8) & 0xff;
    y[15] ^= bits & 0xff;
    _multiply(y);
    return y;
  }

  /// XORs [length] bytes of [input] with the key stream that starts at
  /// counter 2 under [nonce], into [output] at [outOffset].
  void _ctr(
    Uint8List nonce,
    Uint8List input,
    int inOffset,
    int length,
    Uint8List output,
    int outOffset,
  ) {
    final counter = Uint8List(16)..setRange(0, nonceLength, nonce);
    final stream = Uint8List(16);
    var block = 2;
    for (var done = 0; done < length; done += 16) {
      counter[12] = (block >> 24) & 0xff;
      counter[13] = (block >> 16) & 0xff;
      counter[14] = (block >> 8) & 0xff;
      counter[15] = block & 0xff;
      block++;
      _aes.processBlock(counter, 0, stream, 0);
      final n = length - done < 16 ? length - done : 16;
      for (var i = 0; i < n; i++) {
        output[outOffset + done + i] = input[inOffset + done + i] ^ stream[i];
      }
    }
  }

  /// The tag over [ciphertext]'s first [length] bytes under [nonce].
  Uint8List _tag(Uint8List nonce, Uint8List ciphertext, int length) {
    final s = _ghash(ciphertext, length);
    final j0 = Uint8List(16)
      ..setRange(0, nonceLength, nonce)
      ..[15] = 1;
    final mask = Uint8List(16);
    _aes.processBlock(j0, 0, mask, 0);
    for (var i = 0; i < 16; i++) {
      s[i] ^= mask[i];
    }
    return s;
  }

  /// [plaintext] encrypted under [nonce]: the ciphertext, then the tag.
  Uint8List seal(Uint8List nonce, Uint8List plaintext) {
    _checkNonce(nonce);
    final length = plaintext.length;
    final out = Uint8List(length + tagLength);
    _ctr(nonce, plaintext, 0, length, out, 0);
    out.setRange(length, length + tagLength, _tag(nonce, out, length));
    return out;
  }

  /// The plaintext of [sealed] (ciphertext, then tag) under [nonce]. Throws
  /// [FormatException] when the tag does not verify, before decrypting.
  Uint8List open(Uint8List nonce, Uint8List sealed) {
    _checkNonce(nonce);
    if (sealed.length < tagLength) {
      throw const FormatException('sealed message shorter than its tag');
    }
    final length = sealed.length - tagLength;
    final expected = _tag(nonce, sealed, length);
    var difference = 0;
    for (var i = 0; i < tagLength; i++) {
      difference |= expected[i] ^ sealed[length + i];
    }
    if (difference != 0) {
      throw const FormatException('authentication tag does not verify');
    }
    final out = Uint8List(length);
    _ctr(nonce, sealed, 0, length, out, 0);
    return out;
  }

  static void _checkNonce(Uint8List nonce) {
    if (nonce.length != nonceLength) {
      throw ArgumentError.value(
        nonce.length,
        'nonce',
        'an AES-GCM nonce is $nonceLength bytes',
      );
    }
  }
}
