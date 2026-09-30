import 'dart:convert';

import 'package:canonical_json_jcs/canonical_json_jcs.dart';
import 'package:meta/meta.dart' show internal;

/// The top-level field of [record] under which some string -- a key or a
/// value, at any depth -- carries the character U+0000, or null when no
/// string of [record] does.
///
/// Every backend the library ships stores an event's `data`, `metadata`,
/// `initiator` and the rest of its record as JSON; Postgres's JSONB and
/// text columns cannot hold U+0000 in any string, and Sembast can, so the
/// library refuses the character everywhere a record's strings are
/// checked, rather than only where one backend would fail on it.
// Implements: EVS-DEV-event-record/L
// the single helper every U+0000 check (append, ingest and restore
//   classification, a stored read, and a finding's record evidence) shares.
@internal
String? recordFieldWithNulCharacter(Map<String, Object?> record) {
  for (final entry in record.entries) {
    if (entry.key.contains('\u0000') || _valueHasNulCharacter(entry.value)) {
      return entry.key;
    }
  }
  return null;
}

/// Whether some string within [value] -- a map key, a list element or a
/// scalar, at any depth -- carries the character U+0000.
bool _valueHasNulCharacter(Object? value) {
  if (value is String) return value.contains('\u0000');
  if (value is Map) {
    for (final entry in value.entries) {
      final key = entry.key;
      if (key is String && key.contains('\u0000')) return true;
      if (_valueHasNulCharacter(entry.value)) return true;
    }
    return false;
  }
  if (value is Iterable) {
    for (final item in value) {
      if (_valueHasNulCharacter(item)) return true;
    }
    return false;
  }
  return false;
}

/// The value that carries [value] -- a record, or an object embedded in
/// one, such as a delivery's `attributes` -- when no backend the library
/// ships can be trusted to hold it verbatim: [value] itself when
/// [recordFieldWithNulCharacter] finds none of its strings carrying
/// U+0000, and otherwise the base64 (RFC 4648 section 4, with padding) of
/// the UTF-8 bytes of its RFC 8785 canonical JSON. Both forms carry
/// [value] in full; a reader tells them apart by type (an object is the
/// value itself, a string its encoding) and decodes the string form back
/// with [decodeNulSafeEncoding].
@internal
Object nulSafeEncoding(Map<String, Object?> value) =>
    recordFieldWithNulCharacter(value) == null
    ? value
    : base64.encode(canonicalizeBytes(value));

/// The object [encoded] carries ([nulSafeEncoding]'s value): the object
/// itself, or, for its base64 string form, the object decoded back from it
/// (the UTF-8 bytes of its canonical JSON, base64-decoded and parsed).
@internal
Map<String, Object?> decodeNulSafeEncoding(Object encoded) {
  if (encoded is Map) return Map<String, Object?>.from(encoded);
  final decoded = jsonDecode(utf8.decode(base64.decode(encoded as String)));
  return Map<String, Object?>.from(decoded as Map);
}
