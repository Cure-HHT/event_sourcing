// Implements: EVS-PRD-cross-process-event-transport/A — shared
//   discriminator/extraction primitives every JSON envelope codec
//   reuses for type-tag dispatch and field validation.

/// Discriminator and primitive-extraction helpers shared by all wire
/// codecs. Package-private; never exported from reaction.dart.

/// Reads the "type" discriminator from a wire envelope.
String readType(Map<String, Object?> json) {
  final t = json['type'];
  if (t is! String) {
    throw FormatException('missing or non-string "type": ${json['type']}');
  }
  return t;
}

/// Reads a required string field. Throws FormatException on missing
/// or wrong type.
String requireString(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v is! String) {
    throw FormatException('missing or non-string "$key": $v');
  }
  return v;
}

/// Reads an optional string field. Returns null if absent.
String? readString(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v == null) return null;
  if (v is! String) {
    throw FormatException('non-string "$key": $v');
  }
  return v;
}

/// Reads a required int field. Throws on missing or non-int.
int requireInt(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v is! int) {
    throw FormatException('missing or non-int "$key": $v');
  }
  return v;
}

/// Reads a required map field. Throws on missing or non-map.
Map<String, Object?> requireMap(Map<String, Object?> json, String key) {
  final v = json[key];
  if (v is! Map) {
    throw FormatException('missing or non-map "$key": $v');
  }
  return Map<String, Object?>.from(v);
}
