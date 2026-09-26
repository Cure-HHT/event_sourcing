// Implements: EVS-DEV-causal-parents/A
// the causal object every event record carries: exactly kind, eligible,
//   parents (ascending by event identifier, each exactly event_id and
//   event_hash) and reconciles (null, or a non-empty list of the same
//   references in ascending order).

import 'package:collection/collection.dart';

/// The kind an event records in its `causal` object.
///
/// A version replaces its aggregate's state; an annotation is about the
/// aggregate's current version. The vocabulary is a Layer 2 convention; the
/// value recorded on an event is a Layer 1 fact covered by its hash.
enum CausalKind {
  /// Replaces its aggregate's state.
  version('version'),

  /// Is about its aggregate's current version.
  annotation('annotation');

  const CausalKind(this.wireName);

  /// The value the `kind` field of a `causal` object carries.
  final String wireName;

  /// The kind whose [wireName] is [value], or null when none is.
  static CausalKind? fromWireName(Object? value) {
    for (final kind in CausalKind.values) {
      if (kind.wireName == value) return kind;
    }
    return null;
  }
}

/// A reference, inside a `causal` object, to one event by its identifier and
/// its sealed hash.
class CausalRef {
  const CausalRef({required this.eventId, required this.eventHash});

  /// The referenced event's identifier.
  final String eventId;

  /// The referenced event's sealed hash.
  final String eventHash;

  /// The reference as the `causal` object carries it.
  Map<String, Object?> toJson() => <String, Object?>{
    'event_id': eventId,
    'event_hash': eventHash,
  };

  @override
  bool operator ==(Object other) =>
      other is CausalRef &&
      other.eventId == eventId &&
      other.eventHash == eventHash;

  @override
  int get hashCode => Object.hash(eventId, eventHash);

  @override
  String toString() => 'CausalRef($eventId, $eventHash)';
}

/// The `causal` object of an event record: its kind, its eligibility to be
/// named as a parent, the versions of its aggregate it follows, and, for a
/// reconciliation, the skip events it closes.
class CausalRecord {
  /// A causal record built by the library. Throws [ArgumentError] when
  /// [parents] or [reconciles] is not in strictly ascending order of event
  /// identifier, when a reference has an empty identifier or hash, or when
  /// [reconciles] is an empty list.
  factory CausalRecord({
    required CausalKind kind,
    required bool eligible,
    required List<CausalRef> parents,
    List<CausalRef>? reconciles,
  }) {
    final json = <String, Object?>{
      'kind': kind.wireName,
      'eligible': eligible,
      'parents': <Object?>[for (final p in parents) p.toJson()],
      'reconciles': reconciles == null
          ? null
          : <Object?>[for (final r in reconciles) r.toJson()],
    };
    try {
      return CausalRecord.fromJson(json);
    } on FormatException catch (e) {
      throw ArgumentError(e.message);
    }
  }

  const CausalRecord._(
    this.kind,
    this.eligible,
    this.parents,
    this.reconciles,
    this._json,
  );

  /// Decodes a `causal` object. Throws [FormatException], naming the field,
  /// unless [json] is an object with exactly the keys `kind`, `eligible`,
  /// `parents` and `reconciles`, `kind` is `version` or `annotation`,
  /// `eligible` is a boolean, `parents` is a list and `reconciles` null or a
  /// non-empty list, each list in strictly ascending order of event
  /// identifier and each of its items an object with exactly a non-empty
  /// string `event_id` and a non-empty string `event_hash`.
  factory CausalRecord.fromJson(Object? json) {
    if (json is! Map) {
      throw const FormatException('causal: expected an object');
    }
    for (final key in json.keys) {
      if (!_keys.contains(key)) {
        throw FormatException('causal.$key: unexpected key');
      }
    }
    for (final key in _keys) {
      if (!json.containsKey(key)) {
        throw FormatException('causal.$key: missing');
      }
    }
    final kind = CausalKind.fromWireName(json['kind']);
    if (kind == null) {
      throw FormatException(
        'causal.kind: expected "version" or "annotation", '
        'got ${json['kind']}',
      );
    }
    final eligible = json['eligible'];
    if (eligible is! bool) {
      throw FormatException(
        'causal.eligible: expected a boolean, got $eligible',
      );
    }
    final parents = _refs(json['parents'], 'causal.parents');
    final rawReconciles = json['reconciles'];
    List<CausalRef>? reconciles;
    if (rawReconciles != null) {
      reconciles = _refs(rawReconciles, 'causal.reconciles');
      if (reconciles.isEmpty) {
        throw const FormatException(
          'causal.reconciles: expected null or a non-empty list',
        );
      }
    }
    return CausalRecord._(
      kind,
      eligible,
      List<CausalRef>.unmodifiable(parents),
      reconciles == null ? null : List<CausalRef>.unmodifiable(reconciles),
      Map<String, Object?>.unmodifiable(
        json.map((k, v) => MapEntry(k as String, v)),
      ),
    );
  }

  static const Set<String> _keys = <String>{
    'kind',
    'eligible',
    'parents',
    'reconciles',
  };

  static const Set<String> _refKeys = <String>{'event_id', 'event_hash'};

  static List<CausalRef> _refs(Object? value, String field) {
    if (value is! List) {
      throw FormatException('$field: expected a list, got $value');
    }
    final refs = <CausalRef>[];
    for (var i = 0; i < value.length; i++) {
      final at = '$field[$i]';
      final item = value[i];
      if (item is! Map) {
        throw FormatException('$at: expected an object, got $item');
      }
      for (final key in item.keys) {
        if (!_refKeys.contains(key)) {
          throw FormatException('$at.$key: unexpected key');
        }
      }
      for (final key in _refKeys) {
        final v = item[key];
        if (v is! String || v.isEmpty) {
          throw FormatException('$at.$key: expected a non-empty string');
        }
      }
      final ref = CausalRef(
        eventId: item['event_id'] as String,
        eventHash: item['event_hash'] as String,
      );
      if (refs.isNotEmpty && refs.last.eventId.compareTo(ref.eventId) >= 0) {
        throw FormatException(
          '$at: event_id ${ref.eventId} is not above the preceding '
          '${refs.last.eventId}; the list is in strictly ascending order of '
          'event_id',
        );
      }
      refs.add(ref);
    }
    return refs;
  }

  /// Whether the event is a version or an annotation.
  final CausalKind kind;

  /// Whether a later event may name the event as a parent.
  final bool eligible;

  /// The versions of the event's aggregate it follows, in ascending order
  /// of event identifier.
  final List<CausalRef> parents;

  /// The skip events a reconciliation closes, in ascending order of event
  /// identifier; null for an event that is not a reconciliation.
  final List<CausalRef>? reconciles;

  final Map<String, Object?> _json;

  /// The `causal` object as it was decoded, or as the library built it.
  Map<String, Object?> toJson() => _json;

  @override
  bool operator ==(Object other) =>
      other is CausalRecord &&
      other.kind == kind &&
      other.eligible == eligible &&
      const ListEquality<CausalRef>().equals(other.parents, parents) &&
      const ListEquality<CausalRef>().equals(other.reconciles, reconciles);

  @override
  int get hashCode => Object.hash(
    kind,
    eligible,
    const ListEquality<CausalRef>().hash(parents),
    reconciles == null
        ? null
        : const ListEquality<CausalRef>().hash(reconciles),
  );

  @override
  String toString() =>
      'CausalRecord(${kind.wireName}, eligible: $eligible, '
      'parents: $parents, reconciles: $reconciles)';
}
