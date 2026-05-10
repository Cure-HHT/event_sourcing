import 'package:event_sourcing/src/promoters/promoter_spec.dart';

class PromoterRegistry {
  // Key: "viewName|entryType|fromVersion"
  final Map<String, PromoterSpec> _byKey = {};
  bool _sealed = false;

  String _key(String view, String entry, int from) => '$view|$entry|$from';

  void register(PromoterSpec spec) {
    if (_sealed) {
      throw StateError('PromoterRegistry: cannot register after seal()');
    }
    final k = _key(spec.viewName, spec.entryType, spec.fromVersion);
    if (_byKey.containsKey(k)) {
      throw StateError(
        'PromoterRegistry: duplicate registration for '
        '(${spec.viewName}, ${spec.entryType}, v${spec.fromVersion})',
      );
    }
    _byKey[k] = spec;
  }

  /// Returns the chain of PromoterSpecs that promotes a payload from
  /// [fromVersion] to [toVersion] for ([viewName], [entryType]). Throws
  /// when any step in the chain is unregistered.
  List<PromoterSpec> chain({
    required String viewName,
    required String entryType,
    required int fromVersion,
    required int toVersion,
  }) {
    if (fromVersion == toVersion) return const [];
    if (fromVersion > toVersion) {
      throw StateError(
        'PromoterRegistry.chain: cannot promote backward '
        '(from=$fromVersion, to=$toVersion)',
      );
    }
    final out = <PromoterSpec>[];
    var v = fromVersion;
    while (v < toVersion) {
      final spec = _byKey[_key(viewName, entryType, v)];
      if (spec == null) {
        throw StateError(
          'PromoterRegistry.chain: no PromoterSpec registered for '
          '($viewName, $entryType, v$v -> v${v + 1}). '
          'Register a spec covering this transition.',
        );
      }
      out.add(spec);
      v = spec.toVersion;
    }
    return out;
  }

  void seal() {
    _sealed = true;
  }

  bool get isSealed => _sealed;
}
