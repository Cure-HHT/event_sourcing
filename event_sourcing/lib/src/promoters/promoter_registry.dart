// Implements: EVS-DEV-ingest-promotes-before-fold/C
// Implements: EVS-DEV-snapshot-promotion-on-open/B
import 'package:event_sourcing/src/promoters/primitives/transform.dart';
import 'package:event_sourcing/src/promoters/promoter_spec.dart';
import 'package:event_sourcing/src/versions.dart';

/// The promoter steps registered for each (view, entry type).
///
/// A step leads from one entry-type version to the next: within a major to
/// the next minor, or across a major to minor 0 of the next major. A minor
/// step is compatible by definition, so its transforms are `DefaultField`
/// only; `RenameField` and `DropField` require a major step. [register]
/// refuses any other step. A minor step that is not registered is the
/// identity: it changes nothing.
// Implements: EVS-DEV-version-compatibility/B
// register refuses a same-major step that is not to the next minor or whose
//   transforms are not all DefaultField, a cross-major step that is not to
//   minor 0 of the next major, and a second major step from one major; chain
//   treats a missing minor step as the identity.
class PromoterRegistry {
  // Key: "viewName|entryType|fromVersion"
  final Map<String, PromoterSpec> _byKey = {};
  // Key: "viewName|entryType|fromMajor" -> the major step from that major.
  final Map<String, PromoterSpec> _majorStepByKey = {};
  bool _sealed = false;

  String _key(String view, String entry, EntryTypeVersion from) =>
      '$view|$entry|$from';

  String _majorKey(String view, String entry, int fromMajor) =>
      '$view|$entry|$fromMajor';

  /// Registers [spec]. Throws [ArgumentError] after [seal], for a duplicate
  /// `(view, entry type, from)`, and for a step the rule above refuses.
  void register(PromoterSpec spec) {
    final label =
        '(${spec.viewName}, ${spec.entryType}, '
        '${spec.fromVersion} -> ${spec.toVersion})';
    if (_sealed) {
      throw ArgumentError.value(
        label,
        'spec',
        'PromoterRegistry: cannot register after seal()',
      );
    }
    final from = spec.fromVersion;
    final to = spec.toVersion;
    final isMajorStep = to.major != from.major;
    if (!isMajorStep) {
      if (to != from.nextMinor) {
        throw ArgumentError.value(
          label,
          'spec',
          'PromoterRegistry: a step within one major must lead to the next '
              'minor (${from.nextMinor})',
        );
      }
      if (spec.transforms.any((t) => t is! DefaultField)) {
        throw ArgumentError.value(
          label,
          'spec',
          'PromoterRegistry: a minor step is compatible by definition, so '
              'its transforms must all be DefaultField; RenameField and '
              'DropField require a major step',
        );
      }
    } else if (to != EntryTypeVersion(from.major + 1, 0)) {
      throw ArgumentError.value(
        label,
        'spec',
        'PromoterRegistry: a step across majors must lead to minor 0 of the '
            'next major (${from.major + 1}.0)',
      );
    }
    final k = _key(spec.viewName, spec.entryType, from);
    if (_byKey.containsKey(k)) {
      throw ArgumentError.value(
        label,
        'spec',
        'PromoterRegistry: duplicate registration',
      );
    }
    final mk = _majorKey(spec.viewName, spec.entryType, from.major);
    if (isMajorStep && _majorStepByKey.containsKey(mk)) {
      final existing = _majorStepByKey[mk]!;
      throw ArgumentError.value(
        label,
        'spec',
        'PromoterRegistry: a second major step from major ${from.major} '
            '(already registered: ${existing.fromVersion} -> '
            '${existing.toVersion})',
      );
    }
    _byKey[k] = spec;
    if (isMajorStep) _majorStepByKey[mk] = spec;
  }

  /// Returns the chain of registered steps that promotes a payload from
  /// [fromVersion] to [toVersion] for ([viewName], [entryType]).
  ///
  /// Walking from [fromVersion]: a step registered from the current
  /// version is applied; otherwise, when the current version is of
  /// [toVersion]'s major, or its minor is below the start of the major
  /// step registered from its major, the step to the next minor is the
  /// identity; otherwise the chain throws [StateError] naming the missing
  /// step. A [fromVersion] at or above [toVersion] within one major yields
  /// the empty chain; a [fromVersion] of a higher major throws.
  List<PromoterSpec> chain({
    required String viewName,
    required String entryType,
    required EntryTypeVersion fromVersion,
    required EntryTypeVersion toVersion,
  }) {
    final (steps, gap) = _walk(viewName, entryType, fromVersion, toVersion);
    if (gap != null) throw StateError('PromoterRegistry.chain: $gap');
    return steps;
  }

  /// Why no chain leads from [fromVersion] to [toVersion] for
  /// ([viewName], [entryType]), or null when [chain] would return one.
  String? chainGap({
    required String viewName,
    required String entryType,
    required EntryTypeVersion fromVersion,
    required EntryTypeVersion toVersion,
  }) => _walk(viewName, entryType, fromVersion, toVersion).$2;

  (List<PromoterSpec>, String?) _walk(
    String viewName,
    String entryType,
    EntryTypeVersion fromVersion,
    EntryTypeVersion toVersion,
  ) {
    if (fromVersion.major > toVersion.major) {
      return (
        const <PromoterSpec>[],
        'cannot promote ($viewName, $entryType) from $fromVersion to the '
            'lower major of $toVersion',
      );
    }
    final out = <PromoterSpec>[];
    var v = fromVersion;
    while (v < toVersion) {
      final spec = _byKey[_key(viewName, entryType, v)];
      if (spec != null) {
        out.add(spec);
        v = spec.toVersion;
        continue;
      }
      if (v.major == toVersion.major) {
        v = v.nextMinor;
        continue;
      }
      final majorStep =
          _majorStepByKey[_majorKey(viewName, entryType, v.major)];
      if (majorStep != null && v.minor < majorStep.fromVersion.minor) {
        v = v.nextMinor;
        continue;
      }
      return (
        out,
        'no step registered for ($viewName, $entryType) from $v toward '
            '$toVersion: '
            '${majorStep == null ? 'no major step is registered from major '
                      '${v.major}' : '$v is past the major step registered '
                      'from ${majorStep.fromVersion}'}. Register a step '
            'covering this transition.',
      );
    }
    return (out, null);
  }

  void seal() {
    _sealed = true;
  }

  bool get isSealed => _sealed;
}
