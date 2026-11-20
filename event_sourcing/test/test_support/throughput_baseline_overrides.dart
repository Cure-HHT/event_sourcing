// The throughput guard's dependency pins (EVS-DEV-chain-verification/T): the
// baseline build resolves each hosted dependency it shares with the current
// tree at the version the current tree resolved, so the guard compares the
// library's code on the same drivers rather than two driver releases.

import 'package:yaml/yaml.dart';

/// The dependency overrides the baseline build is resolved with: `versions`
/// maps each pinned package to its version, and `yaml` is the
/// `pubspec_overrides.yaml` text that pins them.
typedef BaselineDependencyOverrides = ({
  Map<String, String> versions,
  String yaml,
});

/// Pins each direct hosted dependency of [baselinePubspec] (the baseline
/// package's `pubspec.yaml`) that [currentLock] (the current package's
/// `pubspec.lock`) resolved from a hosted source, at the version the current
/// tree resolved.
///
/// A path, git or sdk dependency of the baseline, a dev dependency, and a
/// package the current tree did not resolve from a hosted source stay
/// unpinned. Both inputs are the files pub itself wrote or reads, so their
/// shape is pub's.
BaselineDependencyOverrides baselineDependencyOverrides({
  required String baselinePubspec,
  required String currentLock,
}) {
  final pubspec = loadYaml(baselinePubspec) as YamlMap;
  final lock = loadYaml(currentLock) as YamlMap;
  final dependencies = (pubspec['dependencies'] as YamlMap?) ?? YamlMap();
  final packages = (lock['packages'] as YamlMap?) ?? YamlMap();

  final versions = <String, String>{};
  for (final entry in dependencies.entries) {
    final name = entry.key as String;
    if (!_isHostedConstraint(entry.value)) continue;
    final locked = packages[name];
    if (locked is! YamlMap || locked['source'] != 'hosted') continue;
    versions[name] = locked['version'] as String;
  }

  final sorted = versions.keys.toList()..sort();
  final buffer = StringBuffer('dependency_overrides:\n');
  for (final name in sorted) {
    buffer.writeln('  $name: "${versions[name]}"');
  }
  return (
    versions: {for (final name in sorted) name: versions[name]!},
    yaml: buffer.toString(),
  );
}

/// True when a `dependencies` entry's value names a hosted package: a bare
/// version constraint, no value at all, or a map carrying `hosted` or only a
/// `version`.
bool _isHostedConstraint(Object? value) {
  if (value == null || value is String) return true;
  if (value is YamlMap) {
    return value.containsKey('hosted') ||
        (value.length == 1 && value.containsKey('version'));
  }
  return false;
}
