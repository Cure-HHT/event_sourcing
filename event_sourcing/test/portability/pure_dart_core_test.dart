// Verifies: EVS-PRD-portability/A — the library's core SOURCE (event_sourcing/
//   lib) is pure Dart: no Flutter or web-only imports, and the runtime
//   `dependencies:` block names only the Dart SDK + pure-Dart packages (no
//   `sdk: flutter`). A regression that pulled Flutter into the core source or
//   its runtime deps would break the VM/server target; this guard fails closed
//   on the first offender.
//
//   Scope note: the package's pubspec still pins `environment.flutter` because
//   its TEST toolchain uses `flutter_test`. That is a dev/test-tooling
//   dependency, not a core runtime one, so it is deliberately out of scope for
//   /A — this guard asserts core-source + runtime-dependency purity, not that
//   the package can be resolved without the Flutter SDK at all.
@TestOn('vm')
library;

import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

void main() {
  group('portability/A — pure-Dart core', () {
    test('no Flutter or web-only imports anywhere under lib/', () {
      final forbidden = RegExp(
        r'''import\s+['"](?:package:flutter/|package:flutter_test/|dart:ui|dart:html|dart:js)''',
      );
      final offenders = <String>[];
      for (final entity in Directory('lib').listSync(recursive: true)) {
        if (entity is! File || !entity.path.endsWith('.dart')) continue;
        for (final line in entity.readAsLinesSync()) {
          if (forbidden.hasMatch(line)) {
            offenders.add('${entity.path}: ${line.trim()}');
          }
        }
      }
      expect(
        offenders,
        isEmpty,
        reason:
            'Core lib must be pure Dart; found non-pure imports:\n'
            '${offenders.join('\n')}',
      );
    });

    test('runtime dependencies block names no Flutter-SDK package', () {
      // Scope: the runtime `dependencies:` block only. The package's
      // `environment.flutter` constraint (driven by the flutter_test dev
      // toolchain) is intentionally NOT asserted here — /A is about the core's
      // own runtime, not the test harness. `dependency_overrides` is likewise
      // out of scope: it is dev-local (a gitignored pubspec_overrides.yaml),
      // not part of the committed package contract.
      final pubspec = File('pubspec.yaml').readAsStringSync();
      final depsStart = pubspec.indexOf(
        RegExp('^dependencies:', multiLine: true),
      );
      expect(depsStart, greaterThanOrEqualTo(0), reason: 'no dependencies:');
      final devStart = pubspec.indexOf(
        RegExp('^dev_dependencies:', multiLine: true),
      );
      final end = devStart > depsStart ? devStart : pubspec.length;
      final runtimeDeps = pubspec.substring(depsStart, end);
      expect(
        runtimeDeps.contains('sdk: flutter'),
        isFalse,
        reason:
            'Core runtime dependencies must not name a Flutter-SDK package.',
      );
    });
  });
}
