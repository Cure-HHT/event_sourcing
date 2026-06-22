// Verifies: EVS-PRD-portability/A — the library's core (event_sourcing/lib) is
//   pure Dart: it contains no Flutter or web-only imports, and its runtime
//   `dependencies:` declare no Flutter SDK dependency (only the Dart SDK and
//   pure-Dart packages). A regression that pulled Flutter into the core would
//   break the VM/server target; this guard fails closed on the first offender.
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

    test('runtime dependencies declare no Flutter SDK dependency', () {
      final pubspec = File('pubspec.yaml').readAsStringSync();
      final depsStart = pubspec.indexOf(
        RegExp(r'^dependencies:', multiLine: true),
      );
      expect(depsStart, greaterThanOrEqualTo(0), reason: 'no dependencies:');
      final devStart = pubspec.indexOf(
        RegExp(r'^dev_dependencies:', multiLine: true),
      );
      final end = devStart > depsStart ? devStart : pubspec.length;
      final runtimeDeps = pubspec.substring(depsStart, end);
      // dev_dependencies may legitimately use flutter_test; the runtime block
      // must not pull in the Flutter SDK.
      expect(
        runtimeDeps.contains('sdk: flutter'),
        isFalse,
        reason: 'Core must not depend on the Flutter SDK at runtime.',
      );
    });
  });
}
