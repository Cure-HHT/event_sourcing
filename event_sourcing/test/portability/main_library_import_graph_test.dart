// The main library's import graph never reaches the Postgres driver, so a
//   web build that imports package:event_sourcing/event_sourcing.dart never
//   compiles it (the driver declares 64-bit integer constants dart2js
//   refuses). The walk follows every import, export and part directive under
//   lib/ as written, taking every branch of a conditional import, so a
//   driver reached on any runtime fails it. The Postgres backend is reached
//   only through package:event_sourcing/postgres.dart.
@TestOn('vm')
library;

import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

/// A directive that names other files: an import (with its conditional
/// branches), an export or a part, up to its terminating semicolon. A
/// `part of` directive names the enclosing library and is not followed.
final _directive = RegExp(
  r'^(?:import|export|part)\b(?!\s+of\b)[^;]*;',
  multiLine: true,
);

/// A quoted URI inside a directive.
final _uri = RegExp('''['"]([^'"]+)['"]''');

/// Every URI the files reachable from [entry] name, keyed by the URI, with
/// the file that first named it. A `package:event_sourcing/` URI or a
/// relative URI is followed into `lib/`; every other URI is a leaf.
Map<String, String> _reachableUris(String entry) {
  final named = <String, String>{};
  final visited = <String>{};
  final pending = <String>[p.normalize(entry)];
  while (pending.isNotEmpty) {
    final file = pending.removeLast();
    if (!visited.add(file)) continue;
    final text = File(file).readAsStringSync();
    for (final directive in _directive.allMatches(text)) {
      for (final match in _uri.allMatches(directive[0]!)) {
        final uri = match[1]!;
        named.putIfAbsent(uri, () => file);
        final String target;
        if (uri.startsWith('package:event_sourcing/')) {
          target = p.join(
            'lib',
            uri.substring('package:event_sourcing/'.length),
          );
        } else if (!uri.contains(':')) {
          target = p.normalize(p.join(p.dirname(file), uri));
        } else {
          continue;
        }
        named.putIfAbsent(target, () => file);
        pending.add(target);
      }
    }
  }
  return named;
}

/// The reached URIs and files that are the Postgres driver or the Postgres
/// backend's own sources, each with the file that named it.
List<String> _postgresReach(Map<String, String> reached) => <String>[
  for (final entry in reached.entries)
    if (entry.key.startsWith('package:postgres/') ||
        p.isWithin(p.join('lib', 'src', 'storage', 'postgres'), entry.key))
      '${entry.key} (named by ${entry.value})',
];

void main() {
  // Verifies: EVS-PRD-portability/B
  group('portability/B — the main library compiles on the web', () {
    test('the main library never reaches the Postgres driver', () {
      final reached = _reachableUris('lib/event_sourcing.dart');
      expect(reached.keys, contains(p.join('lib', 'src', 'event_store.dart')));
      expect(reached.keys, contains('package:sembast/sembast.dart'));
      expect(
        _postgresReach(reached),
        isEmpty,
        reason:
            'package:event_sourcing/event_sourcing.dart must not reach the '
            'Postgres driver or the Postgres backend: import the backend '
            'through package:event_sourcing/postgres.dart',
      );
    });

    test('the Postgres library reaches the driver, so the walk detects '
        'it', () {
      expect(_postgresReach(_reachableUris('lib/postgres.dart')), isNotEmpty);
    });
  });
}
