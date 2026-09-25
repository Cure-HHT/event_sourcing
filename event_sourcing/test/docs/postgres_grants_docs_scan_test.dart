// Verifies: EVS-DEV-postgres-backend/K
//
// The runtime role's privileges are documented where a deployment reads
// them: the "Runtime role privileges" table of `spec/postgres-backend.md`
// lists exactly the table/privilege pairs of `postgresRuntimeRoleGrants`,
// the set the runtime-role test grants and proves sufficient and needed,
// and the constant names every table provisioning creates.
import 'dart:io';

import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/storage/postgres/postgres_schema.dart'
    show postgresLibraryTables;
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

const String _heading = '### Runtime role privileges';

/// The table/privilege pairs the markdown table under [_heading] in
/// [document] lists, or null when the document has no such table.
Map<String, Set<String>>? documentedGrants(String document) {
  final at = document.indexOf(_heading);
  if (at < 0) return null;
  final grants = <String, Set<String>>{};
  var inTable = false;
  for (final line in document.substring(at + _heading.length).split('\n')) {
    final trimmed = line.trim();
    if (!trimmed.startsWith('|')) {
      if (inTable) break;
      if (trimmed.startsWith('#')) break;
      continue;
    }
    inTable = true;
    final cells = trimmed
        .substring(1, trimmed.length - 1)
        .split('|')
        .map((c) => c.trim())
        .toList();
    if (cells.length != 2) continue;
    final table = cells[0].replaceAll('`', '');
    if (table == 'Table' || table.startsWith('-')) continue;
    grants[table] = <String>{
      for (final privilege in cells[1].split(','))
        if (privilege.trim().isNotEmpty) privilege.trim(),
    };
  }
  return inTable ? grants : null;
}

/// Null when [document] documents exactly [expected]; otherwise what
/// differs.
String? grantsProblem(
  String document, {
  Map<String, Set<String>> expected = postgresRuntimeRoleGrants,
}) {
  final documented = documentedGrants(document);
  if (documented == null) return 'no "$_heading" table';
  final pairs = <String>{
    for (final e in documented.entries)
      for (final privilege in e.value) '${e.key} $privilege',
  };
  final wanted = <String>{
    for (final e in expected.entries)
      for (final privilege in e.value) '${e.key} $privilege',
  };
  final missing = wanted.difference(pairs);
  final extra = pairs.difference(wanted);
  if (missing.isEmpty && extra.isEmpty) return null;
  return 'missing: ${missing.toList()..sort()}; '
      'not granted: ${extra.toList()..sort()}';
}

void main() {
  final repoRoot = p.dirname(Directory.current.path);

  test('spec/postgres-backend.md lists exactly the runtime-role grants', () {
    final document = File(
      p.join(repoRoot, 'spec', 'postgres-backend.md'),
    ).readAsStringSync();
    expect(grantsProblem(document), isNull);
  });

  test('the grants name every table provisioning creates, and no other', () {
    expect(
      postgresRuntimeRoleGrants.keys.toSet(),
      postgresLibraryTables.toSet(),
    );
  });

  group('the scan rejects', () {
    String table(Map<String, String> rows) => <String>[
      _heading,
      '',
      '| Table | Privileges |',
      '| --- | --- |',
      for (final e in rows.entries) '| `${e.key}` | ${e.value} |',
      '',
    ].join('\n');

    Map<String, String> documented() => <String, String>{
      for (final e in postgresRuntimeRoleGrants.entries)
        e.key: e.value.join(', '),
    };

    test('the unchanged synthetic table is accepted', () {
      expect(grantsProblem(table(documented())), isNull);
    });

    test('a table with one privilege missing', () {
      final rows = documented()..['backend_state'] = 'SELECT, INSERT, UPDATE';
      expect(grantsProblem(table(rows)), contains('backend_state DELETE'));
    });

    test('a table with a privilege the role is not granted', () {
      final rows = documented()..['events'] = 'SELECT, INSERT, UPDATE';
      expect(grantsProblem(table(rows)), contains('events UPDATE'));
    });

    test('a table with a table missing', () {
      final rows = documented()..remove('idempotency');
      expect(grantsProblem(table(rows)), contains('idempotency SELECT'));
    });

    test('a document without the table', () {
      expect(
        grantsProblem('# Postgres Backend\n\nNo grants here.\n'),
        isNotNull,
      );
    });
  });
}
