// Verifies: EVS-DEV-postgres-backend/O
//
// The setup for an application's own tables in the library's Postgres
// database is documented beside the runtime role's privileges -- in the
// doc comment of `postgresRuntimeRoleGrants` -- and in the package README's
// Postgres section: a schema and a role of its own, no privilege on a
// library table beyond SELECT, and no membership through which the role can
// inherit or set a declared library role, the owner or pg_write_all_data.
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';

const String _heading = "An application's own tables";

/// The statements the section must make, each as the phrases that make it.
const Map<String, List<String>> _statements = <String, List<String>>{
  'a schema of its own': <String>['schema of its own'],
  'a role of its own': <String>['role of its own'],
  'at most SELECT on a library table': <String>['beyond', '`SELECT`'],
  'no membership in a declared role, the owner or pg_write_all_data': <String>[
    'no membership',
    'inherit',
    'set its role',
    'declared',
    'owner',
    '`pg_write_all_data`',
  ],
};

/// The text of the section headed [_heading] in [document], up to the next
/// heading of the same or a higher level, or null when there is none.
String? ownTablesSection(String document) {
  final lines = document.split('\n');
  final start = lines.indexWhere(
    (l) => RegExp(r'^(///\s*)?#+\s').hasMatch(l) && l.contains(_heading),
  );
  if (start < 0) return null;
  final level = RegExp('#+').firstMatch(lines[start])!.group(0)!.length;
  final section = <String>[];
  for (final line in lines.skip(start + 1)) {
    final heading = RegExp(r'^(///\s*)?(#+)\s').firstMatch(line);
    if (heading != null && heading.group(2)!.length <= level) break;
    section.add(line.replaceFirst(RegExp(r'^\s*///\s?'), ''));
  }
  return section.join(' ').replaceAll(RegExp(r'\s+'), ' ');
}

/// Null when [document] carries the section and it makes every statement;
/// otherwise what is missing.
String? ownTablesProblem(String document) {
  final section = ownTablesSection(document);
  if (section == null) return 'no "$_heading" section';
  final missing = <String>[
    for (final MapEntry(key: statement, value: phrases) in _statements.entries)
      if (!phrases.every(section.contains)) statement,
  ];
  return missing.isEmpty ? null : 'the section does not state: $missing';
}

void main() {
  test('the doc comment of postgresRuntimeRoleGrants documents the '
      "application's own-table setup", () {
    final source = File(
      'lib/src/storage/postgres/postgres_grants.dart',
    ).readAsStringSync();
    expect(ownTablesProblem(source), isNull);
  });

  test("the README's Postgres section documents the application's "
      'own-table setup', () {
    final readme = File('README.md').readAsStringSync();
    expect(ownTablesProblem(readme), isNull);
  });

  group('the scan rejects', () {
    const complete =
        '## $_heading\n\n'
        'A schema of its own; a role of its own; no privilege beyond '
        '`SELECT`; no membership through which it can inherit or set its '
        'role to a declared role, the owner or `pg_write_all_data`.\n';

    test('the complete synthetic section is accepted', () {
      expect(ownTablesProblem(complete), isNull);
    });

    test('a document without the section', () {
      expect(ownTablesProblem('# Postgres\n\nNothing here.\n'), isNotNull);
    });

    test('a section that admits more than SELECT', () {
      expect(
        ownTablesProblem(complete.replaceAll('beyond `SELECT`', 'at all')),
        contains('SELECT'),
      );
    });

    test('a section that does not rule out pg_write_all_data', () {
      expect(
        ownTablesProblem(complete.replaceAll(' or `pg_write_all_data`', '')),
        contains('pg_write_all_data'),
      );
    });

    test('a section ends at the next heading of its level', () {
      expect(
        ownTablesProblem(
          '## $_heading\n\nA schema of its own.\n\n## Other\n\n'
          'a role of its own; no membership; beyond `SELECT`',
        ),
        isNotNull,
      );
    });
  });
}
