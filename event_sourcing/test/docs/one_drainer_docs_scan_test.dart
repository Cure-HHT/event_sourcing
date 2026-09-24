// Tooling test: verifies documentation, not a requirement's behaviour.
//
// The one-drainer rule is stated where an adopter meets it, and every
// statement points back to the requirement that is its source of truth:
// the guide's section on several processes sharing one database and its
// "Open a storage backend" step, the README's `PostgresBackend` entry, the
// dartdoc of `SyncCycle`, of `PostgresBackend.open` and `provision`, of
// `DestinationRegistry.readDeliveryStatus` and `deleteDestination`, the
// package CHANGELOG's one-drainer section, and the CLAUDE.md trust
// boundaries. Each location names `EVS-PRD-destinations/V`.
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

/// The requirement every location names.
const String requirement = 'EVS-PRD-destinations/V';

/// The markdown text under the heading [heading] of [document] (the heading
/// line excluded), up to the next heading of the same or a higher level;
/// null when the document has no such heading.
String? markdownSection(String document, String heading) {
  final lines = document.split('\n');
  final level = heading.indexOf(' ');
  final start = lines.indexWhere((l) => l.trimRight() == heading);
  if (start < 0) return null;
  final body = <String>[];
  var inFence = false;
  for (final line in lines.skip(start + 1)) {
    if (line.trimLeft().startsWith('```')) inFence = !inFence;
    if (!inFence) {
      final m = RegExp('^(#+) ').firstMatch(line);
      if (m != null && m.group(1)!.length <= level) break;
    }
    body.add(line);
  }
  return body.join('\n');
}

/// The dartdoc block (`///` lines) immediately above the first line of
/// [source] that contains [declaration], ignoring the `//` annotation
/// comments and metadata between them; null when there is none.
String? dartdocOf(String source, String declaration) {
  final lines = source.split('\n');
  final at = lines.indexWhere((l) => l.contains(declaration));
  if (at < 0) return null;
  final doc = <String>[];
  for (var i = at - 1; i >= 0; i--) {
    final t = lines[i].trimLeft();
    if (t.startsWith('///')) {
      doc.insert(0, t);
    } else if (t.startsWith('//') || t.startsWith('@')) {
      if (doc.isNotEmpty) break;
    } else {
      break;
    }
  }
  return doc.isEmpty ? null : doc.join('\n');
}

/// The markdown list item of [section] that starts with [prefix], up to the
/// next list item or blank line; null when there is none.
String? listItemOf(String? section, String prefix) {
  if (section == null) return null;
  final lines = section.split('\n');
  final start = lines.indexWhere((l) => l.startsWith(prefix));
  if (start < 0) return null;
  final item = <String>[lines[start]];
  for (final line in lines.skip(start + 1)) {
    if (line.trim().isEmpty || line.startsWith('- ')) break;
    item.add(line);
  }
  return item.join('\n');
}

/// A location the rule must be stated at: a description, and the text of
/// the location (null when the location does not exist).
class Location {
  const Location(this.name, this.text);

  final String name;
  final String? text;
}

/// Null when [location] exists and names [requirement]; otherwise why not.
String? locationProblem(Location location) {
  final text = location.text;
  if (text == null) return '${location.name}: not found';
  if (!text.contains(requirement)) {
    return '${location.name}: does not name $requirement';
  }
  return null;
}

List<Location> locations(String repoRoot) {
  String read(String rel) => File(p.join(repoRoot, rel)).readAsStringSync();
  final guide = read('docs/event-sourcing-guide.md');
  final readme = read('event_sourcing/README.md');
  final changelog = File(p.join(repoRoot, 'event_sourcing/CHANGELOG.md'));
  final claude = read('CLAUDE.md');
  final syncCycle = read('event_sourcing/lib/src/sync/sync_cycle.dart');
  final postgres = read(
    'event_sourcing/lib/src/storage/postgres/postgres_backend.dart',
  );
  final registry = read(
    'event_sourcing/lib/src/destinations/destination_registry.dart',
  );
  final crossProcess = markdownSection(
    guide,
    '## Cross-process client/server deployments',
  );
  return <Location>[
    Location(
      'guide, "Several processes sharing one database"',
      crossProcess == null
          ? null
          : markdownSection(
              crossProcess,
              '### Several processes sharing one database',
            ),
    ),
    Location(
      'guide, "1. Open a storage backend"',
      markdownSection(guide, '### 1. Open a storage backend'),
    ),
    Location(
      'README, "Storage backends", the PostgresBackend entry',
      listItemOf(
        markdownSection(readme, '## Storage backends'),
        '- **`PostgresBackend`**',
      ),
    ),
    Location(
      'SyncCycle dartdoc',
      dartdocOf(syncCycle, 'final class SyncCycle'),
    ),
    Location(
      'PostgresBackend.open dartdoc',
      dartdocOf(postgres, 'static Future<PostgresBackend> open('),
    ),
    Location(
      'PostgresBackend.provision dartdoc',
      dartdocOf(postgres, 'static Future<void> provision('),
    ),
    Location(
      'DestinationRegistry.readDeliveryStatus dartdoc',
      dartdocOf(registry, 'Future<DeliveryStatus> readDeliveryStatus()'),
    ),
    Location(
      'DestinationRegistry.deleteDestination dartdoc',
      dartdocOf(registry, 'Future<void> deleteDestination('),
    ),
    Location(
      'CHANGELOG, "Delivery: one drainer per database"',
      changelog.existsSync()
          ? markdownSection(
              changelog.readAsStringSync(),
              '### Delivery: one drainer per database',
            )
          : null,
    ),
    Location(
      'CLAUDE.md, "Trust boundaries"',
      markdownSection(claude, '## Trust boundaries'),
    ),
  ];
}

void main() {
  final repoRoot = p.dirname(Directory.current.path);

  test('every location of the one-drainer rule names $requirement', () {
    final problems = <String>[
      for (final location in locations(repoRoot)) ?locationProblem(location),
    ];
    expect(problems, isEmpty, reason: problems.join('\n'));
  });

  group('the scan', () {
    test('refuses a synthetic document that states the rule without the '
        'requirement', () {
      const document =
          '# Guide\n\n## Storage backends\n\nOne process drains a database; '
          'the others stand by.\n\n## Next\n\nSee $requirement.\n';
      expect(
        locationProblem(
          Location(
            'synthetic',
            markdownSection(document, '## Storage backends'),
          ),
        ),
        contains('does not name'),
      );
    });

    test('refuses a missing section', () {
      expect(
        locationProblem(
          Location('synthetic', markdownSection('# Guide\n', '## Missing')),
        ),
        contains('not found'),
      );
    });

    test('refuses a synthetic dartdoc that does not name the requirement', () {
      const source =
          '/// At most one delivery cycle drains a database.\n'
          '// Implements: $requirement\n'
          'final class SyncCycle {}\n';
      expect(
        locationProblem(
          Location('synthetic', dartdocOf(source, 'final class SyncCycle')),
        ),
        contains('does not name'),
      );
    });

    test('reads one list item, not its neighbours', () {
      const section =
          '- **`A`** one.\n  more of A.\n- **`B`** ($requirement).\n\nAfter.';
      final item = listItemOf(section, '- **`A`**');
      expect(item, '- **`A`** one.\n  more of A.');
      expect(
        locationProblem(Location('synthetic', item)),
        contains('does not name'),
      );
      expect(listItemOf(section, '- **`C`**'), isNull);
    });

    test('accepts a synthetic section that names the requirement', () {
      const document =
          '## Storage backends\n\nOne process drains ($requirement).\n'
          '### Detail\n\nMore.\n## Next\n';
      final section = markdownSection(document, '## Storage backends');
      expect(section, contains('### Detail'));
      expect(section, isNot(contains('## Next')));
      expect(locationProblem(Location('synthetic', section)), isNull);
    });
  });
}
