// Tooling test: verifies repository content, not a requirement.
//
// Committed text states the library as it is and cites requirement labels,
// never the labels of the work that produced it. This scan reads every
// committed source, test, spec and document tree of the repository and
// refuses:
//
// - an implementation-plan label: a package label (a capital P, digits and
//   an optional capital letter) or a decision label (a capital D and
//   digits), as whole words;
// - a ticket identifier (the tracker prefix, a dash and digits), except on a
//   requirement's Changelog line, which records the history of that
//   requirement, and on the lines listed in [_allowedTicketLines];
// - in documentation and in code comments, the name of a hosting product:
//   deployment requirements are stated as properties, so that they hold
//   wherever the library runs.
//
// This file names the patterns it looks for, so the scan skips it.

@TestOn('vm')
library;

import 'dart:io';

import 'package:path/path.dart' as p;
import 'package:test/test.dart';

/// The trees the scan reads, relative to the repository root.
const List<String> scannedRoots = <String>[
  'spec',
  'docs',
  'event_sourcing/lib',
  'event_sourcing/test',
  'event_sourcing/tool',
  'event_sourcing/example/lib',
  'event_sourcing/example/test',
  'event_sourcing/example/integration_test',
  'event_sourcing/example_action_permissions/bin',
  'event_sourcing/example_action_permissions/lib',
  'event_sourcing/example_action_permissions/test',
  'event_sourcing/example_action_permissions/tool',
  'event_sourcing/example_clinical_scopes/lib',
  'event_sourcing/example_clinical_scopes/test',
  'reaction/lib',
  'reaction/test',
  'reaction/example/bin',
  'reaction/example/lib',
  'reaction/example/test',
  'reaction/example/test_driver',
  'reaction/example/e2e',
  'reaction_widgets/lib',
  'reaction_widgets/test',
  'reaction_widgets_testing/lib',
  'reaction_widgets_testing/test',
];

/// Single files the scan reads besides every `README.md` of the repository.
const List<String> scannedFiles = <String>[
  'CLAUDE.md',
  'event_sourcing/CHANGELOG.md',
];

/// Directories never read: generated output, tool caches, and the
/// gitignored working documents.
const Set<String> _skippedDirectories = <String>{
  'build',
  '.dart_tool',
  'node_modules',
  'test-results',
  'playwright-report',
  'superpowers',
};

const Set<String> _scannedExtensions = <String>{
  '.dart',
  '.md',
  '.yaml',
  '.yml',
  '.ts',
  '.js',
  '.sh',
  '.toml',
};

/// This file, which names the patterns.
const String _self = 'event_sourcing/test/docs/plan_label_scan_test.dart';

final RegExp _planLabel = RegExp(r'\b(?:P[0-9]+[A-Z]?|D[0-9]+)\b');
final RegExp _ticketId = RegExp(r'\bCUR-[0-9]+');

/// A requirement's Changelog line: `- <date> | <hash or -> | ...`.
final RegExp _changelogLine = RegExp(r'^\s*- \d{4}-\d{2}-\d{2} \|');

/// Lines allowed to carry a ticket identifier, with the reason. Each entry
/// is the file and a phrase the line contains.
const Map<String, String> _allowedTicketLines = <String, String>{
  // The branch-naming convention shows the tracker's branch shape by
  // example; it describes how work is named, not the library.
  'CLAUDE.md': '**Branch naming**',
};

/// Hosting products no committed document or code comment names. A bare
/// `GCP` is not among them: in a clinical text it is Good Clinical
/// Practice, and `Google Cloud` names the product.
final RegExp _hostingProduct = RegExp(
  'Cloud SQL|Cloud Run|App Engine|Cloud Functions|Google Cloud|'
  r'\bGKE\b|Kubernetes|\bAWS\b|Amazon RDS|\bRDS\b|Aurora|Azure|Heroku|'
  r'Fly\.io|Vercel|Netlify|Supabase|Firebase Hosting',
);

/// One finding: where, and what.
class ScanHit {
  const ScanHit(this.path, this.line, this.kind, this.text);

  final String path;
  final int line;
  final String kind;
  final String text;

  @override
  String toString() => '$path:$line [$kind] $text';
}

bool _isDocumentation(String path) =>
    path.endsWith('.md') ||
    path.startsWith('docs/') ||
    path.startsWith('spec/');

/// The comment text of a source [line], or null when it has none. Covers
/// line comments (dartdoc included) and the lines of a block comment the
/// caller tracks.
String? _commentOf(String line) {
  final at = line.indexOf('//');
  if (at < 0) return null;
  // A `//` inside a string literal (a URL) is not a comment start when a
  // quote precedes it on the line.
  final before = line.substring(0, at);
  final quotes = RegExp('[\'"]').allMatches(before).length;
  if (quotes.isOdd) return null;
  return line.substring(at);
}

/// The findings of [contents], read as the repository file at [path]
/// (repository-relative, `/`-separated).
List<ScanHit> scanFile(String path, String contents) {
  final hits = <ScanHit>[];
  final lines = contents.split('\n');
  final documentation = _isDocumentation(path);
  var inBlockComment = false;
  for (var i = 0; i < lines.length; i++) {
    final line = lines[i];
    final number = i + 1;
    if (_planLabel.hasMatch(line)) {
      hits.add(ScanHit(path, number, 'plan label', line.trim()));
    }
    if (_ticketId.hasMatch(line) &&
        !_changelogLine.hasMatch(line) &&
        !(_allowedTicketLines[path] != null &&
            line.contains(_allowedTicketLines[path]!))) {
      hits.add(ScanHit(path, number, 'ticket id', line.trim()));
    }
    String? prose;
    if (documentation) {
      prose = line;
    } else {
      final trimmed = line.trimLeft();
      if (inBlockComment) {
        prose = line;
        if (line.contains('*/')) inBlockComment = false;
      } else if (trimmed.startsWith('/*')) {
        prose = line;
        inBlockComment = !line.contains('*/');
      } else if (trimmed.startsWith('#')) {
        // YAML and shell comments.
        prose = line;
      } else {
        prose = _commentOf(line);
      }
    }
    if (prose != null && _hostingProduct.hasMatch(prose)) {
      hits.add(ScanHit(path, number, 'hosting product', line.trim()));
    }
  }
  return hits;
}

/// Every file the scan reads under [repoRoot], repository-relative.
List<String> scannedPaths(String repoRoot) {
  final paths = <String>{};
  void walk(Directory dir) {
    if (!dir.existsSync()) return;
    for (final entity in dir.listSync(followLinks: false)) {
      final name = p.basename(entity.path);
      if (entity is Directory) {
        if (_skippedDirectories.contains(name) || name.startsWith('.')) {
          continue;
        }
        walk(entity);
      } else if (entity is File) {
        final rel = p.posix.joinAll(
          p.split(p.relative(entity.path, from: repoRoot)),
        );
        if (name == 'README.md' ||
            _scannedExtensions.contains(p.extension(name))) {
          paths.add(rel);
        }
      }
    }
  }

  for (final root in scannedRoots) {
    walk(Directory(p.join(repoRoot, root)));
  }
  // Every README.md of the repository, outside the skipped directories.
  void readmes(Directory dir) {
    for (final entity in dir.listSync(followLinks: false)) {
      final name = p.basename(entity.path);
      if (entity is Directory) {
        if (_skippedDirectories.contains(name) || name.startsWith('.')) {
          continue;
        }
        readmes(entity);
      } else if (name == 'README.md') {
        paths.add(
          p.posix.joinAll(p.split(p.relative(entity.path, from: repoRoot))),
        );
      }
    }
  }

  readmes(Directory(repoRoot));
  for (final file in scannedFiles) {
    if (File(p.join(repoRoot, file)).existsSync()) paths.add(file);
  }
  paths.remove(_self);
  return paths.toList()..sort();
}

void main() {
  final repoRoot = p.dirname(Directory.current.path);

  test('no committed text carries a plan label, a ticket id or a hosting '
      'product', () {
    final paths = scannedPaths(repoRoot);
    expect(paths, contains('CLAUDE.md'));
    expect(paths, contains('event_sourcing/CHANGELOG.md'));
    expect(paths, contains('spec/prd-destinations.md'));
    expect(paths, contains('reaction_widgets/lib/reaction_widgets.dart'));
    final hits = <ScanHit>[
      for (final path in paths)
        ...scanFile(path, File(p.join(repoRoot, path)).readAsStringSync()),
    ];
    expect(hits, isEmpty, reason: hits.join('\n'));
  });

  group('the scan refuses', () {
    test('a plan label on a synthetic spec line', () {
      final hits = scanFile(
        'spec/prd-destinations.md',
        'The drainer honours the halt (P7) before any send.\n',
      );
      expect(hits.map((h) => h.kind), <String>['plan label']);
    });

    test('a decision label in a synthetic dartdoc line', () {
      final hits = scanFile(
        'event_sourcing/lib/src/sync/sync_cycle.dart',
        '/// Standby is the default on every platform (D3).\n'
            'final class SyncCycle {}\n',
      );
      expect(hits.map((h) => h.kind), <String>['plan label']);
    });

    test('a package label in a synthetic test name', () {
      final hits = scanFile(
        'event_sourcing/test/sync/drain_test.dart',
        "  test('P4B boot runs in one transaction', () {});\n",
      );
      expect(hits.map((h) => h.kind), <String>['plan label']);
    });

    test('a ticket id in a synthetic reaction_widgets comment', () {
      final hits = scanFile(
        'reaction_widgets/lib/src/view/view_builder.dart',
        '// Follow-up tracked in CUR-1234.\nclass ViewBuilder {}\n',
      );
      expect(hits.map((h) => h.kind), <String>['ticket id']);
    });

    test('a hosting product in a synthetic dartdoc line', () {
      final hits = scanFile(
        'event_sourcing/lib/src/storage/postgres/postgres_backend.dart',
        '/// Deploy the lock connection through Cloud SQL Auth Proxy.\n',
      );
      expect(hits.map((h) => h.kind), <String>['hosting product']);
    });

    test('a hosting product in a synthetic document line', () {
      final hits = scanFile(
        'docs/event-sourcing-guide.md',
        'Run the server on Kubernetes with two replicas.\n',
      );
      expect(hits.map((h) => h.kind), <String>['hosting product']);
    });
  });

  group('the scan accepts', () {
    test("a ticket id on a requirement's Changelog line", () {
      expect(
        scanFile(
          'spec/scoped-permissions.md',
          '- 2026-05-14 | d3eee322 | - | Developer | Initial authoring under '
              'CUR-1331\n',
        ),
        isEmpty,
      );
    });

    test('a hosting product named in code, not in a comment', () {
      expect(
        scanFile(
          'event_sourcing/test/storage/x_test.dart',
          "const hosts = <String>['AWS'];\n",
        ),
        isEmpty,
      );
    });

    test('Good Clinical Practice (GCP) in a scenario document', () {
      expect(
        scanFile(
          'docs/scenarios/medical-diary.md',
          "or a sponsor's Good Clinical Practice (GCP) auditor asks\n",
        ),
        isEmpty,
      );
    });

    test('words that only contain a label', () {
      expect(
        scanFile('docs/x.md', 'SHA-256, DP7X, PD1, P7a and 3D1 are words.\n'),
        isEmpty,
      );
    });
  });
}
