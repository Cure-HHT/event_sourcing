// Verifies: EVS-PRD-destinations/L
//
// The precondition of the storage trust boundary is stated where an
// adopter meets the boundary: the `StorageBackend` dartdoc, the README's
// storage section and the CLAUDE.md `StorageBackend` trust entry. Each
// occurrence names every kind of persisted state it covers.
import 'dart:io';

import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

/// The clause every statement of the precondition carries.
const _clause = "changes only through the library's operations";

/// Each kind of persisted state the precondition names: the phrase every
/// statement of it must contain, mapped to what the phrase names. A change
/// that adds a kind of library record adds its phrase here, so a statement
/// that is not widened with it fails.
const stateKinds = <String, String>{
  'queue': 'destination queues',
  'view': 'views',
  'records it keeps beside them': 'the records kept beside them',
  'fill positions': 'fill positions',
  'schedules': 'schedules',
  'replay requests': 'replay requests',
  'wedge records': 'wedge records',
  'registry check record': 'the registry check record',
  'database identity': 'the database identity',
  'generation records': 'the generation records',
  'catch-up marks': 'the view catch-up marks',
  'security context': 'the security context stored beside each event',
};

/// Returns null when [text] states the precondition naming every entry of
/// [kinds] in one paragraph; otherwise a description of what is missing.
String? preconditionProblem(
  String text, {
  Map<String, String> kinds = stateKinds,
}) {
  final paragraphs = text
      .split(RegExp(r'\n\s*(?:///|//)?\s*\n'))
      .map(
        (para) => para
            .split('\n')
            .map((l) => l.replaceFirst(RegExp(r'^\s*(?:///|//|[-*>])?\s*'), ''))
            .join(' ')
            .replaceAll(RegExp(r'\s+'), ' ')
            .toLowerCase(),
      )
      .where((para) => para.contains(_clause))
      .toList();
  if (paragraphs.isEmpty) return 'no paragraph states "$_clause"';
  for (final para in paragraphs) {
    final missing = <String>[
      for (final entry in kinds.entries)
        if (!para.contains(entry.key)) entry.value,
    ];
    if (missing.isEmpty) return null;
  }
  return 'the precondition does not name every kind of persisted state';
}

String _between(String text, String start, String end) {
  final from = text.indexOf(start);
  if (from < 0) throw StateError('"$start" not found');
  final to = text.indexOf(end, from + start.length);
  return text.substring(from, to < 0 ? text.length : to);
}

void main() {
  final libraryRoot = Directory.current.path;
  final repoRoot = p.dirname(libraryRoot);

  group('the storage precondition is stated', () {
    test('in the StorageBackend dartdoc', () {
      final source = File(
        p.join(libraryRoot, 'lib', 'src', 'storage', 'storage_backend.dart'),
      ).readAsStringSync();
      final classAt = source.indexOf('abstract class StorageBackend');
      final doc = source
          .substring(0, classAt)
          .split('\n')
          .where((l) => l.trimLeft().startsWith('///'))
          .join('\n');
      expect(preconditionProblem(doc), isNull);
    });

    test('in the README storage section', () {
      final readme = File(p.join(libraryRoot, 'README.md')).readAsStringSync();
      final section = _between(readme, '## Storage backends', '\n## ');
      expect(preconditionProblem(section), isNull);
    });

    test('in the CLAUDE.md StorageBackend trust entry', () {
      final claude = File(p.join(repoRoot, 'CLAUDE.md')).readAsStringSync();
      final trust = _between(claude, '## Trust boundaries', '\n## ');
      final entry = _between(
        trust,
        '- **`StorageBackend` implementation.**',
        '\n- **',
      );
      expect(preconditionProblem(entry), isNull);
    });
  });

  group('the matcher rejects', () {
    test('a document without the precondition', () {
      expect(
        preconditionProblem(
          'StorageBackend is the trusted persistence seam: correct reads '
          'and writes, transaction atomicity and durability.',
        ),
        isNotNull,
      );
    });

    test('a precondition naming only queues and views', () {
      expect(
        preconditionProblem(
          'Delivery holds only while destination queues and the views it '
          "materializes change only through the library's operations.",
        ),
        isNotNull,
      );
      expect(
        preconditionProblem(
          'Its persisted state (destination queues and the views it '
          "materializes) changes only through the library's operations.",
        ),
        isNotNull,
      );
    });

    test('a precondition that omits a kind of state the list names', () {
      expect(
        preconditionProblem(
          'Its persisted state (destination queues, the views it '
          'materializes, and the records it keeps beside them, such as fill '
          "positions and schedules) changes only through the library's "
          'operations.',
        ),
        isNotNull,
      );
      expect(
        preconditionProblem(
          'Its persisted state (destination queues, the views it '
          'materializes, the records it keeps beside them, such as fill '
          'positions and schedules, and the security context it stores) '
          "changes only through the library's operations.",
          kinds: const <String, String>{
            ...stateKinds,
            'halt requests': 'open halt requests',
          },
        ),
        isNotNull,
      );
    });

    test('a precondition split across paragraphs', () {
      expect(
        preconditionProblem(
          'It keeps destination queues, views, and the records it keeps '
          "beside them.\n\nIts state changes only through the library's "
          'operations.',
        ),
        isNotNull,
      );
    });
  });
}
