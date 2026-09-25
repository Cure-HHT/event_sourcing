// Verifies: EVS-DEV-storage-capability/C
// Verifies: EVS-DEV-storage-capability/D
// Verifies: EVS-DEV-storage-capability/F
// Verifies: EVS-DEV-storage-capability/I
//
// The members accessible outside their declaring Dart library, on every
// type whose instances the library hands to application code, are exactly
// those of the committed list (handed_out_surface.txt), by name and
// signature: a member added to that surface -- a new writing method under
// any name, a getter returning the backend -- fails here until it is
// reviewed and added. No transaction handle type yields an engine handle
// through such a member, and no public constructor in the security or
// Postgres storage code builds a store over a backend or a pool other than
// those the application itself holds.
@TestOn('vm')
library;

import 'dart:io';

import 'package:analyzer/dart/element/element.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:path/path.dart' as p;

import '../test_support/surface_scan.dart';
import 'handed_out_surface.dart';

/// The public constructors under `lib/src/security` and
/// `lib/src/storage/postgres` that take storage, each with the reason it
/// builds nothing over storage the library opened.
const Map<String, String> _constructorsOverApplicationStorage =
    <String, String>{
      'SembastSecurityContextStore.new':
          'the security-context store an application builds for the backend '
          'it constructed and names as application-supplied storage; no '
          'handed-out object yields a backend the library opened',
      'PostgresSecurityContextStore.new':
          'the security-context store an application builds for the backend '
          'it constructed and names as application-supplied storage; no '
          'handed-out object yields a backend the library opened',
      'PostgresIdempotencyStore.over':
          'an idempotency store over a pool the application opened under its '
          'own role, in its own schema',
    };

const List<String> _storeDirectories = <String>[
  'lib/src/security',
  'lib/src/storage/postgres',
];

String _read(String relative) =>
    File(p.join(packageRoot(), relative)).readAsStringSync();

/// [relative]'s content with [insertion] placed right after the first
/// occurrence of [anchor].
String _insertAfter(String relative, String anchor, String insertion) {
  final source = _read(relative);
  final at = source.indexOf(anchor);
  if (at < 0) throw StateError('anchor "$anchor" not found in $relative');
  final end = at + anchor.length;
  return source.substring(0, end) + insertion + source.substring(end);
}

Future<List<LibraryElement>> _libraries(Map<String, String> overlays) =>
    SurfaceScanner(overlays: overlays).librariesUnder('lib');

void main() {
  late List<LibraryElement> libraries;

  setUpAll(() async {
    libraries = await _libraries(const <String, String>{});
  });

  group('the handed-out surface', () {
    test('equals the committed list, by name and signature', () {
      final actual = handedOutSurface(libraries);
      final differences = surfaceDifferences(
        actual: actual,
        committed: readCommittedSurface(packageRoot()),
      );
      expect(
        differences,
        isEmpty,
        reason:
            'review each difference; the surface is now:\n'
            '${actual.join('\n')}',
      );
    });

    test('no transaction handle type yields an engine transaction, session '
        'or database through a member reachable outside its library', () {
      final handles = transactionHandleClasses(libraries);
      final names = handles.map((c) => c.name).toSet();
      expect(<String>[
        for (final name in <String>[
          'Transaction',
          '_SembastTxn',
          '_PostgresTxn',
        ])
          if (!names.contains(name)) 'no transaction handle class $name',
        ...handleYieldsNoEngineRule(handles),
      ], isEmpty);
    });

    test('no public constructor in the security or Postgres storage code '
        'builds a store over storage the library opened', () {
      expect(
        noPublicConstructorOverStorageRule(
          libraries: libraries,
          root: packageRoot(),
          directories: _storeDirectories,
          allowlist: _constructorsOverApplicationStorage,
        ),
        isEmpty,
      );
    });
  });

  group('fixtures laid over the library fail the rules', () {
    test('a new public transaction method on the event store is not in the '
        'committed list', () async {
      final libs = await _libraries(<String, String>{
        'lib/src/event_store.dart': _insertAfter(
          'lib/src/event_store.dart',
          'class EventStore {',
          '\n  Future<void> fooTxn(Transaction txn) async {}\n',
        ),
      });
      final differences = surfaceDifferences(
        actual: handedOutSurface(libs),
        committed: readCommittedSurface(packageRoot()),
      );
      expect(
        differences,
        contains(
          'not in the committed list: EventStore.fooTxn: '
          'Future<void> fooTxn(Transaction txn)',
        ),
      );
    });

    test('a public getter returning the backend on the destination registry '
        'is not in the committed list', () async {
      final libs = await _libraries(<String, String>{
        'lib/src/destinations/destination_registry.dart': _insertAfter(
          'lib/src/destinations/destination_registry.dart',
          'class DestinationRegistry {',
          '\n  StorageBackend get leakedBackend => _eventStore._backend;\n',
        ),
      });
      final differences = surfaceDifferences(
        actual: handedOutSurface(libs),
        committed: readCommittedSurface(packageRoot()),
      );
      expect(
        differences,
        contains(
          'not in the committed list: DestinationRegistry.leakedBackend: '
          'StorageBackend get leakedBackend',
        ),
      );
    });

    test('an engine-yielding member on a transaction handle fails', () async {
      final libs = await _libraries(<String, String>{
        'lib/src/storage/sembast_backend.dart': _insertAfter(
          'lib/src/storage/sembast_backend.dart',
          'class _SembastTxn extends Transaction {',
          '\n  sembast.Transaction get engine => _sembastTxn;\n',
        ),
      });
      expect(
        handleYieldsNoEngineRule(transactionHandleClasses(libs)),
        contains('_SembastTxn.engine yields a raw engine handle (Transaction)'),
      );
    });

    test('a public constructor over a backend in the Postgres storage code '
        'fails', () async {
      final libs = await _libraries(<String, String>{
        'lib/src/storage/postgres/postgres_idempotency_store.dart':
            _insertAfter(
              'lib/src/storage/postgres/postgres_idempotency_store.dart',
              'class PostgresIdempotencyStore implements IdempotencyStore {',
              '\n  PostgresIdempotencyStore.overBackend(PostgresBackend b)'
                  ' : _run = throw UnimplementedError();\n',
            ),
      });
      expect(
        noPublicConstructorOverStorageRule(
          libraries: libs,
          root: packageRoot(),
          directories: _storeDirectories,
          allowlist: _constructorsOverApplicationStorage,
        ),
        contains(
          'PostgresIdempotencyStore.overBackend is a public constructor over '
          'storage (PostgresBackend b)',
        ),
      );
    });
  });
}
