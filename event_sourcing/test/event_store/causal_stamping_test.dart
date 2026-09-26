// Runs the causal-stamping scenarios of causal_stamping_conformance.dart on
// Sembast (in memory), and checks the hash coverage of the causal object and
// the append signatures, which no backend changes.
//
// The scenarios' assertions are cited on their own tests in
// causal_stamping_conformance.dart.
import 'package:analyzer/dart/element/element.dart';
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/surface_scan.dart';
import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;
import 'causal_stamping_conformance.dart';

class _SembastCausalDatabase implements VersionTestDatabase {
  _SembastCausalDatabase(this._db);

  final Database _db;

  @override
  Future<StorageBackend> openBackend() async => SembastBackend(database: _db);

  @override
  MutableSecurityContextStore securityFor(StorageBackend backend) =>
      SembastSecurityContextStore(backend: backend as SembastBackend);

  @override
  Future<void> stop(EventStore store) async {}

  @override
  Future<void> close() => _db.close();
}

var _dbCounter = 0;

Future<VersionTestDatabase> _memoryDatabase() async {
  _dbCounter += 1;
  return _SembastCausalDatabase(
    await newDatabaseFactoryMemory().openDatabase('causal-$_dbCounter.db'),
  );
}

/// A sealed record carrying a causal object that names [parents].
Map<String, Object?> _record(List<CausalRef> parents) {
  final record = <String, Object?>{
    'event_id': 'causal-hash-event',
    'aggregate_id': 'causal-hash-note',
    'aggregate_type': 'note',
    'entry_type': 'causal_note',
    'entry_type_version': const EntryTypeVersion(1, 0).toJson(),
    'lib_format_version': LibVersion.dataFormat.toJson(),
    'event_type': 'revised',
    'sequence_number': 3,
    'data': <String, Object?>{'title': 't'},
    'metadata': <String, Object?>{'provenance': <Object?>[]},
    'initiator': const UserInitiator('u').toJson(),
    'flow_token': null,
    'client_timestamp': '2026-09-01T12:00:00.000Z',
    'previous_event_hash': null,
    'causal': CausalRecord(
      kind: CausalKind.version,
      eligible: true,
      parents: parents,
    ).toJson(),
  };
  record['event_hash'] = canonicalEventHash(record);
  return record;
}

void main() {
  runCausalStampingScenarios(
    openDatabase: _memoryDatabase,
    openOtherDatabase: _memoryDatabase,
    backendLabel: 'sembast (memory)',
  );

  group('the causal object in the event hash', () {
    // Verifies: EVS-DEV-event-record/K
    // Verifies: EVS-PRD-hash-chain-integrity/A
    test('changing the parents a causal object names changes the event '
        'hash', () {
      final one = _record(const <CausalRef>[
        CausalRef(eventId: 'p-1', eventHash: 'h-1'),
      ]);
      final other = _record(const <CausalRef>[
        CausalRef(eventId: 'p-1', eventHash: 'h-2'),
      ]);
      expect(canonicalEventHash(other), isNot(canonicalEventHash(one)));
      expect(
        canonicalEventHash(_record(const <CausalRef>[])),
        isNot(canonicalEventHash(one)),
      );
    });

    // Verifies: EVS-DEV-event-record/J
    // Verifies: EVS-PRD-hash-chain-integrity/D
    test('a parsed record writes its causal object back as it carried it, '
        'and hashes as it did before parsing', () {
      final record = _record(const <CausalRef>[
        CausalRef(eventId: 'p-1', eventHash: 'h-1'),
      ]);
      final event = StoredEvent.fromMap(record, 0);
      expect(event.toMap()['causal'], record['causal']);
      expect(canonicalEventHash(event.toMap()), record['event_hash']);
    });
  });

  group('the append signatures', () {
    // Verifies: EVS-DEV-causal-parents/G
    // the resolved signatures of append, appendInTxn and the reserved
    //   append carry no parameter named causal or typed as any part of the
    //   causal object.
    test(
      'append, appendInTxn and the reserved append take no causal argument',
      () async {
        final scanner = SurfaceScanner();
        final library = await scanner.library('lib/src/event_store.dart');
        final eventStore = classNamed(<LibraryElement>[library!], 'EventStore');
        const causalTypes = <String>{'CausalRecord', 'CausalKind', 'CausalRef'};
        for (final name in const <String>[
          'append',
          'appendInTxn',
          '_appendReservedInTxn',
        ]) {
          final method = eventStore.methods.firstWhere(
            (m) => m.name == name,
            orElse: () => throw StateError('EventStore.$name not found'),
          );
          expect(
            [for (final p in method.formalParameters) p.name],
            contains('entryType'),
            reason: name,
          );
          final offending = <String>[
            for (final p in method.formalParameters)
              if ((p.name ?? '').toLowerCase().contains('causal') ||
                  (p.name ?? '').toLowerCase().contains('parent') ||
                  causalTypes.any(p.type.getDisplayString().contains))
                '${p.name}: ${p.type.getDisplayString()}',
          ];
          expect(offending, isEmpty, reason: 'EventStore.$name');
        }
      },
      timeout: const Timeout(Duration(minutes: 3)),
    );
  });
}
