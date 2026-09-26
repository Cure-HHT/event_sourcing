// Runs the security-finding scenarios of security_finding_conformance.dart
// on Sembast (in memory), and checks the parts of a finding no backend
// changes: its declaration, its identity and the exact evidence of every
// kind.
//
// The scenarios' assertions are cited on their own tests in
// security_finding_conformance.dart.
import 'package:event_sourcing/event_sourcing.dart';
import 'package:event_sourcing/src/security/security_context_store.dart';
import 'package:event_sourcing/src/security/security_finding.dart'
    show checkFindingEvidence, securityFindingId;
import 'package:event_sourcing/src/security/system_entry_types.dart'
    show kReservedEventShapes;
import 'package:flutter_test/flutter_test.dart';
import 'package:sembast/sembast_memory.dart';

import '../test_support/version_compatibility_conformance.dart'
    show VersionTestDatabase;
import 'security_finding_conformance.dart';

class _SembastFindingDatabase implements VersionTestDatabase {
  _SembastFindingDatabase(this._db);

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
  return _SembastFindingDatabase(
    await newDatabaseFactoryMemory().openDatabase('finding-$_dbCounter.db'),
  );
}

const Map<String, Object?> _record = <String, Object?>{
  'event_id': 'e-1',
  'data': <String, Object?>{},
};

const Map<String, Object?> _delivery = <String, Object?>{
  'delivery_number': 3,
  'delivery_hash': 'dh-3',
};

/// A valid evidence of every kind the library records, with the exact key
/// set its kind fixes.
const Map<String, Map<String, Object?>> _validEvidence =
    <String, Map<String, Object?>>{
      'fork_unrecorded': <String, Object?>{
        'database_id': 'db-1',
        'previous_event_hash': null,
      },
      'position_reused': <String, Object?>{
        'database_id': 'db-1',
        'origin_sequence_number': 4,
      },
      'hash_mismatch': <String, Object?>{
        'event_id': 'e-1',
        'carried_hash': 'h-carried',
        'recomputed_hash': 'h-recomputed',
      },
      'predecessor_break': <String, Object?>{
        'database_id': 'db-1',
        'event_hash': 'h-1',
        'previous_event_hash': 'h-0',
      },
      'identity_mismatch': <String, Object?>{
        'event_id': 'e-1',
        'held_hash': 'h-held',
        'record': _record,
      },
      'event_malformed': <String, Object?>{
        'reason': 'record_malformed',
        'record': _record,
      },
      'delivery_hash_mismatch': <String, Object?>{
        'channel': 'ch-1',
        'delivery_number': 3,
        'carried_hash': 'dh-carried',
        'recomputed_hash': 'dh-recomputed',
      },
      'own_event_ingested': <String, Object?>{
        'event_id': 'e-1',
        'sealed_hash': 'h-1',
        'record': null,
      },
      'foreign_event': <String, Object?>{
        'channel': 'ch-1',
        'delivery_number': 3,
        'event_id': 'e-1',
        'sealed_hash': 'h-1',
      },
      'channel_unexplained': <String, Object?>{
        'channel': 'ch-1',
        'sender_record': _delivery,
        'receiver_record': _delivery,
        'recorded_receiver_database_id': null,
        'responding_receiver_database_id': 'db-2',
      },
      'sender_regressed': <String, Object?>{
        'channel': 'ch-1',
        'sender_record': _delivery,
        'receiver_record': _delivery,
        'recorded_receiver_database_id': 'db-2',
        'responding_receiver_database_id': 'db-2',
      },
      'restore_unverified': <String, Object?>{
        'channel': 'ch-1',
        'delivery_number': 3,
        'event_id': null,
        'check': 'delivery_link',
      },
      'succession_ahead': <String, Object?>{
        'channel': 'ch-1',
        'receiver_record': _delivery,
        'succession_record': _delivery,
      },
      'storage_link_break': <String, Object?>{
        'local_sequence_number': 7,
        'event_id': 'e-7',
        'field': 'previous_ingest_hash',
        'expected': 'h-6',
        'actual': 'h-x',
      },
      'sequence_missing': <String, Object?>{'local_sequence_number': 7},
      'parent_invalid': <String, Object?>{
        'local_sequence_number': 7,
        'event_id': 'e-7',
        'parent': <String, Object?>{'event_id': 'p', 'event_hash': 'h-p'},
        'reason': 'annotation',
      },
      'parents_not_stamped': <String, Object?>{
        'local_sequence_number': 7,
        'event_id': 'e-7',
        'expected': <Object?>[],
        'actual': <Object?>[
          <String, Object?>{'event_id': 'p', 'event_hash': 'h-p'},
        ],
      },
    };

/// The closed value lists the evidence of some kinds names: kind, key and
/// every value the list holds.
const Map<(String, String), List<String>> _closedLists =
    <(String, String), List<String>>{
      ('event_malformed', 'reason'): <String>[
        'record_malformed',
        'reserved_type_undeclared',
        'audit_identity_invalid',
      ],
      ('restore_unverified', 'check'): <String>[
        'delivery_link',
        'delivery_hash',
        'originator',
        'receiver_entry',
      ],
      ('parent_invalid', 'reason'): <String>[
        'other_aggregate',
        'annotation',
        'ineligible',
        'held_under_other_hash',
      ],
      ('storage_link_break', 'field'): <String>[
        'ingest_sequence_number',
        'previous_ingest_hash',
      ],
    };

void main() {
  runSecurityFindingScenarios(
    openDatabase: _memoryDatabase,
    backendLabel: 'sembast (memory)',
  );

  group('the declaration of the security finding', () {
    // Verifies: EVS-DEV-security-findings/A
    // Verifies: EVS-DEV-causal-parents/E
    test('system.security_finding is a reserved entry type with one event '
        'type, declared an ineligible annotation', () {
      expect(kSecurityFindingEntryType, 'system.security_finding');
      expect(kReservedSystemEntryTypeIds, contains(kSecurityFindingEntryType));
      final shape = kReservedEventShapes[kSecurityFindingEntryType]!;
      expect(shape.aggregateType, 'security_finding');
      expect(shape.eventTypes, <String>{'security_finding_recorded'});
      final definition = kSystemEntryTypes.singleWhere(
        (d) => d.id == kSecurityFindingEntryType,
      );
      expect(definition.declarations, hasLength(1));
      final declaration = definition.declarationFor(
        'security_finding_recorded',
      );
      expect(declaration.kind, CausalKind.annotation);
      expect(declaration.eligible, isFalse);
    });

    // Verifies: EVS-DEV-security-findings/H
    test('the kinds the library records are exactly the listed ones', () {
      expect(
        <String>[for (final k in FindingKind.values) k.wire],
        <String>[
          'hash_mismatch',
          'identity_mismatch',
          'event_malformed',
          'delivery_hash_mismatch',
          'predecessor_break',
          'fork_unrecorded',
          'position_reused',
          'own_event_ingested',
          'foreign_event',
          'channel_unexplained',
          'sender_regressed',
          'restore_unverified',
          'succession_ahead',
          'storage_link_break',
          'sequence_missing',
          'parent_invalid',
          'parents_not_stamped',
        ],
      );
      expect(_validEvidence.keys.toSet(), <String>{
        for (final k in FindingKind.values) k.wire,
      });
      for (final kind in FindingKind.values) {
        expect(kind.isKnown, isTrue);
        expect(FindingKind.fromWire(kind.wire), same(kind));
      }
    });

    // Verifies: EVS-DEV-destination-drain/L
    // the finding kind is an open value: an unknown string is carried
    //   verbatim, equal to itself and to no known kind.
    test('an unknown kind is carried verbatim', () {
      final future = FindingKind.fromWire('future_kind');
      expect(future.wire, 'future_kind');
      expect(future.isKnown, isFalse);
      expect(future, FindingKind.fromWire('future_kind'));
      expect(FindingKind.values, isNot(contains(future)));
    });

    // Verifies: EVS-DEV-security-findings/Q
    test('a detector role is ingest, restore, sender or walk', () {
      expect(
        <String>[for (final r in FindingRole.values) r.wire],
        <String>['ingest', 'restore', 'sender', 'walk'],
      );
    });

    // Verifies: EVS-DEV-security-findings/C
    test('the identity is the digest of the detector database, the role, '
        'the kind and the evidence', () {
      final evidence = forkEvidence();
      final id = securityFindingId(
        databaseId: 'db-1',
        role: FindingRole.walk,
        kind: FindingKind.forkUnrecorded,
        evidence: evidence,
      );
      expect(
        id,
        expectedFindingId(
          databaseId: 'db-1',
          role: 'walk',
          kind: 'fork_unrecorded',
          evidence: evidence,
        ),
      );
      expect(RegExp(r'^[0-9a-f]{64}$').hasMatch(id), isTrue);
      expect(
        securityFindingId(
          databaseId: 'db-2',
          role: FindingRole.walk,
          kind: FindingKind.forkUnrecorded,
          evidence: evidence,
        ),
        isNot(id),
      );
    });
  });

  group('the evidence of each kind', () {
    for (final entry in _validEvidence.entries) {
      final kind = FindingKind.fromWire(entry.key);
      final valid = entry.value;

      // Verifies: EVS-DEV-security-findings/J
      // Verifies: EVS-DEV-security-findings/K
      // Verifies: EVS-DEV-security-findings/L
      // Verifies: EVS-DEV-security-findings/M
      // Verifies: EVS-DEV-security-findings/R
      // Verifies: EVS-DEV-security-findings/D
      test('${entry.key}: exactly its keys', () {
        checkFindingEvidence(kind, valid);
        for (final key in valid.keys) {
          expect(
            () => checkFindingEvidence(
              kind,
              <String, Object?>{...valid}..remove(key),
            ),
            throwsArgumentError,
            reason: 'without $key',
          );
        }
        expect(
          () => checkFindingEvidence(kind, <String, Object?>{
            ...valid,
            'detected_at': '2026-09-26T00:00:00Z',
          }),
          throwsArgumentError,
          reason: 'with an extra key',
        );
      });
    }

    // Verifies: EVS-DEV-security-findings/R
    test('a named reason, check or field outside its closed list is '
        'refused', () {
      for (final entry in _closedLists.entries) {
        final (kindWire, key) = entry.key;
        final kind = FindingKind.fromWire(kindWire);
        final valid = _validEvidence[kindWire]!;
        for (final value in entry.value) {
          checkFindingEvidence(kind, <String, Object?>{...valid, key: value});
        }
        expect(
          () => checkFindingEvidence(kind, <String, Object?>{
            ...valid,
            key: 'unknown_value',
          }),
          throwsArgumentError,
          reason: '$kindWire.$key',
        );
      }
    });

    // Verifies: EVS-DEV-security-findings/R
    test('a delivery record is an object with exactly delivery_number and '
        'delivery_hash', () {
      for (final kindWire in <String>[
        'channel_unexplained',
        'sender_regressed',
        'succession_ahead',
      ]) {
        final kind = FindingKind.fromWire(kindWire);
        final valid = _validEvidence[kindWire]!;
        for (final key in valid.keys.where((k) => k.endsWith('_record'))) {
          for (final bad in <Object?>[
            null,
            'dh-3',
            <String, Object?>{'delivery_number': 3},
            <String, Object?>{..._delivery, 'generation': 1},
          ]) {
            expect(
              () => checkFindingEvidence(kind, <String, Object?>{
                ...valid,
                key: bad,
              }),
              throwsArgumentError,
              reason: '$kindWire.$key = $bad',
            );
          }
        }
      }
    });

    // Verifies: EVS-DEV-security-findings/H
    test('a kind outside the listed ones is refused', () {
      expect(
        () => checkFindingEvidence(
          FindingKind.fromWire('future_kind'),
          const <String, Object?>{},
        ),
        throwsArgumentError,
      );
    });
  });

  group('the reserved namespace', () {
    // Verifies: EVS-DEV-destination-drain/L
    // the reserved namespace is every identifier beginning with system.
    //   and the six fixed identifiers.
    test('holds every system. identifier and the six fixed ones', () {
      for (final id in <String>[
        'system.anything_new',
        'system.security_finding',
        'system.',
        'security_context_redacted',
        'security_context_compacted',
        'security_context_purged',
        'lib_version_initialized',
        'lib_version_changed',
        'ingest-audit',
      ]) {
        expect(isReservedEntryType(id), isTrue, reason: id);
      }
      for (final id in <String>[
        'note',
        'systemx',
        'System.x',
        'ingest_audit',
      ]) {
        expect(isReservedEntryType(id), isFalse, reason: id);
      }
      for (final id in kReservedSystemEntryTypeIds) {
        expect(isReservedEntryType(id), isTrue, reason: id);
      }
    });
  });
}
